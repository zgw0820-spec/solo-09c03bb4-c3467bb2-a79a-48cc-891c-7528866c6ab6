"""Asyncio TFTP server (RFC 1350) with RFC 7440 windowsize support.

Serves one fixed rescue image in octet mode to diskless machines:

* 512-byte DATA blocks (blksize is always negotiated back to 512)
* RFC 7440 windowsize negotiation, capped at 16 blocks
* a retransmitted RRQ (e.g. the OACK got lost) is answered by the *original*
  transfer port of the existing session, never by a fresh session
* every client gets an independent session with its own cumulative-ACK state
* on timeout only the unacknowledged blocks of the window are re-sent
* when retries are exhausted the transfer port is released
* 16-bit block numbers wrap 65535 -> 0 and the transfer keeps going in order
* an image whose length is an exact multiple of 512 ends with an empty block
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import struct
import time

from common.imagegen import DEFAULT_SEED, DEFAULT_SIZE, generate_image
from common.protocol import (
    BLKSIZE,
    ERR_BAD_OPTION,
    ERR_FILE_NOT_FOUND,
    ERR_ILLEGAL_OP,
    ERR_NOT_DEFINED,
    MAX_WINDOW,
    OP_ACK,
    OP_ERROR,
    OP_RRQ,
    block_slice,
    build_error,
    build_oack,
    build_data,
    parse_rrq,
    total_blocks_for,
    wire_num,
)

log = logging.getLogger("tftp.server")


class TransferSession:
    """One windowed transfer to one client, living on its own UDP port."""

    def __init__(self, server: "TftpServerService", client, window: int, oack_options: dict):
        self.server = server
        self.client = client
        self.window = window
        self.image = server.image
        self.total_blocks = total_blocks_for(len(self.image))
        self.send_base = 1        # first unacknowledged absolute block index
        self.next_to_send = 1     # next absolute block index never sent yet
        self.last_sent = 0        # highest absolute block index sent
        self.awaiting_ack0 = bool(oack_options)
        self.oack_payload = build_oack(oack_options) if oack_options else None
        self.retries = 0
        self.transport = None
        self.port = None
        self.done = False
        self._timer = None
        self._max_acked = 0  # highest absolute block index known to be acked
        self.started = time.monotonic()
        self.stats = {
            "client": f"{client[0]}:{client[1]}",
            "port": None,
            "window": window,
            "total_blocks": self.total_blocks,
            "oack_sent": 0,
            "duplicate_rrq": 0,
            "data_sent": 0,
            "resent_packets": 0,
            "resend_events": 0,
            "resend_ranges": [],  # [first, last, max_acked] per retransmit event
            "acks": 0,
            "acks_ignored": 0,
            "result": None,
        }

    async def start(self):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _SessionProtocol(self),
            local_addr=(self.server.bind_host, 0),
        )
        if self.done:  # server stopped while the socket was being bound
            transport.close()
            return
        self.transport = transport
        self.port = transport.get_extra_info("sockname")[1]
        self.stats["port"] = self.port
        log.info(
            "client=%s session start: tid=%d window=%d blocks=%d",
            self.client, self.port, self.window, self.total_blocks,
        )
        if self.oack_payload is not None:
            self._send_oack("initial")
        else:
            self._send_window()
        self._rearm()

    # ---------------------------------------------------------------- send
    def _sendto(self, payload: bytes):
        if self.transport is not None:
            self.transport.sendto(payload, self.client)

    def _send_oack(self, reason: str):
        self.stats["oack_sent"] += 1
        log.info("client=%s tid=%s OACK (%s)", self.client, self.port, reason)
        self._sendto(self.oack_payload)

    def _send_data(self, index: int):
        self.stats["data_sent"] += 1
        self._sendto(build_data(index, block_slice(self.image, index)))

    def _send_window(self):
        limit = min(self.send_base + self.window - 1, self.total_blocks)
        while self.next_to_send <= limit:
            self._send_data(self.next_to_send)
            self.next_to_send += 1
        self.last_sent = max(self.last_sent, self.next_to_send - 1)

    def _resend_unacked(self, reason: str):
        """Retransmit only the still-unacknowledged blocks of the window."""
        first, last = self.send_base, self.last_sent
        if first > last:
            return
        self.stats["resend_events"] += 1
        self.stats["resent_packets"] += last - first + 1
        self.stats["resend_ranges"].append([first, last, self._max_acked])
        log.info(
            "client=%s tid=%s %s: retransmitting unacked blocks %d..%d (%d)",
            self.client, self.port, reason, first, last, last - first + 1,
        )
        for index in range(first, last + 1):
            self._send_data(index)

    # ------------------------------------------------------------- receive
    def on_datagram(self, data: bytes, addr):
        if self.done or addr != self.client or len(data) < 2:
            return
        op = struct.unpack("!H", data[:2])[0]
        if op == OP_ACK and len(data) >= 4:
            self._on_ack(struct.unpack("!H", data[2:4])[0])
        elif op == OP_ERROR:
            self._finish("aborted: client sent ERROR")
        elif op == OP_RRQ:
            self.on_duplicate_rrq()

    def on_duplicate_rrq(self):
        """The client re-sent its RRQ (our OACK or first DATA got lost).

        Answer from *this* transfer port instead of opening a new session.
        """
        self.stats["duplicate_rrq"] += 1
        self.retries = 0
        if self.awaiting_ack0:
            log.info(
                "client=%s duplicate RRQ -> re-sending OACK from original tid=%s",
                self.client, self.port,
            )
            self._send_oack("duplicate RRQ")
        else:
            log.info(
                "client=%s duplicate RRQ -> re-sending unacked window from original tid=%s",
                self.client, self.port,
            )
            self._resend_unacked("duplicate RRQ")
        self._rearm()

    def _on_ack(self, wire: int):
        if self.done:
            return
        if self.awaiting_ack0:
            if wire == 0:  # ACK 0 acknowledges the OACK
                self.awaiting_ack0 = False
                self.retries = 0
                self._send_window()
                self._rearm()
            return
        # unwrap the 16-bit number into the absolute index space; the window
        # is tiny next to 65536 so this is unambiguous
        candidate = self.send_base + (wire - wire_num(self.send_base)) % 65536
        if candidate > self.last_sent:
            self.stats["acks_ignored"] += 1  # stale or duplicate ACK
            return
        self.stats["acks"] += 1
        self.send_base = candidate + 1
        self._max_acked = max(self._max_acked, candidate)
        self.retries = 0
        if self.send_base > self.total_blocks:
            self._finish("completed")
            return
        self._send_window()
        self._rearm()

    # ---------------------------------------------------- timeout / finish
    def _rearm(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self.done:
            self._timer = asyncio.get_running_loop().call_later(
                self.server.timeout, self._on_timeout
            )

    def _on_timeout(self):
        if self.done:
            return
        self.retries += 1
        if self.retries > self.server.max_retries:
            log.warning(
                "client=%s tid=%s retries exhausted (%d) -> releasing transfer port",
                self.client, self.port, self.server.max_retries,
            )
            self._finish("aborted: retries exhausted")
            return
        if self.awaiting_ack0:
            self._send_oack(f"timeout {self.retries}")
        else:
            self._resend_unacked(f"timeout {self.retries}")
        self._rearm()

    def _finish(self, result: str):
        if self.done:
            return
        self.done = True
        self.stats["result"] = result
        self.stats["elapsed_s"] = round(time.monotonic() - self.started, 3)
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self.transport is not None:
            self.transport.close()  # release the transfer port
        log.info("client=%s tid=%s %s (port released)", self.client, self.port, result)
        self.server.session_finished(self)


class _SessionProtocol(asyncio.DatagramProtocol):
    def __init__(self, session: TransferSession):
        self.session = session

    def datagram_received(self, data, addr):
        self.session.on_datagram(data, addr)

    def error_received(self, exc):  # ICMP on unconnected sockets: ignore
        pass


class TftpServerService:
    """Port-69 front door plus the registry of per-client sessions."""

    def __init__(self, image: bytes, filename: str = "rescue.img",
                 bind_host: str = "0.0.0.0", port: int = 69,
                 timeout: float = 1.0, max_retries: int = 5):
        self.image = image
        self.filename = filename
        self.bind_host = bind_host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.sessions = {}      # client addr -> TransferSession
        self.history = []       # stats of finished sessions
        self.transport = None
        self._tasks = set()

    async def start(self):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: _MainProtocol(self), local_addr=(self.bind_host, self.port)
        )
        self.port = self.transport.get_extra_info("sockname")[1]
        log.info(
            "listening on udp %s:%d, serving %r (%d bytes)",
            self.bind_host, self.port, self.filename, len(self.image),
        )

    async def stop(self):
        for session in list(self.sessions.values()):
            session._finish("aborted: server shutdown")
        if self.transport is not None:
            self.transport.close()

    def datagram_received(self, data: bytes, addr):
        if len(data) < 2:
            return
        op = struct.unpack("!H", data[:2])[0]
        if op != OP_RRQ:
            return
        existing = self.sessions.get(addr)
        if existing is not None:
            existing.on_duplicate_rrq()
            return
        try:
            filename, mode, options = parse_rrq(data)
        except ValueError as exc:
            self._send_error(addr, ERR_ILLEGAL_OP, f"malformed RRQ: {exc}")
            return
        if filename != self.filename:
            self._send_error(addr, ERR_FILE_NOT_FOUND, "file not found")
            return
        if mode.lower() != "octet":
            self._send_error(addr, ERR_NOT_DEFINED, "only octet mode is supported")
            return
        accepted = {}
        window = 1
        if "blksize" in options:
            accepted["blksize"] = str(BLKSIZE)  # fixed at 512
        if "tsize" in options:
            accepted["tsize"] = str(len(self.image))
        if "windowsize" in options:
            try:
                requested = int(options["windowsize"])
                if requested < 1:
                    raise ValueError
            except ValueError:
                self._send_error(addr, ERR_BAD_OPTION, "invalid windowsize")
                return
            window = min(requested, MAX_WINDOW)  # cap at 16 blocks
            accepted["windowsize"] = str(window)
        session = TransferSession(self, addr, window, accepted)
        self.sessions[addr] = session
        task = asyncio.get_running_loop().create_task(self._run_session(session))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_session(self, session: TransferSession):
        try:
            await session.start()
        except Exception:
            log.exception("failed to start session for %s", session.client)
            session._finish("aborted: setup failure")

    def session_finished(self, session: TransferSession):
        if self.sessions.get(session.client) is session:
            del self.sessions[session.client]
        self.history.append(session.stats)

    def _send_error(self, addr, code: int, message: str):
        log.info("client=%s error %d %s", addr, code, message)
        if self.transport is not None:
            self.transport.sendto(build_error(code, message), addr)


class _MainProtocol(asyncio.DatagramProtocol):
    def __init__(self, service: TftpServerService):
        self.service = service

    def datagram_received(self, data, addr):
        self.service.datagram_received(data, addr)

    def error_received(self, exc):
        pass


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


async def _amain():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    size = _env_int("IMAGE_SIZE", DEFAULT_SIZE)
    seed = os.environ.get("IMAGE_SEED", DEFAULT_SEED)
    filename = os.environ.get("IMAGE_FILENAME", "rescue.img")
    image = generate_image(size, seed)
    log.info("image %r ready: %d bytes, sha256=%s",
             filename, size, hashlib.sha256(image).hexdigest())
    service = TftpServerService(
        image,
        filename=filename,
        bind_host=os.environ.get("TFTP_BIND_HOST", "0.0.0.0"),
        port=_env_int("TFTP_PORT", 69),
        timeout=_env_float("TFTP_TIMEOUT", 1.0),
        max_retries=_env_int("TFTP_MAX_RETRIES", 5),
    )
    await service.start()
    await asyncio.Event().wait()  # serve forever


def main():
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
