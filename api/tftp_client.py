"""Async TFTP client used by the API to perform *real* UDP downloads.

Loss simulation (for drills):
* ``loss``            drop inbound DATA packets with this probability
* ``ack_loss``        drop outbound ACKs with this probability
* ``drop_first_oack`` ignore the first OACK so the RRQ has to be re-sent
"""
from __future__ import annotations

import asyncio
import random
import socket
import struct
import time
from dataclasses import dataclass

from common.protocol import (
    BLKSIZE,
    OP_DATA,
    OP_ERROR,
    OP_OACK,
    build_ack,
    build_rrq,
    parse_error,
    parse_oack,
    wire_num,
)


@dataclass
class ClientConfig:
    host: str
    port: int
    filename: str = "rescue.img"
    window: int = 16
    timeout: float = 1.0
    max_retries: int = 8
    loss: float = 0.0
    ack_loss: float = 0.0
    drop_first_oack: bool = False
    seed: int = 0
    linger: float = 0.2


@dataclass
class ClientResult:
    ok: bool
    error: str | None
    data: bytes
    stats: dict


class _ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.queue = asyncio.Queue()

    def datagram_received(self, data, addr):
        self.queue.put_nowait((data, addr))

    def error_received(self, exc):
        pass


class TftpClient:
    def __init__(self, config: ClientConfig):
        self.cfg = config

    async def download(self) -> ClientResult:
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(
            self.cfg.host, self.cfg.port, type=socket.SOCK_DGRAM
        )
        server = infos[0][4]
        transport, proto = await loop.create_datagram_endpoint(
            _ClientProtocol, local_addr=("0.0.0.0", 0)
        )
        try:
            return await self._run(transport, proto.queue, server)
        finally:
            transport.close()

    # ------------------------------------------------------------------ core
    async def _run(self, transport, queue, server) -> ClientResult:
        cfg = self.cfg
        rng = random.Random(cfg.seed)
        started = time.monotonic()
        stats = {
            "packets": 0,
            "data_packets": 0,
            "blocks": 0,
            "dropped_data": 0,
            "acks": 0,
            "ack_drops": 0,
            "timeouts": 0,
            "rrq_sent": 1,
            "out_of_order": 0,
            "server_ports": set(),
            "first_oack_port": None,
            "negotiated": {},
            "max_block_index": 0,
            "last_block_size": None,
        }
        rrq = build_rrq(cfg.filename, cfg.window)
        transport.sendto(rrq, server)
        # when ACK loss is simulated, linger long enough to re-ACK at least
        # one retransmission of the final block (server timeout is unknown,
        # so use our own timeout as the yardstick)
        linger = cfg.linger if cfg.ack_loss == 0 else max(cfg.linger, 2 * cfg.timeout + 0.2)
        state = "oack"          # "oack" -> "data"
        expected = 1            # next absolute (1-based) block index wanted
        retries = 0
        server_addr = None      # transfer port, learned from the first reply
        oack_dropped = False
        buf = bytearray()

        while True:
            try:
                data, addr = await asyncio.wait_for(queue.get(), cfg.timeout)
            except asyncio.TimeoutError:
                retries += 1
                stats["timeouts"] += 1
                if retries > cfg.max_retries:
                    return self._finish(False, f"no response after {cfg.max_retries} retries",
                                        b"", stats, started)
                if state == "oack":
                    transport.sendto(rrq, server)  # re-send the read request
                    stats["rrq_sent"] += 1
                else:
                    self._ack(transport, server_addr, wire_num(expected - 1), rng, stats)
                continue

            stats["packets"] += 1
            stats["server_ports"].add(addr[1])
            if len(data) < 2:
                continue
            op = struct.unpack("!H", data[:2])[0]
            if op == OP_ERROR:
                code, message = parse_error(data)
                return self._finish(False, f"server ERROR {code}: {message}",
                                    b"", stats, started)

            if state == "oack":
                if op == OP_OACK:
                    if cfg.drop_first_oack and not oack_dropped:
                        oack_dropped = True
                        stats["first_oack_port"] = addr[1]
                        continue  # pretend the OACK never arrived
                    server_addr = addr
                    stats["negotiated"] = parse_oack(data)
                    state = "data"
                    retries = 0
                    self._ack(transport, server_addr, 0, rng, stats)  # ACK 0
                    continue
                if op != OP_DATA:
                    continue
                # server sent DATA straight away (no option negotiation)
                server_addr = addr
                state = "data"
                retries = 0

            # ---- state == "data" ----
            if addr != server_addr:
                continue
            if op == OP_OACK:
                # the server never saw our ACK 0 -> send it again
                self._ack(transport, server_addr, 0, rng, stats)
                continue
            if op != OP_DATA or len(data) < 4:
                continue
            if rng.random() < cfg.loss:
                stats["dropped_data"] += 1
                continue  # simulated packet loss
            blk = struct.unpack("!H", data[2:4])[0]
            payload = data[4:]
            stats["data_packets"] += 1
            if blk == wire_num(expected):
                buf += payload
                stats["blocks"] += 1
                stats["max_block_index"] = expected
                stats["last_block_size"] = len(payload)
                self._ack(transport, server_addr, blk, rng, stats)
                expected += 1
                retries = 0
                if len(payload) < BLKSIZE:  # short/empty block ends it
                    await self._linger(transport, queue, server_addr, blk, rng, stats, linger)
                    return self._finish(True, None, bytes(buf), stats, started)
            else:
                stats["out_of_order"] += 1
                # re-affirm the highest contiguous block we have
                self._ack(transport, server_addr, wire_num(expected - 1), rng, stats)

    # ---------------------------------------------------------------- helpers
    def _ack(self, transport, addr, wire, rng, stats):
        stats["acks"] += 1
        if rng.random() < self.cfg.ack_loss:
            stats["ack_drops"] += 1
            return  # simulated ACK loss
        transport.sendto(build_ack(wire), addr)

    async def _linger(self, transport, queue, server_addr, final_wire, rng, stats, linger):
        """Briefly re-ACK duplicate final blocks so the server can finish."""
        deadline = time.monotonic() + linger
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                data, addr = await asyncio.wait_for(queue.get(), remaining)
            except asyncio.TimeoutError:
                return
            if addr != server_addr or len(data) < 4:
                continue
            op, blk = struct.unpack("!HH", data[:4])
            if op == OP_DATA and blk == final_wire:
                self._ack(transport, server_addr, blk, rng, stats)

    def _finish(self, ok, error, data, stats, started) -> ClientResult:
        stats["elapsed_s"] = round(time.monotonic() - started, 3)
        stats["server_ports"] = sorted(stats["server_ports"])
        return ClientResult(ok, error, data, stats)
