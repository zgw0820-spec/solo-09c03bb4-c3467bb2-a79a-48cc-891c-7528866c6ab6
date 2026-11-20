"""End-to-end checks for the asyncio TFTP server + client (no docker needed).

Run from the repo root:  python3 tests/e2e.py
"""
import asyncio
import hashlib
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.imagegen import generate_image
from common.protocol import BLKSIZE, total_blocks_for, wire_num
from server.tftp_server import TftpServerService
from api.tftp_client import ClientConfig, TftpClient

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def start_server(size, seed="test-seed", timeout=1.0, max_retries=5):
    image = generate_image(size, seed)
    service = TftpServerService(image, bind_host="127.0.0.1", port=0,
                                timeout=timeout, max_retries=max_retries)
    await service.start()
    return service, image


def cfg_for(service, **kw):
    kw.setdefault("host", "127.0.0.1")
    kw.setdefault("port", service.port)
    return ClientConfig(**kw)


def unit_checks():
    print("== unit: wire-number wrap and block counting ==")
    check("wire(1) == 1", wire_num(1) == 1)
    check("wire(65535) == 65535", wire_num(65535) == 65535)
    check("wire(65536) == 0  (65535 -> 0 wrap)", wire_num(65536) == 0)
    check("wire(65537) == 1", wire_num(65537) == 1)
    check("exact multiple gets an extra empty block", total_blocks_for(4 * BLKSIZE) == 5)
    check("short tail needs no empty block", total_blocks_for(1000) == 2)


async def scenario_basic():
    print("== scenario: basic windowed download, exact-multiple image ==")
    service, image = await start_server(256 * BLKSIZE, timeout=0.5)
    try:
        res = await TftpClient(cfg_for(service, window=16, timeout=0.5, seed=1)).download()
        check("download ok", res.ok, res.error or "")
        check("sha256 matches", sha(res.data) == sha(image))
        check("257 blocks = 256 data + empty terminator",
              res.stats["blocks"] == 257, str(res.stats["blocks"]))
        check("last block is the empty terminator", res.stats["last_block_size"] == 0)
        check("negotiated windowsize=16",
              res.stats["negotiated"].get("windowsize") == "16", str(res.stats["negotiated"]))
        await asyncio.sleep(0.05)
        check("server session completed and was unregistered",
              not service.sessions and service.history[0]["result"] == "completed",
              str(service.history[0]["result"]))
        check("no retransmissions without loss", service.history[0]["resent_packets"] == 0)

        res2 = await TftpClient(cfg_for(service, window=64, timeout=0.5, seed=2)).download()
        check("windowsize capped at 16 by the server",
              res2.ok and res2.stats["negotiated"].get("windowsize") == "16",
              str(res2.stats["negotiated"]))

        res3 = await TftpClient(cfg_for(service, filename="nope.img",
                                        timeout=0.3, max_retries=2)).download()
        check("unknown filename rejected with ERROR 1",
              not res3.ok and "ERROR 1" in (res3.error or ""), res3.error or "")
    finally:
        await service.stop()


async def scenario_short_tail():
    print("== scenario: non-multiple image ends with a short block ==")
    service, image = await start_server(1000, timeout=0.5)
    try:
        res = await TftpClient(cfg_for(service, window=4, timeout=0.5, seed=1)).download()
        check("download ok", res.ok, res.error or "")
        check("sha256 matches", sha(res.data) == sha(image))
        check("2 blocks, last one short (488)",
              res.stats["blocks"] == 2 and res.stats["last_block_size"] == 488,
              f"blocks={res.stats['blocks']} last={res.stats['last_block_size']}")
    finally:
        await service.stop()


async def scenario_loss():
    print("== scenario: 20% DATA loss + 15% ACK loss -> retransmit unacked only ==")
    service, image = await start_server(128 * BLKSIZE, timeout=0.4, max_retries=10)
    try:
        res = await TftpClient(cfg_for(service, window=16, timeout=0.4, max_retries=15,
                                       loss=0.2, ack_loss=0.15, seed=42)).download()
        check("download ok despite loss", res.ok, res.error or "")
        check("sha256 matches", sha(res.data) == sha(image))
        check("client dropped inbound DATA (simulated)", res.stats["dropped_data"] > 0,
              str(res.stats["dropped_data"]))
        check("client dropped outbound ACKs (simulated)", res.stats["ack_drops"] > 0,
              str(res.stats["ack_drops"]))
        h = service.history[0]
        check("server session completed", h["result"] == "completed", h["result"])
        check("server retransmitted after timeouts", h["resent_packets"] > 0,
              f"resent={h['resent_packets']} events={h['resend_events']}")
        ranges = h["resend_ranges"]
        check("every retransmit starts exactly at the first unacked block",
              bool(ranges) and all(first == max_acked + 1 for first, _, max_acked in ranges),
              f"events={len(ranges)}")
        check("retransmits never exceed the negotiated window",
              all(last - first + 1 <= 16 for first, last, _ in ranges))
    finally:
        await service.stop()


async def scenario_duplicate_rrq():
    print("== scenario: OACK lost -> client re-sends RRQ -> original transfer port answers ==")
    # server timeout (5s) >> client timeout (0.4s) so the duplicate-RRQ path,
    # not the server timeout, is what re-sends the OACK
    service, image = await start_server(16 * BLKSIZE, timeout=5.0, max_retries=3)
    try:
        res = await TftpClient(cfg_for(service, window=8, timeout=0.4, max_retries=5,
                                       drop_first_oack=True, seed=3)).download()
        check("download ok", res.ok, res.error or "")
        check("sha256 matches", sha(res.data) == sha(image))
        check("client re-sent the RRQ", res.stats["rrq_sent"] >= 2,
              f"rrq_sent={res.stats['rrq_sent']}")
        check("first (dropped) OACK port was recorded",
              res.stats["first_oack_port"] is not None)
        ports = res.stats["server_ports"]
        check("every server packet came from ONE transfer port", len(ports) == 1, str(ports))
        check("the re-sent RRQ was answered from the ORIGINAL transfer port",
              ports == [res.stats["first_oack_port"]],
              f"first_oack={res.stats['first_oack_port']} ports={ports}")
        h = service.history[0]
        check("server registered the duplicate RRQ", h["duplicate_rrq"] >= 1,
              f"duplicate_rrq={h['duplicate_rrq']}")
        check("server sent the OACK twice from the same session", h["oack_sent"] >= 2,
              f"oack_sent={h['oack_sent']}")
    finally:
        await service.stop()


async def scenario_retries_exhausted():
    print("== scenario: all ACKs lost -> retries exhausted -> transfer port released ==")
    service, image = await start_server(8 * BLKSIZE, timeout=0.3, max_retries=3)
    try:
        res = await TftpClient(cfg_for(service, window=4, timeout=0.3, max_retries=3,
                                       ack_loss=1.0, seed=9)).download()
        check("client fails when no ACK gets through", not res.ok, res.error or "")
        await asyncio.sleep(2.5)  # let the server run out of retries
        check("server released the transfer port (no sessions left)",
              not service.sessions, f"open sessions={list(service.sessions)}")
        check("server marked the transfer aborted",
              any(h["result"].startswith("aborted") for h in service.history),
              str([h["result"] for h in service.history]))
        res2 = await TftpClient(cfg_for(service, window=4, timeout=0.5, seed=10)).download()
        check("server accepts new transfers afterwards",
              res2.ok and sha(res2.data) == sha(image), res2.error or "")
    finally:
        await service.stop()


async def scenario_concurrent():
    print("== scenario: 3 machines download concurrently, independent windows ==")
    service, image = await start_server(48 * BLKSIZE, timeout=0.5, max_retries=8)
    try:
        clients = [
            TftpClient(cfg_for(service, window=4, loss=0.05, ack_loss=0.0,
                               timeout=0.4, max_retries=10, seed=101)),
            TftpClient(cfg_for(service, window=8, loss=0.10, ack_loss=0.05,
                               timeout=0.4, max_retries=10, seed=102)),
            TftpClient(cfg_for(service, window=16, loss=0.20, ack_loss=0.10,
                               timeout=0.4, max_retries=10, seed=103)),
        ]
        results = await asyncio.gather(*(c.download() for c in clients))
        want = sha(image)
        for i, res in enumerate(results, 1):
            check(f"client {i} ok and sha256 matches",
                  res.ok and sha(res.data) == want, res.error or "")
        await asyncio.sleep(0.05)
        done = [h for h in service.history if h["result"] == "completed"]
        clients_seen = {h["client"] for h in done}
        check("3 independent server sessions, one per machine",
              len(done) == 3 and len(clients_seen) == 3, str(sorted(clients_seen)))
    finally:
        await service.stop()


async def scenario_wraparound():
    print("== scenario: block-number wrap 65535 -> 0 (33.5 MB image) ==")
    size = 65540 * BLKSIZE  # 65540 data blocks + 1 empty terminator = 65541
    service, image = await start_server(size, timeout=1.0, max_retries=5)
    try:
        res = await TftpClient(cfg_for(service, window=16, timeout=1.0,
                                       max_retries=8, seed=5)).download()
        check("download ok", res.ok, res.error or "")
        check("sha256 matches (33.5 MB)", sha(res.data) == sha(image))
        check("received all 65541 blocks", res.stats["blocks"] == 65541,
              str(res.stats["blocks"]))
        check("block index went past 65536 (wire number wrapped to 0)",
              res.stats["max_block_index"] == 65541, str(res.stats["max_block_index"]))
        check("ended with the empty terminator", res.stats["last_block_size"] == 0)
        check("server completed the session",
              service.history[0]["result"] == "completed", service.history[0]["result"])
    finally:
        await service.stop()


async def amain():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    unit_checks()
    await scenario_basic()
    await scenario_short_tail()
    await scenario_loss()
    await scenario_duplicate_rrq()
    await scenario_retries_exhausted()
    await scenario_concurrent()
    await scenario_wraparound()
    failed = [name for name, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    if failed:
        print("FAILED:", *failed, sep="\n  - ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
