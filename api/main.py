"""FastAPI front-end: trigger real TFTP downloads of the rescue image.

Open /docs (Swagger UI) and POST /download to run an actual UDP TFTP
transfer against the TFTP server container, optionally with simulated
packet loss, and verify the SHA-256 of what came back.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query

from api.tftp_client import ClientConfig, TftpClient
from common.imagegen import DEFAULT_SEED, DEFAULT_SIZE, image_sha256
from common.protocol import BLKSIZE, MAX_WINDOW, total_blocks_for

TFTP_SERVER_HOST = os.environ.get("TFTP_SERVER_HOST", "127.0.0.1")
TFTP_SERVER_PORT = int(os.environ.get("TFTP_SERVER_PORT", "6969"))
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", str(DEFAULT_SIZE)))
IMAGE_SEED = os.environ.get("IMAGE_SEED", DEFAULT_SEED)
IMAGE_FILENAME = os.environ.get("IMAGE_FILENAME", "rescue.img")

_hash_cache: str | None = None


def expected_sha256() -> str:
    global _hash_cache
    if _hash_cache is None:
        _hash_cache = image_sha256(IMAGE_SIZE, IMAGE_SEED)
    return _hash_cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    # hash the image once, off the event loop, before serving requests
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, expected_sha256)
    yield


app = FastAPI(
    title="Rescue image TFTP download",
    description=(
        "Triggers a real UDP TFTP download of the fixed rescue image from "
        f"`{TFTP_SERVER_HOST}:{TFTP_SERVER_PORT}` (octet, 512-byte blocks, "
        "RFC 7440 windowsize capped at 16), with optional packet-loss "
        "simulation, and checks the SHA-256 of the received image."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root():
    return {"service": "rescue-image tftp download", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/image/info")
def image_info():
    return {
        "filename": IMAGE_FILENAME,
        "size_bytes": IMAGE_SIZE,
        "sha256": expected_sha256(),
        "blksize": BLKSIZE,
        "total_blocks": total_blocks_for(IMAGE_SIZE),
        "ends_with_empty_block": IMAGE_SIZE % BLKSIZE == 0,
        "tftp_server": f"{TFTP_SERVER_HOST}:{TFTP_SERVER_PORT}",
    }


@app.post("/download")
async def download(
    window: int = Query(MAX_WINDOW, ge=1, le=64,
                        description="windowsize to request; the server caps it at 16"),
    loss: float = Query(0.0, ge=0.0, le=0.95,
                        description="probability of dropping each inbound DATA packet"),
    ack_loss: float = Query(0.0, ge=0.0, le=1.0,
                            description="probability of dropping each outbound ACK"),
    drop_first_oack: bool = Query(False,
                                  description="drop the first OACK to force an RRQ retransmission"),
    seed: int = Query(1, description="RNG seed for the loss simulation"),
    timeout: float = Query(1.0, ge=0.1, le=10.0, description="per-packet wait, seconds"),
    max_retries: int = Query(8, ge=1, le=30, description="give up after this many retries"),
):
    cfg = ClientConfig(
        host=TFTP_SERVER_HOST,
        port=TFTP_SERVER_PORT,
        filename=IMAGE_FILENAME,
        window=window,
        timeout=timeout,
        max_retries=max_retries,
        loss=loss,
        ack_loss=ack_loss,
        drop_first_oack=drop_first_oack,
        seed=seed,
    )
    result = await TftpClient(cfg).download()
    received = hashlib.sha256(result.data).hexdigest() if result.ok else None
    expected = expected_sha256()
    return {
        "ok": result.ok,
        "error": result.error,
        "match": result.ok and received == expected,
        "sha256": received,
        "expected_sha256": expected,
        "bytes_received": len(result.data),
        **result.stats,
    }
