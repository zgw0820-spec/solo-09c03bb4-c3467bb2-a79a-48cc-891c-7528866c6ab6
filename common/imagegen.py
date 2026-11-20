"""Deterministic generation of the fixed rescue image.

Both containers derive the very same bytes from (seed, size), so the API can
verify the SHA-256 of a download without a side channel to the TFTP server.
"""
from __future__ import annotations

import hashlib

DEFAULT_SEED = "rescue-image-v1"
DEFAULT_SIZE = 256 * 512  # exact multiple of 512 -> ends with an empty block


def _blocks(seed: str):
    counter = 0
    while True:
        yield hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1


def generate_image(size: int, seed: str = DEFAULT_SEED) -> bytes:
    out = bytearray()
    for chunk in _blocks(seed):
        if len(out) >= size:
            break
        out.extend(chunk)
    return bytes(out[:size])


def image_sha256(size: int, seed: str = DEFAULT_SEED) -> str:
    digest = hashlib.sha256()
    remaining = size
    for chunk in _blocks(seed):
        if remaining <= 0:
            break
        take = min(remaining, len(chunk))
        digest.update(chunk[:take])
        remaining -= take
    return digest.hexdigest()
