"""Shared TFTP wire-format helpers (RFC 1350 + RFC 7440 windowsize)."""
from __future__ import annotations

import struct

BLKSIZE = 512          # fixed block size, one block per DATA packet
MAX_WINDOW = 16        # server-side cap for the RFC 7440 windowsize option

OP_RRQ = 1
OP_DATA = 3
OP_ACK = 4
OP_ERROR = 5
OP_OACK = 6

ERR_NOT_DEFINED = 0
ERR_FILE_NOT_FOUND = 1
ERR_ILLEGAL_OP = 4
ERR_BAD_OPTION = 8


def wire_num(index: int) -> int:
    """16-bit wire block number for a 1-based absolute block index.

    1 -> 1, ..., 65535 -> 65535, 65536 -> 0, 65537 -> 1 (the wrap).
    """
    return index % 65536


def total_blocks_for(size: int) -> int:
    """Number of DATA blocks for ``size`` bytes.

    Always ``size // 512 + 1``: a short tail block when there is a remainder,
    or an extra *empty* terminator block when the size is an exact multiple
    of the block size (that is how TFTP signals end-of-transfer then).
    """
    return size // BLKSIZE + 1


def block_slice(image: bytes, index: int) -> bytes:
    """Payload of the 1-based block ``index`` (empty for the terminator)."""
    start = (index - 1) * BLKSIZE
    return image[start:start + BLKSIZE]


def build_rrq(filename: str, window: int) -> bytes:
    return (
        struct.pack("!H", OP_RRQ)
        + filename.encode() + b"\x00octet\x00"
        + b"blksize\x00" + str(BLKSIZE).encode() + b"\x00"
        + b"windowsize\x00" + str(window).encode() + b"\x00"
    )


def build_ack(wire: int) -> bytes:
    return struct.pack("!HH", OP_ACK, wire)


def build_data(index: int, payload: bytes) -> bytes:
    return struct.pack("!HH", OP_DATA, wire_num(index)) + payload


def build_oack(options: dict) -> bytes:
    payload = struct.pack("!H", OP_OACK)
    for name, value in options.items():
        payload += name.encode() + b"\x00" + str(value).encode() + b"\x00"
    return payload


def parse_oack(data: bytes) -> dict:
    parts = data[2:].split(b"\x00")
    if parts and parts[-1] == b"":
        parts = parts[:-1]
    options = {}
    for i in range(0, len(parts) - 1, 2):
        name = parts[i].decode("ascii", "replace").lower()
        options[name] = parts[i + 1].decode("ascii", "replace")
    return options


def build_error(code: int, message: str) -> bytes:
    return struct.pack("!HH", OP_ERROR, code) + message.encode() + b"\x00"


def parse_error(data: bytes):
    code = struct.unpack("!H", data[2:4])[0] if len(data) >= 4 else -1
    return code, data[4:].rstrip(b"\x00").decode("ascii", "replace")


def parse_rrq(data: bytes):
    """Return (filename, mode, options) or raise ValueError."""
    parts = data[2:].split(b"\x00")
    if len(parts) < 3 or parts[-1] != b"":
        raise ValueError("malformed RRQ")
    parts = parts[:-1]
    filename = parts[0].decode("ascii", "replace")
    mode = parts[1].decode("ascii", "replace")
    rest = parts[2:]
    if len(rest) % 2:
        raise ValueError("odd number of option fields")
    options = {}
    for i in range(0, len(rest), 2):
        name = rest[i].decode("ascii", "replace").lower()
        options[name] = rest[i + 1].decode("ascii", "replace")
    return filename, mode, options
