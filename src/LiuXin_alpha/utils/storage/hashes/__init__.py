from __future__ import annotations

import hashlib
from typing import Union

BytesLike = Union[bytes, bytearray, memoryview]


def sane_hash(
    data: str | BytesLike,
    algo: str = "sha256",
    encoding: str = "utf-8",
    errors: str = "strict",
    hexdigest: bool = True,
) -> str | bytes:
    """
    Hash `data` (bytes-like or str) using a standard algorithm (default: SHA-256).

    Returns:
        - hex string if hexdigest=True (default)
        - raw digest bytes if hexdigest=False

    Examples:
        sane_hash(b"abc")                       -> '...'
        sane_hash("abc")                        -> '...'
        sane_hash("abc", algo="blake2b")        -> '...'
        sane_hash(b"abc", hexdigest=False)      -> b'...'
    """
    if isinstance(data, str):
        b = data.encode(encoding, errors)
    elif isinstance(data, (bytes, bytearray, memoryview)):
        b = bytes(data)
    else:
        raise TypeError(f"Unsupported type: {type(data)!r} (expected str or bytes-like)")

    try:
        h = hashlib.new(algo)
    except ValueError as e:
        raise ValueError(
            f"Unknown hash algorithm {algo!r}. "
            f"Try one of: {sorted(hashlib.algorithms_available)}"
        ) from e

    h.update(b)
    return h.hexdigest() if hexdigest else h.digest()