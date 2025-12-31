# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``lzx`` extension.

The compiled extension provides LZX decompression (used for CHM / Microsoft formats).
Implementing LZX in pure Python is non-trivial; this fallback focuses on importability.

API:
    - init(window: int) -> None
    - reset() -> None
    - decompress(data: bytes, outlen: int) -> bytes

The functions will raise NotImplementedError unless you install a compatible
external decompressor and replace `decompress` accordingly.
"""

from __future__ import annotations


class LZXError(Exception):
    pass


_LZX_WINDOW = 0


def init(window: int) -> None:
    global _LZX_WINDOW
    _LZX_WINDOW = int(window)


def reset() -> None:
    # In the compiled version this resets the decompressor state.
    return None


def decompress(data: bytes, outlen: int) -> bytes:
    raise NotImplementedError("LZX decompression is not implemented in the pure-python fallback")
