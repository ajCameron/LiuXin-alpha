# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``msdes`` extension.

Implements classic DES in ECB mode, matching the C extension API:
    - deskey(key8: bytes, edf: int) -> None
      edf must be 0 (EN0) for encryption, or 1 (DE1) for decryption.
    - des(data: bytes) -> bytes
      data length must be a multiple of 8. Operates block-by-block.

This is intended for compatibility (slow but functional), not high performance.
"""

from __future__ import annotations

from typing import List

EN0 = 0  # encrypt
DE1 = 1  # decrypt


class MsDesError(Exception):
    pass


# --- DES tables ---
IP = [
    58, 50, 42, 34, 26, 18, 10, 2,
    60, 52, 44, 36, 28, 20, 12, 4,
    62, 54, 46, 38, 30, 22, 14, 6,
    64, 56, 48, 40, 32, 24, 16, 8,
    57, 49, 41, 33, 25, 17, 9, 1,
    59, 51, 43, 35, 27, 19, 11, 3,
    61, 53, 45, 37, 29, 21, 13, 5,
    63, 55, 47, 39, 31, 23, 15, 7,
]
FP = [
    40, 8, 48, 16, 56, 24, 64, 32,
    39, 7, 47, 15, 55, 23, 63, 31,
    38, 6, 46, 14, 54, 22, 62, 30,
    37, 5, 45, 13, 53, 21, 61, 29,
    36, 4, 44, 12, 52, 20, 60, 28,
    35, 3, 43, 11, 51, 19, 59, 27,
    34, 2, 42, 10, 50, 18, 58, 26,
    33, 1, 41, 9, 49, 17, 57, 25,
]
E = [
    32, 1, 2, 3, 4, 5,
    4, 5, 6, 7, 8, 9,
    8, 9, 10, 11, 12, 13,
    12, 13, 14, 15, 16, 17,
    16, 17, 18, 19, 20, 21,
    20, 21, 22, 23, 24, 25,
    24, 25, 26, 27, 28, 29,
    28, 29, 30, 31, 32, 1,
]
P = [
    16, 7, 20, 21,
    29, 12, 28, 17,
    1, 15, 23, 26,
    5, 18, 31, 10,
    2, 8, 24, 14,
    32, 27, 3, 9,
    19, 13, 30, 6,
    22, 11, 4, 25,
]
PC1 = [
    57, 49, 41, 33, 25, 17, 9,
    1, 58, 50, 42, 34, 26, 18,
    10, 2, 59, 51, 43, 35, 27,
    19, 11, 3, 60, 52, 44, 36,
    63, 55, 47, 39, 31, 23, 15,
    7, 62, 54, 46, 38, 30, 22,
    14, 6, 61, 53, 45, 37, 29,
    21, 13, 5, 28, 20, 12, 4,
]
PC2 = [
    14, 17, 11, 24, 1, 5,
    3, 28, 15, 6, 21, 10,
    23, 19, 12, 4, 26, 8,
    16, 7, 27, 20, 13, 2,
    41, 52, 31, 37, 47, 55,
    30, 40, 51, 45, 33, 48,
    44, 49, 39, 56, 34, 53,
    46, 42, 50, 36, 29, 32,
]
SHIFTS = [1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1]

SBOXES = [
    # S1
    [
        [14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7],
        [0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8],
        [4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0],
        [15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13],
    ],
    # S2
    [
        [15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10],
        [3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5],
        [0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15],
        [13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9],
    ],
    # S3
    [
        [10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8],
        [13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1],
        [13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7],
        [1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12],
    ],
    # S4
    [
        [7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15],
        [13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9],
        [10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4],
        [3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14],
    ],
    # S5
    [
        [2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9],
        [14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6],
        [4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14],
        [11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3],
    ],
    # S6
    [
        [12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11],
        [10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8],
        [9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6],
        [4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13],
    ],
    # S7
    [
        [4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1],
        [13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6],
        [1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2],
        [6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12],
    ],
    # S8
    [
        [13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7],
        [1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2],
        [7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8],
        [2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11],
    ],
]


_subkeys: List[int] = []  # 16 subkeys, each 48-bit


def _permute(x: int, table: List[int], in_bits: int) -> int:
    """Generic bit permutation. table entries are 1-based bit indices from MSB."""
    out = 0
    for t in table:
        out = (out << 1) | ((x >> (in_bits - t)) & 1)
    return out


def _left_rotate28(v: int, s: int) -> int:
    v &= (1 << 28) - 1
    return ((v << s) | (v >> (28 - s))) & ((1 << 28) - 1)


def deskey(key: bytes, edf: int) -> None:
    global _subkeys
    if not isinstance(key, (bytes, bytearray, memoryview)):
        raise TypeError("Key must be bytes-like")
    keyb = bytes(key)
    if len(keyb) != 8:
        raise MsDesError("Key length incorrect")
    if edf not in (EN0, DE1):
        raise MsDesError("En/decryption direction invalid")

    # Pack to 64-bit int
    k = int.from_bytes(keyb, "big", signed=False)
    # Apply PC1 -> 56 bits
    k56 = _permute(k, PC1, 64)
    c = (k56 >> 28) & ((1 << 28) - 1)
    d = k56 & ((1 << 28) - 1)

    subs: List[int] = []
    for shift in SHIFTS:
        c = _left_rotate28(c, shift)
        d = _left_rotate28(d, shift)
        cd = (c << 28) | d
        sub = _permute(cd, PC2, 56)  # 48-bit
        subs.append(sub)

    if edf == DE1:
        subs.reverse()

    _subkeys = subs


def _f(r32: int, k48: int) -> int:
    # Expand 32->48
    e = _permute(r32, E, 32)
    x = e ^ k48
    # S-box substitution: split into 8 groups of 6 bits
    out32 = 0
    for i in range(8):
        block = (x >> (42 - 6 * i)) & 0x3F
        row = ((block & 0x20) >> 4) | (block & 0x01)
        col = (block >> 1) & 0x0F
        s = SBOXES[i][row][col]
        out32 = (out32 << 4) | s
    # Permute with P
    return _permute(out32, P, 32)


def _des_block(block8: bytes) -> bytes:
    if len(_subkeys) != 16:
        raise MsDesError("No key schedule set; call deskey() first")

    x = int.from_bytes(block8, "big", signed=False)
    ip = _permute(x, IP, 64)
    l = (ip >> 32) & 0xFFFFFFFF
    r = ip & 0xFFFFFFFF

    for k in _subkeys:
        l, r = r, (l ^ _f(r, k)) & 0xFFFFFFFF

    preout = (r << 32) | l  # note swap
    fp = _permute(preout, FP, 64)
    return fp.to_bytes(8, "big", signed=False)


def des(data: bytes) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("Input must be bytes-like")
    b = bytes(data)
    if len(b) == 0 or (len(b) % 8) != 0:
        raise MsDesError("Input length not a multiple of the block size")

    out = bytearray()
    for i in range(0, len(b), 8):
        out.extend(_des_block(b[i:i + 8]))
    return bytes(out)
