# -*- coding: utf-8 -*-
# A port of the DES calibre's msdes extension uses (src/calibre/utils/msdes/des.c,
# Richard Outerbridge's public-domain D3DES), with the S-box tables of openclit's
# spr.h (Copyright (C) 2002 Dan A. Jackson, GPL), as calibre builds it.
# SPDX-License-Identifier: GPL-3.0-only
"""
DES as Microsoft LIT books use it: calibre's ``msdes`` extension, in Python.

LIT books are sealed with DES, but not the standard tables: calibre's extension
builds Richard Outerbridge's D3DES with the S-box tables of openclit's ``spr.h``,
and its answers differ from the standard's (the NIST test vector does not hold).
This follows that code step for step, so a LIT book's title key decrypts as it does
in calibre.

Example:
    >>> deskey(bytes.fromhex("133457799bbcdff1"), EN0)
    >>> des(bytes.fromhex("0123456789abcdef")).hex()
    'b95a0c9072207039'
    >>> deskey(bytes.fromhex("133457799bbcdff1"), DE1)
    >>> des(bytes.fromhex("b95a0c9072207039")).hex()
    '0123456789abcdef'
"""

from __future__ import annotations

EN0 = 0  # encrypt
DE1 = 1  # decrypt


class MsDesError(Exception):
    """
    A key or block calibre's ``msdes`` would refuse.

    Example:
        >>> issubclass(MsDesError, Exception)
        True
    """


_BYTEBIT = (0o200, 0o100, 0o40, 0o20, 0o10, 0o4, 0o2, 0o1)
_BIGBYTE = tuple(1 << (23 - i) for i in range(24))
_PC1 = (
    56, 48, 40, 32, 24, 16, 8, 0, 57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18, 10, 2,
    59, 51, 43, 35, 62, 54, 46, 38, 30, 22, 14, 6, 61, 53, 45, 37, 29, 21, 13, 5, 60, 52, 44, 36,
    28, 20, 12, 4, 27, 19, 11, 3,
)  # fmt: skip
_TOTROT = (1, 2, 4, 6, 8, 10, 12, 14, 15, 17, 19, 21, 23, 25, 27, 28)
_PC2 = (
    13, 16, 10, 23, 0, 4, 2, 27, 14, 5, 20, 9, 22, 18, 11, 3, 25, 7, 15, 6, 26, 19, 12, 1,
    40, 51, 30, 36, 46, 54, 29, 39, 50, 44, 32, 47, 43, 48, 38, 55, 33, 52, 45, 41, 49, 35, 28, 31,
)  # fmt: skip

# The S-box tables of openclit's spr.h, which calibre's des.c includes.
# fmt: off
SP1 = (
    0x02080800, 0x00080000, 0x02000002, 0x02080802,
    0x02000000, 0x00080802, 0x00080002, 0x02000002,
    0x00080802, 0x02080800, 0x02080000, 0x00000802,
    0x02000802, 0x02000000, 0x00000000, 0x00080002,
    0x00080000, 0x00000002, 0x02000800, 0x00080800,
    0x02080802, 0x02080000, 0x00000802, 0x02000800,
    0x00000002, 0x00000800, 0x00080800, 0x02080002,
    0x00000800, 0x02000802, 0x02080002, 0x00000000,
    0x00000000, 0x02080802, 0x02000800, 0x00080002,
    0x02080800, 0x00080000, 0x00000802, 0x02000800,
    0x02080002, 0x00000800, 0x00080800, 0x02000002,
    0x00080802, 0x00000002, 0x02000002, 0x02080000,
    0x02080802, 0x00080800, 0x02080000, 0x02000802,
    0x02000000, 0x00000802, 0x00080002, 0x00000000,
    0x00080000, 0x02000000, 0x02000802, 0x02080800,
    0x00000002, 0x02080002, 0x00000800, 0x00080802,
)

SP2 = (
    0x40108010, 0x00000000, 0x00108000, 0x40100000,
    0x40000010, 0x00008010, 0x40008000, 0x00108000,
    0x00008000, 0x40100010, 0x00000010, 0x40008000,
    0x00100010, 0x40108000, 0x40100000, 0x00000010,
    0x00100000, 0x40008010, 0x40100010, 0x00008000,
    0x00108010, 0x40000000, 0x00000000, 0x00100010,
    0x40008010, 0x00108010, 0x40108000, 0x40000010,
    0x40000000, 0x00100000, 0x00008010, 0x40108010,
    0x00100010, 0x40108000, 0x40008000, 0x00108010,
    0x40108010, 0x00100010, 0x40000010, 0x00000000,
    0x40000000, 0x00008010, 0x00100000, 0x40100010,
    0x00008000, 0x40000000, 0x00108010, 0x40008010,
    0x40108000, 0x00008000, 0x00000000, 0x40000010,
    0x00000010, 0x40108010, 0x00108000, 0x40100000,
    0x40100010, 0x00100000, 0x00008010, 0x40008000,
    0x40008010, 0x00000010, 0x40100000, 0x00108000,
)

SP3 = (
    0x04000001, 0x04040100, 0x00000100, 0x04000101,
    0x00040001, 0x04000000, 0x04000101, 0x00040100,
    0x04000100, 0x00040000, 0x04040000, 0x00000001,
    0x04040101, 0x00000101, 0x00000001, 0x04040001,
    0x00000000, 0x00040001, 0x04040100, 0x00000100,
    0x00000101, 0x04040101, 0x00040000, 0x04000001,
    0x04040001, 0x04000100, 0x00040101, 0x04040000,
    0x00040100, 0x00000000, 0x04000000, 0x00040101,
    0x04040100, 0x00000100, 0x00000001, 0x00040000,
    0x00000101, 0x00040001, 0x04040000, 0x04000101,
    0x00000000, 0x04040100, 0x00040100, 0x04040001,
    0x00040001, 0x04000000, 0x04040101, 0x00000001,
    0x00040101, 0x04000001, 0x04000000, 0x04040101,
    0x00040000, 0x04000100, 0x04000101, 0x00040100,
    0x04000100, 0x00000000, 0x04040001, 0x00000101,
    0x04000001, 0x00040101, 0x00000100, 0x04040000,
)

SP4 = (
    0x00401008, 0x10001000, 0x00000008, 0x10401008,
    0x00000000, 0x10400000, 0x10001008, 0x00400008,
    0x10401000, 0x10000008, 0x10000000, 0x00001008,
    0x10000008, 0x00401008, 0x00400000, 0x10000000,
    0x10400008, 0x00401000, 0x00001000, 0x00000008,
    0x00401000, 0x10001008, 0x10400000, 0x00001000,
    0x00001008, 0x00000000, 0x00400008, 0x10401000,
    0x10001000, 0x10400008, 0x10401008, 0x00400000,
    0x10400008, 0x00001008, 0x00400000, 0x10000008,
    0x00401000, 0x10001000, 0x00000008, 0x10400000,
    0x10001008, 0x00000000, 0x00001000, 0x00400008,
    0x00000000, 0x10400008, 0x10401000, 0x00001000,
    0x10000000, 0x10401008, 0x00401008, 0x00400000,
    0x10401008, 0x00000008, 0x10001000, 0x00401008,
    0x00400008, 0x00401000, 0x10400000, 0x10001008,
    0x00001008, 0x10000000, 0x10000008, 0x10401000,
)

SP5 = (
    0x08000000, 0x00010000, 0x00000400, 0x08010420,
    0x08010020, 0x08000400, 0x00010420, 0x08010000,
    0x00010000, 0x00000020, 0x08000020, 0x00010400,
    0x08000420, 0x08010020, 0x08010400, 0x00000000,
    0x00010400, 0x08000000, 0x00010020, 0x00000420,
    0x08000400, 0x00010420, 0x00000000, 0x08000020,
    0x00000020, 0x08000420, 0x08010420, 0x00010020,
    0x08010000, 0x00000400, 0x00000420, 0x08010400,
    0x08010400, 0x08000420, 0x00010020, 0x08010000,
    0x00010000, 0x00000020, 0x08000020, 0x08000400,
    0x08000000, 0x00010400, 0x08010420, 0x00000000,
    0x00010420, 0x08000000, 0x00000400, 0x00010020,
    0x08000420, 0x00000400, 0x00000000, 0x08010420,
    0x08010020, 0x08010400, 0x00000420, 0x00010000,
    0x00010400, 0x08010020, 0x08000400, 0x00000420,
    0x00000020, 0x00010420, 0x08010000, 0x08000020,
)

SP6 = (
    0x80000040, 0x00200040, 0x00000000, 0x80202000,
    0x00200040, 0x00002000, 0x80002040, 0x00200000,
    0x00002040, 0x80202040, 0x00202000, 0x80000000,
    0x80002000, 0x80000040, 0x80200000, 0x00202040,
    0x00200000, 0x80002040, 0x80200040, 0x00000000,
    0x00002000, 0x00000040, 0x80202000, 0x80200040,
    0x80202040, 0x80200000, 0x80000000, 0x00002040,
    0x00000040, 0x00202000, 0x00202040, 0x80002000,
    0x00002040, 0x80000000, 0x80002000, 0x00202040,
    0x80202000, 0x00200040, 0x00000000, 0x80002000,
    0x80000000, 0x00002000, 0x80200040, 0x00200000,
    0x00200040, 0x80202040, 0x00202000, 0x00000040,
    0x80202040, 0x00202000, 0x00200000, 0x80002040,
    0x80000040, 0x80200000, 0x00202040, 0x00000000,
    0x00002000, 0x80000040, 0x80002040, 0x80202000,
    0x80200000, 0x00002040, 0x00000040, 0x80200040,
)

SP7 = (
    0x00004000, 0x00000200, 0x01000200, 0x01000004,
    0x01004204, 0x00004004, 0x00004200, 0x00000000,
    0x01000000, 0x01000204, 0x00000204, 0x01004000,
    0x00000004, 0x01004200, 0x01004000, 0x00000204,
    0x01000204, 0x00004000, 0x00004004, 0x01004204,
    0x00000000, 0x01000200, 0x01000004, 0x00004200,
    0x01004004, 0x00004204, 0x01004200, 0x00000004,
    0x00004204, 0x01004004, 0x00000200, 0x01000000,
    0x00004204, 0x01004000, 0x01004004, 0x00000204,
    0x00004000, 0x00000200, 0x01000000, 0x01004004,
    0x01000204, 0x00004204, 0x00004200, 0x00000000,
    0x00000200, 0x01000004, 0x00000004, 0x01000200,
    0x00000000, 0x01000204, 0x01000200, 0x00004200,
    0x00000204, 0x00004000, 0x01004204, 0x01000000,
    0x01004200, 0x00000004, 0x00004004, 0x01004204,
    0x01000004, 0x01004200, 0x01004000, 0x00004004,
)

SP8 = (
    0x20800080, 0x20820000, 0x00020080, 0x00000000,
    0x20020000, 0x00800080, 0x20800000, 0x20820080,
    0x00000080, 0x20000000, 0x00820000, 0x00020080,
    0x00820080, 0x20020080, 0x20000080, 0x20800000,
    0x00020000, 0x00820080, 0x00800080, 0x20020000,
    0x20820080, 0x20000080, 0x00000000, 0x00820000,
    0x20000000, 0x00800000, 0x20020080, 0x20800080,
    0x00800000, 0x00020000, 0x20820000, 0x00000080,
    0x00800000, 0x00020000, 0x20000080, 0x20820080,
    0x00020080, 0x20000000, 0x00000000, 0x00820000,
    0x20800080, 0x20020080, 0x20020000, 0x00800080,
    0x20820000, 0x00000080, 0x00800080, 0x20020000,
    0x20820080, 0x00800000, 0x20800000, 0x20000080,
    0x00820000, 0x00020080, 0x20020080, 0x20800000,
    0x00000080, 0x20820000, 0x00820080, 0x00000000,
    0x20000000, 0x20800080, 0x00020000, 0x00820080,
)
# fmt: on

_M32 = 0xFFFFFFFF
_KnL: list[int] | None = None


def _cookey(raw: list[int]) -> list[int]:
    """
    Rearrange the raw round keys as D3DES's ``cookey`` does.

    Example:
        >>> len(_cookey([0] * 32))
        32


    :param raw: Thirty-two raw half-round keys.
    :return: The cooked keys ``desfunc`` uses.
    """
    dough = []
    for i in range(0, 32, 2):
        raw0, raw1 = raw[i], raw[i + 1]
        dough.append(
            ((raw0 & 0x00FC0000) << 6)
            | ((raw0 & 0x00000FC0) << 10)
            | ((raw1 & 0x00FC0000) >> 10)
            | ((raw1 & 0x00000FC0) >> 6)
        )
        dough.append(
            ((raw0 & 0x0003F000) << 12)
            | ((raw0 & 0x0000003F) << 16)
            | ((raw1 & 0x0003F000) >> 4)
            | (raw1 & 0x0000003F)
        )
    return dough


def deskey(key: bytes, edf: int) -> None:
    """
    Set the key, to encrypt (``EN0``) or decrypt (``DE1``) with.

    Example:
        See the module.


    :param key: Eight bytes.
    :param edf: ``EN0`` or ``DE1``.
    :raises TypeError: If the key is not bytes.
    :raises MsDesError: For a key that is not eight bytes, or another direction.
    """
    global _KnL
    if not isinstance(key, (bytes, bytearray, memoryview)):
        raise TypeError("Key must be bytes-like")
    keyb = bytes(key)
    if len(keyb) != 8:
        raise MsDesError("Key length incorrect")
    if edf not in (EN0, DE1):
        raise MsDesError("En/decryption direction invalid")
    pc1m = [1 if keyb[bit >> 3] & _BYTEBIT[bit & 0o7] else 0 for bit in _PC1]
    kn = [0] * 32
    for i in range(16):
        m = (15 - i) << 1 if edf == DE1 else i << 1
        n = m + 1
        shift = _TOTROT[i]
        pcr = [
            pc1m[j + shift] if j + shift < 28 else pc1m[j + shift - 28]
            for j in range(28)
        ]
        pcr += [
            pc1m[j + shift] if j + shift < 56 else pc1m[j + shift - 28]
            for j in range(28, 56)
        ]
        for j in range(24):
            if pcr[_PC2[j]]:
                kn[m] |= _BIGBYTE[j]
            if pcr[_PC2[j + 24]]:
                kn[n] |= _BIGBYTE[j]
    _KnL = _cookey(kn)


def _desfunc(left: int, right: int, keys: list[int]) -> tuple[int, int]:  # noqa: PLR0915 - D3DES's steps
    """
    Run one 64-bit block through D3DES's ``desfunc``, on 32-bit halves.

    Example:
        >>> from LiuXin_alpha.utils.plugins.fallbacks import msdes
        >>> msdes.deskey(bytes(8), EN0)
        >>> _desfunc(0, 0, msdes._KnL or [])[0] <= 0xFFFFFFFF
        True


    :param left: The block's first four bytes, big-endian.
    :param right: Its last four.
    :param keys: The cooked round keys.
    :return: The two halves of the result.
    """
    work = ((left >> 4) ^ right) & 0x0F0F0F0F
    right ^= work
    left ^= work << 4
    work = ((left >> 16) ^ right) & 0x0000FFFF
    right ^= work
    left ^= work << 16
    work = ((right >> 2) ^ left) & 0x33333333
    left ^= work
    right ^= work << 2
    work = ((right >> 8) ^ left) & 0x00FF00FF
    left ^= work
    right ^= work << 8
    left &= _M32
    right &= _M32
    right = ((right << 1) | ((right >> 31) & 1)) & _M32
    work = (left ^ right) & 0xAAAAAAAA
    left ^= work
    right ^= work
    left = ((left << 1) | ((left >> 31) & 1)) & _M32
    k = 0
    for _ in range(8):
        for _half in range(2):
            work = (((right << 28) | (right >> 4)) & _M32) ^ keys[k]
            fval = (
                SP7[work & 0x3F]
                | SP5[(work >> 8) & 0x3F]
                | SP3[(work >> 16) & 0x3F]
                | SP1[(work >> 24) & 0x3F]
            )
            work = right ^ keys[k + 1]
            fval |= (
                SP8[work & 0x3F]
                | SP6[(work >> 8) & 0x3F]
                | SP4[(work >> 16) & 0x3F]
                | SP2[(work >> 24) & 0x3F]
            )
            left ^= fval
            k += 2
            left, right = right, left
    right = ((right << 31) | (right >> 1)) & _M32
    work = (left ^ right) & 0xAAAAAAAA
    left ^= work
    right ^= work
    left = ((left << 31) | (left >> 1)) & _M32
    work = ((left >> 8) ^ right) & 0x00FF00FF
    right ^= work
    left ^= work << 8
    work = ((left >> 2) ^ right) & 0x33333333
    right ^= work
    left ^= work << 2
    work = ((right >> 16) ^ left) & 0x0000FFFF
    left ^= work
    right ^= work << 16
    work = ((right >> 4) ^ left) & 0x0F0F0F0F
    left ^= work
    right ^= work << 4
    return right & _M32, left & _M32


def des(data: bytes) -> bytes:
    """
    Encrypt or decrypt blocks of eight bytes with the key set, each on its own (ECB).

    Example:
        See the module.


    :param data: A whole number of eight-byte blocks.
    :return: The result, as long as the input.
    :raises TypeError: If the data is not bytes.
    :raises MsDesError: For no key, or data not a whole number of blocks.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("Input must be bytes-like")
    b = bytes(data)
    if len(b) == 0 or (len(b) % 8) != 0:
        raise MsDesError("Input length not a multiple of the block size")
    if _KnL is None:
        raise MsDesError("No key schedule set; call deskey() first")
    out = bytearray()
    for i in range(0, len(b), 8):
        first, second = _desfunc(
            int.from_bytes(b[i : i + 4], "big"),
            int.from_bytes(b[i + 4 : i + 8], "big"),
            _KnL,
        )
        out += first.to_bytes(4, "big") + second.to_bytes(4, "big")
    return bytes(out)
