# -*- coding: utf-8 -*-
"""
Provide cPalmdoc utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise cPalmdoc through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from typing import ByteString


def decompress(data: ByteString) -> bytes:
    """
    Perform the decompress utility operation under explicit compatibility rules.

    Example:
        Exercise decompress through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    b = bytes(data)
    out = bytearray()
    i = 0
    n = len(b)
    while i < n:
        c = b[i]
        i += 1
        if 1 <= c <= 8:
            # copy next c bytes literally
            if i + c > n:
                # Corrupt stream; mimic C extension best-effort by stopping
                break
            out.extend(b[i:i + c])
            i += c
        elif c <= 0x7F:
            # literal
            out.append(c)
        elif c >= 0xC0:
            # space + ASCII char
            out.append(0x20)
            out.append(c ^ 0x80)
        else:
            # 0x80-0xBF: back-reference
            if i >= n:
                break
            c2 = (c << 8) + b[i]
            i += 1
            di = (c2 & 0x3FFF) >> 3
            ln = (c2 & 0x7) + 3
            if di <= 0 or di > len(out):
                # Corrupt; stop
                break
            # copy from output[o-di] forward
            for _ in range(ln):
                out.append(out[-di])
    return bytes(out)


def _memcmp(a: bytes, ai: int, b: bytes, bi: int, ln: int) -> bool:
    """
    Perform the memcmp utility operation under explicit compatibility rules.

    Example:
        Exercise  memcmp through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param a: Value supplied for a under the utility contract.
    :param ai: Value supplied for ai under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :param bi: Value supplied for bi under the utility contract.
    :param ln: Value supplied for ln under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return a[ai:ai + ln] == b[bi:bi + ln]


def _rfind(data: bytes, pos: int, chunk_len: int) -> int:
    # Search backwards for a previous occurrence of data[pos:pos+chunk_len]
    """
    Perform the rfind utility operation under explicit compatibility rules.

    Example:
        Exercise  rfind through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :param pos: Value supplied for pos under the utility contract.
    :param chunk_len: Value supplied for chunk len under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    needle = data[pos:pos + chunk_len]
    for i in range(pos - chunk_len, -1, -1):
        if data[i:i + chunk_len] == needle:
            return i
    return pos


def compress(data: ByteString) -> bytes:
    """
    Perform the compress utility operation under explicit compatibility rules.

    Example:
        Exercise compress through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    b = bytes(data)
    out = bytearray()
    i = 0
    n = len(b)
    while i < n:
        c = b[i]
        # do repeats (backrefs) only when there is enough history and enough lookahead
        if i > 10 and (n - i) > 10:
            found = False
            for chunk_len in range(10, 2, -1):
                j = _rfind(b, i, chunk_len)
                dist = i - j
                if j < i and dist <= 2047:
                    found = True
                    compound = (dist << 3) + (chunk_len - 3)
                    out.append(0x80 + ((compound >> 8) & 0xFF))
                    out.append(compound & 0xFF)
                    i += chunk_len
                    break
            if found:
                continue

        # write single character
        if c == 0 or (9 <= c <= 0x7F):
            out.append(c)
            i += 1
            continue

        if c == 0x20 and i + 1 < n:
            nxt = b[i + 1]
            if 0x40 <= nxt <= 0x7F:
                out.append(nxt ^ 0x80)
                i += 2
                continue

        # otherwise, write a "copy N bytes" literal run, up to 8 bytes
        temp = bytearray()
        j = i
        temp.append(c)
        j += 1
        while j < n and len(temp) < 8:
            c2 = b[j]
            if c2 == 0 or (9 <= c2 <= 0x7F):
                break
            temp.append(c2)
            j += 1
        out.append(len(temp))
        out.extend(temp)
        i += len(temp)

    return bytes(out)
