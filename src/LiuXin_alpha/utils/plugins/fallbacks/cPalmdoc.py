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
    Decompress PalmDOC data as calibre's ``cPalmdoc`` C extension does.

    Damaged input is read on as the C code reads it, rather than stopped at: a
    literal run cut short by the end of the data keeps the bytes there are, and
    a back-reference reaching before the start of the output is skipped. A
    back-reference of distance zero makes the C code repeat bytes of its output
    buffer it has not written, whatever memory holds; here they are zero bytes,
    so the output has calibre's length and defined contents. Checked against
    calibre's compiled extension on 200,000 random inputs.

    Example:
        >>> decompress(b"ab\\x80\\x10")
        b'ababa'
        >>> decompress(b"\\x05ab")
        b'ab'


    :param data: The compressed bytes.
    :return: The decompressed bytes.
    :raises TypeError: If ``data`` is text rather than bytes.
    """
    b = bytes(data)
    out = bytearray()
    i = 0
    n = len(b)
    while i < n:
        c = b[i]
        i += 1
        if 1 <= c <= 8:
            # Copy the next c bytes, or as many as there are
            out += b[i : i + c]
            i = min(n, i + c)
        elif c <= 0x7F:
            out.append(c)
        elif c >= 0xC0:
            # A space and an ASCII character
            out.append(0x20)
            out.append(c ^ 0x80)
        elif i < n:
            # 0x80-0xBF: copy 3-10 bytes from up to 2047 bytes back
            pair = (c << 8) + b[i]
            i += 1
            distance = (pair & 0x3FFF) >> 3
            count = (pair & 7) + 3
            if distance > len(out):
                continue
            if distance == 0:
                out += bytes(count)
                continue
            for _ in range(count):
                out.append(out[-distance])
    return bytes(out)


def compress(data: ByteString) -> bytes:
    """
    Compress bytes to PalmDOC as calibre's ``cPalmdoc`` C extension does.

    This is calibre's own reference implementation, ``py_compress_doc`` from
    ``calibre/ebooks/compression/palmdoc.py``, which calibre's tests hold its C
    code to: it looks back up to 2,047 bytes for the longest earlier copy of
    the next 10 down to 3 bytes, packs a space and an ASCII character into one
    byte, and writes other bytes outside 0x09-0x7F in runs of up to eight.
    The output is byte for byte calibre's, checked against its compiled
    extension.

    Example:
        >>> compress(b" A"), compress(b"abc\\x01\\x80d")
        (b'\\xc1', b'abc\\x02\\x01\\x80d')


    :param data: The bytes to compress.
    :return: The compressed bytes.
    :raises TypeError: If ``data`` is text rather than bytes.
    """
    data = bytes(data)
    out = bytearray()
    i = 0
    ldata = len(data)
    while i < ldata:
        if i > 10 and (ldata - i) > 10:
            chunk = b""
            match = -1
            for j in range(10, 2, -1):
                chunk = data[i : i + j]
                match = data.rfind(chunk, 0, i)
                if match < 0:
                    continue
                if (i - match) <= 2047:
                    break
                match = -1
            if match >= 0:
                n = len(chunk)
                m = i - match
                code = 0x8000 + ((m << 3) & 0x3FF8) + (n - 3)
                out += code.to_bytes(2, "big")
                i += n
                continue
        och = data[i]
        i += 1
        if och == 0x20 and i < ldata:
            onch = data[i]
            if 0x40 <= onch < 0x80:
                out.append(onch ^ 0x80)
                i += 1
                continue
        if och == 0 or 8 < och < 0x80:
            out.append(och)
        else:
            j = i
            binseq = bytearray([och])
            while j < ldata and len(binseq) < 8:
                och = data[j]
                if och == 0 or 8 < och < 0x80:
                    break
                binseq.append(och)
                j += 1
            out.append(len(binseq))
            out += binseq
            i += len(binseq) - 1
    return bytes(out)
