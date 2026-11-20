"""
Decode the stream filters a PDF's metadata, object streams and cross-reference streams use.

Flate (with PNG and TIFF predictors), ASCIIHex and ASCII85 are enough for those
streams; image filters are refused. Every filter stops at a size limit, and Flate data
is inflated chunk by chunk, keeping what decodes before any corruption.

Example:
    >>> import zlib
    >>> apply_filter("FlateDecode", {}, zlib.compress(b"metadata"), 100)
    b'metadata'
"""

from __future__ import annotations

import base64
import zlib
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.file_formats.pdf.errors import PdfReadError
from LiuXin_alpha.file_formats.pdf.syntax import WHITESPACE

__all__ = ["FILTER_NAMES", "apply_filter", "inflate", "predict"]

#: The abbreviated filter names of inline images, which some writers use in streams too.
FILTER_NAMES = {"Fl": "FlateDecode", "AHx": "ASCIIHexDecode", "A85": "ASCII85Decode"}


def _hex_digits(data: bytes) -> bytes:
    """
    Keep the hexadecimal digits of ASCIIHex data before its ``>``, padded to an even count.

    Example:
        >>> _hex_digits(b"41 4>junk")
        b'4140'


    :param data: The data.
    :return: The digits.
    """
    end = data.find(b">")
    digits = bytes(byte for byte in (data if end < 0 else data[:end]) if chr(byte) in "0123456789abcdefABCDEF")
    return digits + b"0" if len(digits) % 2 else digits


def apply_filter(name: str, parms: Mapping[str, Any], data: bytes, limit: int) -> bytes:
    """
    Apply one decoding filter, with its predictor, stopping at a size limit.

    Example:
        >>> apply_filter("ASCIIHexDecode", {}, b"41 42>", 100)
        b'AB'
        >>> apply_filter("ASCII85Decode", {}, b"87cURD]i,\\"Ebo80~>", 100)
        b'Hello World!'


    :param name: The filter's name.
    :param parms: Its ``/DecodeParms``.
    :param data: The data to decode.
    :param limit: The most bytes the output may have.
    :return: The decoded data.
    :raises PdfReadError: The filter is unknown, its data corrupt, or the output too large.
    """
    if name == "FlateDecode":
        out = predict(inflate(data, limit), parms)
    elif name == "ASCIIHexDecode":
        out = bytes.fromhex(_hex_digits(data).decode("ascii"))
    elif name == "ASCII85Decode":
        body = bytes(byte for byte in data if byte not in WHITESPACE)
        end = body.find(b"~>")
        body = body[:end] if end >= 0 else body
        if body.startswith(b"<~"):
            body = body[2:]
        try:
            out = base64.a85decode(body)
        except ValueError as err:
            raise PdfReadError(f"corrupt ASCII85 data: {err}") from err
    else:
        raise PdfReadError(f"cannot decode the {name} filter")
    if len(out) > limit:
        raise PdfReadError("a stream decodes to more than the reading limit")
    return out


def inflate(data: bytes, limit: int) -> bytes:
    """
    Inflate Flate data, keeping what decodes before any corruption, and stopping past a limit.

    Example:
        >>> inflate(zlib.compress(b"x" * 10), 100)
        b'xxxxxxxxxx'
        >>> inflate(zlib.compress(b"x" * 1000), 100)
        Traceback (most recent call last):
        ...
        LiuXin_alpha.file_formats.pdf.errors.PdfReadError: a stream decodes to more than the reading limit


    :param data: The compressed data.
    :param limit: The most bytes the output may have.
    :return: The inflated data.
    :raises PdfReadError: Nothing decodes, or the output would pass the limit.
    """
    for wbits in (15, -15):
        inflater = zlib.decompressobj(wbits)
        out = bytearray()
        for start in range(0, len(data), 1 << 16):
            try:
                out += inflater.decompress(data[start : start + (1 << 16)], limit + 1 - len(out))
            except zlib.error:
                break
            if len(out) > limit:
                raise PdfReadError("a stream decodes to more than the reading limit")
            if inflater.eof:
                break
        if out:
            return bytes(out)
    if not data:
        return b""
    raise PdfReadError("corrupt Flate data")


def predict(data: bytes, parms: Mapping[str, Any]) -> bytes:
    """
    Undo a PNG or TIFF predictor, as cross-reference streams use.

    Example:
        >>> predict(bytes([2, 1, 2, 2, 1, 1]), {"Predictor": 12, "Columns": 2})
        b'\\x01\\x02\\x02\\x03'


    :param data: The inflated rows.
    :param parms: The ``/DecodeParms`` with ``/Predictor``, ``/Columns``, ``/Colors`` and ``/BitsPerComponent``.
    :return: The rows without the predictor.
    :raises PdfReadError: The parameters are out of range or a row has an unknown filter type.
    """
    predictor = parms.get("Predictor", 1)
    if not isinstance(predictor, int) or predictor == 1:
        return data
    columns, colors, bits = parms.get("Columns", 1), parms.get("Colors", 1), parms.get("BitsPerComponent", 8)
    if (
        not all(isinstance(item, int) for item in (columns, colors, bits))
        or not 1 <= columns <= 1 << 24
        or not 1 <= colors <= 32
        or bits not in (1, 2, 4, 8, 16)
    ):
        raise PdfReadError("a predictor has parameters out of range")
    pixel = max(1, colors * bits // 8)
    row_size = (columns * colors * bits + 7) // 8
    if predictor == 2:
        if bits != 8:
            raise PdfReadError("TIFF predictors are supported for 8-bit components only")
        out = bytearray(data)
        for row_start in range(0, len(out) - row_size + 1, row_size):
            for at in range(row_start + pixel, row_start + row_size):
                out[at] = (out[at] + out[at - pixel]) & 0xFF
        return bytes(out)
    if not 10 <= predictor <= 15:
        raise PdfReadError(f"unknown predictor {predictor}")
    out = bytearray()
    previous = bytes(row_size)
    stride = row_size + 1
    for start in range(0, len(data) - stride + 1, stride):
        kind = data[start]
        row = bytearray(data[start + 1 : start + stride])
        if kind == 1:
            for at in range(pixel, row_size):
                row[at] = (row[at] + row[at - pixel]) & 0xFF
        elif kind == 2:
            row = bytearray((a + b) & 0xFF for a, b in zip(row, previous, strict=True))
        elif kind == 3:
            for at in range(row_size):
                left = row[at - pixel] if at >= pixel else 0
                row[at] = (row[at] + ((left + previous[at]) >> 1)) & 0xFF
        elif kind == 4:
            for at in range(row_size):
                left = row[at - pixel] if at >= pixel else 0
                up = previous[at]
                corner = previous[at - pixel] if at >= pixel else 0
                estimate = left + up - corner
                distance_left, distance_up, distance_corner = abs(estimate - left), abs(estimate - up), abs(estimate - corner)
                if distance_left <= distance_up and distance_left <= distance_corner:
                    best = left
                elif distance_up <= distance_corner:
                    best = up
                else:
                    best = corner
                row[at] = (row[at] + best) & 0xFF
        elif kind != 0:
            raise PdfReadError(f"unknown PNG row filter {kind}")
        out += row
        previous = bytes(row)
    return bytes(out)
