"""
Build LRF books byte by byte for reader tests.

An LRF book is a header, its zlib-packed meta info, a thumbnail (from version
800 on), its objects, and an index of the objects. Each object is a run of
tags: a start tag naming its ID and type, the object's own tags, and an end tag.

Example:
    Exercise file format lrf through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_unpack_limits.py
"""

from __future__ import annotations

import struct
import zlib

PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
)

INFO = (
    '<?xml version="1.0" encoding="UTF-16"?><Info version="1.1"><BookInfo>'
    '<Title reading="Title Sort">LRF Title</Title><Author reading="Doe, Jane">Jane Doe</Author>'
    "<BookID>FB0123456789</BookID><Publisher>LRF Publisher</Publisher><Label>Label</Label>"
    "<Category>Fiction</Category><Classification>Novel</Classification><FreeText>A comment.</FreeText>"
    "</BookInfo><DocInfo><Language>en</Language><Creator>Creator</Creator>"
    "<CreationDate>2008-01-01</CreationDate><Producer>Producer</Producer><SumPage>1</SumPage></DocInfo></Info>"
)


def tag(tag_id: int, contents: bytes = b"") -> bytes:
    """
    Make an LRF tag: its low byte, ``0xF5``, then its contents.

    Example:
        >>> tag(0x01)
        b'\\x01\\xf5'


    :param tag_id: The tag's low byte.
    :param contents: The bytes after it.
    :return: The tag's bytes.
    """
    return bytes([tag_id, 0xF5]) + contents


def lrf_object(obj_id: int, obj_type: int, body: bytes) -> bytes:
    """
    Wrap tags in an object's start and end tags.

    Example:
        >>> lrf_object(1, 0x0C, b"")[:2]
        b'\\x00\\xf5'


    :param obj_id: The object's ID.
    :param obj_type: Its type: ``0x0C`` for an image, ``0x11`` for an image stream.
    :param body: Its tags.
    :return: The object's bytes.
    """
    return tag(0x00, struct.pack("<IH", obj_id, obj_type)) + body + tag(0x01)


def image_stream(obj_id: int, data: bytes, flags: int = 0x12) -> bytes:
    """
    Make an image stream object: flags, size, then the stream between its start and end tags.

    Example:
        >>> len(image_stream(2, b"abc")) > 3
        True


    :param obj_id: The object's ID.
    :param data: The stream as stored; ``0x100`` in the flags means a declared size, then zlib.
    :param flags: The stream flags; the low byte is the image type, ``0x12`` for PNG.
    :return: The object's bytes.
    """
    body = tag(0x54, struct.pack("<H", flags)) + tag(0x04, struct.pack("<I", len(data))) + tag(0x05) + data + tag(0x06)
    return lrf_object(obj_id, 0x11, body)


def packed_stream(data: bytes, declared: int | None = None) -> bytes:
    """
    Pack a stream as LRF does with flag ``0x100``: its unpacked size, then zlib.

    Example:
        >>> packed_stream(b"abc")[:4]
        b'\\x03\\x00\\x00\\x00'


    :param data: The unpacked stream.
    :param declared: The size to declare, if not the real one.
    :return: The stored stream.
    """
    return struct.pack("<I", len(data) if declared is None else declared) + zlib.compress(data)


def image(obj_id: int, refstream: int) -> bytes:
    """
    Make an image object that points at an image stream.

    Example:
        >>> image(1, 2)[-6:-2]
        b'\\x02\\x00\\x00\\x00'


    :param obj_id: The object's ID.
    :param refstream: The image stream's ID.
    :return: The object's bytes.
    """
    return lrf_object(obj_id, 0x0C, tag(0x4C, struct.pack("<I", refstream)))


def lrf_book(
    *,
    info: bytes = INFO.encode("utf-16"),
    objects: list[tuple[int, bytes]] | None = None,
    packed_info: bytes | None = None,
) -> bytes:
    """
    Make a version 1000 LRF book: header, packed meta info, thumbnail, objects and index.

    Example:
        >>> lrf_book()[:6].decode("utf-16le")
        'LRF'


    :param info: The meta info XML.
    :param objects: ``(id, bytes)`` per object; by default an image and its PNG stream.
    :param packed_info: The packed meta info as stored, in place of ``zlib.compress(info)``.
    :return: The book's bytes.
    """
    if objects is None:
        objects = [(1, image(1, 2)), (2, image_stream(2, PNG))]
    packed = zlib.compress(info) if packed_info is None else packed_info
    header = bytearray(0x58)
    header[0:6] = "LRF".encode("utf-16le")
    struct.pack_into("<H", header, 0x08, 1000)
    struct.pack_into("<H", header, 0x4C, len(packed) + 4)
    struct.pack_into("<H", header, 0x4E, 0x12)
    struct.pack_into("<I", header, 0x50, len(PNG))
    struct.pack_into("<I", header, 0x54, len(info))
    body = bytes(header) + packed + PNG
    index = b""
    for obj_id, data in objects:
        index += struct.pack("<IIII", obj_id, len(body), len(data), 0)
        body += data
    book = bytearray(body + index)
    struct.pack_into("<Q", book, 0x10, len(objects))
    struct.pack_into("<Q", book, 0x18, len(body))
    return bytes(book)


def zero_stream_bomb(megabytes: int) -> bytes:
    """
    Pack ``megabytes`` of zeros a megabyte at a time, as an LRF stream declaring their size.

    Example:
        >>> len(zero_stream_bomb(1)) < 2_000
        True


    :param megabytes: How much the stream unpacks to.
    :return: The stored stream: about a thousandth of what it unpacks to.
    """
    packer = zlib.compressobj(9)
    packed = b"".join(packer.compress(bytes(1 << 20)) for _ in range(megabytes)) + packer.flush()
    return struct.pack("<I", megabytes << 20) + packed
