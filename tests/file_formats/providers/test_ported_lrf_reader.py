"""
Check the LRF reader ported from upstream calibre.

calibre's LRF reader takes the book's information from the zlib-packed XML
after the header, and its cover from the first image object. The provider reads
both. Two LiuXin fixes are checked here: the information is unpacked to at most
a megabyte and parsed once, where calibre parsed it again for every field and a
6.6 KB book took 51 s; and an image stream is unpacked to no more than it
declares and no more than 32 MiB, where a 390 KB book filled 790 MB.

The books are built here byte by byte: a header, the packed information, a
thumbnail, the objects and their index.
"""

from __future__ import annotations

import importlib.util
import io
import logging
import struct
import sys
import time
import zlib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import CoverImage
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import (
    html_entities,
    lrf,
    lrf_objects,
    lrf_package,
)

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
    :param data: The stream as stored: ``0x100`` in the flags means a declared size and zlib.
    :param flags: The stream flags; the low byte is the image type, ``0x12`` for PNG.
    :return: The object's bytes.
    """
    body = tag(0x54, struct.pack("<H", flags)) + tag(0x04, struct.pack("<I", len(data))) + tag(0x05) + data + tag(0x06)
    return lrf_object(obj_id, 0x11, body)


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
    version: int = 1000,
    packed_info: bytes | None = None,
) -> bytes:
    """
    Make an LRF book: header, packed information, thumbnail, objects and object index.

    Example:
        >>> lrf_book()[:6].decode("utf-16le")
        'LRF'


    :param info: The information XML.
    :param objects: ``(id, bytes)`` per object; by default an image and its PNG stream.
    :param version: The LRF version; above 800 the header holds a thumbnail.
    :param packed_info: The packed information as stored, in place of ``zlib.compress(info)``.
    :return: The book's bytes.
    """
    if objects is None:
        objects = [(1, image(1, 2)), (2, image_stream(2, PNG))]
    packed = zlib.compress(info) if packed_info is None else packed_info
    header = bytearray(0x58 if version > 800 else 0x53)
    header[0:6] = "LRF".encode("utf-16le")
    struct.pack_into("<H", header, 0x08, version)
    struct.pack_into("<H", header, 0x4C, len(packed) + 4)
    thumbnail = b""
    if version > 800:
        thumbnail = PNG
        struct.pack_into("<H", header, 0x4E, 0x12)
        struct.pack_into("<I", header, 0x50, len(thumbnail))
        struct.pack_into("<I", header, 0x54, len(info))
    body = bytes(header) + packed + thumbnail
    index = b""
    for obj_id, data in objects:
        index += struct.pack("<IIII", obj_id, len(body), len(data), 0)
        body += data
    book = bytearray(body + index)
    struct.pack_into("<Q", book, 0x10, len(objects))
    struct.pack_into("<Q", book, 0x18, len(body))
    return bytes(book)


@pytest.fixture(autouse=True)
def empty_cache() -> None:
    """
    Start every test with no remembered readings.

    Example:
        >>> "empty_cache" in globals()
        True


    :return: None.
    """
    fork_metadata._cache.clear()


def read_book(book: Path) -> tuple[dict[str, Any], CoverImage | None]:
    """
    Read a book with the ported LRF reader's provider.

    Example:
        >>> read_book(Path("/tmp/b.lrf"))  # doctest: +SKIP


    :param book: The file.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader("Read LRF metadata")()
    work = ProviderContext(
        work_dir=book.parent, options={}, log=logging.getLogger("tests.ported-lrf"), report=ConversionReport()
    )
    fields = provider.read_calibre_metadata(book, "lrf", work)
    return dict(fields.values), provider.extract_cover(book, "lrf", work)


def test_lrf_books_give_their_information_and_cover(tmp_path: Path) -> None:
    """
    Read the title, authors, sort fields, publisher, tags, comments and cover.

    Example:
        >>> test_lrf_books_give_their_information_and_cover(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.lrf"
    book.write_bytes(lrf_book())
    fields, cover = read_book(book)
    assert fields["title"] == "LRF Title"
    assert list(fields["authors"]) == ["Jane Doe"]
    assert fields["title_sort"] == "Title Sort"
    assert fields["author_sort"] == "Doe, Jane"
    assert fields["publisher"] == "LRF Publisher"
    assert list(fields["tags"]) == ["Fiction", "Novel"]
    assert fields["comments"] == "A comment."
    assert cover is not None and cover.data == PNG and cover.image_format == "png"


def test_placeholder_values_and_missing_covers_read_as_calibre_reads_them() -> None:
    """
    Drop calibre's placeholder title and publisher, and give no cover for a book without images.

    Example:
        >>> test_placeholder_values_and_missing_covers_read_as_calibre_reads_them()


    :return: None; the assertions are the check.
    """
    info = INFO.replace("LRF Title", "Unknown").replace("LRF Publisher", "Some Publisher").encode("utf-16")
    mi = lrf.get_metadata(io.BytesIO(lrf_book(info=info, objects=[])))
    assert mi.title == "Unknown"
    assert mi.publisher is None
    assert mi.cover_data == (None, None)


def test_a_packed_cover_stream_is_unpacked() -> None:
    """
    Read a cover whose stream is zlib-packed behind its declared size.

    Example:
        >>> test_a_packed_cover_stream_is_unpacked()


    :return: None; the assertions are the check.
    """
    packed = struct.pack("<I", len(PNG)) + zlib.compress(PNG)
    book = lrf_book(objects=[(1, image(1, 2)), (2, image_stream(2, packed, flags=0x112))])
    assert lrf.get_metadata(io.BytesIO(book)).cover_data == ("png", PNG)
    truncated = struct.pack("<I", len(PNG)) + zlib.compress(PNG)[:-8]
    book = lrf_book(objects=[(1, image(1, 2)), (2, image_stream(2, truncated, flags=0x112))])
    with pytest.raises(zlib.error):
        lrf.get_metadata(io.BytesIO(book))


def test_the_information_is_parsed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Unpack and parse the information once, where calibre did it for every field.

    Example:
        >>> test_the_information_is_parsed_once(pytest.MonkeyPatch())  # doctest: +SKIP


    :param monkeypatch: Counts the parses.
    :return: None; the assertions are the check.
    """
    calls = []
    original = lrf.dom.parseString

    def counting(source: Any) -> Any:
        """
        Count a parse and do it.

        Example:
            See the test.


        :param source: The XML.
        :return: The document.
        """
        calls.append(len(source))
        return original(source)

    monkeypatch.setattr(lrf.dom, "parseString", counting)
    meta = lrf.LRFMetaFile(io.BytesIO(lrf_book()))
    lrf.get_metadata(meta)
    assert len(calls) == 1
    meta.title = "New Title"  # a write unpacks the new information afresh
    assert meta.title == "New Title"
    assert len(calls) == 2


def test_information_that_unpacks_past_a_megabyte_is_refused() -> None:
    """
    Refuse 6.6 KB of packed information holding 1.6 million elements, which took calibre 51 s.

    Example:
        >>> test_information_that_unpacks_past_a_megabyte_is_refused()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    xml = ('<?xml version="1.0"?><Info><BookInfo>' + "<a/>" * 1_600_000 + "<Title>T</Title></BookInfo></Info>").encode()
    book = lrf_book(info=xml)
    assert len(book) < 8_000
    started = time.monotonic()
    with pytest.raises(lrf.LRFException, match="unpacks to more than 1048576 bytes"):
        lrf.get_metadata(io.BytesIO(book))
    assert time.monotonic() - started < 2
    truncated = lrf_book(packed_info=zlib.compress(INFO.encode("utf-16"))[:-8])
    with pytest.raises(lrf.LRFException, match="Unable to decompress"):
        lrf.get_metadata(io.BytesIO(truncated))


def test_an_image_stream_that_unpacks_past_its_limit_is_refused() -> None:
    """
    Refuse a 390 KB stream that unpacks to 400 MB, and one that unpacks past its declared size.

    Example:
        >>> test_an_image_stream_that_unpacks_past_its_limit_is_refused()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    unpacker = zlib.compressobj(9)
    packed = b"".join(unpacker.compress(bytes(1_000_000)) for _ in range(400)) + unpacker.flush()
    bomb = lrf_book(objects=[(1, image(1, 2)), (2, image_stream(2, struct.pack("<I", 400_000_000) + packed, 0x112))])
    with pytest.raises(lrf_package.LRFParseError, match="unpacks to more than 33554432 bytes"):
        lrf.get_metadata(io.BytesIO(bomb))
    understated = struct.pack("<I", 1_000) + packed
    book = lrf_book(objects=[(1, image(1, 2)), (2, image_stream(2, understated, flags=0x112))])
    with pytest.raises(lrf_package.LRFParseError, match="decompressed size is wrong"):
        lrf.get_metadata(io.BytesIO(book))


def load_porter() -> ModuleType:
    """
    Load ``scripts/port_upstream_calibre.py`` as a module.

    Example:
        >>> load_porter().PKG
        'LiuXin_alpha.metadata.upstream_calibre'


    :return: The module.
    """
    path = Path(__file__).resolve().parents[3] / "scripts" / "port_upstream_calibre.py"
    spec = importlib.util.spec_from_file_location("port_upstream_calibre", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


def test_the_lrf_fixes_are_in_the_port_and_guarded() -> None:
    """
    Keep the ported fixes, and stop a re-port whose calibre code has moved.

    Example:
        >>> test_the_lrf_fixes_are_in_the_port_and_guarded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    meta = Path(lrf.__file__).read_text(encoding="utf-8")
    assert "MAX_INFO = 1024 * 1024" in meta and "# LiuXin: parse once" in meta
    objects = Path(lrf_objects.__file__).read_text(encoding="utf-8")
    assert "MAX_UNPACKED = 32 * 1024 * 1024" in objects
    porter = load_porter()
    for path in ("ebooks/lrf/meta.py", "ebooks/lrf/objects.py"):
        with pytest.raises(SystemExit, match="no longer matches"):
            porter.apply_patches("x = 1\n", path)


def test_partial_ports_keep_only_what_the_reader_uses() -> None:
    """
    Port the entity table and its lookup, and the LRF package's error and profile, alone.

    Example:
        >>> test_partial_ports_keep_only_what_the_reader_uses()


    :return: None; the assertions are the check.
    """
    assert html_entities.html5_entities["eacute"] == "é"
    assert not hasattr(html_entities, "find_tests")
    assert not hasattr(lrf_package, "Book")
    assert issubclass(lrf_package.LRFParseError, Exception)
    assert lrf_package.PRS500_PROFILE.dpi == 166
