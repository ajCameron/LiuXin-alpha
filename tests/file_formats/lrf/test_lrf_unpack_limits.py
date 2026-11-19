"""
Check that LiuXin's LRF reader unpacks with limits and parses its meta info once, and that its writer runs.

A book's meta info is zlib-packed XML whose 64 KB can unpack to 64 MB, and the
reader unpacked and parsed it with minidom for every field it read: 1.6 million
empty elements in a 6.4 KB book took 44 s. Image streams were unpacked whole
before their declared size was checked: a 390 KB book added 380 MB. The same
code in calibre is upstream bug 13 in ``dev-docs/calibre_compat/upstream-calibre-bugs.md``.

The writer (``pylrs``) wrote tag streams into a text buffer and opened image
files with Python 2's ``file()``, so a book with a text block or an image
could not be written.
"""

from __future__ import annotations

import io
import time
import zlib
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.lrf import meta
from LiuXin_alpha.file_formats.lrf.meta import LRFException, LRFMetaFile
from LiuXin_alpha.file_formats.lrf.objects import LRFParseError
from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import (
    Book,
    ImageBlock,
    ImageStream,
    Paragraph,
)
from LiuXin_alpha.metadata.file_sources.lrf import LrfFormatError, get_metadata
from tests.support.file_format_lrf import (
    INFO,
    PNG,
    image,
    image_stream,
    lrf_book,
    packed_stream,
    zero_stream_bomb,
)


def test_books_read_their_meta_info_and_cover() -> None:
    """
    Read the title, authors, sort fields, publisher, tags, comments and cover.

    Example:
        >>> test_books_read_their_meta_info_and_cover()


    :return: None; the assertions are the check.
    """
    mi = get_metadata(io.BytesIO(lrf_book()))
    assert mi.title == "LRF Title"
    assert mi.authors == ["Jane Doe"]
    assert (mi.title_sort, mi.author_sort) == ("Title Sort", "Doe, Jane")
    assert mi.publisher == "LRF Publisher"
    assert mi.tags == ["Fiction", "Novel"]
    assert mi.comments == "A comment."
    assert mi.cover_data == ("png", PNG)


def test_the_meta_info_is_parsed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Unpack and parse the meta info once for all the fields, and afresh after a write.

    Example:
        >>> test_the_meta_info_is_parsed_once(pytest.MonkeyPatch())  # doctest: +SKIP


    :param monkeypatch: Counts the parses.
    :return: None; the assertions are the check.
    """
    calls: list[int] = []
    original = meta.dom.parseString

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

    monkeypatch.setattr(meta.dom, "parseString", counting)
    book = LRFMetaFile(io.BytesIO(lrf_book()))
    get_metadata(book)
    assert len(calls) == 1
    book.title = "New Title"
    assert book.title == "New Title"
    assert len(calls) == 2


def test_meta_info_that_unpacks_past_a_megabyte_is_refused() -> None:
    """
    Refuse 6.6 KB of packed meta info holding 1.6 million elements, and keep zlib's error for a truncated one.

    Example:
        >>> test_meta_info_that_unpacks_past_a_megabyte_is_refused()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    xml = ('<?xml version="1.0"?><Info><BookInfo>' + "<a/>" * 1_600_000 + "<Title>T</Title></BookInfo></Info>").encode()
    book = lrf_book(info=xml)
    assert len(book) < 8_000
    started = time.monotonic()
    with pytest.raises(LRFException, match="unpacks to more than 1048576 bytes"):
        _ = LRFMetaFile(io.BytesIO(book)).title
    with pytest.raises(LrfFormatError):
        get_metadata(io.BytesIO(book))
    assert time.monotonic() - started < 2
    truncated = lrf_book(packed_info=zlib.compress(INFO.encode("utf-16"))[:-8])
    with pytest.raises(LRFException, match="Unable to decompress"):
        _ = LRFMetaFile(io.BytesIO(truncated)).title


def test_image_streams_unpack_within_their_declared_size_and_the_limit() -> None:
    """
    Read a packed cover, and refuse a 400 MB stream, an understated one and a truncated one.

    Example:
        >>> test_image_streams_unpack_within_their_declared_size_and_the_limit()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """

    def cover_of(stored: bytes) -> Any:
        """
        Read the cover of a book whose image stream is ``stored``, packed.

        Example:
            See the test.


        :param stored: The stream as stored.
        :return: The cover.
        """
        book = lrf_book(objects=[(1, image(1, 2)), (2, image_stream(2, stored, flags=0x112))])
        return LRFMetaFile(io.BytesIO(book)).get_cover()

    assert cover_of(packed_stream(PNG)) == ("png", PNG)
    with pytest.raises(LRFParseError, match="unpacks to more than 33554432 bytes"):
        cover_of(zero_stream_bomb(400))
    with pytest.raises(LRFParseError, match="decompressed size is wrong"):
        cover_of(packed_stream(bytes(100_000), declared=1_000))
    with pytest.raises(zlib.error):
        cover_of(packed_stream(PNG)[:-8])


def test_books_with_text_and_images_are_written_and_read_back(tmp_path: Path) -> None:
    """
    Write a book with a text block, an image and a thumbnail, and read it back.

    Example:
        >>> test_books_with_text_and_images_are_written_and_read_back(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the image.
    :return: None; the assertions are the check.
    """
    picture = tmp_path / "cover.png"
    picture.write_bytes(PNG)
    book = Book(title="Written Title", author="Ann Writer", thumbnail=str(picture))
    page = book.create_page()
    stream = ImageStream(file=str(picture))
    book.append(stream)
    page.append(ImageBlock(stream, x1=1, y1=1, xsize=1, ysize=1, blockwidth=1, blockheight=1))
    block = book.create_text_block()
    block.append(Paragraph("Hello"))
    page.append(block)
    book.append(page)

    class Kept(io.BytesIO):
        """
        Keep the bytes after the writer closes its file.

        Example:
            >>> Kept(b"x").close()
        """

        def close(self) -> None:
            """
            Leave the buffer open.

            Example:
                >>> Kept().close()


            :return: None.
            """

    written = Kept()
    book.renderLrf(written)
    mi = get_metadata(io.BytesIO(written.getvalue()))
    assert mi.title == "Written Title"
    assert mi.authors == ["Ann Writer"]
    assert mi.cover_data == ("png", PNG)
