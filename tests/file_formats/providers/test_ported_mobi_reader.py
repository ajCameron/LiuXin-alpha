"""
Check the Kindle-family (MOBI, PRC, AZW, AZW3, AZW4, POBI) reader ported from upstream calibre.

calibre's own language-code, PalmDOC and page-break tests run on the port,
and LiuXin's pure-Python PalmDOC codec is checked against digests taken from
calibre's C extension. The provider reads EXTH metadata and the markup of
old books without EXTH, and keeps the cover as the book stores it.

Most of the file is hostile input. calibre reads the text of EXTH-less books
under 4 MB, and that path held quadratic and worse regular expressions and
scans, XPath built from the book's own text, a HUFF/CDIC decompression bomb
and an overlapping section table. Each is shown here to finish quickly, and
each linear-time replacement is compared with calibre's original on random
text.
"""

from __future__ import annotations

import hashlib
import io
import logging
import random
import re
import struct
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from lxml import etree

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import CoverImage
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import (
    _linear_time,
    chardet,
    huffcdic,
    mobi,
    mobi6,
    mobi_headers,
    mobi_langcodes,
    palmdoc,
)
from LiuXin_alpha.metadata.upstream_calibre.mobi_package import MobiError
from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc
from tests.support.file_format_mobi import build_mobi_record0, build_palmdb

JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb" + b"x" * 200 + b"\xff\xd9"
PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
)
EXTH = [
    (503, "MOBI Test"),
    (100, "Jane Doe"),
    (101, "Pub Co"),
    (103, "<p>A blurb.</p>"),
    (104, "9780000000002"),
    (105, "Fiction; Drama"),
    (106, "2001-02-03"),
    (109, "Some rights"),
    (113, "B00TESTASIN"),
    (524, "fr"),
    (201, struct.pack(">L", 0)),
]
EMBEDDED = (
    b"<html><head><metadata><dc-metadata><dc:Title>Embedded Title</dc:Title><dc:Creator>Old Author</dc:Creator>"
    b"<dc:Publisher>Old Pub</dc:Publisher></dc-metadata></metadata></head><body><p>Old book</p></body></html>"
)


def book_bytes(html: bytes, *, exth: list[Any] | None = None, image: bytes | None = None) -> bytes:
    """
    Build a MOBI book with uncompressed text and, optionally, EXTH records and one image.

    Example:
        >>> book_bytes(b"<html></html>")[60:68]
        b'BOOKMOBI'


    :param html: The book's markup.
    :param exth: EXTH records; without them the book has no EXTH header.
    :param image: An image record, the book's first.
    :return: The book file's bytes.
    """
    texts = [html[i : i + 4096] for i in range(0, len(html), 4096)] or [b""]
    record0 = build_mobi_record0(
        title="Header Title",
        exth_records=exth,
        include_exth=exth is not None,
        first_image_index=len(texts) + 1 if image is not None else 0xFFFFFFFF,
        text_record_count=len(texts),
    )
    return build_palmdb([record0, *texts, *([image] if image is not None else [])], name="MOBI Test", ident=b"BOOKMOBI")


def context(work_dir: Path) -> ProviderContext:
    """
    Build a provider context with a fresh report.

    Example:
        >>> context(Path.cwd()).options
        {}


    :param work_dir: The provider's work folder.
    :return: The context.
    """
    return ProviderContext(
        work_dir=work_dir, options={}, log=logging.getLogger("tests.ported-mobi"), report=ConversionReport()
    )


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
    Read a book with the ported MOBI reader's provider.

    Example:
        >>> read_book(Path("/tmp/b.mobi"))  # doctest: +SKIP


    :param book: The file; its extension is its format.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader("Read MOBI metadata")()
    book_format = book.suffix[1:]
    work = context(book.parent)
    fields = provider.read_calibre_metadata(book, book_format, work)
    return dict(fields.values), provider.extract_cover(book, book_format, work)


def read_quickly(data: bytes, seconds: float = 5) -> Any:
    """
    Read a book's metadata with calibre's entry point, failing if it is slow.

    Example:
        >>> read_quickly(book_bytes(b"<html><body>x</body></html>")).title
        'Header Title'


    :param data: The book file's bytes.
    :param seconds: The most the read may take.
    :return: The metadata.
    """
    started = time.monotonic()
    result = mobi.get_metadata(io.BytesIO(data))
    assert time.monotonic() - started < seconds
    return result


def test_calibres_mobi_tests_pass_on_the_port() -> None:
    """
    Run calibre's language-code, PalmDOC and page-break tests on the port.

    calibre's MOBI utility tests check its writer's colour and guide helpers,
    which are not ported, so they are not run.

    Example:
        >>> test_calibres_mobi_tests_pass_on_the_port()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    for module, count in ((mobi_langcodes, 2), (palmdoc, 4)):
        result = unittest.TestResult()
        module.find_tests().run(result)
        assert result.testsRun == count, module.__name__
        assert not result.failures + result.errors, module.__name__
    mobi6.test_mbp_regex()


def test_palmdoc_codec_matches_calibres_c_extension() -> None:
    """
    Compress and decompress as calibre's ``cPalmdoc`` C extension does.

    The digests were taken from calibre 9.18's ``palmdoc.c``, compiled, on
    these inputs: well-formed text, and random bytes that are not PalmDOC.

    Example:
        >>> test_palmdoc_codec_matches_calibres_c_extension()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    chooser = random.Random(42)
    texts = [
        bytes(chooser.choice(alphabet) for _ in range(size))
        for size in (0, 1, 7, 64, 300, 4096)
        for alphabet in (b"ab ", b"The quick brown fox \x80\xff", bytes(range(256)))
    ]
    corrupt = [bytes(chooser.randrange(256) for _ in range(chooser.randint(0, 50))) for _ in range(300)]

    compressed = [cPalmdoc.compress(text) for text in texts]
    assert hashlib.sha256(b"".join(compressed)).hexdigest() == (
        "d20605fadffb45c40dbb7d01c50cf805d51631c896094e5516ebd16fd4b9e785"
    )
    assert [cPalmdoc.decompress(data) for data in compressed] == texts
    assert hashlib.sha256(b"".join(cPalmdoc.decompress(data) for data in corrupt)).hexdigest() == (
        "483fdd86352ea5bf0327a84358dc9271b04dc3161061075a07f4cb857919b9f7"
    )


@pytest.mark.parametrize("extension", ["mobi", "prc", "azw", "azw3", "azw4", "pobi"])
def test_kindle_books_are_read_from_their_exth_header(tmp_path: Path, extension: str) -> None:
    """
    Read every Kindle extension's EXTH metadata, as calibre reads it.

    Example:
        >>> test_kindle_books_are_read_from_their_exth_header(Path("/tmp"), "mobi")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param extension: The book's extension.
    :return: None; the assertions are the check.
    """
    book = tmp_path / f"exth.{extension}"
    book.write_bytes(book_bytes(b"<html><body><p>Words</p></body></html>", exth=EXTH, image=JPEG))
    values, cover = read_book(book)

    assert values["title"] == "MOBI Test"
    assert values["authors"] == ("Jane Doe",)
    assert values["publisher"] == "Pub Co"
    assert values["comments"] == "<p>A blurb.</p>"
    assert values["tags"] == ("Fiction", "Drama")
    assert values["pubdate"] == "2001-02-03T00:00:00+00:00"
    assert values["languages"] == ("fra",)
    assert dict(values["identifiers"]) == {"isbn": "9780000000002", "mobi-asin": "B00TESTASIN"}
    assert cover is not None and cover.data == JPEG and cover.image_format == "jpeg"


def test_the_cover_is_kept_as_the_book_stores_it(tmp_path: Path) -> None:
    """
    Return a PNG cover's own bytes, named PNG.

    calibre re-encodes every MOBI cover as a JPEG of at most 1650x2200
    through Qt; LiuXin keeps what the book holds unless the
    ``read_mobi_covers_as`` setting asks for calibre's cover (see
    ``tests/metadata/file_sources/test_mobi_cover_reading.py``).

    Example:
        >>> test_the_cover_is_kept_as_the_book_stores_it(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "png.azw3"
    book.write_bytes(book_bytes(b"<html><body><p>Words</p></body></html>", exth=EXTH, image=PNG))
    _, cover = read_book(book)

    assert cover is not None and cover.data == PNG and cover.image_format == "png"


def test_books_without_exth_are_read_from_their_markup(tmp_path: Path) -> None:
    """
    Read an old book's title, author and publisher from its embedded metadata.

    Example:
        >>> test_books_without_exth_are_read_from_their_markup(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "old.prc"
    book.write_bytes(book_bytes(EMBEDDED, image=JPEG))
    values, cover = read_book(book)

    assert values["title"] == "Embedded Title"
    assert values["authors"] == ("Old Author",)
    assert values["publisher"] == "Old Pub"
    assert cover is not None and cover.data == JPEG


def test_an_exth_record_of_size_zero_ends_the_header() -> None:
    """
    Stop reading EXTH at a record claiming size 0, which calibre reads again forever.

    calibre steps by each record's size, so a size-0 record is read once
    for every record the header claims: up to 2**32 times, holding all of
    them.

    Example:
        >>> test_an_exth_record_of_size_zero_ends_the_header()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    zero = struct.pack(">LL", 100, 0)
    record0 = build_mobi_record0(title="T", exth_records=[zero], first_image_index=0xFFFFFFFF, text_record_count=1)
    at = record0.index(b"EXTH")
    record0 = record0[: at + 8] + struct.pack(">L", 0xFFFFFFFF) + record0[at + 12 :]
    data = build_palmdb([record0, b"<html><body>x</body></html>"], name="T", ident=b"BOOKMOBI")

    started = time.monotonic()
    header = mobi_headers.MetadataHeader(io.BytesIO(data), logging.getLogger("tests.ported-mobi"))
    assert time.monotonic() - started < 5
    assert header.exth is not None and header.exth.mi.authors in ([], None, ["Unknown"])


def tagless_region_links() -> bytes:
    """
    Make links to positions inside a long stretch with no tags.

    Example:
        >>> tagless_region_links()[:6]
        b'<html>'


    :return: The markup.
    """
    count = 30_000
    start = 22 * count + 26
    links = b"".join(b"<a filepos=%010d>" % (start + 7 * i) for i in range(count))
    return b"<html>" + links + b"x" * 300_000


def repeated(unit: bytes, prefix: bytes = b"", suffix: bytes = b"", size: int = 1_000_000) -> Callable[[], bytes]:
    """
    Make markup of one unit repeated to about ``size`` bytes.

    Example:
        >>> repeated(b"ab", size=6)()
        b'ababab'


    :param unit: The repeated text.
    :param prefix: Text before it.
    :param suffix: Text after it.
    :param size: The length to fill.
    :return: A function making the markup.
    """
    return lambda: prefix + unit * (size // len(unit)) + suffix


NESTED_P = b"<html><body><p><span><p>x</p></span></p>"
CRAFTED = {
    "entity anchors": repeated(b"&x<a filepos=1>"),
    "open xml declarations": repeated(b"<?xml "),
    "spaces after <": repeated(b" ", b"<html><body><p><"),
    "unclosed o:p": repeated(b"<o:p "),
    "unclosed namespaced tags": repeated(b"a: "),
    "style-tag run": repeated(b"<b>  ", b"<html><body>"),
    "unclosed </p": repeated(b"</p "),
    "closing-div run": repeated(b"</div>", suffix=b"</p"),
    "unclosed <p": repeated(b"<p "),
    "unclosed page breaks": repeated(b"<mbp:pagebreak a/"),
    "page break spaces": repeated(b" ", b"<html><body><p><mbp:pagebreak", b"x/"),
    "unclosed link": repeated(b" filepos=" + b"1" * 50, b"<"),
    "links into tagless text": tagless_region_links,
    "unique styles": lambda: b"<html><body>" + b"".join(b"<p style='a:%d'>x</p>" % i for i in range(40_000)),
    "svg siblings": repeated(b"<svg><img src='a'></svg>", b"<html><body><div>"),
    "page-break anchors": lambda: b"<html><body><div>"
    + b"".join(b"<div class='mbp_pagebreak' filepos-id='f%d'></div>" % i for i in range(20_000)),
    "unclosed metadata": repeated(b"<metadata>", NESTED_P),
    "unclosed guide": repeated(b"<guide>", NESTED_P),
    "xml encoding quotes": repeated(b'encoding="', b"<?"),
    "meta spaces": repeated(b" ", b"<meta"),
    "meta charsets": repeated(b"charset=a ", b'<meta content="'),
}


@pytest.mark.parametrize("name", sorted(CRAFTED))
def test_crafted_books_without_exth_are_read_in_linear_time(name: str) -> None:
    """
    Read a crafted 1 MB book quickly; calibre took minutes to hours on each.

    Example:
        >>> test_crafted_books_without_exth_are_read_in_linear_time("svg siblings")  # doctest: +SKIP


    :param name: Which crafted book.
    :return: None; the assertions are the check.
    """
    assert read_quickly(book_bytes(CRAFTED[name]())).title in ("Header Title", "Unknown")


def test_guide_hrefs_are_not_run_as_xpath() -> None:
    """
    Look up guide targets by value: a quote in one neither fails the read nor runs as XPath.

    calibre pasted the toc and cover hrefs into its XPath; the rest of such
    an href ran as an expression of the book's choosing.

    Example:
        >>> test_guide_hrefs_are_not_run_as_xpath()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    slow = '"] | //*[count(//*/following::*) > 1] | //*[@id="'
    for kind, body in (("toc", b"<p id='t'>toc</p><p><a href='#x'>x</a></p>" * 3000), ("cover", b"<p>x</p>" * 20_000)):
        guide = f"<guide><reference type='{kind}' title='t' href='#t{slow}'/></guide>".encode()
        head = b"<metadata><dc-metadata><dc:Title>Embedded</dc:Title></dc-metadata></metadata>" if kind == "cover" else b""
        html = b"<html><head>" + head + guide + b"</head><body>" + body + b"</body></html>"
        read_quickly(book_bytes(html))


def huff_book(depth: int, width: int) -> bytes:
    """
    Make a HUFF/CDIC book whose one text byte unpacks to ``width ** depth`` bytes.

    Each byte is one code; dictionary entry ``k`` is ``width`` copies of
    entry ``k - 1``, and entry 0 is the letter A.

    Example:
        >>> huff_book(1, 2)[60:68]
        b'BOOKMOBI'


    :param depth: How deep the entries nest.
    :param width: How many copies each holds.
    :return: The book file's bytes.
    """
    dict1 = struct.pack(">256L", *[(255 << 8) | 0x80 | 8] * 256)
    huff = b"HUFF\x00\x00\x00\x18" + struct.pack(">LL", 24, 24 + len(dict1)) + b"\0" * 8 + dict1 + b"\0" * 256
    entries = [(b"A", True)] + [(bytes([256 - level]) * width, False) for level in range(1, depth + 1)]
    table, blobs = b"", b""
    for blob, literal in entries:
        table += struct.pack(">H", 2 * len(entries) + len(blobs))
        blobs += struct.pack(">H", len(blob) | (0x8000 if literal else 0)) + blob
    cdic = b"CDIC\x00\x00\x00\x10" + struct.pack(">LL", len(entries), 8) + table + blobs
    record0 = bytearray(
        build_mobi_record0(title="T", include_exth=False, first_image_index=0xFFFFFFFF, text_record_count=1, compression=b"DH")
    )
    struct.pack_into(">LL", record0, 0x70, 2, 2)
    return build_palmdb([bytes(record0), bytes([255 - depth]), huff, cdic], name="T", ident=b"BOOKMOBI")


def test_huff_cdic_text_is_unpacked_within_a_budget() -> None:
    """
    Refuse a 5 KB book whose text unpacks to a gigabyte, and read one that unpacks to 4 MB.

    Example:
        >>> test_huff_cdic_text_is_unpacked_within_a_budget()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    assert len(huff_book(3, 1000)) < 5000
    with pytest.raises(MobiError, match="unpacks to more than"):
        read_quickly(huff_book(3, 1000))
    assert read_quickly(huff_book(2, 2000)).title == "T"
    assert huffcdic.MAX_UNPACKED == 32 * 1024 * 1024


def test_overlapping_sections_are_refused() -> None:
    """
    Refuse a section table that names the same megabyte hundreds of times.

    Example:
        >>> test_overlapping_sections_are_refused()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    record0 = build_mobi_record0(title="T", include_exth=False, first_image_index=0xFFFFFFFF, text_record_count=1)
    blob = b"<html><body><p>x</p></body></html>" + b" " * 1_000_000
    pairs = 300
    records = [record0, blob] + [b""] * (2 * pairs)
    first = 78 + 8 * len(records) + 2
    start, end = first + len(record0), first + len(record0) + len(blob)
    offsets = [first, start] + [end, start] * pairs
    data = build_palmdb(records, name="T", ident=b"BOOKMOBI", record_offsets=offsets)

    with pytest.raises(MobiError, match="sections overlap"):
        read_quickly(data)
    assert read_quickly(build_palmdb([record0, blob], name="T", ident=b"BOOKMOBI")).title == "T"


ORIGINAL_PAGE_BREAK = re.compile(
    r"<\s*/{0,1}\s*mbp:pagebreak((?:\s+[^/>]*){0,1})/{0,1}\s*>\s*(?:<\s*/{0,1}\s*mbp:pagebreak\s*/{0,1}\s*>)*",
    re.IGNORECASE,
)
STYLE_TAGS = ["h1", "H22", "i", "b", "U", "em", "small", "big", "strong", "tt", "p", "x"]
SWAP_A = r"(?i)(?P<styletags>(<(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})(?P<para><p[^>]*>)"
SWAP_B = r"(?i)(?P<para></p[^>]*>)\s*(?P<styletags>(</(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})"
SWAP_C = r"(?i)(?P<blockquote>(</(blockquote|div)[^>]*>\s*){1,})(?P<para></p[^>]*>)"
SWAP_D = r"(?i)(?P<para><p[^>]*>)\s*(?P<blockquote>(<(blockquote|div)[^>]*>\s*){1,})"
ENTITY_ANCHOR = rb'&([^;]*?)(<a id="filepos\d+"></a>)([^;]*);'
ORIGINAL_LINK = re.compile(rb"""<[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>""", re.IGNORECASE)
PORTED_LINK = re.compile(rb"""<(?=[^<>]*>)[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>""", re.IGNORECASE)

# Each: alphabet, calibre's substitution, the port's, as mobi6.py calls them.
EQUIVALENT: dict[str, tuple[list[Any], Callable[[Any], Any], Callable[[Any], Any]]] = {
    "namespaced tags": (
        ["<", "</", "a", "B", ":", " ", ">", "x", "\n", "ab:"],
        lambda t: re.sub(r"</{0,1}[a-zA-Z]+:\s+[^>]*>", "", t),
        lambda t: _linear_time.sub_before_last(re.compile(r"</{0,1}[a-zA-Z]+:\s+[^>]*>"), "", t, ">"),
    ),
    "metadata": (
        ["<metadata>", "<METADATA>", "</metadata>", "</Metadata>", "x", "\n", " ", "<", ">"],
        lambda t: re.sub(r"<metadata>.+?</metadata>", "", t, flags=re.I),
        lambda t: _linear_time.sub_skipping_failures(
            re.compile(r"<metadata>.+?</metadata>", re.I), "", t, re.compile(r"<metadata>", re.I), "\n"
        ),
    ),
    "xml declarations": (
        ["<?xml", "<?XML", " ", ">", "x", "<", "?"],
        lambda t: re.sub(r"<\?xml[^>]*>", "", t),
        lambda t: _linear_time.sub_before_last(re.compile(r"<\?xml[^>]*>"), "", t, ">"),
    ),
    "o:p": (
        ["<", "/", "o:p", "O:P", " ", "\n", ">", "x", "o", ":"],
        lambda t: re.sub(r"<\s*(/?)\s*o:p[^>]*>", "", t),
        lambda t: _linear_time.sub_before_last(re.compile(r"<\s*+(/?)\s*+o:p[^>]*>"), "", t, ">"),
    ),
    "style tags before <p>": (
        [f"<{s}>" for s in STYLE_TAGS] + ["<p", "<P x>", ">", " ", "\n", "<", "x", "<h"],
        lambda t: re.sub(SWAP_A, r"\g<para>\g<styletags>", t),
        lambda t: _linear_time.sub_from_run_starts(
            re.compile(SWAP_A), r"\g<para>\g<styletags>", t, re.compile(r"(?i)(?:<(?:h\d+|i|b|u|em|small|big|strong|tt)>\s*)+")
        ),
    ),
    "style tags after </p>": (
        [f"</{s}>" for s in STYLE_TAGS] + ["</p", "</P x>", ">", " ", "\n", "<", "x", "</"],
        lambda t: re.sub(SWAP_B, r"\g<styletags>\g<para>", t),
        lambda t: _linear_time.sub_skipping_failures(
            re.compile(SWAP_B), r"\g<styletags>\g<para>", t, re.compile(r"(?i)</p"), ">"
        ),
    ),
    "blocks before </p>": (
        ["</div", "</DIV>", "</blockquote", "</blockquote x>", "</p", "</p>", ">", " ", "\n", "<", "x", "</"],
        lambda t: re.sub(SWAP_C, r"\g<para>\g<blockquote>", t),
        lambda t: _linear_time.sub_from_run_starts(
            re.compile(SWAP_C), r"\g<para>\g<blockquote>", t, re.compile(r"(?i)(?:</(?:blockquote|div)[^>]*>\s*)+"), ">"
        ),
    ),
    "blocks after <p>": (
        ["<div", "<DIV>", "<blockquote", "<blockquote x>", "<p", "<p>", "<P", ">", " ", "\n", "<", "x"],
        lambda t: re.sub(SWAP_D, r"\g<blockquote>\g<para>", t),
        lambda t: _linear_time.sub_skipping_failures(
            re.compile(SWAP_D), r"\g<blockquote>\g<para>", t, re.compile(r"(?i)<p"), ">"
        ),
    ),
    "page breaks": (
        ["<", "</", "/", "mbp:pagebreak", "MBP:PageBreak", " ", "\n", ">", "x", "/>", "<mbp:pagebreak", "y"],
        lambda t: ORIGINAL_PAGE_BREAK.sub(r'<div \1 class="mbp_pagebreak" />', t),
        lambda t: _linear_time.sub_page_breaks(mobi6.MobiReader.PAGE_BREAK_PAT, r'<div \1 class="mbp_pagebreak" />', t),
    ),
    "entity anchors": (
        [b"&", b";", b"x", b'<a id="filepos12"></a>', b'<a id="filepos', b"\n", b" "],
        lambda t: re.sub(ENTITY_ANCHOR, rb"&\1\3;\2", t),
        lambda t: _linear_time.sub_skipping_failures(
            re.compile(ENTITY_ANCHOR), rb"&\1\3;\2", t, re.compile(b"&"), b";", b";"
        ),
    ),
    "filepos links": (
        [b"<", b">", b"a", b" filepos=", b"FILEPOS=", b"12", b'"', b"'", b" ", b"x"],
        lambda t: [(m.span(), m.group(1)) for m in ORIGINAL_LINK.finditer(t)],
        lambda t: [(m.span(), m.group(1)) for m in PORTED_LINK.finditer(t)],
    ),
}


@pytest.mark.parametrize("name", sorted(EQUIVALENT))
def test_linear_passes_give_calibres_results(name: str) -> None:
    """
    Give exactly what calibre's pattern gives, on random text built from its pieces.

    Example:
        >>> test_linear_passes_give_calibres_results("page breaks")  # doctest: +SKIP


    :param name: Which pass.
    :return: None; the assertions are the check.
    """
    alphabet, original, ported = EQUIVALENT[name]
    chooser = random.Random(name)
    for _ in range(3000):
        text = alphabet[0][:0].join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 40)))
        assert ported(text) == original(text), text


def test_page_break_pattern_is_calibres_read_possessively() -> None:
    """
    Keep calibre's page-break matches, groups included, on random text.

    Example:
        >>> test_page_break_pattern_is_calibres_read_possessively()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    alphabet = ["<", "</", "/", "mbp:pagebreak", " ", "\t", ">", "x", "/>", "y=1"]
    chooser = random.Random(5)
    for _ in range(5000):
        text = "".join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 25)))
        expected = [m.regs for m in ORIGINAL_PAGE_BREAK.finditer(text)]
        assert [m.regs for m in mobi6.MobiReader.PAGE_BREAK_PAT.finditer(text)] == expected, text


def test_forward_finder_answers_as_find_and_rfind() -> None:
    """
    Answer like ``bytes.find`` and ``bytes.rfind`` for growing positions.

    Example:
        >>> test_forward_finder_answers_as_find_and_rfind()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    chooser = random.Random(9)
    for _ in range(3000):
        text = bytes(chooser.choice(b"<>ab") for _ in range(chooser.randint(0, 40)))
        finder = _linear_time.ForwardFinder(text)
        pos = 0
        for _ in range(chooser.randint(1, 20)):
            pos += chooser.randint(0, 5)
            assert finder.find(b"<", pos) == text.find(b"<", pos)
            assert finder.find(b">", pos) == text.find(b">", pos)
            assert finder.rfind(b"<", pos + 1) == text.rfind(b"<", 0, pos + 1)


def serialised_after(insert: Callable[[Any, Any, list[Any]], None], seed: int) -> list[bytes]:
    """
    Build random trees, move images and add anchors with ``insert``, and serialise them.

    Example:
        >>> serialised_after(lambda parent, tag, images: None, 1)[0][:6]
        b'<body>'


    :param insert: Moves ``images`` beside ``tag`` in ``parent`` and adds an anchor after it.
    :param seed: Seed for the trees.
    :return: The trees, serialised.
    """
    chooser = random.Random(seed)
    out = []
    for _ in range(300):
        body = etree.Element("body")
        body.text = "t"
        for index in range(chooser.randint(1, 6)):
            child = etree.Comment("c") if chooser.random() < 0.2 else etree.SubElement(body, "svg" if index % 2 else "p")
            if child.getparent() is None:
                body.append(child)
            child.tail = chooser.choice([None, "tail", " "])
        etree.SubElement(body, "svg").tail = chooser.choice([None, "end"])
        tag = chooser.choice([c for c in body if isinstance(c.tag, str)])
        images = []
        for number in range(chooser.randint(0, 3)):
            image = etree.SubElement(tag, "img", src=str(number))
            image.tail = "after"
            images.append(image)
        insert(body, tag, images)
        out.append(etree.tostring(body))
    return out


def test_images_and_anchors_land_where_calibres_inserts_put_them() -> None:
    """
    Place svg images and page-break anchors exactly where calibre's index-based inserts did.

    Example:
        >>> test_images_and_anchors_land_where_calibres_inserts_put_them()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """

    def calibre(parent: Any, tag: Any, images: list[Any]) -> None:
        """
        Move and insert as calibre 9.18 does, by index.

        Example:
            See the test.


        :param parent: The tag's parent.
        :param tag: The svg or page break.
        :param images: The images to move out of it.
        :return: None.
        """
        index = parent.index(tag)
        for img in images:
            img.getparent().remove(img)
            img.tail = img.text = None
            parent.insert(index, img)
        parent.insert(parent.index(tag) + 1, etree.Element("a"))

    def ported(parent: Any, tag: Any, images: list[Any]) -> None:
        """
        Move and insert as the port does, beside the tag.

        Example:
            See the test.


        :param parent: The tag's parent.
        :param tag: The svg or page break.
        :param images: The images to move out of it.
        :return: None.
        """
        before = tag
        for img in images:
            img.getparent().remove(img)
            img.tail = img.text = None
            before.addprevious(img)
            before = img
        tag.addnext(etree.Element("a"))

    assert serialised_after(ported, 11) == serialised_after(calibre, 11)


def test_repeated_styles_share_a_class() -> None:
    """
    Give tags with the same style the class calibre's scan found first.

    Example:
        >>> test_repeated_styles_share_a_class()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    html = b"<html><body><p width='10'>a</p><p width='20'>b</p><p width='10'>c</p></body></html>"
    reader = mobi6.MobiReader(io.BytesIO(book_bytes(html)), logging.getLogger("tests.ported-mobi"))
    root = etree.fromstring(html, etree.HTMLParser())
    reader.upshift_markup(root)
    classes = [p.get("class") for p in root.iter("p")]

    assert classes[0] == classes[2] != classes[1]
    assert len(reader.tag_css_rules) == 2


XML_DECLARATION = re.compile(r'<\?[^<>]+encoding\s*=\s*[\'"](.*?)[\'"][^<>]*>', re.IGNORECASE)
HTML4_CHARSET = re.compile(
    r"""<meta\s+?[^<>]*?content\s*=\s*['"][^'"]*?charset=([-_a-z0-9]+)[^'"]*?['"][^<>]*>(?:\s*</meta>){0,1}""",
    re.IGNORECASE,
)


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize("which", ["xml", "html4"])
def test_encoding_declarations_are_found_as_calibre_finds_them(which: str, binary: bool) -> None:
    """
    Find, replace and search exactly as calibre's encoding-declaration patterns do.

    Example:
        >>> test_encoding_declarations_are_found_as_calibre_finds_them("xml", False)  # doctest: +SKIP


    :param which: The XML-declaration or the HTML 4 charset pattern.
    :param binary: Whether the text is bytes.
    :return: None; the assertions are the check.
    """
    pattern = XML_DECLARATION if which == "xml" else HTML4_CHARSET
    if which == "xml":
        alphabet = ["<?", "<", ">", "?>", "encoding", "ENCODING", "encodıng", "=", " = ", "'", '"', "x", "\n", " "]
    else:
        alphabet = ["<meta", "<META", " ", "\n", "content", "=", '"', "'", "charset=", "chaRSET=", "ſ", "utf-8", "K", "<", ">", "</meta>"]
    if binary:
        pattern = re.compile(pattern.pattern.encode(), re.IGNORECASE)
        alphabet = [piece.encode() for piece in alphabet if piece.isascii()]
    ported = _linear_time.linear_pattern(pattern)
    assert ported is not pattern
    chooser = random.Random(which + str(binary))
    for _ in range(3000):
        text = alphabet[0][:0].join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 30)))
        assert [m.regs for m in ported.finditer(text)] == [m.regs for m in pattern.finditer(text)], text
        assert ported.sub(text[:0], text) == pattern.sub(text[:0], text), text
        found = ported.search(text)
        expected = pattern.search(text)
        assert (found and found.regs) == (expected and expected.regs), text


def test_encoding_declarations_are_found_in_linear_time() -> None:
    """
    Decode 1 MB of unclosed declarations quickly; calibre's patterns are cubic on them.

    Example:
        >>> test_encoding_declarations_are_found_in_linear_time()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    for raw in (
        b"<?" + b'encoding="' * 100_000,
        b"<meta" + b" " * 1_000_000,
        b'<meta content="' + b"charset=a " * 100_000,
        b"<meta " + b'content="charset=a" ' * 50_000 + b"<",
    ):
        started = time.monotonic()
        chardet.xml_to_unicode(raw, strip_encoding_pats=True)
        chardet.strip_encoding_declarations(raw.decode())
        assert time.monotonic() - started < 5


def test_the_mobi_fixes_are_in_the_port() -> None:
    """
    Keep the ported modules' fixes, marked as LiuXin's.

    Example:
        >>> test_the_mobi_fixes_are_in_the_port()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    reader = Path(mobi6.__file__).read_text(encoding="utf-8")
    for fix in (
        "_linear_time.sub_page_breaks(",
        "root.xpath('//*[@id=$id]', id=href)  # LiuXin:",
        "ncls = classes_by_rule.get(rule)  # LiuXin:",
        "before.addprevious(img)  # LiuXin:",
        "tag.addnext(a)  # LiuXin:",
        "raise MobiError('MOBI sections overlap')",
        "finder = _linear_time.ForwardFinder(self.mobi_html)  # LiuXin:",
    ):
        assert fix in reader, fix
    assert "if size == 0:  # LiuXin:" in Path(mobi_headers.__file__).read_text(encoding="utf-8")
    assert "mi.cover_data = mobi_covers.read_mobi_cover(data)  # LiuXin:" in Path(mobi.__file__).read_text(encoding="utf-8")
    assert "self.unpacked > MAX_UNPACKED:  # LiuXin:" in Path(huffcdic.__file__).read_text(encoding="utf-8")
    assert "_linear_time.linear_pattern(" in Path(chardet.__file__).read_text(encoding="utf-8")
