"""
Check that the MOBI 6 reader handles crafted markup in linear time, with its old results.

Books without an EXTH header, under 4 MB, have their whole text run through
``MobiReader`` when metadata is read, and every MOBI 6 book does in
conversion. Its clean-up patterns, anchor pass, CSS classes and inserts took
minutes to hours on crafted books, and it pasted guide hrefs into XPath.
Each crafted book here is read in seconds, each linear-time pass is compared
with the pattern it replaces on random text, and the reader reads every
book as it did before the change.
"""

from __future__ import annotations

import io
import logging
import os
import random
import re
import struct
import tempfile
import time
from collections.abc import Callable
from typing import Any

import pytest
from lxml import etree

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
from LiuXin_alpha.metadata.file_sources.mobi import get_metadata
from LiuXin_alpha.utils import linear_time
from tests.support.file_format_mobi import build_mobi_record0, build_palmdb

LOG = logging.getLogger("tests.mobi6-linear-time")


def book_bytes(html: bytes, *, huff: tuple[bytes, bytes, list[bytes]] | None = None) -> bytes:
    """
    Build a MOBI book without EXTH, with uncompressed or HUFF/CDIC text.

    Example:
        >>> book_bytes(b"<html></html>")[60:68]
        b'BOOKMOBI'


    :param html: The markup, for uncompressed text.
    :param huff: The HUFF and CDIC records and the text records, for HUFF/CDIC text.
    :return: The book file's bytes.
    """
    if huff is None:
        texts = [html[i : i + 4096] for i in range(0, len(html), 4096)] or [b""]
        record0 = build_mobi_record0(title="T", include_exth=False, first_image_index=0xFFFFFFFF, text_record_count=len(texts))
        return build_palmdb([record0, *texts], name="T", ident=b"BOOKMOBI")
    huff_record, cdic, texts = huff
    record0 = bytearray(
        build_mobi_record0(
            title="T", include_exth=False, first_image_index=0xFFFFFFFF, text_record_count=len(texts), compression=b"DH"
        )
    )
    struct.pack_into(">LL", record0, 0x70, len(texts) + 1, 2)
    return build_palmdb([bytes(record0), *texts, huff_record, cdic], name="T", ident=b"BOOKMOBI")


def extract(data: bytes, seconds: float = 5) -> MobiReader:
    """
    Run the reader's content extraction, as conversion and metadata reading do, failing if it is slow.

    Example:
        >>> extract(book_bytes(b"<html><body><p>x</p></body></html>")).htmlfile.endswith("index.html")
        True


    :param data: The book file's bytes.
    :param seconds: The most it may take.
    :return: The reader.
    """
    previous = os.getcwd()
    with tempfile.TemporaryDirectory() as folder:
        os.chdir(folder)
        try:
            started = time.monotonic()
            reader = MobiReader(io.BytesIO(data), LOG)
            reader.extract_content(folder, {})
            assert time.monotonic() - started < seconds
        finally:
            os.chdir(previous)
    return reader


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


CRAFTED = {
    "ampersands": repeated(b"&a "),
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
    "xml encoding quotes": repeated(b'encoding="', b"<html><body><p>x</p><?"),
}


@pytest.mark.parametrize("name", sorted(CRAFTED))
def test_crafted_books_are_read_in_linear_time(name: str) -> None:
    """
    Extract a crafted 1 MB book quickly; before, each took minutes to hours.

    Some crafted books are rejected as empty documents, as they were before;
    the time is what is checked.

    Example:
        >>> test_crafted_books_are_read_in_linear_time("svg siblings")  # doctest: +SKIP


    :param name: Which crafted book.
    :return: None; the assertions are the check.
    """
    data = book_bytes(CRAFTED[name]())
    started = time.monotonic()
    try:
        extract(data)
    except etree.ParserError:
        pass
    assert time.monotonic() - started < 5


def test_guide_hrefs_are_not_run_as_xpath() -> None:
    """
    Look guide targets up by value: a quote neither fails the read nor runs as XPath.

    Example:
        >>> test_guide_hrefs_are_not_run_as_xpath()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    slow = '"] | //*[count(//*/following::*) > 1] | //*[@id="'
    for kind, body in (("toc", b"<p id='t'>toc</p><p><a href='#x'>x</a></p>" * 3000), ("cover", b"<p>x</p>" * 20_000)):
        guide = f"<guide><reference type='{kind}' title='t' href='#t{slow}'/></guide>".encode()
        head = b"<metadata><dc-metadata><dc:Title>Embedded</dc:Title></dc-metadata></metadata>" if kind == "cover" else b""
        extract(book_bytes(b"<html><head>" + head + guide + b"</head><body>" + body + b"</body></html>"))


def huff_book(records: int) -> bytes:
    """
    Make a HUFF/CDIC book of one-byte text records, each unpacking to 4,096 bytes.

    Example:
        >>> len(huff_book(2)) < 5000
        True


    :param records: How many text records.
    :return: The book file's bytes.
    """
    dict1 = struct.pack(">256L", *[(255 << 8) | 0x80 | 8] * 256)
    huff_record = b"HUFF\x00\x00\x00\x18" + struct.pack(">LL", 24, 24 + len(dict1)) + b"\0" * 8 + dict1 + b"\0" * 256
    blob = b"<p>" + b"x" * 4089 + b"</p>"
    cdic = (
        b"CDIC\x00\x00\x00\x10" + struct.pack(">LL", 1, 8) + struct.pack(">H", 2) + struct.pack(">H", len(blob) | 0x8000) + blob
    )
    return book_bytes(b"", huff=(huff_record, cdic, [b"\xff"] * records))


def test_metadata_reads_unpack_at_most_32_mib_of_text() -> None:
    """
    Refuse, when reading metadata, a small book whose text unpacks past 32 MiB.

    Conversion still allows 128 MiB. Each one-byte record here unpacks to
    4,096 bytes, the most a record may.

    Example:
        >>> test_metadata_reads_unpack_at_most_32_mib_of_text()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    assert get_metadata(io.BytesIO(huff_book(100)), extract_cover=False) is not None
    big = huff_book(9000)
    assert len(big) < 4 * 1024 * 1024
    started = time.monotonic()
    with pytest.raises(MobiError, match="limit"):
        get_metadata(io.BytesIO(big), extract_cover=False)
    assert time.monotonic() - started < 5


ORIGINAL_PAGE_BREAK = re.compile(
    r"<\s*/{0,1}\s*mbp:pagebreak((?:\s+[^/>]*){0,1})/{0,1}\s*>\s*(?:<\s*/{0,1}\s*mbp:pagebreak\s*/{0,1}\s*>)*",
    re.IGNORECASE,
)
STYLE_TAGS = ["h1", "H22", "i", "b", "U", "em", "small", "big", "strong", "tt", "p", "x"]
SWAP_A = r"(?i)(?P<styletags>(<(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})(?P<para><p[^>]*>)"
SWAP_B = r"(?i)(?P<para></p[^>]*>)\s*(?P<styletags>(</(h\d+|i|b|u|em|small|big|strong|tt)>\s*){1,})"
SWAP_C = r"(?i)(?P<blockquote>(</(blockquote|div)[^>]*>\s*){1,})(?P<para></p[^>]*>)"
SWAP_D = r"(?i)(?P<para><p[^>]*>)\s*(?P<blockquote>(<(blockquote|div)[^>]*>\s*){1,})"
ENTITY_ANCHOR = r'&([^;]*?)(<a id="filepos\d+"></a>)([^;]*);'
ORIGINAL_LINK = re.compile(rb"""<[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>""", re.IGNORECASE)
PATCHED_LINK = re.compile(rb"""<(?=[^<>]*>)[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>""", re.IGNORECASE)

# Each: alphabet, the original substitution, the reader's, as mobi6.py calls them.
EQUIVALENT: dict[str, tuple[list[Any], Callable[[Any], Any], Callable[[Any], Any]]] = {
    "namespaced tags": (
        ["<", "</", "a", "B", ":", " ", ">", "x", "\n", "ab:"],
        lambda t: re.sub(r"</{0,1}[a-zA-Z]+:\s+[^>]*>", "", t),
        lambda t: linear_time.sub_before_last(re.compile(r"</{0,1}[a-zA-Z]+:\s+[^>]*>"), "", t, ">"),
    ),
    "xml declarations": (
        ["<?xml", "<?XML", " ", ">", "x", "<", "?"],
        lambda t: re.sub(r"<\?xml[^>]*>", "", t),
        lambda t: linear_time.sub_before_last(re.compile(r"<\?xml[^>]*>"), "", t, ">"),
    ),
    "o:p": (
        ["<", "/", "o:p", "O:P", " ", "\n", ">", "x", "o", ":"],
        lambda t: re.sub(r"<\s*(/?)\s*o:p[^>]*>", r"", t),
        lambda t: linear_time.sub_before_last(re.compile(r"<\s*+(/?)\s*+o:p[^>]*>"), "", t, ">"),
    ),
    "style tags before <p>": (
        [f"<{s}>" for s in STYLE_TAGS] + ["<p", "<P x>", ">", " ", "\n", "<", "x", "<h"],
        lambda t: re.sub(SWAP_A, r"\g<para>\g<styletags>", t),
        lambda t: linear_time.sub_from_run_starts(
            re.compile(SWAP_A), r"\g<para>\g<styletags>", t, re.compile(r"(?i)(?:<(?:h\d+|i|b|u|em|small|big|strong|tt)>\s*)+")
        ),
    ),
    "style tags after </p>": (
        [f"</{s}>" for s in STYLE_TAGS] + ["</p", "</P x>", ">", " ", "\n", "<", "x", "</"],
        lambda t: re.sub(SWAP_B, r"\g<styletags>\g<para>", t),
        lambda t: linear_time.sub_skipping_failures(re.compile(SWAP_B), r"\g<styletags>\g<para>", t, re.compile(r"(?i)</p"), ">"),
    ),
    "blocks before </p>": (
        ["</div", "</DIV>", "</blockquote", "</blockquote x>", "</p", "</p>", ">", " ", "\n", "<", "x", "</"],
        lambda t: re.sub(SWAP_C, r"\g<para>\g<blockquote>", t),
        lambda t: linear_time.sub_from_run_starts(
            re.compile(SWAP_C), r"\g<para>\g<blockquote>", t, re.compile(r"(?i)(?:</(?:blockquote|div)[^>]*>\s*)+"), ">"
        ),
    ),
    "blocks after <p>": (
        ["<div", "<DIV>", "<blockquote", "<blockquote x>", "<p", "<p>", "<P", ">", " ", "\n", "<", "x"],
        lambda t: re.sub(SWAP_D, r"\g<blockquote>\g<para>", t),
        lambda t: linear_time.sub_skipping_failures(re.compile(SWAP_D), r"\g<blockquote>\g<para>", t, re.compile(r"(?i)<p"), ">"),
    ),
    "page breaks": (
        ["<", "</", "/", "mbp:pagebreak", "MBP:PageBreak", " ", "\n", ">", "x", "/>", "<mbp:pagebreak", "y"],
        lambda t: ORIGINAL_PAGE_BREAK.sub(r'<div \1 class="mbp_pagebreak" />', t),
        lambda t: linear_time.sub_page_breaks(MobiReader.PAGE_BREAK_PAT, r'<div \1 class="mbp_pagebreak" />', t),
    ),
    "entity anchors": (
        ["&", ";", "x", '<a id="filepos12"></a>', '<a id="filepos', "\n", " "],
        lambda t: re.sub(ENTITY_ANCHOR, r"&\1\3;\2", t),
        lambda t: linear_time.sub_skipping_failures(re.compile(ENTITY_ANCHOR), r"&\1\3;\2", t, re.compile("&"), ";", ";"),
    ),
    "filepos links": (
        [b"<", b">", b"a", b" filepos=", b"FILEPOS=", b"12", b'"', b"'", b" ", b"x"],
        lambda t: [(m.span(), m.group(1)) for m in ORIGINAL_LINK.finditer(t)],
        lambda t: [(m.span(), m.group(1)) for m in PATCHED_LINK.finditer(t)],
    ),
}


@pytest.mark.parametrize("name", sorted(EQUIVALENT))
def test_linear_passes_give_the_original_results(name: str) -> None:
    """
    Give exactly what the replaced pattern gives, on random text built from its pieces.

    Example:
        >>> test_linear_passes_give_the_original_results("page breaks")  # doctest: +SKIP


    :param name: Which pass.
    :return: None; the assertions are the check.
    """
    alphabet, original, patched = EQUIVALENT[name]
    chooser = random.Random(name)
    for _ in range(3000):
        text = alphabet[0][:0].join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 40)))
        assert patched(text) == original(text), text


def test_page_break_pattern_keeps_its_matches() -> None:
    """
    Keep the page-break pattern's matches, groups included, now read possessively.

    Example:
        >>> test_page_break_pattern_keeps_its_matches()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    alphabet = ["<", "</", "/", "mbp:pagebreak", " ", "\t", ">", "x", "/>", "y=1"]
    chooser = random.Random(5)
    for _ in range(5000):
        text = "".join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 25)))
        expected = [m.regs for m in ORIGINAL_PAGE_BREAK.finditer(text)]
        assert [m.regs for m in MobiReader.PAGE_BREAK_PAT.finditer(text)] == expected, text


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


def test_images_and_anchors_land_where_index_inserts_put_them() -> None:
    """
    Place svg images and page-break anchors exactly where the index-based inserts did.

    Example:
        >>> test_images_and_anchors_land_where_index_inserts_put_them()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """

    def by_index(parent: Any, tag: Any, images: list[Any]) -> None:
        """
        Move and insert by index, as the reader did.

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

    def beside(parent: Any, tag: Any, images: list[Any]) -> None:
        """
        Move and insert beside the tag, as the reader now does.

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

    assert serialised_after(beside, 11) == serialised_after(by_index, 11)


def test_repeated_styles_share_a_class() -> None:
    """
    Give tags with the same style the class first made for it.

    Example:
        >>> test_repeated_styles_share_a_class()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    html = b"<html><body><p width='10'>a</p><p width='20'>b</p><p width='10'>c</p></body></html>"
    reader = MobiReader(io.BytesIO(book_bytes(html)), LOG)
    root = etree.fromstring(html, etree.HTMLParser())
    reader.upshift_markup(root)
    classes = [p.get("class") for p in root.iter("p")]

    assert classes[0] == classes[2] != classes[1]
    assert len(reader.tag_css_rules) == 2
