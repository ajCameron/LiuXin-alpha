"""
Check the HTMLZ, TXTZ, HTML, FB2 and ODT readers ported from upstream calibre.

calibre's own HTML tests run on the port, and its FB2 test where the html2text
package it needs is installed. Each provider is checked on a sample book, and
on hostile input: entities, oversized archives and HTML comments that made
calibre's pattern backtrack for minutes.
"""

from __future__ import annotations

import base64
import importlib.util
import logging
import random
import re
import struct
import time
import unittest
import zipfile
import zlib
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.archive_preflight import ArchivePreflightError
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import CoverImage
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import fb2, html

pytest.importorskip("dateutil")


def png(width: int, height: int) -> bytes:
    """
    Make a red PNG image of the given size.

    Example:
        >>> png(2, 3)[:8]
        b'\\x89PNG\\r\\n\\x1a\\n'


    :param width: Width in pixels.
    :param height: Height in pixels.
    :return: The image file's bytes.
    """

    def chunk(kind: bytes, data: bytes) -> bytes:
        """
        Frame one PNG chunk.

        Example:
            See ``png``.


        :param kind: The chunk type.
        :param data: Its data.
        :return: The chunk with its length and checksum.
        """
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    rows = b"".join(b"\0" + b"\xff\x00\x00" * width for _ in range(height))
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b"")


COVER = png(120, 160)

PACKAGE = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Zipped Test</dc:title>
  <dc:creator opf:role="aut" opf:file-as="Doe, Jane">Jane Doe</dc:creator>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:language>fr</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:description>A blurb.</dc:description>
  <meta name="calibre:series" content="Saga"/>
  <meta name="calibre:series_index" content="2"/>
  <meta name="cover" content="cover"/>
 </metadata>
 <manifest><item id="cover" href="cover.png" media-type="image/png"/></manifest>
 <spine/>
</package>"""

HTML = """<html><head>
<title>A Title Tag</title>
<meta name="dc.title" content="HTML Test"/>
<meta name="dc.creator" content="Jane Doe"/>
<meta name="dc.publisher" content="Pub Co"/>
<meta name="dc.language" content="fr"/>
<meta name="dc.date.published" content="2001-02-03"/>
<meta name="series" content="Saga [2]"/>
<meta name="tags" content="Fiction, Drama"/>
<meta name="dc.identifier.isbn" content="9780000000002"/>
<!-- AUTHOR="Comment Author" -->
</head><body><p>Words</p></body></html>"""

FB2 = """<?xml version="1.0" encoding="UTF-8"?>{doctype}
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0" xmlns:l="http://www.w3.org/1999/xlink">
<description><title-info>
<genre>sf</genre>
<author><first-name>Jane</first-name><last-name>Doe</last-name></author>
<book-title>FB2 Test</book-title>
<annotation><p>{annotation}</p></annotation>
<coverpage><image l:href="#cover.png"/></coverpage>
<lang>fr</lang>
<sequence name="Saga" number="2"/>
</title-info>
<publish-info><publisher>Pub Co</publisher><isbn>978-0-00-000000-2</isbn></publish-info>
</description><body><section><p>Words</p></section></body>
<binary id="cover.png" content-type="image/png">{cover}</binary>
</FictionBook>"""

ODF_NAMESPACES = (
    'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"'
    ' xmlns:meta="urn:oasis:names:tc:opendocument:xmlns:meta:1.0"'
    ' xmlns:dc="http://purl.org/dc/elements/1.1/"'
    ' xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"'
    ' xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"'
    ' xmlns:svg="urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0"'
    ' xmlns:xlink="http://www.w3.org/1999/xlink"'
)

ODT_META = f"""<?xml version='1.0' encoding='UTF-8'?>{{doctype}}
<office:document-meta {ODF_NAMESPACES} office:version="1.2"><office:meta>
<dc:title>ODT Test</dc:title><meta:initial-creator>Jane Doe</meta:initial-creator>
<dc:language>fr</dc:language><meta:keyword>Fiction, Drama</meta:keyword>
<dc:description>{{description}}</dc:description>{{user_defined}}
</office:meta></office:document-meta>"""

OPF_USER_DEFINED = (
    '<meta:user-defined meta:name="opf.metadata" meta:value-type="boolean">true</meta:user-defined>'
    '<meta:user-defined meta:name="opf.series">Saga</meta:user-defined>'
    '<meta:user-defined meta:name="opf.seriesindex">2</meta:user-defined>'
    '<meta:user-defined meta:name="opf.publisher">Pub Co</meta:user-defined>'
    '<meta:user-defined meta:name="opf.isbn">9780000000002</meta:user-defined>'
    '<meta:user-defined meta:name="opf.rating">8</meta:user-defined>'
)

ODT_CONTENT = f"""<?xml version='1.0' encoding='UTF-8'?>{{doctype}}
<office:document-content {ODF_NAMESPACES} office:version="1.2"><office:body><office:text>
<text:p>{{text}}<draw:frame svg:width="12cm" svg:height="16cm" draw:name="{{frame}}">
<draw:image xlink:href="Pictures/cover.png"/></draw:frame></text:p>
</office:text></office:body></office:document-content>"""

ODT_MANIFEST = """<?xml version='1.0' encoding='UTF-8'?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
<manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.text"/>
<manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
<manifest:file-entry manifest:full-path="meta.xml" manifest:media-type="text/xml"/>
<manifest:file-entry manifest:full-path="Pictures/cover.png" manifest:media-type="image/png"/>
</manifest:manifest>"""

LAUGHS = "<!DOCTYPE x [" + '<!ENTITY a0 "lol">' + "".join(
    f'<!ENTITY a{i} "{f"&a{i - 1};" * 10}">' for i in range(1, 10)
) + "]>"


def write_zip(path: Path, members: dict[str, bytes | str]) -> Path:
    """
    Write a ZIP file, deflating every member.

    Example:
        >>> import tempfile
        >>> zipfile.ZipFile(write_zip(Path(tempfile.mkdtemp()) / "a.zip", {"a": "b"})).namelist()
        ['a']


    :param path: Where to write it.
    :param members: The members, by name.
    :return: The path.
    """
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def make_odt(
    path: Path,
    *,
    user_defined: str = OPF_USER_DEFINED,
    frame: str = "opf.cover",
    image: bytes = COVER,
    doctype: str = "",
    description: str = "A blurb.",
    text: str = "",
) -> Path:
    """
    Write an ODT document holding one framed image.

    Example:
        >>> import tempfile
        >>> "meta.xml" in zipfile.ZipFile(make_odt(Path(tempfile.mkdtemp()) / "a.odt")).namelist()
        True


    :param path: Where to write it.
    :param user_defined: ``meta:user-defined`` elements for ``meta.xml``.
    :param frame: The frame's name.
    :param image: The image.
    :param doctype: A document type declaration for both XML parts.
    :param description: The description in ``meta.xml``.
    :param text: Text before the frame in ``content.xml``.
    :return: The path.
    """
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/vnd.oasis.opendocument.text", compress_type=zipfile.ZIP_STORED)
        archive.writestr("META-INF/manifest.xml", ODT_MANIFEST)
        archive.writestr(
            "meta.xml", ODT_META.format(doctype=doctype, description=description, user_defined=user_defined)
        )
        archive.writestr("content.xml", ODT_CONTENT.format(doctype=doctype, text=text, frame=frame))
        archive.writestr("Pictures/cover.png", image)
    return path


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
        work_dir=work_dir, options={}, log=logging.getLogger("tests.ported-text"), report=ConversionReport()
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


def read_book(plugin: str, book: Path) -> tuple[dict[str, Any], CoverImage | None]:
    """
    Read a book with one ported reader's provider.

    Example:
        >>> read_book("Read HTML metadata", Path("/tmp/b.html"))  # doctest: +SKIP


    :param plugin: The calibre plugin's name.
    :param book: The file; its extension is its format.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader(plugin)()
    book_format = book.suffix[1:]
    work = context(book.parent)
    fields = provider.read_calibre_metadata(book, book_format, work)
    return dict(fields.values), provider.extract_cover(book, book_format, work)


def test_calibres_html_tests_pass_on_the_port() -> None:
    """
    Run calibre's HTML metadata tests on the port, with lxml parsing for html5-parser.

    Example:
        >>> test_calibres_html_tests_pass_on_the_port()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    result = unittest.TestResult()
    html.find_tests().run(result)

    assert result.testsRun == 5
    assert not result.failures + result.errors


@pytest.mark.skipif(importlib.util.find_spec("html2text") is None, reason="calibre's FB2 test writes comments with html2text")
def test_calibres_fb2_test_passes_on_the_port() -> None:
    """
    Run calibre's FB2 round-trip test, which also writes FB2, on the port.

    Example:
        >>> test_calibres_fb2_test_passes_on_the_port()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    result = unittest.TestResult()
    fb2.find_tests().run(result)

    assert result.testsRun == 1
    assert not result.failures + result.errors


@pytest.mark.parametrize(("plugin", "name"), [("Read HTMLZ metadata", "book.htmlz"), ("Read TXTZ metadata", "book.txtz")])
def test_htmlz_and_txtz_are_read_from_their_package(tmp_path: Path, plugin: str, name: str) -> None:
    """
    Read HTMLZ and TXTZ books from the OPF package inside them, with its cover.

    Example:
        >>> test_htmlz_and_txtz_are_read_from_their_package(Path("/tmp"), "Read TXTZ metadata", "b.txtz")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param plugin: The calibre plugin's name.
    :param name: The book's file name.
    :return: None; the assertions are the check.
    """
    book = write_zip(tmp_path / name, {"index.html": "<p>Words</p>", "metadata.opf": PACKAGE, "cover.png": COVER})

    values, cover = read_book(plugin, book)

    assert values["title"] == "Zipped Test"
    assert values["author_sort"] == "Doe, Jane"
    assert values["languages"] == ("fra",)
    assert values["publisher"] == "Pub Co"
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    # calibre labels the cover with its extension, dot and all; LiuXin names it by its bytes
    assert cover == CoverImage(COVER, "png")


def test_html_is_read_as_calibre_reads_it(tmp_path: Path) -> None:
    """
    Read an HTML page's meta tags, with metadata comments taking precedence.

    Example:
        >>> test_html_is_read_as_calibre_reads_it(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the page.
    :return: None; the assertions are the check.
    """
    page = tmp_path / "book.html"
    page.write_text(HTML, encoding="utf-8")

    values, cover = read_book("Read HTML metadata", page)

    assert values["title"] == "HTML Test"
    assert values["authors"] == ("Comment Author",)
    assert values["publisher"] == "Pub Co"
    assert values["languages"] == ("fr",)
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    assert values["tags"] == ("Fiction", "Drama")
    assert values["identifiers"] == {"isbn": "9780000000002"}
    assert values["pubdate"].startswith("2001-02-03")
    assert cover is None


@pytest.mark.parametrize("name", ["book.fb2", "book.fbz"])
def test_fb2_and_fbz_are_read_with_their_cover(tmp_path: Path, name: str) -> None:
    """
    Read an FB2 book, plain or zipped, with the cover held in it.

    Example:
        >>> test_fb2_and_fbz_are_read_with_their_cover(Path("/tmp"), "book.fbz")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param name: The book's file name.
    :return: None; the assertions are the check.
    """
    text = FB2.format(doctype="", annotation="A blurb.", cover=base64.b64encode(COVER).decode())
    book = tmp_path / name
    if name.endswith(".fbz"):
        write_zip(book, {"book.fb2": text})
    else:
        book.write_text(text, encoding="utf-8")

    values, cover = read_book("Read FB2 metadata", book)

    assert values["title"] == "FB2 Test"
    assert values["authors"] == ("Jane Doe",)
    assert values["publisher"] == "Pub Co"
    assert values["tags"] == ("sf",)
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    assert values["identifiers"] == {"isbn": "978-0-00-000000-2"}
    assert values["comments"].strip() == "A blurb."
    assert cover == CoverImage(COVER, "png")


def test_odt_is_read_with_its_named_cover(tmp_path: Path) -> None:
    """
    Read an ODT document's metadata, calibre's OPF fields and the frame named as its cover.

    Example:
        >>> test_odt_is_read_with_its_named_cover(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the document.
    :return: None; the assertions are the check.
    """
    values, cover = read_book("Read ODT metadata", make_odt(tmp_path / "book.odt"))

    assert values["title"] == "ODT Test"
    assert values["authors"] == ("Jane Doe",)
    assert values["languages"] == ("fra",)
    assert values["tags"] == ("Fiction", "Drama")
    assert values["publisher"] == "Pub Co"
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    assert values["rating"] == 8.0
    assert values["identifiers"] == {"isbn": "9780000000002"}
    assert cover == CoverImage(COVER, "png")


def test_odt_takes_a_first_image_of_cover_shape(tmp_path: Path) -> None:
    """
    Take a document's first image as its cover only when it is shaped and sized like one.

    Example:
        >>> test_odt_takes_a_first_image_of_cover_shape(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the documents.
    :return: None; the assertions are the check.
    """
    portrait = make_odt(tmp_path / "portrait.odt", user_defined="", frame="Image1")
    small = make_odt(tmp_path / "small.odt", user_defined="", frame="Image1", image=png(10, 10))

    assert read_book("Read ODT metadata", portrait)[1] == CoverImage(COVER, "png")
    assert read_book("Read ODT metadata", small)[1] is None


def test_entities_are_neither_fetched_nor_expanded(tmp_path: Path) -> None:
    """
    Leave external and nested entities alone in FB2 books and ODT documents.

    Example:
        >>> test_entities_are_neither_fetched_nor_expanded(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the books and a secret file.
    :return: None; the assertions are the check.
    """
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET", encoding="utf-8")
    external = f'<!DOCTYPE x [<!ENTITY s SYSTEM "file://{secret.as_posix()}">]>'
    books = []
    for label, doctype, entity in (("external", external, "&s;"), ("nested", LAUGHS, "&a9;")):
        fb2_book = tmp_path / f"{label}.fb2"
        fb2_book.write_text(FB2.format(doctype=doctype, annotation=entity, cover=""), encoding="utf-8")
        books.append(("Read FB2 metadata", fb2_book, "FB2 Test"))
        odt_book = make_odt(tmp_path / f"{label}.odt", doctype=doctype, description=entity, text=entity)
        books.append(("Read ODT metadata", odt_book, "ODT Test"))

    for plugin, book, title in books:
        started = time.monotonic()
        values, _ = read_book(plugin, book)
        assert time.monotonic() - started < 5
        assert values["title"] == title
        assert "TOP SECRET" not in repr(values)
        assert "lollollol" not in repr(values)


@pytest.mark.parametrize(
    ("plugin", "name"),
    [("Read HTMLZ metadata", "bomb.htmlz"), ("Read FB2 metadata", "bomb.fbz"), ("Read ODT metadata", "bomb.odt")],
)
def test_archives_that_expand_enormously_are_refused(tmp_path: Path, plugin: str, name: str) -> None:
    """
    Refuse, before calibre's code reads it, an archive whose member expands enormously.

    Example:
        >>> test_archives_that_expand_enormously_are_refused(Path("/tmp"), "Read FB2 metadata", "b.fbz")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param plugin: The calibre plugin's name.
    :param name: The book's file name.
    :return: None; the assertions are the check.
    """
    book = write_zip(tmp_path / name, {"metadata.opf": PACKAGE, "book.fb2": b" " * (64 * 1024 * 1024)})
    assert book.stat().st_size < 1024 * 1024

    with pytest.raises(ArchivePreflightError, match="compression ratio"):
        read_book(plugin, book)


def test_long_html_comments_are_read_in_linear_time(tmp_path: Path) -> None:
    """
    Read a page whose comments run on without spaces quickly.

    calibre's comment pattern backtracks over the whole run from every
    character, so these 150,000-character comments, the most calibre reads,
    took minutes; LiuXin's fix tries it only where a run starts.

    Example:
        >>> test_long_html_comments_are_read_in_linear_time(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the pages.
    :return: None; the assertions are the check.
    """
    for label, comment in (("run", "a" * 149_000), ("equals", "a=" * 74_500)):
        page = tmp_path / f"{label}.html"
        page.write_text(f"<html><head><!-- {comment} TITLE=\"Found\" --></head></html>", encoding="utf-8")
        started = time.monotonic()
        values, _ = read_book("Read HTML metadata", page)
        assert time.monotonic() - started < 5, label
        assert values["title"] == "Found"


def test_comment_matching_finds_what_calibres_pattern_finds() -> None:
    """
    Find exactly the matches ``finditer`` finds, on random comments.

    Example:
        >>> test_comment_matching_finds_what_calibres_pattern_finds()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    pattern = re.compile(rf"""(?P<name>\S+)\s*=\s*{html.attr_pat}""")
    chooser = random.Random(758)
    alphabet = ["a", "T", "=", "'", '"', " ", "\t", "\n", "\xa0", "　", "&"]
    for _ in range(20_000):
        text = "".join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 40)))
        expected = [(m.span(), m.groupdict()) for m in pattern.finditer(text)]
        found = [(m.span(), m.groupdict()) for m in html._comment_matches(pattern, text)]
        assert found == expected, text


def test_the_html_fix_is_in_the_port() -> None:
    """
    Keep the ported HTML reader's fix, marked as LiuXin's.

    Example:
        >>> test_the_html_fix_is_in_the_port()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    ported = Path(html.__file__).read_text(encoding="utf-8")
    assert "for match in _comment_matches(_handle_comment_pat, data):  # LiuXin: linear time" in ported
    assert "# LiuXin fixes are marked '# LiuXin:'." in ported
