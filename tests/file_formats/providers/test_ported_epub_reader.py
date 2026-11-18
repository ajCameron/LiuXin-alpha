"""
Check the EPUB reader ported from upstream calibre, and the helpers it brings.

calibre's own EPUB and damaged-ZIP tests run on the port. The provider is
checked on EPUB 2 and 3 books and a KEPUB, on each way calibre finds a cover,
on damaged and oversized archives, and on books whose cover pages point
outside the book.
"""

from __future__ import annotations

import importlib.util
import io
import logging
import os
import sys
import time
import unittest
import zipfile
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.archive_preflight import ArchivePreflightError
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import CoverImage
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import _compat, ebooks, epub, localunzip

CONTAINER = (
    '<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">'
    '<rootfiles><rootfile full-path="OEBPS/content.opf" '
    'media-type="application/oebps-package+xml"/></rootfiles></container>'
)

OPF2 = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Field Test</dc:title>
  <dc:creator opf:role="aut" opf:file-as="Doe, Jane">Jane Doe</dc:creator>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:language>fr</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:subject>Fiction</dc:subject>
  <dc:description>A blurb.</dc:description>
  <dc:date>2001-02-03</dc:date>
  <meta name="calibre:series" content="Saga"/>
  <meta name="calibre:series_index" content="2"/>
  <meta name="cover" content="cover"/>
 </metadata>
 <manifest>
  <item id="cover" href="images/cover.png" media-type="image/png"/>
  <item id="text" href="text.xhtml" media-type="application/xhtml+xml"/>
 </manifest>
 <spine><itemref idref="text"/></spine>
</package>
"""

OPF3 = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:title>Three Test</dc:title>
  <dc:creator id="c1">Jane Doe</dc:creator>
  <meta refines="#c1" property="role" scheme="marc:relators">aut</meta>
  <meta refines="#c1" property="file-as">Doe, Jane</meta>
  <dc:identifier id="id">urn:isbn:9780000000002</dc:identifier>
  <dc:language>de</dc:language>
  <meta property="belongs-to-collection" id="s">Saga</meta>
  <meta refines="#s" property="collection-type">series</meta>
  <meta refines="#s" property="group-position">3</meta>
  <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
 </metadata>
 <manifest>
  <item id="cover" href="images/cover.jpg" media-type="image/jpeg" properties="cover-image"/>
  <item id="text" href="text.xhtml" media-type="application/xhtml+xml"/>
 </manifest>
 <spine><itemref idref="text"/></spine>
</package>
"""

PNG = b"\x89PNG\r\n\x1a\n" + bytes(32)
JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01" + b"cover" * 8


def spine_only_opf(first_page: str) -> str:
    """
    Make an OPF 2 package naming no cover image, with one page in its spine.

    Example:
        >>> "text.xhtml" in spine_only_opf("text.xhtml")
        True


    :param first_page: The page's href, relative to the package.
    :return: The OPF.
    """
    return f"""<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier id="id">x</dc:identifier>
<dc:title>Spine Test</dc:title></metadata>
<manifest><item id="p" href="{first_page}" media-type="application/xhtml+xml"/></manifest>
<spine><itemref idref="p"/></spine></package>"""


def make_epub(path: Path, opf: str, members: dict[str, bytes | str] | None = None) -> Path:
    """
    Write an EPUB with the package at ``OEBPS/content.opf``.

    Example:
        >>> import tempfile
        >>> book = make_epub(Path(tempfile.mkdtemp()) / "b.epub", spine_only_opf("t.xhtml"))
        >>> zipfile.ZipFile(book).namelist()[0]
        'mimetype'


    :param path: Where to write it.
    :param opf: The package document.
    :param members: Other files, by name in the archive.
    :return: The path.
    """
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        archive.writestr("META-INF/container.xml", CONTAINER)
        archive.writestr("OEBPS/content.opf", opf)
        for name, data in (members or {}).items():
            archive.writestr(name, data)
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
        work_dir=work_dir,
        options={},
        log=logging.getLogger("tests.ported-epub"),
        report=ConversionReport(),
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


def read_book(book: Path, book_format: str = "epub") -> tuple[dict[str, Any], CoverImage | None]:
    """
    Read a book with the ported EPUB reader's provider.

    Example:
        >>> read_book(Path("/tmp/b.epub"))  # doctest: +SKIP


    :param book: The file.
    :param book_format: ``epub`` or ``kepub``.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader("Read EPUB metadata")()
    work = context(book.parent)
    fields = provider.read_calibre_metadata(book, book_format, work)
    return dict(fields.values), provider.extract_cover(book, book_format, work)


def has_beautiful_soup() -> bool:
    """
    Say whether Beautiful Soup, which calibre's HTML cover-page check needs, is installed.

    Example:
        >>> isinstance(has_beautiful_soup(), bool)
        True


    :return: Whether ``bs4`` can be imported.
    """
    return importlib.util.find_spec("bs4") is not None


@pytest.mark.parametrize("module", [epub, localunzip], ids=["epub", "localunzip"])
def test_calibres_own_tests_pass_on_the_port(module: Any) -> None:
    """
    Run calibre's EPUB metadata and damaged-ZIP tests against the ported code.

    calibre's check that cover-page images stay inside the book parses HTML
    with Beautiful Soup, which LiuXin does not require; without it that test
    is left out here and its SVG half is run by the next test.

    Example:
        >>> test_calibres_own_tests_pass_on_the_port(epub)  # doctest: +SKIP


    :param module: The ported module holding the tests.
    :return: None; the assertions are the check.
    """
    suite = module.find_tests()
    if not has_beautiful_soup():
        kept = unittest.TestSuite()
        for test in suite:
            if not test.id().endswith("test_html_cover_images_confined_to_root"):
                kept.addTest(test)
        suite = kept
    result = unittest.TestResult()
    suite.run(result)

    problems = [f"{test.id()}: {trace}" for test, trace in result.failures + result.errors]
    assert result.testsRun > 0
    assert not problems, "\n".join(problems)


def test_svg_cover_page_images_stay_inside_the_book(tmp_path: Path) -> None:
    """
    Take an SVG cover page's image only from inside the book, without Beautiful Soup.

    Example:
        >>> test_svg_cover_page_images_stay_inside_the_book(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book's files and an image outside it.
    :return: None; the assertions are the check.
    """
    root = tmp_path / "book"
    (root / "images").mkdir(parents=True)
    (root / "text").mkdir()
    (root / "images" / "cover.png").write_bytes(PNG)
    (tmp_path / "outside.png").write_bytes(PNG)

    def page(href: str) -> str:
        """
        Make an SVG cover page showing one image.

        Example:
            See the test.


        :param href: The image's href.
        :return: The page.
        """
        return (
            '<html xmlns="http://www.w3.org/1999/xhtml"><body><svg xmlns="http://www.w3.org/2000/svg"'
            f' xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="{href}"/></svg></body></html>'
        )

    text = str(root / "text")
    found = ebooks.extract_cover_from_embedded_svg(page("../images/cover.png"), text, _compat.default_log, root=str(root))
    assert found == PNG
    for outside in ("../../outside.png", (tmp_path / "outside.png").as_posix()):
        assert ebooks.extract_cover_from_embedded_svg(page(outside), text, _compat.default_log, root=str(root)) is None


def test_epub2_is_read_as_calibre_reads_it(tmp_path: Path) -> None:
    """
    Read an EPUB 2 book's fields and its PNG cover, named as a PNG.

    calibre labels every EPUB cover ``jpg``; the provider names it by its bytes.

    Example:
        >>> test_epub2_is_read_as_calibre_reads_it(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    book = make_epub(tmp_path / "book.epub", OPF2, {"OEBPS/images/cover.png": PNG, "OEBPS/text.xhtml": "<html/>"})

    values, cover = read_book(book)

    assert values["title"] == "Field Test"
    assert values["authors"] == ("Jane Doe",)
    assert values["author_sort"] == "Doe, Jane"
    assert values["identifiers"] == {"isbn": "9780000000002"}
    assert values["languages"] == ("fra",)
    assert values["publisher"] == "Pub Co"
    assert values["tags"] == ("Fiction",)
    assert values["comments"] == "A blurb."
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    assert values["pubdate"].startswith("2001-02-03")
    assert "timestamp" not in values
    assert cover == CoverImage(PNG, "png")


@pytest.mark.parametrize("book_format", ["epub", "kepub"])
def test_epub3_and_kepub_are_read(tmp_path: Path, book_format: str) -> None:
    """
    Read an EPUB 3 book, and the same book as a KEPUB, with its cover-image cover.

    Example:
        >>> test_epub3_and_kepub_are_read(Path("/tmp"), "kepub")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param book_format: The format to read it as.
    :return: None; the assertions are the check.
    """
    book = make_epub(
        tmp_path / f"book.{book_format}", OPF3, {"OEBPS/images/cover.jpg": JPEG, "OEBPS/text.xhtml": "<html/>"}
    )

    values, cover = read_book(book, book_format)

    assert values["title"] == "Three Test"
    assert values["author_sort"] == "Doe, Jane"
    assert values["languages"] == ("deu",)
    assert (values["series"], values["series_index"]) == ("Saga", 3.0)
    assert cover == CoverImage(JPEG, "jpeg")


def test_the_format_reaches_calibres_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Pass the format to calibre's ``get_metadata``, as calibre's plugin does.

    Example:
        >>> test_the_format_reaches_calibres_reader(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param monkeypatch: Replaces calibre's function with a recorder.
    :return: None; the assertion is the check.
    """
    book = make_epub(tmp_path / "book.kepub", OPF3)
    seen: list[str] = []
    real = epub.get_metadata

    def recording(stream: Any, extract_cover: bool = True, ftype: str = "epub") -> Any:
        """
        Note the format, then read.

        Example:
            See the test.


        :param stream: The book.
        :param extract_cover: Whether to look for a cover.
        :param ftype: The format.
        :return: calibre's answer.
        """
        seen.append(ftype)
        return real(stream, extract_cover, ftype)

    monkeypatch.setattr(epub, "get_metadata", recording)
    read_book(book, "kepub")

    assert seen == ["kepub"]


def test_first_page_covers_are_found(tmp_path: Path) -> None:
    """
    Find a cover from the first page when the book names no cover image.

    A JPEG as the first spine item is the cover, as is the only image of an
    SVG cover page. A page of text gives none: calibre would render it, and
    LiuXin does not port that renderer.

    Example:
        >>> test_first_page_covers_are_found(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the books.
    :return: None; the assertions are the check.
    """
    svg_page = (
        '<html xmlns="http://www.w3.org/1999/xhtml"><body><svg xmlns="http://www.w3.org/2000/svg"'
        ' xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="images/c.png"/></svg></body></html>'
    )
    jpeg_first = make_epub(tmp_path / "manga.epub", spine_only_opf("images/c.jpg"), {"OEBPS/images/c.jpg": JPEG})
    svg_first = make_epub(
        tmp_path / "svg.epub", spine_only_opf("cover.xhtml"), {"OEBPS/cover.xhtml": svg_page, "OEBPS/images/c.png": PNG}
    )
    text_first = make_epub(
        tmp_path / "text.epub", spine_only_opf("text.xhtml"), {"OEBPS/text.xhtml": "<html><body><p>Words</p></body></html>"}
    )

    assert read_book(jpeg_first)[1] == CoverImage(JPEG, "jpeg")
    assert read_book(svg_first)[1] == CoverImage(PNG, "png")
    values, cover = read_book(text_first)
    assert values["title"] == "Spine Test"
    assert cover is None


def test_a_first_page_outside_the_book_is_not_its_cover(tmp_path: Path) -> None:
    """
    Ignore a spine entry that climbs out of the book, rather than reading the file it names.

    This is the one LiuXin fix in the ported EPUB reader.

    Example:
        >>> test_a_first_page_outside_the_book_is_not_its_cover(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book and a file outside it.
    :return: None; the assertions are the check.
    """
    outside = tmp_path / "private.jpg"
    outside.write_bytes(JPEG)
    climb = "../" * 40 + outside.as_posix().lstrip("/")
    book = make_epub(tmp_path / "book.epub", spine_only_opf(climb), {"OEBPS/text.xhtml": "<html/>"})

    values, cover = read_book(book)

    assert values["title"] == "Spine Test"
    assert cover is None


def test_an_encrypted_cover_is_not_returned(tmp_path: Path) -> None:
    """
    Leave out a cover image that ``encryption.xml`` says is encrypted.

    Example:
        >>> test_an_encrypted_cover_is_not_returned(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    encryption = (
        '<encryption xmlns="urn:oasis:names:tc:opendocument:xmlns:container"'
        ' xmlns:enc="http://www.w3.org/2001/04/xmlenc#"><enc:EncryptedData>'
        '<enc:EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes128-cbc"/>'
        '<enc:CipherData><enc:CipherReference URI="OEBPS/images/cover.png"/></enc:CipherData>'
        "</enc:EncryptedData></encryption>"
    )
    book = make_epub(
        tmp_path / "book.epub",
        OPF2,
        {"OEBPS/images/cover.png": PNG, "OEBPS/text.xhtml": "<html/>", "META-INF/encryption.xml": encryption},
    )

    values, cover = read_book(book)

    assert values["title"] == "Field Test"
    assert cover is None


def test_a_book_without_a_central_directory_is_salvaged(tmp_path: Path) -> None:
    """
    Read a book whose ZIP central directory is missing, from its local headers.

    Example:
        >>> test_a_book_without_a_central_directory_is_salvaged(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        archive.writestr("META-INF/container.xml", CONTAINER)
        archive.writestr("OEBPS/content.opf", OPF2)
        archive.writestr("OEBPS/images/cover.png", PNG)
        end_of_entries = buffer.tell()
    book = tmp_path / "damaged.epub"
    book.write_bytes(buffer.getvalue()[:end_of_entries])

    values, cover = read_book(book)

    assert values["title"] == "Field Test"
    assert cover == CoverImage(PNG, "png")


@pytest.mark.parametrize(
    "member",
    ["OEBPS/content.opf", "OEBPS/padding.bin"],
    ids=["package-file", "unreferenced-member"],
)
def test_archives_that_expand_enormously_are_refused(tmp_path: Path, member: str) -> None:
    """
    Refuse, before calibre's code runs, a book that expands far beyond its size.

    calibre reads the package file whole, and extracts the whole book when it
    looks for a cover page, so either member would fill memory or disk.

    Example:
        >>> test_archives_that_expand_enormously_are_refused(Path("/tmp"), "OEBPS/padding.bin")  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :param member: The member that expands.
    :return: None; the assertions are the check.
    """
    padding = b" " * (64 * 1024 * 1024)
    opf = spine_only_opf("text.xhtml")
    members: dict[str, bytes | str] = {"OEBPS/text.xhtml": "<html/>"}
    if member == "OEBPS/content.opf":
        opf = opf.replace("<dc:title>", "<dc:title>" + padding.decode(), 1)
    else:
        members[member] = padding
    book = make_epub(tmp_path / "bomb.epub", opf, members)
    assert book.stat().st_size < 1024 * 1024

    started = time.monotonic()
    with pytest.raises(ArchivePreflightError, match="compression ratio"):
        read_book(book)
    assert time.monotonic() - started < 5


def test_a_large_ordinary_book_is_still_read(tmp_path: Path) -> None:
    """
    Read a book with a large but ordinary member: the limits are for bombs.

    Example:
        >>> test_a_large_ordinary_book_is_still_read(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertions are the check.
    """
    noise = os.urandom(4 * 1024 * 1024)
    book = make_epub(
        tmp_path / "big.epub", OPF2, {"OEBPS/images/cover.png": PNG, "OEBPS/audio.bin": noise, "OEBPS/text.xhtml": "<html/>"}
    )

    values, cover = read_book(book)

    assert values["title"] == "Field Test"
    assert cover is not None


def load_porter() -> Any:
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


def test_the_liuxin_fix_is_in_the_port_and_guarded() -> None:
    """
    Keep the ported EPUB reader's fix, and stop a re-port whose calibre code has moved.

    Example:
        >>> test_the_liuxin_fix_is_in_the_port_and_guarded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    porter = load_porter()
    ported = Path(epub.__file__).read_text(encoding="utf-8")
    assert "is_existing_subpath(cpage, tdir):  # LiuXin:" in ported
    assert "# LiuXin fixes are marked '# LiuXin:'." in ported

    with pytest.raises(SystemExit, match="no longer matches"):
        porter.apply_patches("def render_cover(): pass\n", "ebooks/metadata/epub.py")
    assert porter.apply_patches("x = 1\n", "utils/imghdr.py") == "x = 1\n"
