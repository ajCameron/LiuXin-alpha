"""
Keep the EPUB reader's cover search to the book's own files.

A cover page names its image by a relative path, and a spine entry names the
first page the same way; neither may reach a file outside the book.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from LiuXin_alpha.metadata.file_sources import epub

CONTAINER = (
    '<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">'
    '<rootfiles><rootfile full-path="OEBPS/content.opf" '
    'media-type="application/oebps-package+xml"/></rootfiles></container>'
)
PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"


def svg_page(href: str) -> str:
    """
    Make an SVG cover page showing one image.

    Example:
        >>> "cover.png" in svg_page("cover.png")
        True


    :param href: The image's href.
    :return: The page.
    """
    return (
        '<html xmlns="http://www.w3.org/1999/xhtml"><body><svg xmlns="http://www.w3.org/2000/svg"'
        f' xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="{href}"/></svg></body></html>'
    )


def make_epub(path: Path, first_page: str, members: dict[str, bytes | str]) -> Path:
    """
    Write an EPUB naming no cover image, with one page in its spine.

    Example:
        >>> import tempfile
        >>> book = make_epub(Path(tempfile.mkdtemp()) / "b.epub", "p.xhtml", {})
        >>> zipfile.ZipFile(book).namelist()[0]
        'mimetype'


    :param path: Where to write it.
    :param first_page: The first page's href, relative to the package.
    :param members: Other files, by name in the archive.
    :return: The path.
    """
    opf = f"""<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier id="id">x</dc:identifier>
<dc:title>Cover Test</dc:title></metadata>
<manifest><item id="p" href="{first_page}" media-type="application/xhtml+xml"/></manifest>
<spine><itemref idref="p"/></spine></package>"""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        archive.writestr("META-INF/container.xml", CONTAINER)
        archive.writestr("OEBPS/content.opf", opf)
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def cover_of(book: Path) -> bytes | None:
    """
    Read a book's metadata and return the cover's bytes.

    Example:
        >>> cover_of(Path("/tmp/b.epub"))  # doctest: +SKIP


    :param book: The file.
    :return: The cover, or ``None``.
    """
    with book.open("rb") as stream:
        metadata = epub.get_metadata(stream, extract_cover=True, calibre_metadata=True)
    assert metadata.title == "Cover Test"
    cover_data = getattr(metadata, "cover_data", None)
    return cover_data[1] if cover_data else None


def test_an_svg_cover_page_inside_the_book_gives_its_image(tmp_path: Path) -> None:
    """
    Find the cover an SVG first page shows, from inside the book.

    Example:
        >>> test_an_svg_cover_page_inside_the_book_gives_its_image(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book.
    :return: None; the assertion is the check.
    """
    book = make_epub(
        tmp_path / "book.epub",
        "text/cover.xhtml",
        {"OEBPS/text/cover.xhtml": svg_page("../images/cover.png"), "OEBPS/images/cover.png": PNG},
    )

    assert cover_of(book) == PNG


def test_a_cover_page_cannot_show_an_image_outside_the_book(tmp_path: Path) -> None:
    """
    Ignore a cover page's image that lies outside the book, by relative or absolute path.

    Example:
        >>> test_a_cover_page_cannot_show_an_image_outside_the_book(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the books and an image outside them.
    :return: None; the assertions are the check.
    """
    private = tmp_path / "private.png"
    private.write_bytes(PNG)
    for name, href in (
        ("relative", "../" * 40 + private.as_posix().lstrip("/")),
        ("absolute", private.as_posix()),
    ):
        book = make_epub(tmp_path / f"{name}.epub", "cover.xhtml", {"OEBPS/cover.xhtml": svg_page(href)})
        assert cover_of(book) is None, name


def test_a_first_page_outside_the_book_is_not_its_cover_page(tmp_path: Path) -> None:
    """
    Ignore a spine entry that climbs out of the book.

    Example:
        >>> test_a_first_page_outside_the_book_is_not_its_cover_page(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the book and a page and image outside it.
    :return: None; the assertion is the check.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "private.png").write_bytes(PNG)
    page = outside / "page.xhtml"
    page.write_text(svg_page("private.png"), encoding="utf-8")
    book = make_epub(
        tmp_path / "book.epub", "../" * 40 + page.as_posix().lstrip("/"), {"OEBPS/text.xhtml": "<html/>"}
    )

    assert cover_of(book) is None
