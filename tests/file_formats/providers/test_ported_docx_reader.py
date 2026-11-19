"""
Check the DOCX reader ported from upstream calibre.

calibre's DOCX reader takes the core and app properties and the default
language from the package, and its cover from the first image in a drawing that
is portrait and large enough. The provider reads both, after LiuXin's archive
limits. One LiuXin fix is checked here: calibre's cover XPath slows down faster
than the drawings squared and the image is read again for each drawing (64,000
drawings naming a 1 MB image took 16 s); the port finds the images in one pass
with ``linear_time.first_images`` and reads each once.

The DOCX writer's container (``xml2str``, ``update_doc_props``) and the
namespace table (all but ``generate_anchor``) are ported in part.
"""

from __future__ import annotations

import importlib.util
import io
import logging
import struct
import sys
import time
import unicodedata
import zipfile
import zlib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from LiuXin_alpha.file_formats.archive_preflight import ArchivePreflightError
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import CoverImage
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import (
    docx,
    docx_container,
    docx_names,
    docx_writer_container,
)
from tests.support.file_format_docx import build_unicode_docx

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE = "http://schemas.openxmlformats.org/package/2006/relationships"


def png(width: int, height: int, padding: int = 0) -> bytes:
    """
    Make a PNG whose header gives a size, padded with a text chunk.

    Example:
        >>> png(1, 1)[:8]
        b'\\x89PNG\\r\\n\\x1a\\n'


    :param width: The width the header gives.
    :param height: The height the header gives.
    :param padding: Bytes of text chunk, to make the file larger.
    :return: The PNG's bytes.
    """

    def chunk(kind: bytes, data: bytes) -> bytes:
        """
        Frame one PNG chunk.

        Example:
            See ``png``.


        :param kind: The chunk type.
        :param data: Its data.
        :return: The chunk.
        """
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"tEXt", b"pad\0" + bytes(padding)) + chunk(b"IEND", b"")


def drawings_book(drawings: list[str], images: dict[str, bytes]) -> bytes:
    """
    Make a DOCX whose body holds one drawing per entry, each naming an image by relationship ID.

    Example:
        >>> drawings_book(["rId1"], {"rId1": png(1, 1)})[:2]
        b'PK'


    :param drawings: The relationship ID each drawing's ``a:blip`` names, in order.
    :param images: The image for each relationship ID.
    :return: The book's bytes.
    """
    body = "".join(f'<w:r><w:drawing><a:blip r:embed="{rid}"/></w:drawing></w:r>' for rid in drawings)
    links = "".join(f'<Relationship Id="{rid}" Type="{R}/image" Target="media/{rid}.png"/>' for rid in images)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.'
            'wordprocessingml.document.main+xml"/></Types>',
        )
        archive.writestr(
            "_rels/.rels",
            f'<Relationships xmlns="{PACKAGE}"><Relationship Id="rId1" Type="{R}/officeDocument" '
            'Target="word/document.xml"/></Relationships>',
        )
        archive.writestr("word/_rels/document.xml.rels", f'<Relationships xmlns="{PACKAGE}">{links}</Relationships>')
        archive.writestr(
            "word/document.xml",
            f'<w:document xmlns:w="{W}" xmlns:a="{A}" xmlns:r="{R}"><w:body><w:p>{body}</w:p></w:body></w:document>',
        )
        for rid, data in images.items():
            archive.writestr(f"word/media/{rid}.png", data, compress_type=zipfile.ZIP_STORED)
    return out.getvalue()


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
    Read a book with the ported DOCX reader's provider.

    Example:
        >>> read_book(Path("/tmp/b.docx"))  # doctest: +SKIP


    :param book: The file.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader("Read DOCX metadata")()
    work = ProviderContext(
        work_dir=book.parent, options={}, log=logging.getLogger("tests.ported-docx"), report=ConversionReport()
    )
    fields = provider.read_calibre_metadata(book, "docx", work)
    return dict(fields.values), provider.extract_cover(book, "docx", work)


def test_docx_books_give_their_properties(tmp_path: Path) -> None:
    """
    Read the title, authors, publisher, tags, comments and language from the package.

    Example:
        >>> test_docx_books_give_their_properties(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.docx"
    build_unicode_docx(book)
    fields, cover = read_book(book)
    assert fields["title"] == "DOCX Καλημέρα 世界"
    assert list(fields["authors"]) == ["José", "Иван"]
    assert fields["publisher"] == "Éditions Δ"
    tags = [unicodedata.normalize("NFC", tag) for tag in fields["tags"]]  # the fixture spells café decomposed
    assert tags == ["containers_ unicode", "Κατηγορία", "タグ", "café"]
    assert fields["comments"].startswith("DOCX container description")
    assert list(fields["languages"]) == ["eng"]
    assert cover is None  # its only image is 16 by 16, too small for a cover


def test_the_cover_is_the_first_portrait_image_large_enough(tmp_path: Path) -> None:
    """
    Pass over a wide image and take the portrait one after it, as calibre does.

    Example:
        >>> test_the_cover_is_the_first_portrait_image_large_enough(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    cover = png(400, 500)
    book = tmp_path / "book.docx"
    book.write_bytes(drawings_book(["rIdWide", "rIdCover"], {"rIdWide": png(4000, 10), "rIdCover": cover}))
    _, found = read_book(book)
    assert found is not None and found.data == cover and found.image_format == "png"


def test_a_million_drawings_of_one_image_are_read_quickly(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Search 200,000 drawings that all name one wide 1 MB image, reading it once.

    calibre took 16 s for 64,000 drawings, and its time grows faster than their square.

    Example:
        >>> test_a_million_drawings_of_one_image_are_read_quickly(pytest.MonkeyPatch())  # doctest: +SKIP


    :param monkeypatch: Counts the reads.
    :return: None; the assertions are the check.
    """
    reads: list[str] = []
    original = docx_container.DOCX.read

    def counting(self: docx_container.DOCX, name: str) -> bytes:
        """
        Count a read and do it.

        Example:
            See the test.


        :param self: The container.
        :param name: The member.
        :return: Its bytes.
        """
        reads.append(name)
        return original(self, name)

    monkeypatch.setattr(docx_container.DOCX, "read", counting)
    data = drawings_book(["rId9"] * 200_000, {"rId9": png(4000, 10, 1_000_000)})
    started = time.monotonic()
    mi = docx.get_metadata(io.BytesIO(data))
    assert time.monotonic() - started < 10
    assert mi.cover_data == (None, None)
    assert reads.count("word/media/rId9.png") == 1


def test_zip_bombs_are_refused_before_calibre_reads_them(tmp_path: Path) -> None:
    """
    Apply LiuXin's archive limits to DOCX, as to the other ZIP formats.

    Example:
        >>> test_zip_bombs_are_refused_before_calibre_reads_them(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "bomb.docx"
    with zipfile.ZipFile(book, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", bytes(8 * 1024 * 1024))
    with pytest.raises(ArchivePreflightError, match="compression ratio"):
        read_book(book)


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


def test_the_docx_fix_is_in_the_port_and_guarded() -> None:
    """
    Keep the ported fix, and stop a re-port whose calibre code has moved.

    Example:
        >>> test_the_docx_fix_is_in_the_port_and_guarded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    ported = Path(docx.__file__).read_text(encoding="utf-8")
    assert "_linear_time.first_images(doc, docx.namespace.namespaces['r'])" in ported
    assert "rid_map[rid] not in tried" in ported
    with pytest.raises(SystemExit, match="no longer matches"):
        load_porter().apply_patches("x = 1\n", "ebooks/metadata/docx.py")


def test_partial_ports_keep_only_what_the_reader_uses() -> None:
    """
    Port the namespace table without its anchor generator, and the writer's two helpers alone.

    Example:
        >>> test_partial_ports_keep_only_what_the_reader_uses()


    :return: None; the assertions are the check.
    """
    assert not hasattr(docx_names, "generate_anchor")
    assert docx_names.DOCXNamespace().namespaces["r"] == R
    assert {"xml2str", "update_doc_props"} <= set(vars(docx_writer_container))
    assert not hasattr(docx_writer_container, "DOCX")
