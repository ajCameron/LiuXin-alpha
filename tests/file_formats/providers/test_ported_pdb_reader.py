"""
Check the PDB-family (eReader, Plucker, Haodoo) reader ported from upstream calibre.

calibre's PDB reader looks at the book's identity and hands eReader, Plucker
and Haodoo books to their own readers; any other PDB book gives the title in
its header. The provider reads each kind, keeps the eReader cover as the book
stores it, and refuses a section table whose offsets go back and forth, which
made calibre's Haodoo reader keep the whole file once per section.

Three of the modules are ported in part, with the porting script's
``PARTS``: only the names the metadata readers use, and the imports those use.
"""

from __future__ import annotations

import importlib.util
import io
import logging
import resource
import struct
import sys
import time
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
    pdb,
    pdb_ereader_reader132,
    pdb_header,
    pdb_plucker_reader,
    txt_processor,
)
from tests.support.file_format_mobi import build_palmdb

PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
)


def ereader_record0(*, image_offset: int = 2, image_count: int = 1, metadata_offset: int = 3) -> bytes:
    """
    Make a Dropbook eReader header record, 132 bytes, with metadata and one image.

    Example:
        >>> len(ereader_record0())
        132


    :param image_offset: The section of the first image.
    :param image_count: How many images.
    :param metadata_offset: The section of the metadata.
    :return: The record's bytes.
    """
    raw = bytearray(132)
    struct.pack_into(">H", raw, 0, 2)  # PalmDOC compression
    struct.pack_into(">H", raw, 12, 1)  # the first non-text section
    struct.pack_into(">H", raw, 20, image_count)
    struct.pack_into(">H", raw, 24, 1)  # has metadata
    struct.pack_into(">H", raw, 40, image_offset)
    struct.pack_into(">H", raw, 44, metadata_offset)
    struct.pack_into(">H", raw, 52, metadata_offset + 1)
    return bytes(raw)


def ereader_book(ident: bytes = b"PNRdPPrs") -> bytes:
    """
    Make an eReader book with a metadata record and a cover image.

    Example:
        >>> ereader_book()[60:68]
        b'PNRdPPrs'


    :param ident: The book's identity, ``PNRdPPrs`` or ``PNPdPPrs``.
    :return: The book file's bytes.
    """
    image = b"PNG " + b"cover.png".ljust(32, b"\0") + b"\0" * 26 + PNG
    metadata = b"\0".join([b"eReader Title", b"Jane Doe", b"", b"Publisher", b"9780306406157"]) + b"\0"
    return build_palmdb([ereader_record0(), b"text", image, metadata], name="Header Title", ident=ident)


def plucker_record(kind: int, argument: bytes) -> bytes:
    """
    Make one Plucker metadata record: type, length in 16-bit words, argument.

    Example:
        >>> plucker_record(5, b"abc")
        b'\\x00\\x05\\x00\\x02abc\\x00'


    :param kind: The record type.
    :param argument: Its argument, padded to an even length.
    :return: The record's bytes.
    """
    if len(argument) % 2:
        argument += b"\0"
    return struct.pack(">HH", kind, len(argument) // 2) + argument


def plucker_book() -> bytes:
    """
    Make a Plucker book whose metadata section names a charset, authors, a title and a date.

    Example:
        >>> plucker_book()[60:68]
        b'DataPlkr'


    :return: The book file's bytes.
    """
    records = [
        plucker_record(1, struct.pack(">H", 106)),  # UTF-8
        plucker_record(4, "Ann Æsir,Bob Doe".encode()),
        plucker_record(5, b"Plucker Title"),
        plucker_record(6, struct.pack(">I", 1_000_000_000)),
    ]
    body = struct.pack(">H", len(records)) + b"".join(records)
    section = struct.pack(">HHHBB", 1, 0, len(body), 10, 0) + body
    return build_palmdb([struct.pack(">HHH", 1, 2, 0), section], name="Header Title", ident=b"DataPlkr")


def haodoo_book(*, unicode: bool, author: str | None = None) -> bytes:
    """
    Make a Haodoo book: cp950 (``BOOKMTIT``) or UTF-16 (``BOOKMTIU``).

    Example:
        >>> haodoo_book(unicode=True)[60:68]
        b'BOOKMTIU'


    :param unicode: Whether the text is UTF-16 rather than cp950.
    :param author: An author, stored in the PDB name, in the book's encoding, as Haodoo version 2 does.
    :return: The book file's bytes.
    """
    title, chapter = "書名", "第一章"
    if unicode:
        record0 = title.encode("utf_16_le") + b"\x1b\x001\x1b\x00" + chapter.encode("utf_16_le")
        ident, text = b"BOOKMTIU", chapter.encode("utf_16_le")
    else:
        record0 = title.encode("cp950") + b"\x1b1\x1b" + chapter.encode("cp950")
        ident, text = b"BOOKMTIT", chapter.encode("cp950")
    name = (author or "Header Title").encode("utf_16_le" if unicode else "cp950")
    data = bytearray(build_palmdb([record0, text], name=name, ident=ident))
    if author is not None:
        data[35] = 2  # the version byte: the name holds the author
    return bytes(data)


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
        work_dir=work_dir, options={}, log=logging.getLogger("tests.ported-pdb"), report=ConversionReport()
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
    Read a book with the ported PDB reader's provider.

    Example:
        >>> read_book(Path("/tmp/b.pdb"))  # doctest: +SKIP


    :param book: The file; its extension is its format.
    :return: The fields, in calibre's names and shapes, and the cover.
    """
    provider = adapt_ported_reader("Read PDB metadata")()
    book_format = book.suffix[1:]
    work = context(book.parent)
    fields = provider.read_calibre_metadata(book, book_format, work)
    return dict(fields.values), provider.extract_cover(book, book_format, work)


@pytest.mark.parametrize("ident", [b"PNRdPPrs", b"PNPdPPrs"])
def test_ereader_books_give_their_metadata_record_and_cover(tmp_path: Path, ident: bytes) -> None:
    """
    Read an eReader book's title, author, publisher, ISBN and cover.

    Example:
        >>> test_ereader_books_give_their_metadata_record_and_cover(Path("/tmp"), b"PNRdPPrs")  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :param ident: The book's identity.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.pdb"
    book.write_bytes(ereader_book(ident))
    fields, cover = read_book(book)
    assert fields["title"] == "eReader Title"
    assert list(fields["authors"]) == ["Jane Doe"]
    assert fields["publisher"] == "Publisher"
    assert dict(fields["identifiers"]) == {"isbn": "9780306406157"}
    assert cover is not None and cover.data == PNG and cover.image_format == "png"


def test_plucker_books_give_their_metadata_section(tmp_path: Path) -> None:
    """
    Read a Plucker book's title, authors and date in the charset its metadata names.

    Lengths in Plucker metadata count the argument's 16-bit words, as the
    Plucker format describes and calibre reads them.

    Example:
        >>> test_plucker_books_give_their_metadata_section(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.pdb"
    book.write_bytes(plucker_book())
    fields, cover = read_book(book)
    assert fields["title"] == "Plucker Title"
    assert list(fields["authors"]) == ["Ann Æsir", "Bob Doe"]
    assert str(fields["pubdate"]).startswith("2001-09-0")
    assert cover is None


@pytest.mark.parametrize(("unicode", "extension"), [(False, "pdb"), (True, "updb"), (True, "pdb")])
def test_haodoo_books_give_their_title_and_author(tmp_path: Path, unicode: bool, extension: str) -> None:
    """
    Read a Haodoo book's title from its first record and its author from the PDB name.

    Example:
        >>> test_haodoo_books_give_their_title_and_author(Path("/tmp"), False, "pdb")  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :param unicode: Whether the book is UTF-16.
    :param extension: The file's extension.
    :return: None; the assertions are the check.
    """
    book = tmp_path / f"book.{extension}"
    book.write_bytes(haodoo_book(unicode=unicode, author="金庸"))
    fields, _ = read_book(book)
    assert fields["title"] == "書名"
    assert list(fields["authors"]) == ["金庸"]
    assert list(fields["languages"]) == ["zh-tw"]
    assert pdb.get_metadata(io.BytesIO(haodoo_book(unicode=unicode))).authors == ["Unknown"]


def test_other_pdb_books_give_the_title_in_their_header(tmp_path: Path) -> None:
    """
    Read the header's title from a PDB book of another kind, as calibre does.

    Example:
        >>> test_other_pdb_books_give_the_title_in_their_header(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for the book.
    :return: None; the assertions are the check.
    """
    book = tmp_path / "book.pdb"
    book.write_bytes(build_palmdb([b"text"], name="A Palm Doc: Book!", ident=b"TEXtREAd"))
    fields, _ = read_book(book)
    assert fields == {"title": "A Palm Doc_ Book_"}


def back_and_forth(ident: bytes, record0: bytes, sections: int, size: int) -> bytes:
    """
    Make a book whose section offsets go backwards, so each section reads to the end of the file.

    Example:
        >>> len(back_and_forth(b"BOOKMTIT", b"x", 10, 1000)) > 1000
        True


    :param ident: The book's identity.
    :param record0: The first record.
    :param sections: How many sections the table names.
    :param size: The bytes after the first record.
    :return: The book file's bytes.
    """
    first = 78 + 8 * (sections + 1) + 2
    start = first + len(record0)
    offsets = [first] + [start + sections - i for i in range(1, sections)] + [start + sections + 1]
    records = [record0] + [b""] * (sections - 1) + [b"\0" * size]
    return build_palmdb(records, name="T", ident=ident, record_offsets=offsets)


@pytest.mark.parametrize(
    "ident",
    [b"BOOKMTIT", b"DataPlkr", b"PNRdPPrs"],
)
def test_sections_that_go_back_and_forth_are_refused(ident: bytes) -> None:
    """
    Refuse a table that names a megabyte four thousand times, which calibre's Haodoo reader kept.

    calibre kept about 2 GB for this book, and the table can name 65,535 sections.

    Example:
        >>> test_sections_that_go_back_and_forth_are_refused(b"BOOKMTIT")  # doctest: +SKIP


    :param ident: The book's identity.
    :return: None; the assertions are the check.
    """
    record0 = {
        b"BOOKMTIT": "書".encode("cp950") + b"\x1b1\x1b" + "章".encode("cp950"),
        b"DataPlkr": struct.pack(">HHH", 1, 2, 0),
        b"PNRdPPrs": ereader_record0(),
    }[ident]
    data = back_and_forth(ident, record0, 4_000, 1_000_000)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.monotonic()
    with pytest.raises(ValueError, match="PDB sections overlap"):
        pdb.get_metadata(io.BytesIO(data))
    assert time.monotonic() - started < 5
    assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before < 200 * 1024  # KiB


def test_sections_in_order_are_read_whole() -> None:
    """
    Read a damaged table whose last offset runs past the end, as calibre does.

    Example:
        >>> test_sections_in_order_are_read_whole()


    :return: None; the assertions are the check.
    """
    data = haodoo_book(unicode=False)
    header = pdb_header.PdbHeaderReader(io.BytesIO(data))
    sections = [header.section_data(i) for i in range(header.num_sections)]
    assert b"".join(sections) == data[78 + 8 * header.num_sections + 2 :]
    truncated = data[:-2]
    assert pdb.get_metadata(io.BytesIO(truncated)).title == "書名"


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


def test_the_pdb_fix_is_in_the_port_and_guarded() -> None:
    """
    Keep the ported header reader's fix, and stop a re-port whose calibre code has moved.

    Example:
        >>> test_the_pdb_fix_is_in_the_port_and_guarded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    ported = Path(pdb_header.__file__).read_text(encoding="utf-8")
    assert "self.check_sections()  # LiuXin:" in ported
    assert "raise ValueError('PDB sections overlap')" in ported
    with pytest.raises(SystemExit, match="no longer matches"):
        load_porter().apply_patches("class PdbHeaderReader: pass\n", "ebooks/pdb/header.py")


def test_partial_ports_keep_only_what_the_readers_use() -> None:
    """
    Port only the named parts of calibre's conversion readers, with the imports they use.

    Example:
        >>> test_partial_ports_keep_only_what_the_readers_use()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    assert not hasattr(pdb_ereader_reader132, "Reader132")
    assert not hasattr(pdb_plucker_reader, "Reader")
    assert pdb_plucker_reader.DATATYPE_METADATA == 10 and pdb_plucker_reader.MIBNUM_TO_NAME[106] == "utf-8"
    assert {name for name in vars(txt_processor) if not name.startswith("_")} >= {"HTML_TEMPLATE", "opf_writer"}
    assert not hasattr(txt_processor, "convert_markdown")
    assert "# Only HeaderRecord is ported, with the imports it uses;" in Path(
        pdb_ereader_reader132.__file__
    ).read_text(encoding="utf-8")

    porter = load_porter()
    source = "import os\nimport re\n\nA = 1\n\n\ndef f():\n    return os, A\n"
    assert porter.keep_only(source, "x.py", ("A",)) == "A = 1\n"
    with pytest.raises(SystemExit, match="keep A too"):
        porter.keep_only(source, "x.py", ("f",))
    with pytest.raises(SystemExit, match="defines no g"):
        porter.keep_only(source, "x.py", ("g",))
