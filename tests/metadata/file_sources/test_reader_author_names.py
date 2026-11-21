"""
Check that an author's name reaches the reader as it was written, "Smith & Sons" included.

LiuXin's metadata record took the names given to its constructor one at a time, as
author strings, so each was split again on ``&``, `` and `` and `` with `` and
title-cased: "Smith & Sons" came back as two authors and "bell hooks" as "Bell Hooks".
Readers that built a record from a list of names (FB2, LRF and others), or assigned
names one by one (PDF), all lost them, though calibre read the same books correctly.
The RTF reader also split on ``&`` and `` and `` where calibre splits on commas only.

A format whose own rules split on ``&`` still does: an OPF 2 ``dc:creator`` (EPUB 2,
HTMLZ, TXTZ) is an author string to calibre too.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_reader_author_names.py
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.pdf.objects import read_metadata
from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata,
    set_file_type_metadata,
)
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.file_sources.rtf import get_metadata as rtf_metadata
from tests.support.file_format_docx import build_unicode_docx
from tests.support.file_format_epub import build_unicode_epub
from tests.support.file_format_fb2 import fb2_bytes
from tests.support.file_format_lrf import lrf_book

NAMES = ["Ann Writer", "Smith & Sons", "bell hooks"]
PDF = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata" / "plain.pdf"


def _values(raw: Any) -> list[str]:
    """
    List a field's values, whichever container holds them.

    Example:
        >>> _values({"a": 1}), _values("b"), _values(None)
        (['a'], ['b'], [])


    :param raw: The field.
    :return: Its values as text.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(value) for value in raw]


def _write(data: bytes, ftype: str, authors: list[str]) -> bytes:
    """
    Write a title and authors into a book through its writer plugin, as the application does.

    Example:
        >>> rb"{\\author A, B}" in _write(rb"{\\rtf1{\\info}}", "rtf", ["A", "B"])
        True


    :param data: The book.
    :param ftype: Its format.
    :param authors: The names to write.
    :return: The rewritten book.
    """
    errors: list[str] = []
    stream = io.BytesIO(data)
    set_file_type_metadata(
        stream,
        calibreMetadata("Title", authors),
        ftype,
        report_error=lambda _mi, _ftype, trace: errors.append(str(trace)),
    )
    assert not errors
    return stream.getvalue()


def _docx(tmp_path: Path) -> bytes:
    """
    A DOCX to write into.

    Example:
        >>> _docx(Path("/tmp"))[:2]  # doctest: +SKIP
        b'PK'


    :param tmp_path: A folder to build in.
    :return: The book.
    """
    return build_unicode_docx(tmp_path / "book.docx").path.read_bytes()


def _rtf(_tmp_path: Path) -> bytes:
    """
    An RTF to write into.

    Example:
        >>> _rtf(Path("/tmp")).startswith(rb"{\\rtf1")
        True


    :param _tmp_path: Unused.
    :return: The book.
    """
    return rb"{\rtf1\ansi{\info{\title Old}{\author Someone}}Hello.}"


BOOKS: dict[str, tuple[Callable[[Path], bytes], str]] = {
    "docx": (_docx, "docx"),
    "fb2": (lambda _tmp_path: fb2_bytes(), "fb2"),
    "lrf": (lambda _tmp_path: lrf_book(), "lrf"),
    "rtf": (_rtf, "rtf"),
}


@pytest.mark.parametrize("ftype", sorted(BOOKS))
def test_names_come_back_whole_from_both_readers(ftype: str, tmp_path: Path) -> None:
    """
    Names written into a book read back unchanged in LiuXin's reader, as in calibre's.

    Example:
        >>> test_names_come_back_whole_from_both_readers("rtf", Path("/tmp"))  # doctest: +SKIP


    :param ftype: The format.
    :param tmp_path: A folder to build in.
    :return: None after both readings are checked.
    """
    build, ported = BOOKS[ftype]
    out = _write(build(tmp_path), ftype, NAMES)
    ours = get_file_type_metadata(io.BytesIO(out), ftype)
    calibre = ported_reader(ported)(io.BytesIO(out))
    assert _values(ours.authors) == calibre.authors == NAMES


def test_pdf_names_come_back_whole() -> None:
    """
    The PDF Info author string escapes the ampersand, and LiuXin's reader reads each name.

    Example:
        >>> test_pdf_names_come_back_whole()


    :return: None after the field and the reading are checked.
    """
    out = _write(PDF.read_bytes(), "pdf", NAMES)
    assert (
        read_metadata(out).info["Author"] == "Ann Writer & Smith && Sons & bell hooks"
    )
    assert _values(get_file_type_metadata(io.BytesIO(out), "pdf").authors) == NAMES


def test_an_opf2_creator_holding_an_ampersand_is_split_as_calibre_splits_it(
    tmp_path: Path,
) -> None:
    """
    In OPF 2 each ``dc:creator`` is an author string to calibre, so both readers split it.

    calibre writes each name into its own ``dc:creator`` without escaping, so it loses
    "Smith & Sons" in an EPUB 2 the same way; LiuXin follows it rather than read EPUBs
    differently.

    Example:
        >>> test_an_opf2_creator_holding_an_ampersand_is_split_as_calibre_splits_it(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :return: None after both readings are checked.
    """
    book = build_unicode_epub(tmp_path / "book.epub").path.read_bytes()
    out = _write(book, "epub", NAMES)
    ours = get_file_type_metadata(io.BytesIO(out), "epub")
    calibre = ported_reader("epub")(io.BytesIO(out))
    split = ["Ann Writer", "Smith", "Sons", "bell hooks"]
    assert _values(ours.authors) == calibre.authors == split


def test_an_opf3_creator_is_one_name(tmp_path: Path) -> None:
    """
    OPF 3 readers take each ``dc:creator`` as one name, in calibre as in LiuXin.

    Example:
        >>> test_an_opf3_creator_is_one_name(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :return: None after both readings are checked.
    """
    path = build_unicode_epub(tmp_path / "book.epub").path
    with zipfile.ZipFile(path) as source:
        members = {name: source.read(name) for name in source.namelist()}
    opf = next(name for name in members if name.endswith(".opf"))
    members[opf] = members[opf].replace(b'version="2.0"', b'version="3.0"', 1)
    book = io.BytesIO()
    with zipfile.ZipFile(book, "w") as target:
        for name, data in members.items():
            target.writestr(name, data)
    out = _write(book.getvalue(), "epub", NAMES)
    ours = get_file_type_metadata(io.BytesIO(out), "epub")
    calibre = ported_reader("epub")(io.BytesIO(out))
    assert _values(ours.authors) == calibre.authors == NAMES


@pytest.mark.parametrize(
    ("field", "names", "calibre_too"),
    [
        ("Ann Writer, Smith & Sons", ["Ann Writer", "Smith & Sons"], True),
        ("Smith & Sons", ["Smith & Sons"], True),
        ("Jos\\'e9 and Ana\\'efs", ["José and Anaïs"], True),
        # How LiuXin's writer joined authors before it used commas; calibre reads one name.
        ("Ann Writer& Bob Writer", ["Ann Writer", "Bob Writer"], False),
    ],
)
def test_rtf_authors_are_split_on_commas_as_calibre_splits_them(
    field: str, names: list[str], calibre_too: bool
) -> None:
    """
    The RTF ``\\author`` field is split on commas, and on LiuXin's old separator only.

    Example:
        >>> test_rtf_authors_are_split_on_commas_as_calibre_splits_them("A, B", ["A", "B"], True)


    :param field: The field as the file holds it.
    :param names: The names LiuXin's reader should find.
    :param calibre_too: Whether calibre's reader finds the same.
    :return: None after the readings are checked.
    """
    data = (
        b"{\\rtf1\\ansi\\ansicpg1252{\\info{\\title T}{\\author "
        + field.encode()
        + b"}}Hi.}"
    )
    assert _values(rtf_metadata(io.BytesIO(data)).authors) == names
    if calibre_too:
        assert ported_reader("rtf")(io.BytesIO(data)).authors == names


def test_the_constructor_keeps_listed_names_as_given() -> None:
    """
    A list of names is kept name for name; only surrounding space and empty names go.

    Example:
        >>> test_the_constructor_keeps_listed_names_as_given()


    :return: None after the names are checked.
    """
    md = CalibreLikeLiuXinBookMetaData(
        "T", ["Smith & Sons", "bell hooks", "Ann with Bob", " van Gogh ", "", None]
    )
    assert list(md.authors) == [
        "Smith & Sons",
        "bell hooks",
        "Ann with Bob",
        "van Gogh",
    ]
    with pytest.raises(NotImplementedError):
        CalibreLikeLiuXinBookMetaData("T", ["Ann", 3])  # type: ignore[list-item]


def test_the_constructor_parses_an_author_string_once() -> None:
    """
    A string is an author string: ``&`` separates names and ``&&`` stays in one.

    Example:
        >>> test_the_constructor_parses_an_author_string_once()


    :return: None after the names are checked.
    """
    md = CalibreLikeLiuXinBookMetaData("T", "Ann Writer & Smith && Sons")
    assert list(md.authors) == ["Ann Writer", "Smith & Sons"]
