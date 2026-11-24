"""
Check that LocalZipFile reads and rewrites archives whose central directory is damaged.

``LocalZipFile`` reads an archive from its local file headers, for when the central
directory is broken. Its ``safe_replace`` imported a module that no longer exists and
called ``dict.itervalues``, so writing metadata into such an EPUB always failed; and a
missing member raised ``TypeError`` instead of the documented ``ValueError``.

Example:
    Run these checks::

        python -m pytest -q tests/utils/decompression/test_localunzip.py
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata,
    set_file_type_metadata,
)
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.utils.decompression.localunzip import LocalZipFile
from tests.support.file_format_epub import build_unicode_epub


def _without_central_directory(data: bytes) -> bytes:
    """
    Cut an archive off where its central directory starts, leaving only the local entries.

    Example:
        >>> _without_central_directory(b"PK\\x03\\x04 data PK\\x01\\x02 index")
        b'PK\\x03\\x04 data '


    :param data: A ZIP archive.
    :return: The archive without its central directory.
    """
    return data[: data.index(b"PK\x01\x02")]


def _archive() -> bytes:
    """
    A small damaged archive: a stored member and two deflated ones, no central directory.

    Example:
        >>> zipfile.is_zipfile(io.BytesIO(_archive()))
        False


    :return: The damaged archive.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip", zipfile.ZIP_STORED)
        archive.writestr("a.txt", "AAA")
        archive.writestr("b.txt", "BBB" * 100)
    return _without_central_directory(buffer.getvalue())


def test_members_are_read_from_the_local_headers() -> None:
    """
    Each member is found and read; a missing one raises ``ValueError``.

    Example:
        >>> test_members_are_read_from_the_local_headers()


    :return: None after the members are checked.
    """
    archive = LocalZipFile(io.BytesIO(_archive()))
    assert list(archive.file_info) == ["mimetype", "a.txt", "b.txt"]
    assert archive.read("b.txt") == b"BBB" * 100
    with pytest.raises(ValueError, match="no file named: nope"):
        archive.read("nope")
    with pytest.raises(ValueError, match="no file named: nope"):
        archive.getinfo("nope")


def test_safe_replace_rewrites_a_whole_archive() -> None:
    """
    Members are replaced or added, the rest kept, and the result has a central directory.

    Example:
        >>> test_safe_replace_rewrites_a_whole_archive()


    :return: None after the rewritten archive is checked.
    """
    stream = io.BytesIO(_archive())
    LocalZipFile(stream).safe_replace(
        "a.txt",
        io.BytesIO(b"NEW"),
        extra_replacements={"c.txt": io.BytesIO(b"CCC")},
        add_missing=True,
    )
    with zipfile.ZipFile(io.BytesIO(stream.getvalue())) as archive:
        assert archive.testzip() is None
        assert {name: archive.read(name) for name in archive.namelist()} == {
            "mimetype": b"application/epub+zip",
            "a.txt": b"NEW",
            "b.txt": b"BBB" * 100,
            "c.txt": b"CCC",
        }


def test_metadata_is_written_into_an_epub_without_a_central_directory(
    tmp_path: Path,
) -> None:
    """
    The EPUB writer falls back to LocalZipFile for a damaged book, and the write succeeds.

    Example:
        >>> test_metadata_is_written_into_an_epub_without_a_central_directory(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :return: None after the rewritten book is read back.
    """
    book = build_unicode_epub(tmp_path / "book.epub").path.read_bytes()
    damaged = _without_central_directory(book)
    errors: list[str] = []
    stream = io.BytesIO(damaged)
    set_file_type_metadata(
        stream,
        calibreMetadata("Mended Title", ["Ann Writer"]),
        "epub",
        report_error=lambda _mi, _ftype, trace: errors.append(str(trace)),
    )
    assert not errors
    assert zipfile.is_zipfile(io.BytesIO(stream.getvalue()))
    md = get_file_type_metadata(io.BytesIO(stream.getvalue()), "epub")
    assert md.title == "Mended Title"
