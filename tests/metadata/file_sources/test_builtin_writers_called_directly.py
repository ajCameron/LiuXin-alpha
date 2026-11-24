"""
Check that LiuXin's built-in writer plugins behave the same called directly as under the runtime.

A writer plugin called directly on a stream used to rewrite it in place, so a writer
that failed part-way left the book damaged, and a format with one field for tags and
subjects lost the subjects. The plugins now work on a copy and fold the subjects
themselves.

Example:
    Run the checks::

        python -m pytest -q tests/metadata/file_sources/test_builtin_writers_called_directly.py
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pytest

import LiuXin_alpha.metadata.file_sources.builtin_writers as writers
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.file_sources import epub, rtf
from LiuXin_alpha.metadata.file_sources.subject_fields import subjects_of
from tests.support.file_format_epub import build_unicode_epub
from tests.support.subject_reading import subject_reading


def plugin(name: str) -> Any:
    """
    Make one of the built-in writer plugins by its class name.

    Example:
        >>> plugin("EPUBMetadataWriter").name  # doctest: +SKIP
        'Set EPUB metadata'


    :param name: The class name.
    :return: The plugin.
    """
    (cls,) = [cls for cls in writers.get_metadata_set_plugins() if cls.__name__ == name]
    return cls(None)


def record(title: str) -> Any:
    """
    Make a record with a tag and a subject heading.

    Example:
        >>> record("T").subjects
        ['Sea stories']


    :param title: The title.
    :return: The record.
    """
    mi = calibreMetadata(title, ["Ann Lee"])
    mi.tags = ["to-read"]
    mi.subjects = ["Sea stories"]
    return mi


def test_every_built_in_writer_is_wrapped() -> None:
    """
    Wrap the ``set_metadata`` of every built-in writer plugin.

    Example:
        >>> test_every_built_in_writer_is_wrapped()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    classes = writers.get_metadata_set_plugins()
    assert classes
    for cls in classes:
        assert issubclass(cls, writers.BuiltinMetadataWriter), cls.__name__
        assert hasattr(cls.set_metadata, "__wrapped__"), cls.__name__


def test_a_writer_that_fails_part_way_leaves_the_file_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Leave a book on disk as it was when its writer truncates it and then fails.

    Example:
        >>> test_a_writer_that_fails_part_way_leaves_the_file_as_it_was(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :param monkeypatch: Pytest fixture for the failing writer.
    :return: None; the assertions are the check.
    """
    book = build_unicode_epub(tmp_path / "book.epub").path
    before = book.read_bytes()

    def fails_part_way(stream: Any, mi: Any, apply_null: bool = False) -> None:
        """
        Truncate the book, write a little of it, and fail as a full disk would.

        Example:
            >>> fails_part_way(io.BytesIO(), None)  # doctest: +SKIP
            Traceback (most recent call last):
            OSError: No space left on device


        :param stream: The book.
        :param mi: The record, unused.
        :param apply_null: Unused, as the EPUB writer takes it.
        :return: None; it always raises.
        :raises OSError: Always.
        """
        stream.seek(0)
        stream.truncate()
        stream.write(b"PK")
        raise OSError("No space left on device")

    monkeypatch.setattr(writers, "epub_set_metadata", fails_part_way)
    with book.open("rb+") as stream, pytest.raises(OSError, match="No space"):
        plugin("EPUBMetadataWriter").set_metadata(stream, record("New"), "epub")
    assert book.read_bytes() == before


def test_a_writer_called_directly_on_a_file_writes_it_with_the_subjects_among_the_tags(
    tmp_path: Path,
) -> None:
    """
    Write a book on disk through a plugin, and give EPUB, with one field, the subjects among the tags.

    Example:
        >>> test_a_writer_called_directly_on_a_file_writes_it_with_the_subjects_among_the_tags(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :return: None; the assertions are the check.
    """
    book = build_unicode_epub(tmp_path / "book.epub").path
    given = record("New title")
    with book.open("rb+") as stream:
        plugin("EPUBMetadataWriter").set_metadata(stream, given, "epub")
        assert stream.tell() == 0
    written = epub.get_metadata(book, extract_cover=False)
    assert written.title == "New title"
    assert sorted(written.tags) == ["Sea stories", "to-read"]
    assert given.tags == ["to-read"] and given.subjects == ["Sea stories"]


def test_a_writer_for_a_format_with_a_subject_field_gets_the_subjects_as_they_are() -> None:
    """
    Give the RTF writer, whose ``\\subject`` holds subjects by default, the record unchanged.

    Example:
        >>> test_a_writer_for_a_format_with_a_subject_field_gets_the_subjects_as_they_are()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    with subject_reading("subjects"):
        stream = io.BytesIO(b"{\\rtf1\\ansi Hello}")
        plugin("RTFMetadataWriter").set_metadata(stream, record("New title"), "rtf")
        written = rtf.get_metadata(io.BytesIO(stream.getvalue()))
    assert subjects_of(written) == ["Sea stories"]
    assert list(written.tags) == ["to-read"]


def test_the_pdf_writer_protects_its_stream_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Give the PDF writer, which puts the original bytes back itself, the stream without a copy.

    Example:
        >>> test_the_pdf_writer_protects_its_stream_itself(None)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture for the stand-in writer.
    :return: None; the assertions are the check.
    """
    seen: list[Any] = []
    monkeypatch.setattr(writers, "pdf_set_metadata", lambda stream, mi: seen.append(stream))
    stream = io.BytesIO(b"%PDF-1.4")
    plugin("PDFMetadataWriter").set_metadata(stream, record("T"), "pdf")
    assert seen == [stream]


def test_an_appended_update_is_appended() -> None:
    """
    Append a result that only adds to the end of the book, leaving the bytes before it alone.

    Example:
        >>> test_an_appended_update_is_appended()


    :return: None; the assertions are the check.
    """
    writes: list[tuple[int, bytes]] = []

    class Watched(io.BytesIO):
        """
        A stream that records where each write starts.

        Example:
            >>> Watched(b"x").getvalue()  # doctest: +SKIP
            b'x'
        """

        def write(self, data: Any) -> int:
            """
            Record the write, then write.

            Example:
                >>> Watched().write(b"x")  # doctest: +SKIP
                1


            :param data: The bytes.
            :return: How many were written.
            """
            writes.append((self.tell(), bytes(data)))
            return super().write(data)

    stream = Watched(b"original")
    writers.write_through_copy(stream, lambda copy: (copy.seek(0, io.SEEK_END), copy.write(b"+update")))
    assert stream.getvalue() == b"original+update"
    assert writes == [(len(b"original"), b"+update")]
