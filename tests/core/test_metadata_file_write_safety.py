"""
Check that writing metadata into a book file changes only what was asked, and never half-writes the file.

The write service once filled the fields a caller left out with calibre's defaults,
so writing just a title replaced the book's real authors with "Unknown" in eight of
LiuXin's ten writers. It also rewrote a path in place, so a writer failure or a full
disk part-way left the book damaged. These tests run real writers on books built here.

Example:
    Run these checks::

        python -m pytest -q tests/core/test_metadata_file_write_safety.py
"""

from __future__ import annotations

import base64
import io
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services import metadata as service
from LiuXin_alpha.file_formats import plugin_runtime
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from tests.support.file_format_epub import build_unicode_epub
from tests.support.file_format_mobi import build_minimal_mobi

KNOWN = {
    "title": "Before",
    "authors": ["Ann Writer", "Bob Writer"],
    "publisher": "Old Press",
    "tags": ["alpha", "beta"],
}


def _write(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Call the write service as the program API does.

    Example:
        >>> _write({"path": "/books/a.epub", "metadata": {"title": "T"}})  # doctest: +SKIP


    :param payload: The command's payload.
    :return: The receipt.
    """
    return service.metadata_file_write(None, SimpleNamespace(payload=payload))  # type: ignore[arg-type]


def _read(data: bytes, module: str = "epub") -> Any:
    """
    Read a book with calibre's ported reader, independent of the writers under test.

    Example:
        >>> _read(b"")  # doctest: +SKIP


    :param data: The book.
    :param module: The ported reader.
    :return: calibre's metadata.
    """
    return ported_reader(module)(io.BytesIO(data))


@pytest.fixture
def book(tmp_path: Path) -> Path:
    """
    Make an EPUB in a folder of its own, holding known authors, publisher and tags.

    Example:
        >>> book(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: The test's folder.
    :return: The book's path.
    """
    folder = tmp_path / "books"
    folder.mkdir()
    path = folder / "book.epub"
    build_unicode_epub(path)
    _write({"path": str(path), "metadata": dict(KNOWN)})
    return path


def test_a_title_only_write_keeps_the_other_fields(book: Path) -> None:
    """
    Writing a title leaves the authors, publisher and tags as the book had them.

    Example:
        >>> test_a_title_only_write_keeps_the_other_fields(Path("book.epub"))  # doctest: +SKIP


    :param book: The book.
    :return: None after the fields are checked.
    """
    receipt = _write({"path": str(book), "metadata": {"title": "After"}})
    assert receipt["updated"] is True and receipt["content"] is None
    md = _read(book.read_bytes())
    assert md.title == "After"
    assert md.authors == KNOWN["authors"]
    assert md.publisher == KNOWN["publisher"]
    assert sorted(md.tags) == KNOWN["tags"]


def test_a_title_only_write_of_bytes_keeps_the_other_fields(book: Path) -> None:
    """
    The same holds when the book is sent as bytes and returned rewritten.

    Example:
        >>> test_a_title_only_write_of_bytes_keeps_the_other_fields(Path("book.epub"))  # doctest: +SKIP


    :param book: The book.
    :return: None after the fields are checked.
    """
    encoded = base64.b64encode(book.read_bytes()).decode("ascii")
    receipt = _write(
        {"base64": encoded, "file_type": "epub", "metadata": {"title": "After"}}
    )
    md = _read(receipt["content"])
    assert (md.title, md.authors, md.publisher) == (
        "After",
        KNOWN["authors"],
        KNOWN["publisher"],
    )


def test_named_fields_replace_only_themselves(book: Path) -> None:
    """
    A field the caller names replaces the book's; the others stay.

    Example:
        >>> test_named_fields_replace_only_themselves(Path("book.epub"))  # doctest: +SKIP


    :param book: The book.
    :return: None after the fields are checked.
    """
    _write({"path": str(book), "metadata": {"tags": ["gamma"]}})
    md = _read(book.read_bytes())
    assert md.tags == ["gamma"]
    assert (md.title, md.authors) == (KNOWN["title"], KNOWN["authors"])


def test_an_empty_author_list_still_means_unknown(book: Path) -> None:
    """
    Naming the authors as empty is a request for none, written as calibre writes it.

    Example:
        >>> test_an_empty_author_list_still_means_unknown(Path("book.epub"))  # doctest: +SKIP


    :param book: The book.
    :return: None after the authors are checked.
    """
    _write({"path": str(book), "metadata": {"authors": []}})
    assert _read(book.read_bytes()).authors == ["Unknown"]


def test_a_title_only_write_keeps_a_mobi_s_authors() -> None:
    """
    The MOBI writer, like calibre's, writes whatever authors it is given; it is now given the book's.

    Example:
        >>> test_a_title_only_write_keeps_a_mobi_s_authors()


    :return: None after the authors are checked.
    """
    original = build_minimal_mobi(title="Old", authors=("Ann Writer",))
    encoded = base64.b64encode(original).decode("ascii")
    receipt = _write(
        {"base64": encoded, "file_type": "mobi", "metadata": {"title": "New"}}
    )
    md = _read(receipt["content"], "mobi")
    assert (md.title, md.authors) == ("New", ["Ann Writer"])


def test_a_failed_replace_leaves_the_file_untouched(
    book: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If the rewritten book cannot be put in place, the old one is as it was and nothing is left over.

    Example:
        >>> test_a_failed_replace_leaves_the_file_untouched(Path("book.epub"), None)  # doctest: +SKIP


    :param book: The book.
    :param monkeypatch: Makes the rename fail, as a full or read-only disk would.
    :return: None after the file and folder are checked.
    """
    original = book.read_bytes()

    def refuse(_source: str, _target: str) -> None:
        """
        Fail as a full disk would.

        Example:
            >>> refuse("a", "b")  # doctest: +SKIP


        :param _source: The new file.
        :param _target: The old one.
        :return: Never returns.
        :raises OSError: Always.
        """
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(service.os, "replace", refuse)
    with pytest.raises(CoreDispatchError) as caught:
        _write({"path": str(book), "metadata": {"title": "After"}})
    assert caught.value.code == "metadata_file_write_failed"
    assert book.read_bytes() == original
    assert [entry.name for entry in book.parent.iterdir()] == ["book.epub"]


def test_a_file_changed_meanwhile_is_not_written(
    book: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If another program changes the book while it is being rewritten, nothing is written over it.

    Example:
        >>> test_a_file_changed_meanwhile_is_not_written(Path("book.epub"), None)  # doctest: +SKIP


    :param book: The book.
    :param monkeypatch: Changes the book during the write.
    :return: None after the file is checked.
    """
    original = book.read_bytes()
    real = plugin_runtime.set_file_type_metadata

    def meanwhile(stream: Any, mi: Any, ftype: str, report_error: Any = None) -> None:
        """
        Rewrite the copy while another program appends to the book.

        Example:
            >>> meanwhile(None, None, "epub")  # doctest: +SKIP


        :param stream: The copy.
        :param mi: The metadata.
        :param ftype: The format.
        :param report_error: The error callback.
        :return: None.
        """
        with book.open("ab") as other:
            other.write(b"another program's change")
        real(stream, mi, ftype, report_error=report_error)

    monkeypatch.setattr(plugin_runtime, "set_file_type_metadata", meanwhile)
    with pytest.raises(CoreDispatchError) as caught:
        _write({"path": str(book), "metadata": {"title": "After"}})
    assert caught.value.code == "metadata_file_changed"
    assert book.read_bytes() == original + b"another program's change"


def test_links_and_permissions_are_kept(book: Path) -> None:
    """
    Written through a symbolic link, the book it names is rewritten; the link and the permissions stay.

    Example:
        >>> test_links_and_permissions_are_kept(Path("book.epub"))  # doctest: +SKIP


    :param book: The book.
    :return: None after the link, mode and title are checked.
    """
    book.chmod(0o640)
    link = book.parent / "link.epub"
    try:
        link.symlink_to(book)
    except OSError:
        pytest.skip("this system cannot make symbolic links here")
    _write({"path": str(link), "metadata": {"title": "Through The Link"}})
    assert link.is_symlink()
    assert book.stat().st_mode & 0o777 == 0o640
    assert _read(book.read_bytes()).title == "Through The Link"
    assert sorted(entry.name for entry in book.parent.iterdir()) == [
        "book.epub",
        "link.epub",
    ]
    assert os.path.realpath(link) == os.path.realpath(book)
