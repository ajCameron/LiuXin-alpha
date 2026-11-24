"""
Check calibre's comic, ZIP and RAR readers, ported.

calibre's comic reader takes a comic's cover from its first image in natural name
order, and the rest from ComicBookInfo JSON in the archive comment. Its ZIP and RAR
readers hand the first book they find inside to the reader for that book's type;
here those are the other ported readers, run in process.

RAR contents need a tool ``rarfile`` can run (``unrar``, ``unar``, 7-Zip or
``bsdtar``), and CB7 needs ``py7zr``; the tests that need them skip without them.
Names are read without a tool, from LiuXin's vendored RAR test files.

Example:
    Run these checks::

        python -m pytest -q tests/file_formats/providers/test_ported_archive_readers.py
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.file_formats.archive_preflight import ArchivePreflightError
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.ported_calibre import (
    PORTED_READERS,
    adapt_ported_reader,
    run_ported_reader,
)
from LiuXin_alpha.metadata.upstream_calibre import _readers, _unrar, comic, libunzip
from tests.support.file_format_epub import build_unicode_epub

RAR_FILES = (
    Path(__file__).resolve().parents[3]
    / "src/LiuXin_alpha/utils/decompression/rarfile/test/files"
)
BOOK_INFO = {
    "ComicBookInfo/1.0": {
        "series": "Saga",
        "title": "Chapter Two",
        "volume": 3,
        "issue": 14,
        "publisher": "Image",
        "publicationYear": 2001,
        "publicationMonth": 4,
        "language": "English",
        "rating": 4,
        "tags": ["space", "war"],
        "credits": [
            {"person": "Doe, Jane", "role": "Writer"},
            {"person": "Roe, Rick", "role": "Artist"},
            {"person": "Ed Itor", "role": "Editor"},
        ],
        "comments": " A story. ",
    }
}


@pytest.fixture(autouse=True)
def empty_cache() -> None:
    """
    Start every test with no remembered readings.

    Example:
        >>> "empty_cache" in globals()
        True


    :return: None after the cache is emptied.
    """
    fork_metadata._cache.clear()


def png(tag: int) -> bytes:
    """
    A stand-in PNG: the signature and a recognisable byte.

    Example:
        >>> png(7)[:4]
        b'\\x89PNG'


    :param tag: The byte that tells the images apart.
    :return: The image's bytes.
    """
    return b"\x89PNG\r\n\x1a\n" + bytes([tag]) * 20


def zip_bytes(members: dict[str, bytes], comment: bytes = b"") -> bytes:
    """
    A ZIP archive of the members, in order, with an archive comment.

    Example:
        >>> zipfile.ZipFile(io.BytesIO(zip_bytes({"a": b"1"}))).namelist()
        ['a']


    :param members: Names and contents.
    :param comment: The archive comment.
    :return: The archive.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
        archive.comment = comment
    return buffer.getvalue()


def comic_bytes() -> bytes:
    """
    A CBZ with ComicBookInfo, pages to sort naturally, and files that are not pages.

    Example:
        >>> zipfile.ZipFile(io.BytesIO(comic_bytes())).namelist()[0]
        'page10.png'


    :return: The comic.
    """
    pages = {
        "page10.png": png(10),
        "page2.png": png(2),
        "Page1.png": png(1),
        "__MACOSX/._page0.png": png(0),
        "notes.nfo": b"not a page",
    }
    return zip_bytes(pages, json.dumps(BOOK_INFO).encode())


def read(
    tmp_path: Path, plugin: str, name: str, data: bytes
) -> tuple[dict[str, Any], Any]:
    """
    Read one file with a ported reader's provider: its fields and cover.

    Example:
        >>> read(Path("/tmp"), "Read comic metadata", "a.cbz", comic_bytes())  # doctest: +SKIP


    :param tmp_path: Where to write the file.
    :param plugin: The calibre plugin name.
    :param name: The file's name; its extension is its format.
    :param data: Its bytes.
    :return: The fields read, as a plain dictionary, and the cover.
    """
    path = tmp_path / name
    path.write_bytes(data)
    context = ProviderContext(
        work_dir=tmp_path,
        options={},
        log=logging.getLogger("tests.ported-archives"),
        report=ConversionReport(),
    )
    provider = adapt_ported_reader(plugin)()
    file_format = name.rpartition(".")[2]
    fields = provider.read_calibre_metadata(path, file_format, context)
    return dict(fields.values), provider.extract_cover(path, file_format, context)


def test_a_comic_gives_its_book_info_and_first_page(tmp_path: Path) -> None:
    """
    The comment's ComicBookInfo fills the fields; the first page in natural order is the cover.

    Example:
        >>> test_a_comic_gives_its_book_info_and_first_page(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the fields and cover are checked.
    """
    fields, cover = read(tmp_path, "Read comic metadata", "saga.cbz", comic_bytes())
    assert fields["title"] == "Chapter Two"
    assert fields["authors"] == ("Jane Doe", "Rick Roe")
    assert (fields["series"], fields["series_index"]) == ("Saga", 3.0)
    assert fields["publisher"] == "Image"
    assert fields["languages"] == ("eng",)
    assert fields["tags"] == ("space", "war")
    assert fields["comments"] == "A story."
    assert str(fields["pubdate"]).startswith("2001-04-15")
    assert cover is not None and cover.data == png(1)


def test_the_series_index_can_be_the_issue() -> None:
    """
    calibre's plugin setting picks the issue number in place of the volume.

    Example:
        >>> test_the_series_index_can_be_the_issue()


    :return: None after both readings are checked.
    """
    assert comic.get_metadata(io.BytesIO(comic_bytes()), "cbz").series_index == 3.0
    issue = comic.get_metadata(io.BytesIO(comic_bytes()), "cbz", series_index="issue")
    assert issue.series_index == 14.0


def test_a_comic_collection_reads_its_first_comic(tmp_path: Path) -> None:
    """
    A CBC lists its comics in ``comics.txt``; the first listed is read.

    Example:
        >>> test_a_comic_collection_reads_its_first_comic(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the reading is checked.
    """
    collection = zip_bytes(
        {
            "comics.txt": b"second.cbz\nfirst.cbz\n",
            "first.cbz": zip_bytes({"p1.png": png(5)}),
            "second.cbz": comic_bytes(),
        }
    )
    fields, cover = read(tmp_path, "Read comic metadata", "set.cbc", collection)
    assert fields["series"] == "Saga"
    assert cover is not None and cover.data == png(1)


def test_a_comic_without_pages_has_no_cover(tmp_path: Path) -> None:
    """
    Files that are not images, and macOS resource forks, are never the cover.

    Example:
        >>> test_a_comic_without_pages_has_no_cover(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the reading is checked.
    """
    data = zip_bytes({"__MACOSX/._a.png": png(1), "read.me": b"x"})
    _, cover = read(tmp_path, "Read comic metadata", "empty.cbz", data)
    assert cover is None


def test_pages_sort_naturally() -> None:
    """
    Numbers in names sort by value, as calibre's ICU numeric collation sorts them.

    Example:
        >>> test_pages_sort_naturally()


    :return: None after the order is checked.
    """
    names = ["p10.jpg", "p9.jpg", "P1.png", "p01a.jpg", "cover.jpg", "007.png"]
    assert sorted(names, key=libunzip.sort_key) == [
        "007.png",
        "cover.jpg",
        "P1.png",
        "p01a.jpg",
        "p9.jpg",
        "p10.jpg",
    ]


def test_a_zip_holding_a_book_reads_the_book(tmp_path: Path) -> None:
    """
    calibre's ZIP reader hands the first book it finds to that book's reader.

    Example:
        >>> test_a_zip_holding_a_book_reads_the_book(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the reading is checked.
    """
    epub = build_unicode_epub(tmp_path / "inner.epub").path.read_bytes()
    data = zip_bytes({"readme.txt": b"hello", "Some Book.epub": epub})
    fields, _ = read(tmp_path, "Read ZIP metadata", "book.zip", data)
    assert fields["title"] == "EPUB Καλημέρα 世界"
    assert fields.get("timestamp") is None


def test_a_zip_of_pages_is_read_as_a_comic(tmp_path: Path) -> None:
    """
    A ZIP holding only images is a comic, as calibre decides.

    Example:
        >>> test_a_zip_of_pages_is_read_as_a_comic(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the reading is checked.
    """
    fields, cover = read(
        tmp_path,
        "Read ZIP metadata",
        "pages.zip",
        comic_bytes().replace(b"notes.nfo", b"notes.png"),
    )
    assert fields["series"] == "Saga"
    assert cover is not None


def test_a_zip_with_no_book_is_refused(tmp_path: Path) -> None:
    """
    A ZIP holding neither a book calibre reads nor only images is refused, as calibre refuses it.

    Example:
        >>> test_a_zip_with_no_book_is_refused(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the refusal is checked.
    """
    path = tmp_path / "notes.zip"
    path.write_bytes(zip_bytes({"a.txt": b"one", "b.doc": b"two"}))
    with pytest.raises(ValueError, match="No ebook found in ZIP archive"):
        run_ported_reader("zip", path, "zip")


def test_a_zip_bomb_is_refused_before_it_is_extracted(tmp_path: Path) -> None:
    """
    calibre extracts the inner book to disk; LiuXin's archive limits refuse a huge one first.

    Example:
        >>> test_a_zip_bomb_is_refused_before_it_is_extracted(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the refusal is checked.
    """
    path = tmp_path / "bomb.zip"
    path.write_bytes(zip_bytes({"huge.epub": bytes(8 * 1024 * 1024)}))
    with pytest.raises(ArchivePreflightError, match="compression ratio"):
        run_ported_reader("zip", path, "zip")


def test_every_ported_format_is_dispatched_inside_archives() -> None:
    """
    The readers calibre's ZIP and RAR readers hand books to are the ported providers' own.

    Example:
        >>> test_every_ported_format_is_dispatched_inside_archives()


    :return: None after the two tables are compared.
    """
    by_format = {
        file_format: module
        for module, formats in PORTED_READERS.values()
        for file_format in formats
    }
    assert by_format == _readers.READER_MODULES


def test_rar_names_are_read_without_a_tool() -> None:
    """
    ``rarfile`` reads names from the headers itself; folders are not names.

    Example:
        >>> test_rar_names_are_read_without_a_tool()


    :return: None after the names are checked.
    """
    assert list(_unrar.names(str(RAR_FILES / "unicode.rar"))) == [
        "уииоотивл.txt",
        "𝐀𝐁𝐁𝐂.txt",
    ]
    with (RAR_FILES / "seektest.rar").open("rb") as stream:
        assert list(_unrar.names(stream)) == ["stest1.txt", "stest2.txt"]


class FakeArchive:
    """
    A stand-in for ``rarfile.RarFile``, for the logic that needs no tool.

    Example:
        >>> FakeArchive("x").comment
        '{}'
    """

    infos = [
        SimpleNamespace(
            filename="folder", file_size=0, compress_size=0, isdir=lambda: True
        ),
        SimpleNamespace(
            filename="p10.png", file_size=21, compress_size=21, isdir=lambda: False
        ),
        SimpleNamespace(
            filename="p2.png", file_size=21, compress_size=21, isdir=lambda: False
        ),
        SimpleNamespace(
            filename="huge.png",
            file_size=300 * 1024 * 1024,
            compress_size=200 * 1024 * 1024,
            isdir=lambda: False,
        ),
    ]
    comment = "{}"

    def __init__(self, path: str) -> None:
        """
        Open nothing.

        Example:
            >>> FakeArchive("x").path
            'x'


        :param path: The archive's path.
        """
        self.path = path

    def __enter__(self) -> FakeArchive:
        """
        Enter.

        Example:
            >>> FakeArchive("x").__enter__().path
            'x'


        :return: Itself.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """
        Leave.

        Example:
            >>> FakeArchive("x").__exit__(None, None, None)


        :param args: The exception, if any.
        """

    def infolist(self) -> list[SimpleNamespace]:
        """
        The headers.

        Example:
            >>> len(FakeArchive("x").infolist())
            4


        :return: The headers.
        """
        return self.infos

    def read(self, info: SimpleNamespace) -> bytes:
        """
        A member's contents.

        Example:
            >>> FakeArchive("x").read(FakeArchive.infos[1])[:4]
            b'\\x89PNG'


        :param info: The member.
        :return: A page named by its number.
        """
        return png(int("".join(c for c in info.filename if c.isdigit()) or 0))


def test_a_cbr_cover_is_its_first_page_and_huge_members_are_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    The RAR stand-in picks pages as calibre does and refuses members past LiuXin's limits.

    Example:
        >>> test_a_cbr_cover_is_its_first_page_and_huge_members_are_refused(None)  # doctest: +SKIP


    :param monkeypatch: Replaces ``rarfile`` with the fake.
    :return: None after the cover and the refusal are checked.
    """
    monkeypatch.setattr(
        _unrar, "_rarfile", lambda: SimpleNamespace(RarFile=FakeArchive)
    )
    assert list(_unrar.names("a.cbr")) == ["p10.png", "p2.png", "huge.png"]
    assert _unrar.extract_member("a.cbr", match=None, name="p10.png") == (
        "p10.png",
        png(10),
    )
    # huge.png sorts first; it is refused before it is read.
    with pytest.raises(ArchivePreflightError, match="too large"):
        _unrar.extract_cover_image("a.cbr")
    monkeypatch.setattr(FakeArchive, "infos", FakeArchive.infos[:3])
    assert _unrar.extract_cover_image("a.cbr") == ("p2.png", png(2))
    assert _unrar.comment("a.cbr") == "{}"


def test_the_rar_port_says_when_it_cannot_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Without a tool to extract RAR contents, the RAR provider reports itself unavailable.

    Example:
        >>> test_the_rar_port_says_when_it_cannot_run(None)  # doctest: +SKIP


    :param monkeypatch: Makes the tool missing.
    :return: None after the report is checked.
    """
    monkeypatch.setattr(_unrar, "available", lambda: (False, "no tool"))
    availability = adapt_ported_reader("Read RAR metadata").availability()
    assert not availability.available
    assert "no tool" in str(availability.reason)


@pytest.mark.skipif(
    not _unrar.available()[0], reason="no tool rarfile can extract RAR contents with"
)
def test_a_rar_comment_is_read_with_a_tool() -> None:
    """
    The comment of a RAR 3 archive is compressed, so a tool reads it.

    Example:
        >>> test_a_rar_comment_is_read_with_a_tool()  # doctest: +SKIP


    :return: None after the comment is checked.
    """
    assert _unrar.comment(str(RAR_FILES / "rar3-comment-plain.rar")) == "RARcomment\n"


def test_a_cb7_cover_is_read_with_py7zr(tmp_path: Path) -> None:
    """
    A CB7's cover comes through ``py7zr``, as in calibre; 7-Zip comics carry no comment.

    Example:
        >>> test_a_cb7_cover_is_read_with_py7zr(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the cover is checked.
    """
    py7zr = pytest.importorskip("py7zr")
    path = tmp_path / "pages.cb7"
    with py7zr.SevenZipFile(path, "w") as archive:
        archive.writestr(png(10), "p10.png")
        archive.writestr(png(9), "p9.png")
    _, cover = read(tmp_path, "Read comic metadata", "pages.cb7", path.read_bytes())
    assert cover is not None and cover.data in {png(9), png(10)}


def test_real_archives_read_as_liuxin_reads_them(
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    """
    On the data repository's archives, the port finds the book LiuXin's reader finds.

    An archive holding no book calibre reads is refused, where LiuXin's reader returns
    empty metadata. RAR archives are read where there is a tool to extract them with.

    Example:
        >>> test_real_archives_read_as_liuxin_reads_them({})  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :return: None after every archive is compared.
    """
    from LiuXin_alpha.file_formats.plugin_runtime import get_file_type_metadata

    kinds = ["zip"] + (["rar"] if _unrar.available()[0] else [])
    checked = 0
    for kind in kinds:
        for path in md_test_files_by_ext.get(kind, []):
            ours = get_file_type_metadata(io.BytesIO(path.read_bytes()), kind)
            if not ours.title:
                with pytest.raises(ValueError, match="No ebook found"):
                    run_ported_reader(kind, path, kind)
            else:
                ported = run_ported_reader(kind, path, kind)
                assert (ported.title, list(ported.authors)) == (
                    ours.title,
                    [str(name) for name in ours.authors],
                ), path.name
            checked += 1
    if not checked:
        pytest.skip("the data repository is not checked out")
