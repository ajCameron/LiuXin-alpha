"""
Verify archive metadata dispatch, member selection and fallback behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test archive metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _build_cbz_stream(comment: bytes, members: dict[str, bytes] | None = None) -> io.BytesIO:
    """
    Perform the build cbz stream test-helper operation with deterministic inputs.

    Example:
        Exercise build cbz stream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :param comment: Value supplied for comment in the focused test operation.
    :param members: Value supplied for members in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    members = members or {"page-001.jpg": b"fake image"}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as zf:
        for name, payload in members.items():
            zf.writestr(name, payload)
        zf.comment = comment
    stream.seek(0)
    return stream


def test_is_comic_uses_non_empty_image_only_heuristic() -> None:
    """
    Verify is comic uses non empty image only heuristic.

    Example:
        Exercise test is comic uses non empty image only heuristic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import is_comic

    assert is_comic(["Page1.JPG", "nested/page2.png", "Thumbs.db"]) is True
    assert is_comic(["cover.jpeg", "note.txt"]) is False
    assert is_comic([]) is False


def test_archive_type_detects_zip_and_rar_and_restores_stream_pos() -> None:
    """
    Verify archive type detects zip and rar and restores stream pos.

    Example:
        Exercise test archive type detects zip and rar and restores stream pos through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import archive_type

    zip_stream = io.BytesIO(b"PK\x03\x04abcd")
    zip_stream.seek(0)
    assert archive_type(zip_stream) == "zip"
    assert zip_stream.tell() == 0

    rar_stream = io.BytesIO(b"Rar!\x1a\x07\x00abcd")
    rar_stream.seek(0)
    assert archive_type(rar_stream) == "rar"
    assert rar_stream.tell() == 0

    unknown_stream = io.BytesIO(b"????")
    assert archive_type(unknown_stream) is None


def test_get_comic_book_info_maps_fields() -> None:
    """
    Verify get comic book info maps fields.

    Example:
        Exercise test get comic book info maps fields through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import get_comic_book_info

    mi = calibreMetaInformation(None, ["Unknown"])
    get_comic_book_info(
        {
            "series": "Mega Saga",
            "volume": "7.5",
            "rating": 4,
            "title": "Issue title",
            "publisher": "Demo Press",
            "tags": ["Sci-Fi", " Space "],
            "credits": [
                {"role": "Writer", "person": "Doe, Jane"},
                {"role": "Artist", "person": "Smith, John"},
                {"role": "Editor", "person": "Ignored, Person"},
            ],
            "comments": "Short note",
            "publicationMonth": 2,
            "publicationYear": 2021,
        },
        mi,
    )

    assert mi.series == "Mega Saga"
    assert mi.series_index == 7.5
    assert mi.rating == 4
    assert mi.title == "Issue title"
    assert mi.publisher == "Demo Press"
    assert mi.tags == ["Sci-Fi", "Space"]
    assert mi.authors == ["Jane Doe", "John Smith"]
    assert mi.comments == "Short note"
    assert mi.pubdate.year == 2021
    assert mi.pubdate.month == 2


def test_get_comic_metadata_reads_cbz_comment_json() -> None:
    """
    Verify get comic metadata reads cbz comment json.

    Example:
        Exercise test get comic metadata reads cbz comment json through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import get_comic_metadata

    payload = {
        "ComicBookInfo/1.0": {
            "series": "Mystery Files",
            "issue": 12,
            "title": "The Red Door",
            "credits": [{"role": "Creator", "person": "Lane, Alex"}],
        }
    }
    stream = _build_cbz_stream(json.dumps(payload).encode("utf-8"))
    mi = get_comic_metadata(stream, "cbz", series_index="issue")

    assert mi.series == "Mystery Files"
    assert mi.series_index == 12.0
    assert mi.title == "The Red Door"
    assert mi.authors == ["Alex Lane"]


def test_get_comic_metadata_tolerates_malformed_comment() -> None:
    """
    Verify get comic metadata tolerates malformed comment.

    Example:
        Exercise test get comic metadata tolerates malformed comment through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import get_comic_metadata

    stream = _build_cbz_stream(b"this is not json")
    mi = get_comic_metadata(stream, "cbz")
    assert mi.title == "Unknown"
    assert mi.authors == ["Unknown"]


def test_archive_extract_unwraps_single_supported_member_zip(tmp_path: Path) -> None:
    """
    Verify archive extract unwraps single supported member zip.

    Example:
        Exercise test archive extract unwraps single supported member zip through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import ArchiveExtract

    archive = tmp_path / "one-book.zip"
    payload = b"epub-bytes"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("book.epub", payload)

    plugin = ArchiveExtract(None)
    extracted_path = plugin.run(str(archive))

    assert extracted_path != str(archive)
    assert extracted_path.endswith(".epub")
    assert Path(extracted_path).read_bytes() == payload


def test_archive_extract_keeps_archive_for_unsupported_or_multi_member(tmp_path: Path) -> None:
    """
    Verify archive extract keeps archive for unsupported or multi member.

    Example:
        Exercise test archive extract keeps archive for unsupported or multi member through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import ArchiveExtract

    unsupported = tmp_path / "unsupported.zip"
    with zipfile.ZipFile(unsupported, "w") as zf:
        zf.writestr("readme.txt", b"text")

    multiple = tmp_path / "multiple.zip"
    with zipfile.ZipFile(multiple, "w") as zf:
        zf.writestr("a.epub", b"a")
        zf.writestr("b.epub", b"b")

    plugin = ArchiveExtract(None)
    assert plugin.run(str(unsupported)) == str(unsupported)
    assert plugin.run(str(multiple)) == str(multiple)


def test_archive_extract_detects_comic_zip_and_keeps_payload(tmp_path: Path) -> None:
    """
    Verify archive extract detects comic zip and keeps payload.

    Example:
        Exercise test archive extract detects comic zip and keeps payload through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_archive_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.archive import ArchiveExtract

    archive = tmp_path / "comic.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("Page_001.JPG", b"one")
        zf.writestr("Page_002.png", b"two")

    plugin = ArchiveExtract(None)
    extracted_path = plugin.run(str(archive))

    assert extracted_path.endswith(".cbz")
    assert Path(extracted_path).read_bytes() == archive.read_bytes()
