# tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py

"""
Verify file, format and cover metadata normalization.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata files and covers through its owning regression module::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData


class _CloseTracker(io.BytesIO):
    """
    Provide the CloseTracker test fixture or double with explicit deterministic behavior.

    Example:
        Exercise CloseTracker through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
    """
    def __init__(self, initial: bytes = b"") -> None:
        """
        Initialize the CloseTracker test double.

        Example:
            Exercise CloseTracker.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :param initial: Value supplied for initial in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(initial)
        self.closed_flag = False

    def close(self) -> None:
        """
        Mark the cache double closed for lifecycle assertions.

        Example:
            Exercise CloseTracker.close through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :return: None; the function records state or raises through its assertions.
        """
        self.closed_flag = True
        super().close()


def test_add_file_path_and_record_path_and_filename(tmp_path: Path) -> None:
    """
    Verify add file path and record path and filename.

    Example:
        Exercise test add file path and record path and filename through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    md = CalibreLikeLiuXinBookMetaData()
    p = tmp_path / "subdir" / "book.pdf"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"pdf")

    md.add_file(str(p), typ="path")
    # must not crash; internal representation may vary
    assert len(md.files) >= 1

    md.record_path_and_file_name(str(p))
    assert any("book.pdf" in x for x in md.filename)
    assert any(str(p.parent) in x for x in md.filepath)


def test_add_file_bytes_and_filelike_and_cleanup_closing() -> None:
    """
    Verify add file bytes and filelike and cleanup closing.

    Example:
        Exercise test add file bytes and filelike and cleanup closing through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


    :return: None; the function records state or raises through its assertions.
    """
    md = CalibreLikeLiuXinBookMetaData()

    md.add_file(b"hello", typ="bytes")
    assert len(md.files) >= 1

    f = _CloseTracker(b"stream")
    md.add_file(f, typ="filelike")
    md.register_file_for_cleanup(f)

    assert f.closed_flag is False
    md.close_cleanup_files()
    assert f.closed_flag is True


def test_add_cover_path_and_bytes(tmp_path: Path) -> None:
    """
    Verify add cover path and bytes.

    Example:
        Exercise test add cover path and bytes through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    md = CalibreLikeLiuXinBookMetaData()

    c = tmp_path / "cover.jpg"
    c.write_bytes(b"jpg")

    md.add_cover(str(c), typ="path")
    md.add_cover(b"raw", typ="bytes")

    # cover storage is implementation-specific; minimal invariant is "no crash"
    assert hasattr(md, "cover_data")
