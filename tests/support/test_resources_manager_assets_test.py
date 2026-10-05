"""
Provide deterministic test resources manager assets test support for the test suite.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test resources manager assets test through a consuming regression::

        python -m pytest -q tests/support/test_resources_manager_assets_test.py
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_bytes(path: Path, data: bytes = b"x") -> None:
    """
    Write bytes for deterministic fixture consumers.

    Example:
        Exercise  write bytes through a consuming regression::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param data: Bytes or structured data consumed by the operation.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_provision_test_books_and_covers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Verify provision test books and covers.

    Example:
        Exercise test provision test books and covers through a consuming regression::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param tmp_path: Pytest-managed temporary directory for generated fixture data.
    :return: None; completion is expressed through state changes or assertions.
    """
    from tests.support.test_resources_manager import TestResourcesManager

    # Arrange: create a fake on-disk LiuXin_data tree.
    src_books = tmp_path / "src_books"
    src_covers = tmp_path / "src_covers"
    _write_bytes(src_books / "a.epub", b"book-a")
    _write_bytes(src_books / "b.mobi", b"book-b")
    _write_bytes(src_covers / "book_id_1.jpg", b"cover-1")
    _write_bytes(src_covers / "book_id_2.jpg", b"cover-2")

    monkeypatch.setenv("LIUXIN_TEST_BOOKS_DIR", str(src_books))
    monkeypatch.setenv("LIUXIN_TEST_COVERS_DIR", str(src_covers))

    mgr = TestResourcesManager(cache_dir=tmp_path / "cache")

    # Act: provision full sets.
    books = mgr.provision_test_books(dst_dir=tmp_path / "dst")
    covers = mgr.provision_test_covers(dst_dir=tmp_path / "dst")

    # Assert: all files copied.
    assert books.root.is_dir()
    assert {p.name for p in books.paths} == {"a.epub", "b.mobi"}
    assert covers.root.is_dir()
    assert {p.name for p in covers.paths} == {"book_id_1.jpg", "book_id_2.jpg"}


def test_provision_test_books_selective(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Verify provision test books selective.

    Example:
        Exercise test provision test books selective through a consuming regression::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param tmp_path: Pytest-managed temporary directory for generated fixture data.
    :return: None; completion is expressed through state changes or assertions.
    """
    from tests.support.test_resources_manager import TestResourcesManager

    src_books = tmp_path / "src_books"
    _write_bytes(src_books / "a.epub", b"book-a")
    _write_bytes(src_books / "b.mobi", b"book-b")
    monkeypatch.setenv("LIUXIN_TEST_BOOKS_DIR", str(src_books))

    mgr = TestResourcesManager(cache_dir=tmp_path / "cache")

    books = mgr.provision_test_books(dst_dir=tmp_path / "dst_sel", names=["b.mobi"])
    assert {p.name for p in books.paths} == {"b.mobi"}
