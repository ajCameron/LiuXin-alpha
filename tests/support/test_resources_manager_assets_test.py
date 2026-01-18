"""Tests for provisioning non-database test assets (books/covers)."""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_bytes(path: Path, data: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_provision_test_books_and_covers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
    from tests.support.test_resources_manager import TestResourcesManager

    src_books = tmp_path / "src_books"
    _write_bytes(src_books / "a.epub", b"book-a")
    _write_bytes(src_books / "b.mobi", b"book-b")
    monkeypatch.setenv("LIUXIN_TEST_BOOKS_DIR", str(src_books))

    mgr = TestResourcesManager(cache_dir=tmp_path / "cache")

    books = mgr.provision_test_books(dst_dir=tmp_path / "dst_sel", names=["b.mobi"])
    assert {p.name for p in books.paths} == {"b.mobi"}
