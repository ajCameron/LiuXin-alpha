from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.api import StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)


def test_on_disk_unmanaged_drive_init_creates_root(tmp_path: pathlib.Path) -> None:
    store_root = tmp_path / "unmanaged_root"
    assert store_root.exists() is False

    store = OnDiskUnmanagedStorageBackend(url=str(store_root))
    assert store.root_path == store_root.resolve()
    assert store.root_path.exists() is True
    assert store.root_path.is_dir() is True
    assert store.url == str(store_root)


def test_on_disk_unmanaged_drive_file_exists_and_path_boundary(tmp_path: pathlib.Path) -> None:
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    inside = tmp_path / "inside.txt"
    inside.write_text("ok", encoding="utf-8")

    assert store.file_exists(str(inside)) is True
    assert store.file_exists("inside.txt") is True
    assert store.file_exists("nope.txt") is False
    assert store.file_exists(str(tmp_path.parent / "outside.txt")) is False

    with pytest.raises(ValueError):
        store.get_file(str(tmp_path.parent / "outside.txt"))


def test_on_disk_unmanaged_drive_get_file_status(tmp_path: pathlib.Path) -> None:
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    p = tmp_path / "sample.bin"
    p.write_bytes(b"abc123")

    status = store.get_file_status(str(p))
    assert status.url == str(p.resolve())
    assert status.size == 6
    assert status.hash
    assert status.recheck_self(all=True) is True


def test_on_disk_unmanaged_drive_true_files_iterates_recursively(tmp_path: pathlib.Path) -> None:
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b.txt").write_text("b", encoding="utf-8")

    urls = {f.file_url for f in store.true_files()}
    assert str((tmp_path / "a.txt").resolve()) in urls
    assert str((nested / "b.txt").resolve()) in urls


def test_on_disk_unmanaged_drive_is_read_only(tmp_path: pathlib.Path) -> None:
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    with pytest.raises(PermissionError):
        store.add_file(b"cannot write")
    with pytest.raises(PermissionError):
        store.delete_file(str(tmp_path / "nope.txt"))


def test_on_disk_unmanaged_drive_startup_and_status_reports_read_only(tmp_path: pathlib.Path) -> None:
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    status_from_startup = store.startup()
    status_from_cache = store.status()

    assert isinstance(status_from_startup, StoreStatus)
    assert status_from_startup is status_from_cache
    assert status_from_startup.url == str(tmp_path.resolve())
    assert isinstance(status_from_startup.checked, bool)
    assert isinstance(status_from_startup.good, bool)
    assert status_from_startup.check_status.write is False
    assert status_from_startup.details.get("mode") == "read_only"

