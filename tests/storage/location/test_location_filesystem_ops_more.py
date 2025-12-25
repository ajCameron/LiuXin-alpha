
from __future__ import annotations

import os
import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


def test_touch_exist_ok_false_raises(store) -> None:
    loc = OnDiskUnmanagedStoreLocation("a.txt", store=store)
    loc.touch()
    with pytest.raises(FileExistsError):
        loc.touch(exist_ok=False)


def test_unlink_missing_ok(store) -> None:
    loc = OnDiskUnmanagedStoreLocation("missing.txt", store=store)
    with pytest.raises(FileNotFoundError):
        loc.unlink(missing_ok=False)
    loc.unlink(missing_ok=True)  # should not raise


def test_mkdir_parents_exist_ok(store) -> None:
    loc = OnDiskUnmanagedStoreLocation("a", "b", "c", store=store)
    loc.mkdir(parents=True, exist_ok=True)
    assert fs_path(store, "a", "b", "c").is_dir()
    # again, should not raise
    loc.mkdir(parents=True, exist_ok=True)


def test_rmdir_non_empty_raises(store) -> None:
    d = OnDiskUnmanagedStoreLocation("dir", store=store)
    d.mkdir()
    f = OnDiskUnmanagedStoreLocation("dir", "file.txt", store=store)
    f.write_text("x")
    with pytest.raises(OSError):
        d.rmdir()


def test_rename_bare_name_stays_in_dir(store) -> None:
    d = OnDiskUnmanagedStoreLocation("d", store=store)
    d.mkdir()
    f = OnDiskUnmanagedStoreLocation("d", "old.txt", store=store)
    f.write_text("hi")
    moved = f.rename("new.txt")
    assert moved.parts == ("d", "new.txt")
    assert fs_path(store, "d", "new.txt").exists()
    assert not fs_path(store, "d", "old.txt").exists()


def test_rename_store_relative_path(store) -> None:
    src = OnDiskUnmanagedStoreLocation("src.txt", store=store)
    src.write_text("x")
    moved = src.rename("subdir/moved.txt")
    assert moved.parts == ("subdir", "moved.txt")
    assert fs_path(store, "subdir", "moved.txt").read_text() == "x"


def test_replace_store_relative_path(store) -> None:
    a = OnDiskUnmanagedStoreLocation("a.txt", store=store)
    b = OnDiskUnmanagedStoreLocation("b.txt", store=store)
    a.write_text("A")
    b.write_text("B")
    out = a.replace("b.txt")
    assert out.parts == ("b.txt",)
    assert fs_path(store, "b.txt").read_text() == "A"
