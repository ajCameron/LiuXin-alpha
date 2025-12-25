
from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


def _mk(store, rel: str, content: str = "x") -> None:
    p = fs_path(store, *pathlib.PurePosixPath(rel).parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def test_glob_matrix(store) -> None:
    _mk(store, "a/1.txt")
    _mk(store, "a/2.bin")
    _mk(store, "a/b/3.txt")
    _mk(store, "a/b/c/4.txt")
    root = OnDiskUnmanagedStoreLocation(store=store)

    # glob in root
    assert {p.as_posix() for p in root.glob("a/*.txt")} == {"a/1.txt"}
    assert {p.as_posix() for p in root.glob("a/*.*")} == {"a/1.txt", "a/2.bin"}

    # rglob
    assert {p.as_posix() for p in root.rglob("*.txt")} == {"a/1.txt", "a/b/3.txt", "a/b/c/4.txt"}
    assert {p.as_posix() for p in root.rglob("b/**/*.txt")} == {"a/b/3.txt", "a/b/c/4.txt"}

    # glob from subdir location
    a = OnDiskUnmanagedStoreLocation("a", store=store)
    assert {p.as_posix() for p in a.glob("**/*.txt")} == {"a/1.txt", "a/b/3.txt", "a/b/c/4.txt"}
