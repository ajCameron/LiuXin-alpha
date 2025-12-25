
from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


def test_builtin_open_accepts_location(store) -> None:
    loc = OnDiskUnmanagedStoreLocation("x.txt", store=store)
    with open(loc, "w", encoding="utf-8") as f:
        f.write("hello")
    assert fs_path(store, "x.txt").read_text(encoding="utf-8") == "hello"


def test_pathlib_path_accepts_location(store) -> None:
    loc = OnDiskUnmanagedStoreLocation("y.bin", store=store)
    p = pathlib.Path(loc)
    assert p == fs_path(store, "y.bin")
