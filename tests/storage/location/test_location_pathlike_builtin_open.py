
from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)

from .conftest import fs_path


def test_builtin_open_accepts_location(store) -> None:
    loc = OnDiskExistingManagedStoreLocation("x.txt", store=store)
    with open(loc, "w", encoding="utf-8") as f:
        f.write("hello")
    assert fs_path(store, "x.txt").read_text(encoding="utf-8") == "hello"


def test_pathlib_path_accepts_location(store) -> None:
    loc = OnDiskExistingManagedStoreLocation("y.bin", store=store)
    p = pathlib.Path(loc)
    assert p == fs_path(store, "y.bin")
