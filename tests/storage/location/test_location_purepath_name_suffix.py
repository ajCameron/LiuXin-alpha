
from __future__ import annotations

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_name_suffix_stem(store, loc_cls) -> None:
    loc = loc_cls("dir", "foo.tar.gz", store=store)
    assert loc.name == "foo.tar.gz"
    assert loc.suffix == ".gz"
    assert loc.suffixes == [".tar", ".gz"]
    assert loc.stem == "foo.tar"


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_with_name(store, loc_cls) -> None:
    loc = loc_cls("dir", "file.txt", store=store)
    new = loc.with_name("other.bin")
    assert new.parts == ("dir", "other.bin")
    with pytest.raises(ValueError):
        loc.with_name("a/b")  # invalid, contains slash


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_with_suffix(store, loc_cls) -> None:
    loc = loc_cls("dir", "file", store=store)
    assert loc.with_suffix(".txt").name == "file.txt"
    loc2 = loc_cls("dir", "file.tar.gz", store=store)
    assert loc2.with_suffix(".xz").name == "file.tar.xz"

    with pytest.raises(ValueError):
        loc_cls(store=store).with_suffix(".txt")  # empty name (root)
    with pytest.raises(ValueError):
        loc2.with_suffix("txt")  # must start with dot
