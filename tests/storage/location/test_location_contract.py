from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation, fs_path


class TestLocationContract:
    """Core invariants that every Location implementation should satisfy."""

    @pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
    def test_store_is_bound_and_immutable(self, store, loc_cls) -> None:
        loc = loc_cls("a", "b", store=store)
        assert loc.store is store
        with pytest.raises(AttributeError):
            loc.store = store  # type: ignore[misc]

    @pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
    def test_tokens_roundtrip(self, store, loc_cls) -> None:
        loc = loc_cls("alpha", "bravo", "charlie", store=store)
        assert loc._tokens == ["alpha", "bravo", "charlie"]

    @pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
    def test_as_store_key_is_absolute_under_root(self, store, loc_cls) -> None:
        root = pathlib.Path(store.url)
        loc = loc_cls("x", "y.txt", store=store)
        key = loc.as_store_key()
        assert isinstance(key, str)
        key_path = pathlib.Path(key)
        assert key_path == fs_path(store, "x", "y.txt")
        assert key_path.resolve().is_relative_to(root.resolve())

    @pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
    def test_root_location_store_key_is_root(self, store, loc_cls) -> None:
        root = pathlib.Path(store.url)
        loc = loc_cls(store=store)
        assert pathlib.Path(loc.as_store_key()) == root
