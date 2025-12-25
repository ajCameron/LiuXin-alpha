
from __future__ import annotations

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_relative_to_location(store, loc_cls) -> None:
    base = loc_cls("a", store=store)
    target = loc_cls("a", "b", "c.txt", store=store)
    rel = target.relative_to(base)
    assert rel.parts == ("b", "c.txt")

    with pytest.raises(ValueError):
        _ = base.relative_to(target)


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_relative_to_string(store, loc_cls) -> None:
    target = loc_cls("a", "b", "c.txt", store=store)
    rel = target.relative_to("a")
    assert rel.parts == ("b", "c.txt")
    assert target.is_relative_to("a") is True
    assert target.is_relative_to("x") is False


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_match_semantics(store, loc_cls) -> None:
    p = loc_cls("a", "b", "c.txt", store=store)
    assert p.match("*.txt") is True
    assert p.match("c.*") is True
    assert p.match("a/*/c.txt") is True
    assert p.match("a/*/*.bin") is False
