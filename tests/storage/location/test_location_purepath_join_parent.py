
from __future__ import annotations

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation


@pytest.mark.parametrize("loc_cls", [OnDiskExistingManagedStoreLocation, AsyncOnDiskLocation])
def test_joinpath_and_division(store, loc_cls) -> None:
    root = loc_cls(store=store)
    loc = root / "a" / "b" / "c.txt"
    assert loc.parts == ("a", "b", "c.txt")
    assert (root.joinpath("a", "b") / "c.txt").parts == loc.parts


@pytest.mark.parametrize("loc_cls", [OnDiskExistingManagedStoreLocation, AsyncOnDiskLocation])
def test_parent_and_parents(store, loc_cls) -> None:
    root = loc_cls(store=store)
    assert root.parent is root
    assert root.parents == ()

    loc = loc_cls("a", "b", "c", store=store)
    assert loc.parent.parts == ("a", "b")
    assert [p.parts for p in loc.parents] == [("a", "b"), ("a",), ()]
    assert loc.parents[0] == loc.parent


@pytest.mark.parametrize("loc_cls", [OnDiskExistingManagedStoreLocation, AsyncOnDiskLocation])
def test_joinpath_refuses_dotdot(store, loc_cls) -> None:
    root = loc_cls(store=store)
    with pytest.raises(ValueError):
        _ = root.joinpath("a", "..", "b")
