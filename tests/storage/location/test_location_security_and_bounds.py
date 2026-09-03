"""Filesystem boundaries are enforced when the owning Store interprets a key."""

from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage import api


@pytest.mark.parametrize("key", ["../escape", "nested/../../escape", "/absolute"])
def test_filesystem_store_refuses_traversal(store, key) -> None:
    with pytest.raises(api.StoreInvalidLocation):
        store.store_bytes(b"escape", location=key)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_filesystem_store_refuses_final_symlink_escape(store, tmp_path) -> None:
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    link = store.root_path / "linked.bin"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(api.StoreInvalidLocation):
        store.read_bytes(store.locate("linked.bin"))


def test_bound_location_cannot_bypass_store_scope(manager, second_store) -> None:
    unknown = api.Location(second_store.store_ref, "../escape")
    with pytest.raises(api.StoreInvalidLocation):
        manager.bind(unknown).read_bytes()
