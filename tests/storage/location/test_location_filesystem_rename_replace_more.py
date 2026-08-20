"""Copy and move replace path-like rename/replace methods."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_copy_requires_explicit_replacement(store) -> None:
    source = store.store_bytes(b"source", location="objects/source")
    destination = store.store_bytes(b"old", location="objects/destination")

    with pytest.raises(api.StoreAlreadyExists):
        store.copy(source.location, destination.location)
    copied = store.copy(
        source.location,
        destination.location,
        mode=api.WriteMode.REPLACE,
    )
    assert store.read_bytes(copied.location) == b"source"


def test_move_returns_destination_and_removes_only_source_version(store) -> None:
    source = store.store_bytes(b"move-me", location="objects/source")
    destination = store.locate("archive/destination")

    moved = store.move(source.location, destination)

    assert moved.location == destination
    assert store.read_bytes(destination) == b"move-me"
    assert not store.exists(source.location)


def test_cross_store_move_refuses_unprotected_source_before_copy(
    manager, store, second_store,
) -> None:
    source = store.store_bytes(b"cross-store", location="source")
    destination = second_store.locate("destination")

    with pytest.raises(api.StoreUnsupportedOperation):
        manager.move(source.location, destination)

    assert manager.read_bytes(source.location) == b"cross-store"
    assert not manager.exists(destination)
