"""A Location is always scoped to exactly one configured Store."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_equal_keys_in_different_stores_are_distinct(store, second_store) -> None:
    first = store.locate("same/key")
    second = second_store.locate("same/key")

    assert first != second
    assert len({first, second}) == 2


def test_manager_routes_same_key_to_independent_stores(manager, store, second_store) -> None:
    first = store.locate("same/key")
    second = second_store.locate("same/key")
    manager.write_bytes(first, b"first")
    manager.write_bytes(second, b"second")

    assert manager.read_bytes(first) == b"first"
    assert manager.read_bytes(second) == b"second"


def test_store_rejects_a_location_owned_by_another_store(store, second_store) -> None:
    foreign = second_store.locate("object")
    with pytest.raises(api.StoreInvalidLocation):
        store.stat(foreign)
