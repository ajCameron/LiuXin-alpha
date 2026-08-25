"""Inventory is a Store operation with explicit prefix semantics."""

from __future__ import annotations


def test_inventory_yields_files_not_synthetic_directories(store) -> None:
    store.store_bytes(b"a", location="top/a.txt")
    store.store_bytes(b"b", location="top/nested/b.txt")

    assert {location.key for location in store.iter_locations()} == {
        "top/a.txt",
        "top/nested/b.txt",
    }


def test_prefix_inventory_uses_an_owned_location(store) -> None:
    store.store_bytes(b"a", location="alpha/a")
    store.store_bytes(b"b", location="alpha/nested/b")
    store.store_bytes(b"c", location="other/c")

    assert {value.key for value in store.iter_locations(prefix=store.locate("alpha"))} == {
        "alpha/a",
        "alpha/nested/b",
    }
