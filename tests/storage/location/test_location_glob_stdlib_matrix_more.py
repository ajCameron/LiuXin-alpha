"""Prefix inventory matrix replaces pathlib glob emulation."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("a", {"a/1.txt", "a/2.bin", "a/b/3.txt", "a/b/c/4.txt"}),
        ("a/b", {"a/b/3.txt", "a/b/c/4.txt"}),
        ("a/b/c", {"a/b/c/4.txt"}),
        ("missing", set()),
    ],
)
def test_prefix_inventory_matrix(store, prefix, expected) -> None:
    for key in ("a/1.txt", "a/2.bin", "a/b/3.txt", "a/b/c/4.txt"):
        if not store.exists(store.locate(key)):
            store.store_bytes(key.encode(), location=key)

    assert {item.key for item in store.iter_locations(prefix=store.locate(prefix))} == expected
