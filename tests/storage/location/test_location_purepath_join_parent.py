"""Generic code cannot infer hierarchy from opaque keys."""

from __future__ import annotations

import pytest


def test_location_has_no_join_parent_or_division_surface(location) -> None:
    assert not hasattr(location, "joinpath")
    assert not hasattr(location, "parent")
    assert not hasattr(location, "parents")
    with pytest.raises(TypeError):
        _ = location / "child"  # type: ignore[operator]


def test_hierarchical_join_is_an_explicit_store_capability(store) -> None:
    assert store.capabilities.hierarchical_object_addresses
    joined = store.location("a", "b", "c.txt")
    assert joined.key == "a/b/c.txt"


def test_joined_location_remains_scoped_to_store(store) -> None:
    joined = store.location("nested", "object")
    assert joined.store_ref == store.store_ref
