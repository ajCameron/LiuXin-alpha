"""Relative-path and pattern operations are intentionally backend-specific."""

from __future__ import annotations


def test_location_has_no_relative_or_pattern_semantics(location) -> None:
    for operation in ("relative_to", "is_relative_to", "match"):
        assert not hasattr(location, operation)


def test_backend_prefix_inventory_is_the_portable_relationship(store) -> None:
    store.store_bytes(b"one", location="a/b/one")
    store.store_bytes(b"two", location="a/c/two")

    under_ab = list(store.iter_locations(prefix=store.locate("a/b")))
    assert [location.key for location in under_ab] == ["a/b/one"]


def test_cross_store_prefix_is_rejected(store, second_store) -> None:
    foreign_prefix = second_store.locate("a")
    try:
        list(store.iter_locations(prefix=foreign_prefix))
    except Exception as error:
        from LiuXin_alpha.storage import api

        assert isinstance(error, api.StoreInvalidLocation)
    else:  # pragma: no cover
        raise AssertionError("foreign prefix was accepted")
