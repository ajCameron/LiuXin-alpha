"""Generic Locations deliberately provide no glob language."""

from __future__ import annotations


def test_location_does_not_treat_backend_keys_as_glob_patterns(store) -> None:
    location = store.locate("books/[draft]*.epub")

    assert location.key == "books/[draft]*.epub"
    assert not hasattr(location, "glob")
    assert not hasattr(location, "rglob")
    assert not hasattr(location, "match")


def test_prefix_filter_is_literal_not_a_glob(store) -> None:
    store.store_bytes(b"literal", location="books/[draft]*.epub")
    store.store_bytes(b"other", location="books/draft-one.epub")

    matches = list(store.iter_locations(prefix=store.locate("books/[draft]")))
    assert [location.key for location in matches] == ["books/[draft]*.epub"]
