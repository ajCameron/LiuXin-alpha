"""Location construction preserves opaque keys; backends canonicalize inputs."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


@pytest.mark.parametrize("key", ["a//b", "a/./b", "a/../b", "scheme:item", "../opaque"])
def test_plain_location_preserves_opaque_key_exactly(store, key) -> None:
    assert api.Location(store.store_ref, key).key == key


@pytest.mark.parametrize("key", ["a//b", "a/./b", "a/../b", "/absolute", "a\\b"])
def test_filesystem_store_rejects_noncanonical_or_escaping_keys(store, key) -> None:
    with pytest.raises(api.StoreInvalidLocation):
        store.locate(key)


def test_store_location_joins_tokens_using_backend_rules(store) -> None:
    assert store.location("authors", "book.epub").key == "authors/book.epub"
