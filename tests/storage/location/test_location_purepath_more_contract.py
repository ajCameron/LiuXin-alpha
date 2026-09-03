"""Location equality is value equality, not PurePath behavior."""

from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage import api


def test_value_equality_uses_store_uuid_and_exact_key(store) -> None:
    same = api.Location(store.store_ref, "a/b")
    clone = api.Location(store.store_ref, "a/b")
    different_key = api.Location(store.store_ref, "a//b")

    assert same == clone
    assert same != different_key


def test_location_is_not_orderable_or_byte_encodable(location) -> None:
    with pytest.raises(TypeError):
        _ = location < location
    with pytest.raises(TypeError):
        bytes(location)
    with pytest.raises(TypeError):
        os.fspath(location)


def test_location_repr_names_identity_fields(location) -> None:
    rendered = repr(location)
    assert "store_ref=" in rendered
    assert "key='objects/book.epub'" in rendered
