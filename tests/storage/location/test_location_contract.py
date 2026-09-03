"""Core durable Location value contracts."""

from __future__ import annotations

import dataclasses
import json
import pickle

import pytest

from LiuXin_alpha.storage import api


def test_location_is_frozen_hashable_and_serializable(location) -> None:
    clone = api.Location(location.store_ref, location.key)

    assert clone == location
    assert hash(clone) == hash(location)
    assert pickle.loads(pickle.dumps(location)) == location
    assert json.loads(json.dumps(dataclasses.asdict(location), default=str)) == {
        "store_ref": str(location.store_ref),
        "key": location.key,
    }
    with pytest.raises(dataclasses.FrozenInstanceError):
        location.key = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("key", ["", "bad\x00key"])
def test_location_rejects_keys_that_cannot_be_persisted(store, key) -> None:
    with pytest.raises(ValueError):
        api.Location(store.store_ref, key)


def test_location_requires_resolved_store_uuid() -> None:
    with pytest.raises(TypeError, match="UUID"):
        api.Location("primary", "object")  # type: ignore[arg-type]
