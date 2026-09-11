"""
Check Location identity, frozen fields, serialization, and construction guards.

Pickle round trips are local to this runtime; JSON uses dataclass projection and
default=str for the UUID. These tests do not validate backend key syntax.
"""

from __future__ import annotations

import dataclasses
import json
import pickle

import pytest

from LiuXin_alpha.storage import api


def test_location_is_frozen_hashable_and_serializable(location) -> None:
    """
    Compare an equivalent Location, its hash, a local pickle round trip, and JSON from
    dataclasses.asdict with UUID stringification. Assigning a new key must raise
    FrozenInstanceError; no backend lookup is involved.

    Example:
        >>> test_location_is_frozen_hashable_and_serializable(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
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
    """
    Require direct Location construction to reject the parameterized empty or NUL-containing key
    with ValueError. This covers those value guards independently of filesystem parsing.

    Example:
        >>> test_location_rejects_keys_that_cannot_be_persisted(store, key)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param key: Parameterized key spelling whose exact retention or rejection is asserted.
    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(ValueError):
        api.Location(store.store_ref, key)


def test_location_requires_resolved_store_uuid() -> None:
    """
    Require a Store-name string in the identity field to raise TypeError mentioning UUID. Location
    construction does not resolve names into configured Store identities.

    Example:
        >>> test_location_requires_resolved_store_uuid()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(TypeError, match="UUID"):
        api.Location("primary", "object")  # type: ignore[arg-type]
