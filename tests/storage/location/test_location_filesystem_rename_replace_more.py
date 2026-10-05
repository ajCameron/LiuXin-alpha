"""
Check Store copy/move outcomes and cross-Store move refusal.

These cases assert collision policy, ordinary move results, and rejection before
cross-Store copying when source deletion cannot enforce a version precondition.
The local move case does not race a source replacement.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_copy_requires_explicit_replacement(store) -> None:
    """
    Create source and occupied destination objects, require default copy to reject the collision,
    then replace explicitly and read the source bytes at the returned destination.

    Example:
        >>> test_copy_requires_explicit_replacement(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    source = store.store_bytes(b"source", location="objects/source")
    destination = store.store_bytes(b"old", location="objects/destination")

    with pytest.raises(api.StoreAlreadyExists):
        store.copy(source.location, destination.location)
    copied = store.copy(
        source.location,
        destination.location,
        mode=api.WriteMode.REPLACE,
    )
    assert store.read_bytes(copied.location) == b"source"


def test_move_returns_destination_and_removes_only_source_version(store) -> None:
    """
    Move one ordinary local object and require the requested destination, complete destination
    bytes, and absent source. The test does not change the source concurrently or assert a raced
    version-precondition outcome.

    Example:
        >>> test_move_returns_destination_and_removes_only_source_version(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    source = store.store_bytes(b"move-me", location="objects/source")
    destination = store.locate("archive/destination")

    moved = store.move(source.location, destination)

    assert moved.location == destination
    assert store.read_bytes(destination) == b"move-me"
    assert not store.exists(source.location)


def test_cross_store_move_refuses_unprotected_source_before_copy(
    manager, store, second_store,
) -> None:
    """
    Attempt a move between the two filesystem Stores and require StoreUnsupportedOperation. Verify
    source bytes remain and the destination was never published because protected source deletion is
    unavailable.

    Example:
        >>> test_cross_store_move_refuses_unprotected_source_before_copy(manager, store, second_store)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    source = store.store_bytes(b"cross-store", location="source")
    destination = second_store.locate("destination")

    with pytest.raises(api.StoreUnsupportedOperation):
        manager.move(source.location, destination)

    assert manager.read_bytes(source.location) == b"cross-store"
    assert not manager.exists(destination)
