"""
Check UUID-scoped equality, byte routing, and foreign-Store rejection.

Identical key strings address independent real filesystem Stores. A direct stat
on the wrong Store must reject the foreign Location.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_equal_keys_in_different_stores_are_distinct(store, second_store) -> None:
    """
    Create the same key in two Store address spaces without publication, then require unequal
    Locations and two set entries. UUID scope distinguishes identical key text.

    Example:
        >>> test_equal_keys_in_different_stores_are_distinct(store, second_store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    first = store.locate("same/key")
    second = second_store.locate("same/key")

    assert first != second
    assert len({first, second}) == 2


def test_manager_routes_same_key_to_independent_stores(manager, store, second_store) -> None:
    """
    Publish different bytes under identical keys in two real Stores and require manager reads to
    return each owning Store's payload.

    Example:
        >>> test_manager_routes_same_key_to_independent_stores(manager, store, second_store)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    first = store.locate("same/key")
    second = second_store.locate("same/key")
    manager.write_bytes(first, b"first")
    manager.write_bytes(second, b"second")

    assert manager.read_bytes(first) == b"first"
    assert manager.read_bytes(second) == b"second"


def test_store_rejects_a_location_owned_by_another_store(store, second_store) -> None:
    """
    Call stat on one Store with the other Store's address and require StoreInvalidLocation before
    ordinary missing-object handling.

    Example:
        >>> test_store_rejects_a_location_owned_by_another_store(store, second_store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    foreign = second_store.locate("object")
    with pytest.raises(api.StoreInvalidLocation):
        store.stat(foreign)
