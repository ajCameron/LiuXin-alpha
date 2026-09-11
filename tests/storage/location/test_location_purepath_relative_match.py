"""
Check absent relative/pattern methods and Store-owned prefix relationships.

Real inventory filtering uses an owned Location. Foreign prefix rejection is
forced by consuming the lazy iterator.
"""

from __future__ import annotations


def test_location_has_no_relative_or_pattern_semantics(location) -> None:
    """
    Require the Location to lack relative_to, is_relative_to, and match attributes without
    evaluating backend hierarchy.

    Example:
        >>> test_location_has_no_relative_or_pattern_semantics(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    for operation in ("relative_to", "is_relative_to", "match"):
        assert not hasattr(location, operation)


def test_backend_prefix_inventory_is_the_portable_relationship(store) -> None:
    """
    Publish sibling subtrees and require an owned a/b inventory prefix to return only its single
    descendant key.

    Example:
        >>> test_backend_prefix_inventory_is_the_portable_relationship(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    store.store_bytes(b"one", location="a/b/one")
    store.store_bytes(b"two", location="a/c/two")

    under_ab = list(store.iter_locations(prefix=store.locate("a/b")))
    assert [location.key for location in under_ab] == ["a/b/one"]


def test_cross_store_prefix_is_rejected(store, second_store) -> None:
    """
    Consume inventory using another Store's prefix and require StoreInvalidLocation. Materializing
    the iterator ensures any deferred ownership check executes.

    Example:
        >>> test_cross_store_prefix_is_rejected(store, second_store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    foreign_prefix = second_store.locate("a")
    try:
        list(store.iter_locations(prefix=foreign_prefix))
    except Exception as error:
        from LiuXin_alpha.storage import api

        assert isinstance(error, api.StoreInvalidLocation)
    else:  # pragma: no cover
        raise AssertionError("foreign prefix was accepted")
