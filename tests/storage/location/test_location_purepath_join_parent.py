"""
Check absent generic hierarchy operations and explicit Store-owned joining.

These cases inspect the filesystem capability flag, joined key, and retained UUID
without publishing a file or inferring hierarchy from a generic Location.
"""

from __future__ import annotations

import pytest


def test_location_has_no_join_parent_or_division_surface(location) -> None:
    """
    Require absent joinpath, parent, and parents attributes and a TypeError for slash division.
    Generic Location supplies no tested hierarchy operation.

    Example:
        >>> test_location_has_no_join_parent_or_division_surface(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    assert not hasattr(location, "joinpath")
    assert not hasattr(location, "parent")
    assert not hasattr(location, "parents")
    with pytest.raises(TypeError):
        _ = location / "child"  # type: ignore[operator]


def test_hierarchical_join_is_an_explicit_store_capability(store) -> None:
    """
    Require the filesystem hierarchy capability and the expected three-token joined key. This
    inspects address construction without performing byte I/O.

    Example:
        >>> test_hierarchical_join_is_an_explicit_store_capability(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    assert store.capabilities.hierarchical_object_addresses
    joined = store.location("a", "b", "c.txt")
    assert joined.key == "a/b/c.txt"


def test_joined_location_remains_scoped_to_store(store) -> None:
    """
    Join two tokens through a Store and require the resulting Location to retain that Store's UUID.

    Example:
        >>> test_joined_location_remains_scoped_to_store(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    joined = store.location("nested", "object")
    assert joined.store_ref == store.store_ref
