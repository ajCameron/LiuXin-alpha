"""
Check explicit deletion policy, unsupported versioned deletion, and stat results.

The optional-stat case covers absence followed by a present object, without
injecting unavailable backends or checking every error category.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_delete_is_strict_by_default_and_optionally_idempotent(manager, location) -> None:
    """
    Require deleting an absent Location to raise StoreNotFound by default and complete without error
    when missing_ok=True. No object is published in this case.

    Example:
        >>> test_delete_is_strict_by_default_and_optionally_idempotent(manager, location)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(api.StoreNotFound):
        manager.delete(location)
    manager.delete(location, missing_ok=True)


def test_filesystem_delete_does_not_claim_an_atomic_version_precondition(
    store, location,
) -> None:
    """
    Publish and replace an object, require distinct versions, and reject deletion using the earlier
    version as unsupported. Confirm the replacement bytes remain after rejection.

    Example:
        >>> test_filesystem_delete_does_not_claim_an_atomic_version_precondition(store, location)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    info = store.write_bytes(location, b"first")
    replacement = store.write_bytes(location, b"second", mode=api.WriteMode.REPLACE)

    assert replacement.version != info.version
    with pytest.raises(api.StoreUnsupportedOperation):
        store.delete(location, if_version=info.version)
    assert store.read_bytes(location) == b"second"


def test_stat_and_try_stat_do_not_hide_availability_categories(manager, location) -> None:
    """
    Check that try_stat returns None for absence and stat reports seven bytes after publication. No
    backend-unavailable error is injected, so this case covers only those two lookup outcomes.

    Example:
        >>> test_stat_and_try_stat_do_not_hide_availability_categories(manager, location)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    assert manager.try_stat(location) is None
    manager.write_bytes(location, b"present")
    assert manager.stat(location).size == 7
