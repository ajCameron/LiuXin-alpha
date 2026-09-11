"""
Check exact-key value equality and intentionally absent native-path conversions.

Ordering, bytes conversion, and os.fspath reject Location. Repr checks require
identity field labels without pinning the entire rendered representation.
"""

from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage import api


def test_value_equality_uses_store_uuid_and_exact_key(store) -> None:
    """
    Compare two identical address values and one with a repeated separator. Equal UUID/key pairs
    compare equal, while a different exact key remains distinct without normalization.

    Example:
        >>> test_value_equality_uses_store_uuid_and_exact_key(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    same = api.Location(store.store_ref, "a/b")
    clone = api.Location(store.store_ref, "a/b")
    different_key = api.Location(store.store_ref, "a//b")

    assert same == clone
    assert same != different_key


def test_location_is_not_orderable_or_byte_encodable(location) -> None:
    """
    Require TypeError from ordering, bytes conversion, and os.fspath on the supplied Location. No
    native-path representation is supplied by these operations.

    Example:
        >>> test_location_is_not_orderable_or_byte_encodable(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(TypeError):
        _ = location < location
    with pytest.raises(TypeError):
        bytes(location)
    with pytest.raises(TypeError):
        os.fspath(location)


def test_location_repr_names_identity_fields(location) -> None:
    """
    Require repr to include the store_ref field label and the fixture's exact key field. The
    assertion does not pin the full UUID or complete repr format.

    Example:
        >>> test_location_repr_names_identity_fields(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    rendered = repr(location)
    assert "store_ref=" in rendered
    assert "key='objects/book.epub'" in rendered
