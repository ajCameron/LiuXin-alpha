"""
Check file-only inventory and explicit owned-Location prefix filtering.

The filesystem Store interprets hierarchy; the generic address adds no directory
or glob methods. Set comparisons deliberately ignore enumeration order.
"""

from __future__ import annotations


def test_inventory_yields_files_not_synthetic_directories(store) -> None:
    """
    Publish two files at different depths and require exactly their keys from inventory, excluding
    synthetic directory entries. Set comparison ignores iteration order.

    Example:
        >>> test_inventory_yields_files_not_synthetic_directories(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    store.store_bytes(b"a", location="top/a.txt")
    store.store_bytes(b"b", location="top/nested/b.txt")

    assert {location.key for location in store.iter_locations()} == {
        "top/a.txt",
        "top/nested/b.txt",
    }


def test_prefix_inventory_uses_an_owned_location(store) -> None:
    """
    Publish two alpha descendants and an unrelated object, then require inventory under the
    Store-owned alpha prefix to contain exactly those descendants.

    Example:
        >>> test_prefix_inventory_uses_an_owned_location(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    store.store_bytes(b"a", location="alpha/a")
    store.store_bytes(b"b", location="alpha/nested/b")
    store.store_bytes(b"c", location="other/c")

    assert {value.key for value in store.iter_locations(prefix=store.locate("alpha"))} == {
        "alpha/a",
        "alpha/nested/b",
    }
