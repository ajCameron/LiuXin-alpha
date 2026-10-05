"""
Compare four literal filesystem inventory prefixes with expected key sets.

The historical glob filename now covers Store prefix selection, not pathlib
pattern syntax or ordering guarantees.
"""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("a", {"a/1.txt", "a/2.bin", "a/b/3.txt", "a/b/c/4.txt"}),
        ("a/b", {"a/b/3.txt", "a/b/c/4.txt"}),
        ("a/b/c", {"a/b/c/4.txt"}),
        ("missing", set()),
    ],
)
def test_prefix_inventory_matrix(store, prefix, expected) -> None:
    """
    Ensure four nested objects exist, enumerate the selected Store-owned prefix, and compare its key
    set with the parameterized expected set. Cases cover three depths and an absent prefix without
    ordering assertions.

    Example:
        >>> test_prefix_inventory_matrix(store, prefix, expected)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param prefix: Parameterized literal inventory prefix interpreted by the filesystem Store.
    :param expected: Expected set of stored keys beneath the selected prefix.
    :return: None after the stated contract assertions pass.
    """
    for key in ("a/1.txt", "a/2.bin", "a/b/3.txt", "a/b/c/4.txt"):
        if not store.exists(store.locate(key)):
            store.store_bytes(key.encode(), location=key)

    assert {item.key for item in store.iter_locations(prefix=store.locate(prefix))} == expected
