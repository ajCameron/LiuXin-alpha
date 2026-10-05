"""
Separate opaque Location construction from filesystem key parsing and joining.

The value preserves supplied nonempty text while the Store rejects selected
noncanonical/traversing spellings and owns hierarchical token joining.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


@pytest.mark.parametrize("key", ["a//b", "a/./b", "a/../b", "scheme:item", "../opaque"])
def test_plain_location_preserves_opaque_key_exactly(store, key) -> None:
    """
    Construct Location directly for each unusual but nonempty key and require exact spelling,
    including repeated separators, dot segments, or scheme-like text. Store path validation is
    deliberately not invoked.

    Example:
        >>> test_plain_location_preserves_opaque_key_exactly(store, key)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param key: Parameterized key spelling whose exact retention or rejection is asserted.
    :return: None after the stated contract assertions pass.
    """
    assert api.Location(store.store_ref, key).key == key


@pytest.mark.parametrize("key", ["a//b", "a/./b", "a/../b", "/absolute", "a\\b"])
def test_filesystem_store_rejects_noncanonical_or_escaping_keys(store, key) -> None:
    """
    Require filesystem locate to reject the parameterized repeated/dot/parent/absolute/backslash
    spelling with StoreInvalidLocation.

    Example:
        >>> test_filesystem_store_rejects_noncanonical_or_escaping_keys(store, key)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param key: Parameterized key spelling whose exact retention or rejection is asserted.
    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(api.StoreInvalidLocation):
        store.locate(key)


def test_store_location_joins_tokens_using_backend_rules(store) -> None:
    """
    Ask the filesystem Store to join two tokens and require the canonical slash-separated key
    without creating a file.

    Example:
        >>> test_store_location_joins_tokens_using_backend_rules(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    assert store.location("authors", "book.epub").key == "authors/book.epub"
