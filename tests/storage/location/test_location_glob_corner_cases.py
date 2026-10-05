"""
Check literal metacharacters in keys and inventory prefixes.

Bracket/star spellings are filesystem key text in these cases. Location exposes
none of the tested glob, recursive-glob, or pattern-match operations.
"""

from __future__ import annotations


def test_location_does_not_treat_backend_keys_as_glob_patterns(store) -> None:
    """
    Resolve a bracket/star-containing key, require its spelling unchanged, and check the Location
    lacks glob, rglob, and match methods. No matching or publication occurs.

    Example:
        >>> test_location_does_not_treat_backend_keys_as_glob_patterns(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    location = store.locate("books/[draft]*.epub")

    assert location.key == "books/[draft]*.epub"
    assert not hasattr(location, "glob")
    assert not hasattr(location, "rglob")
    assert not hasattr(location, "match")


def test_prefix_filter_is_literal_not_a_glob(store) -> None:
    """
    Publish a literal bracket/star filename and a similarly named ordinary file, then require the
    literal bracket prefix to select only the former.

    Example:
        >>> test_prefix_filter_is_literal_not_a_glob(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    store.store_bytes(b"literal", location="books/[draft]*.epub")
    store.store_bytes(b"other", location="books/draft-one.epub")

    matches = list(store.iter_locations(prefix=store.locate("books/[draft]")))
    assert [location.key for location in matches] == ["books/[draft]*.epub"]
