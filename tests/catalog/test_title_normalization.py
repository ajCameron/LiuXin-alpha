"""
Protect Catalog-owned fuzzy title normalization and legacy matching.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.matching.legacy_books import find_identical_books
from LiuXin_alpha.catalog.matching.title_normalization import (
    fuzzy_title,
    fuzzy_title_patterns,
    normalise_fuzzy_title,
)


@pytest.mark.parametrize(
    ("title", "expected"),
    (
        ("Great Expectations", "great expectations"),
        ("The Great Gatsby", "great gatsby"),
        ("A Tale of Two Cities", "tale of two cities"),
        ("Title: Subtitle [2024]", "title subtitle 2024"),
        ("One   Two    Three", "one two three"),
        ("Foo-Bar", "foo bar"),
        ("Dune", "dune"),
        ("", ""),
    ),
)
def test_normalise_fuzzy_title_preserves_legacy_outputs(
    title: str,
    expected: str,
) -> None:
    """
    Keep the moved Catalog policy byte-for-byte compatible.

    Example:
        Run this parametrized regression with pytest::

            python -m pytest -q tests/catalog/test_title_normalization.py


    :param title: Source title supplied by the current parametrized case.
    :param expected: Exact legacy comparison key expected for that title.
    :return: None after both public spellings reproduce the expected key.
    """

    assert normalise_fuzzy_title(title) == expected
    assert fuzzy_title(title) == expected


def test_fuzzy_title_patterns_are_nonempty_and_cached() -> None:
    """
    Retain one process-level compiled pattern tuple.

    Example:
        >>> test_fuzzy_title_patterns_are_nonempty_and_cached()


    :return: None after the pattern contents and object identity are checked.
    """

    first = fuzzy_title_patterns()
    assert first
    assert all(len(item) == 2 for item in first)
    assert fuzzy_title_patterns() is first


def _book_index() -> tuple[
    dict[str, list[int]],
    dict[int, set[int]],
    dict[int, str],
]:
    """
    Return the legacy worker-friendly author/title lookup triple.

    Example:
        >>> len(_book_index())
        3


    :return: Fresh author-name, author-book, and book-title mappings.
    """

    return (
        {"tolkien, j.r.r.": [1], "unknown": [99]},
        {1: {10, 11}, 99: {12}},
        {10: "The Lord of the Rings", 11: "The Hobbit", 12: "A Book"},
    )


def test_find_identical_books_uses_title_and_author_policy() -> None:
    """
    Match exact and article-free titles for an indexed author.

    Example:
        >>> test_find_identical_books_uses_title_and_author_policy()


    :return: None after both expected book identifiers are selected.
    """

    exact = SimpleNamespace(
        title="The Lord of the Rings",
        authors=["Tolkien, J.R.R."],
    )
    without_article = SimpleNamespace(
        title="Hobbit",
        authors=["Tolkien, J.R.R."],
    )

    assert find_identical_books(exact, _book_index()) == {10}
    assert find_identical_books(without_article, _book_index()) == {11}


def test_find_identical_books_rejects_unknown_author_or_wrong_title() -> None:
    """
    Return no candidates when either required evidence disagrees.

    Example:
        >>> test_find_identical_books_rejects_unknown_author_or_wrong_title()


    :return: None after both mismatching candidates produce empty sets.
    """

    unknown_author = SimpleNamespace(
        title="Some Book",
        authors=["NonExistent Author"],
    )
    wrong_title = SimpleNamespace(
        title="Silmarillion",
        authors=["Tolkien, J.R.R."],
    )

    assert find_identical_books(unknown_author, _book_index()) == set()
    assert find_identical_books(wrong_title, _book_index()) == set()


def test_find_identical_books_rejects_missing_author_evidence() -> None:
    """
    Retain the legacy requirement for at least one candidate author.

    Example:
        >>> test_find_identical_books_rejects_missing_author_evidence()


    :return: None after an author-free candidate raises TypeError.
    """

    candidate = SimpleNamespace(title="Dune", authors=[])

    with pytest.raises(TypeError):
        find_identical_books(candidate, _book_index())
