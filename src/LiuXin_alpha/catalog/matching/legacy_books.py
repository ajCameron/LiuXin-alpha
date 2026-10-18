"""
Apply Catalog's legacy title-and-author book matching policy.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol

from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower

from .title_normalization import normalise_fuzzy_title


class _BookMatchMetadata(Protocol):
    """
    Describe the metadata evidence consumed by legacy book matching.

    Example:
        >>> from types import SimpleNamespace
        >>> candidate = SimpleNamespace(title="Dune", authors=["Frank Herbert"])
        >>> isinstance(candidate.title, str)
        True
    """

    title: str
    authors: Iterable[str]


_BookMatchIndex = tuple[
    Mapping[str, Iterable[int]],
    Mapping[int, Iterable[int]],
    Mapping[int, str],
]


def find_identical_books(
    metadata: _BookMatchMetadata,
    data: _BookMatchIndex,
) -> set[int]:
    """
    Find indexed books sharing every requested author and a fuzzy title.

    This preserves the historical worker-friendly operation over three supplied
    mappings.  It performs no database access.  Extra candidate authors remain
    acceptable, and an empty metadata author list retains the historical
    ``TypeError`` behavior.

    Example:
        >>> from types import SimpleNamespace
        >>> candidate = SimpleNamespace(title="Dune", authors=["Frank Herbert"])
        >>> data = ({"frank herbert": [1]}, {1: {7}}, {7: "Dune"})
        >>> find_identical_books(candidate, data)
        {7}

    :param metadata: Metadata-like object exposing title and authors.
    :param data: Author-name, author-book, and book-title lookup mappings.
    :return: IDs whose indexed title and author data satisfy the policy.
    """

    author_map, author_book_map, title_map = data
    found_books: set[int] | None = None

    for author in metadata.authors:
        author_ids = author_map.get(icu_lower(author))
        if author_ids is None:
            return set()
        books_by_author = {
            book_id
            for author_id in author_ids
            for book_id in author_book_map.get(author_id, ())
        }
        if found_books is None:
            found_books = books_by_author
        else:
            found_books &= books_by_author
        if not found_books:
            return set()

    if found_books is None:
        raise TypeError("metadata.authors must contain at least one author")

    title_key = normalise_fuzzy_title(metadata.title)
    return {
        book_id
        for book_id in found_books
        if normalise_fuzzy_title(title_map.get(book_id, "")) == title_key
    }


__all__ = ["find_identical_books"]
