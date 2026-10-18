"""
Protect the single metadata-owned author sort-name policy.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.metadata import author_names
from LiuXin_alpha.metadata.author_names import (
    author_to_author_sort,
    authors_to_sort_string,
)


@pytest.mark.parametrize(
    ("author", "method", "expected"),
    (
        ("", "nocomma", ""),
        ("Plato", "nocomma", "Plato"),
        ("Arthur C Clarke", "copy", "Arthur C Clarke"),
        ("Dr Arthur C Clarke Jr", "nocomma", "Clarke Arthur C Jr"),
        ("Dr Prof", "nocomma", "Dr Prof"),
        ("Jr Sr", "nocomma", "Jr Sr"),
        ("Clarke, Arthur", "comma", "Clarke, Arthur"),
        ("ACME Corporation", "nocomma", "ACME Corporation"),
        ("李 白", "nocomma", "白 李"),
        ("山田 太郎", "nocomma", "太郎 山田"),
    ),
)
def test_author_to_author_sort_preserves_legacy_outputs(
    author: str,
    method: author_names.AuthorSortMethod,
    expected: str,
) -> None:
    """
    Preserve exact outputs from both removed legacy implementations.

    Example:
        Run this parametrized regression with pytest::

            python -m pytest -q tests/metadata/test_author_names.py


    :param author: Source author name supplied by the current case.
    :param method: Explicit author-sort method supplied by the current case.
    :param expected: Exact retained sort value.
    :return: None after the result equals the expected value.
    """

    assert author_to_author_sort(author, method=method) == expected


def test_author_sort_default_is_explicit_but_none_reads_preferences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Distinguish stable comma-default callers from preference-driven compatibility callers.

    Example:
        Run with pytest::

            python -m pytest -q tests/metadata/test_author_names.py


    :param monkeypatch: Pytest fixture used to replace the preference method safely.
    :return: None after default and preference-selected behavior remain distinct.
    """

    monkeypatch.setitem(
        author_names.preferences,
        "author_sort_copy_method",
        "copy",
    )

    assert author_to_author_sort("Ada Lovelace") == "Lovelace, Ada"
    assert author_to_author_sort("Ada Lovelace", method=None) == "Ada Lovelace"


def test_authors_to_sort_string_preserves_order_and_policy() -> None:
    """
    Apply one selected policy to every author and retain encounter order.

    Example:
        >>> test_authors_to_sort_string_preserves_order_and_policy()


    :return: None after comma and no-comma joins produce exact values.
    """

    authors = ["Arthur C Clarke", "Ada Lovelace"]

    assert authors_to_sort_string(authors) == "Clarke, Arthur C & Lovelace, Ada"
    assert authors_to_sort_string(authors, method="nocomma") == (
        "Clarke Arthur C & Lovelace Ada"
    )


def test_legacy_metadata_modules_do_not_reexport_author_sort_policy() -> None:
    """
    Keep the ownership break explicit rather than restoring compatibility aliases.

    Example:
        >>> test_legacy_metadata_modules_do_not_reexport_author_sort_policy()


    :return: None after both former owner modules lack the moved public names.
    """

    from LiuXin_alpha.metadata import ebook_metadata_tools, utils

    for module in (ebook_metadata_tools, utils):
        assert not hasattr(module, "author_to_author_sort")
        assert not hasattr(module, "authors_to_sort_string")
