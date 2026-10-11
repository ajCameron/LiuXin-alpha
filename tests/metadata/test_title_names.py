"""Exercise the metadata-owned bibliographic title-sort policy."""

from __future__ import annotations

from LiuXin_alpha.metadata.title_names import (
    get_title_sort_pattern,
    title_to_title_sort,
)


def test_title_sort_pattern_is_cached_by_requested_language() -> None:
    assert get_title_sort_pattern("spa") is get_title_sort_pattern("spa")


def test_title_to_title_sort_handles_articles_quotes_and_strict_order() -> None:
    assert title_to_title_sort("The Hobbit") == "Hobbit, The"
    assert title_to_title_sort('"The Hobbit') == "Hobbit, The"
    assert (
        title_to_title_sort("The Hobbit", order="strictly_alphabetic") == "The Hobbit"
    )
    assert title_to_title_sort("El Aleph", lang="spa") == "Aleph, El"


def test_title_to_title_sort_handles_foreign_articles() -> None:
    assert title_to_title_sort("L’Étranger", lang="fra") == "Étranger, L’"
    assert title_to_title_sort("Die Verwandlung", lang="deu") == "Verwandlung, Die"
    assert (
        title_to_title_sort("Las ciudades invisibles", lang="spa")
        == "ciudades invisibles, Las"
    )


def test_title_to_title_sort_normalizes_boundary_values() -> None:
    assert title_to_title_sort(None) == ""
    assert title_to_title_sort(42, order="strictly_alphabetic") == "42"
