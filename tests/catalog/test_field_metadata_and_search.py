"""
Verify test field metadata and search behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test field metadata and search through its owning regression module::

        python -m pytest -q tests/catalog/test_field_metadata_and_search.py
"""

from __future__ import annotations

from LiuXin_alpha.catalog.field_metadata import (
    CalibreFieldMetadata,
    FieldMetadata,
    fm_from_dict,
)
from LiuXin_alpha.catalog.search import KeyPairSearch, LRUCache, Search


def test_field_metadata_has_a_truthful_mapping_surface() -> None:
    """
    Verify field metadata has a truthful mapping surface.

    Example:
        Exercise test field metadata has a truthful mapping surface through its owning regression module::

            python -m pytest -q tests/catalog/test_field_metadata_and_search.py


    :return: None; the function records state or raises through its assertions.
    """
    for metadata in (FieldMetadata(), CalibreFieldMetadata()):
        assert metadata
        assert len(metadata) == len(metadata.keys())
        assert dict(metadata.items()) == metadata.copy()
        assert list(metadata.itervalues()) == list(metadata.values())
        assert metadata.label_to_key("title") == "title"


def test_field_metadata_deserializes_plain_python_mappings() -> None:
    """
    Verify field metadata deserializes plain python mappings.

    Example:
        Exercise test field metadata deserializes plain python mappings through its owning regression module::

            python -m pytest -q tests/catalog/test_field_metadata_and_search.py


    :return: None; the function records state or raises through its assertions.
    """
    restored = fm_from_dict(
        {
            "custom_fields": {},
            "user_categories": {
                "@mine": {
                    "kind": "user",
                    "label": "@mine",
                    "search_terms": ["@mine"],
                }
            },
            "search_categories": {},
            "search_term_map": {"mine": "@mine"},
            "custom_label_to_key_map": {},
        }
    )

    assert "@mine" in restored
    assert restored.search_term_to_field_key("mine") == "@mine"


def test_search_helpers_use_python_three_mapping_iteration() -> None:
    """
    Verify search helpers use python three mapping iteration.

    Example:
        Exercise test search helpers use python three mapping iteration through its owning regression module::

            python -m pytest -q tests/catalog/test_field_metadata_and_search.py


    :return: None; the function records state or raises through its assertions.
    """
    field_values = lambda: [({"isbn": "123"}, {7})]
    assert KeyPairSearch()("isbn:123", field_values, {7}, False) == {7}

    cache = LRUCache(limit=2)
    cache.add("first", {1})
    cache.add("second", {2})
    assert list(cache) == [("first", {1}), ("second", {2})]


def test_populate_all_locations_treats_strings_as_column_names() -> None:
    """
    Verify populate all locations treats strings as column names.

    Example:
        Exercise test populate all locations treats strings as column names through its owning regression module::

            python -m pytest -q tests/catalog/test_field_metadata_and_search.py


    :return: None; the function records state or raises through its assertions.
    """
    locations = Search.populate_all_locations(
        {"title": "work_title", "agents": ("agent_name", "agent_sort")}
    )

    assert locations["all"] == ("agent_name", "agent_sort", "work_title")

