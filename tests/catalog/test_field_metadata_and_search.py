"""Regression tests for catalog field metadata and search compatibility."""

from __future__ import annotations

from LiuXin_alpha.catalog.field_metadata import (
    CalibreFieldMetadata,
    FieldMetadata,
    fm_from_dict,
)
from LiuXin_alpha.catalog.search import KeyPairSearch, LRUCache, Search


def test_field_metadata_has_a_truthful_mapping_surface() -> None:
    for metadata in (FieldMetadata(), CalibreFieldMetadata()):
        assert metadata
        assert len(metadata) == len(metadata.keys())
        assert dict(metadata.items()) == metadata.copy()
        assert list(metadata.itervalues()) == list(metadata.values())
        assert metadata.label_to_key("title") == "title"


def test_field_metadata_deserializes_plain_python_mappings() -> None:
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
    field_values = lambda: [({"isbn": "123"}, {7})]
    assert KeyPairSearch()("isbn:123", field_values, {7}, False) == {7}

    cache = LRUCache(limit=2)
    cache.add("first", {1})
    cache.add("second", {2})
    assert list(cache) == [("first", {1}), ("second", {2})]


def test_populate_all_locations_treats_strings_as_column_names() -> None:
    locations = Search.populate_all_locations(
        {"title": "work_title", "agents": ("agent_name", "agent_sort")}
    )

    assert locations["all"] == ("agent_name", "agent_sort", "work_title")

