"""Behavioral contracts for catalog field metadata containers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import LiuXin_alpha.catalog.field_metadata as field_metadata_module
from LiuXin_alpha.catalog.field_metadata import (
    CalibreFieldMetadata,
    FieldMetadata,
    calibre_name_to_liuxin_name,
)


MetadataFactory = Callable[[], FieldMetadata | CalibreFieldMetadata]


@pytest.fixture(params=(FieldMetadata, CalibreFieldMetadata), ids=("liuxin", "calibre"))
def metadata(request: pytest.FixtureRequest) -> FieldMetadata | CalibreFieldMetadata:
    factory: MetadataFactory = request.param
    return factory()


def _add_custom_field(
    metadata: FieldMetadata | CalibreFieldMetadata,
    *,
    label: str,
    datatype: str = "text",
    colnum: int = 1,
    display: dict[str, Any] | None = None,
    is_category: bool = False,
    is_csp: bool = False,
    is_editable: bool = True,
    is_multiple: dict[str, str | None] | None = None,
    in_table: str = "books",
    name: str | None = None,
) -> None:
    metadata.add_custom_field(
        label=label,
        table=f"custom_column_{colnum}",
        column="value",
        datatype=datatype,
        colnum=colnum,
        name=name or f"Custom {label}",
        display=display or {},
        is_editable=is_editable,
        is_multiple=is_multiple or {},
        is_category=is_category,
        is_csp=is_csp,
        in_table=in_table,
    )


def test_mapping_surface_tracks_the_live_metadata_mapping(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    keys = list(metadata.keys())

    assert list(metadata) == keys
    assert list(metadata.iterkeys()) == keys
    assert list(metadata.itervalues()) == list(metadata.values())
    assert list(metadata.iteritems()) == metadata.items()
    assert metadata.copy() == dict(metadata.items())
    assert metadata.all_metadata() == metadata.copy()
    assert len(metadata) == len(keys)
    assert metadata
    assert metadata.has_key("title")
    assert "title_sort" in metadata
    assert metadata["title_sort"] is metadata["sort"]
    assert metadata.get("missing") is None

    with pytest.raises(AttributeError, match="forbidden"):
        metadata["new"] = {}  # type: ignore[index]

    metadata.add_search_category("@temporary", "Temporary")
    del metadata["@temporary"]
    assert "@temporary" not in metadata

    metadata._tb_cats.clear()
    assert not metadata


def test_field_classification_and_metadata_views(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(metadata, label="plain", colnum=1)
    _add_custom_field(metadata, label="computed", datatype="composite", colnum=2)
    _add_custom_field(metadata, label="saga", datatype="series", colnum=3)
    metadata.add_user_category("@Shelf", "Shelf")

    assert "title" in metadata.standard_field_keys()
    assert "#plain" not in metadata.standard_field_keys()
    assert set(metadata.custom_field_keys()) == {"#plain", "#computed", "#saga"}
    assert set(metadata.custom_field_keys(False)) == {"#plain", "#saga"}
    assert set(metadata.custom_field_metadata()) == {"#plain", "#computed", "#saga"}
    assert set(metadata.custom_field_metadata(False)) == {"#plain", "#saga"}
    assert "news" not in metadata.all_field_keys()
    assert "@Shelf" not in metadata.all_field_keys()

    assert "#plain" in metadata.sortable_field_keys()
    assert "news" not in metadata.sortable_field_keys()
    assert "#saga" in metadata.displayable_field_keys()
    assert "#saga_index" not in metadata.displayable_field_keys()
    assert {"au_map", "cover", "marked", "ondevice", "series_sort"}.isdisjoint(
        metadata.displayable_field_keys()
    )

    assert metadata.is_custom_field("#plain")
    assert not metadata.is_custom_field("title")
    assert metadata.is_ignorable_field("#plain")
    assert metadata.is_ignorable_field("@Shelf")
    assert not metadata.is_ignorable_field("title")
    assert {"#plain", "#computed", "#saga", "#saga_index", "@Shelf"}.issubset(
        metadata.ignorable_field_keys()
    )

    assert metadata.is_series_index("series_index")
    assert metadata.is_series_index("#saga_index")
    assert not metadata.is_series_index("#saga")
    assert not metadata.is_series_index("missing")
    assert not metadata.is_series_index(None)  # type: ignore[arg-type]


def test_key_and_label_resolution_prefers_the_requested_namespace(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(metadata, label="title", colnum=1)
    _add_custom_field(metadata, label="mood", colnum=2)
    metadata.add_user_category("@Shelf", "Shelf")
    metadata._tb_cats["internal"] = {
        "kind": "category",
        "label": "public-label",
        "is_custom": False,
    }

    assert metadata.key_to_label("#mood") == "mood"
    assert metadata.key_to_label("@Shelf") == "@Shelf"
    assert metadata.label_to_key("title") == "title"
    assert metadata.label_to_key("title", prefer_custom=True) == "#title"
    assert metadata.label_to_key("mood") == "#mood"
    assert metadata.label_to_key("authors", prefer_custom=True) == "authors"
    assert metadata.label_to_key("public-label") == "internal"

    with pytest.raises(ValueError, match="Unknown key"):
        metadata.label_to_key("does-not-exist")
    with pytest.raises(ValueError, match="Unknown key"):
        metadata.label_to_key("does-not-exist", prefer_custom=True)


def test_custom_fields_reject_invalid_or_conflicting_definitions(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    with pytest.raises(ValueError, match="Unknown datatype"):
        _add_custom_field(metadata, label="invalid", datatype="not-a-type")

    _add_custom_field(metadata, label="mood", colnum=1)

    with pytest.raises(ValueError, match="Duplicate custom field"):
        _add_custom_field(metadata, label="mood", colnum=2)

    with pytest.raises(ValueError, match="Unknown datatype"):
        _add_custom_field(metadata, label="mood", colnum=1, datatype="not-a-type")


def test_custom_field_refresh_is_idempotent_and_builds_series_companions(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(metadata, label="mood", colnum=1)
    original = metadata["#mood"]

    _add_custom_field(
        metadata,
        label="mood",
        colnum=1,
        display={"description": "Refreshed text"},
    )
    assert metadata["#mood"] is original
    assert metadata["#mood"]["display"] == {"description": "Refreshed text"}

    metadata._search_term_map.pop("#mood")

    _add_custom_field(
        metadata,
        label="mood",
        datatype="series",
        colnum=1,
        display={"description": "A series"},
        is_category=True,
        is_csp=True,
        is_editable=False,
    )

    assert metadata["#mood"] is original
    assert metadata["#mood"]["datatype"] == "series"
    assert metadata["#mood"]["display"] == {"description": "A series"}
    assert metadata["#mood"]["is_category"] is True
    assert metadata["#mood"]["is_csp"] is True
    assert metadata["#mood"]["is_editable"] is False
    assert metadata.search_term_to_field_key("#mood") == "#mood"
    assert metadata["#mood_index"]["datatype"] == "float"
    assert metadata["#mood_index"]["in_table"] == "books"
    assert metadata.label_to_key("mood_index") == "#mood_index"

    index_record = metadata["#mood_index"]
    metadata._search_term_map.pop("#mood_index")
    _add_custom_field(metadata, label="mood", datatype="series", colnum=1)

    assert metadata["#mood_index"] is index_record
    assert metadata.search_term_to_field_key("#mood_index") == "#mood_index"

    _add_custom_field(metadata, label="mood", datatype="series", colnum=1)
    assert metadata["#mood_index"] is index_record


def test_new_custom_series_creates_a_searchable_index_field(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(
        metadata,
        label="cycle",
        datatype="series",
        colnum=7,
        in_table="titles",
    )

    assert metadata["#cycle"]["in_table"] == "titles"
    assert metadata["#cycle_index"]["in_table"] == "titles"
    assert metadata.is_series_index("#cycle_index")
    assert metadata.search_term_to_field_key("#cycle_index") == "#cycle_index"
    assert ("#cycle", metadata["#cycle"]) in list(metadata.custom_iteritems())


def test_record_indexes_resolve_builtin_and_custom_name_collisions(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(metadata, label="title", colnum=1)
    _add_custom_field(metadata, label="mood", colnum=2)
    _add_custom_field(metadata, label="cycle", datatype="series", colnum=3)

    metadata.set_field_record_index("title", 10)
    metadata.set_field_record_index("title", 20, prefer_custom=True)
    metadata.set_field_record_index("mood", 30)
    metadata.set_field_record_index("authors", 40, prefer_custom=True)
    metadata.set_field_record_index("cycle", 50)

    assert metadata["title"]["rec_index"] == 10
    assert metadata["#title"]["rec_index"] == 20
    assert metadata["#mood"]["rec_index"] == 30
    assert metadata["authors"]["rec_index"] == 40
    assert metadata.cc_series_index_column_for("#cycle") == 51


def test_dynamic_categories_register_aliases_and_can_be_removed(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    metadata.add_user_category("@Shelf", "Shelf")
    metadata.add_search_category("saved-searches", "Saved searches")

    assert metadata.search_term_to_field_key("@Shelf") == "@Shelf"
    assert metadata.search_term_to_field_key("@shelf") == "@Shelf"
    assert metadata["@Shelf"]["kind"] == "user"
    assert metadata["saved-searches"]["kind"] == "search"

    with pytest.raises(ValueError, match="Duplicate user field"):
        metadata.add_user_category("@Shelf", "Again")
    with pytest.raises(ValueError, match="Duplicate user field"):
        metadata.add_search_category("saved-searches", "Again")

    metadata._search_term_map.pop("@Shelf")
    metadata.remove_user_categories()
    assert "@Shelf" not in metadata
    assert metadata.search_term_to_field_key("@shelf") == "@shelf"
    assert "saved-searches" in metadata

    metadata.add_user_category("@Again", "Again")
    metadata.add_user_category("@lower", "Lower")
    metadata._search_term_map.pop("@Again")
    metadata.remove_dynamic_categories()
    assert "@Again" not in metadata
    assert "@lower" not in metadata
    assert metadata.search_term_to_field_key("@again") == "@again"
    assert "saved-searches" not in metadata


def test_grouped_search_terms_replace_old_groups_and_keep_builtin_terms(
    metadata: FieldMetadata | CalibreFieldMetadata,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata.add_grouped_search_terms(
        {
            "people": ["authors", "publisher"],
            "dates": ["pubdate", "timestamp"],
        }
    )
    assert metadata.search_term_to_field_key("people") == ["authors", "publisher"]

    metadata.add_grouped_search_terms({"identity": ["title", "authors"]})
    assert metadata.search_term_to_field_key("people") == "people"
    assert metadata.search_term_to_field_key("identity") == ["title", "authors"]

    metadata.add_grouped_search_terms({"title": ["authors"]})
    assert "Attempt to add duplicate search term" in capsys.readouterr().err
    assert metadata.search_term_to_field_key("title") == "title"

    metadata._add_search_terms_to_map("unused", None)
    with pytest.raises(ValueError, match="duplicate search term"):
        metadata._add_search_terms_to_map("other", ["title"])


def test_search_term_listing_and_searchable_fields_include_custom_fields(
    metadata: FieldMetadata | CalibreFieldMetadata,
) -> None:
    _add_custom_field(metadata, label="mood", colnum=1)

    terms = metadata.get_search_terms()
    searchable = metadata.searchable_fields()

    assert terms[-2:] == ["all", "search"]
    assert terms[:-2] == sorted(terms[:-2])
    assert "#mood" in terms
    assert metadata.search_term_to_field_key("unknown") == "unknown"
    assert {"title", "#mood"}.issubset(searchable)
    assert "au_map" not in searchable
    assert "news" not in searchable


def test_calibre_field_map_sets_builtin_and_custom_record_indexes() -> None:
    metadata = CalibreFieldMetadata()
    _add_custom_field(metadata, label="mood", colnum=1)

    metadata.set_field_record_index_from_field_map({"title": 3, "mood": 7})

    assert metadata["title"]["rec_index"] == 3
    assert metadata["#mood"]["rec_index"] == 7


@pytest.mark.parametrize("factory", (FieldMetadata, CalibreFieldMetadata))
def test_invalid_builtin_datatypes_fail_during_initialization(
    monkeypatch: pytest.MonkeyPatch,
    factory: MetadataFactory,
) -> None:
    original = field_metadata_module._builtin_field_metadata

    def invalid_builtin_metadata() -> list[tuple[str, dict[str, Any]]]:
        fields = original()
        fields[0][1]["datatype"] = "invalid"
        return fields

    monkeypatch.setattr(
        field_metadata_module,
        "_builtin_field_metadata",
        invalid_builtin_metadata,
    )

    with pytest.raises(ValueError, match="Unknown datatype invalid"):
        factory()


@pytest.mark.parametrize(
    ("calibre_name", "liuxin_name"),
    (
        ("publisher", "publishers"),
        ("rating", "ratings"),
        ("comment", "comments"),
        ("cover", "covers"),
        ("genre", "genres"),
        ("authors", "authors"),
    ),
)
def test_calibre_table_names_translate_to_liuxin_names(
    calibre_name: str,
    liuxin_name: str,
) -> None:
    assert calibre_name_to_liuxin_name(calibre_name) == liuxin_name


def test_legacy_calibre_builtin_metadata_declaration_remains_well_formed() -> None:
    fields = field_metadata_module._calibre_builtin_field_metadata()

    assert fields
    assert fields[0][0] == "authors"
    assert all(isinstance(key, str) and isinstance(record, dict) for key, record in fields)
