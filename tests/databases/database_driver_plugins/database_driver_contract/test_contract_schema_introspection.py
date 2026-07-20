"""Driver contract: schema introspection.

This module exercises the driver's table/column discovery helpers. These are
core building blocks used throughout the higher-level Database APIs.

The tests are intentionally backend-agnostic and run for every selected driver.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

import pytest

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.schema_specs import LinkKind
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper


def _coerce_str_set(values: Iterable[str]) -> set[str]:
    return {str(v) for v in values}


def _discover_views(driver) -> list[str]:
    """Return view names using sqlite_master.

    Not all drivers expose a direct_* view listing helper, so we fall back to a
    direct SQL query against the connection.
    """

    conn = getattr(driver, "conn", None)
    if conn is None:
        return []

    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()
    except Exception:
        # Some connection wrappers expose .get
        rows = conn.get("SELECT name FROM sqlite_master WHERE type='view'")

    out: list[str] = []
    for r in rows or []:
        if r is None:
            continue
        if isinstance(r, (list, tuple)):
            out.append(str(r[0]))
        else:
            out.append(str(r))
    return out


def test_direct_get_tables_is_deterministic_and_cached(driver) -> None:
    tables_first = driver.direct_get_tables(force_refresh=True)
    tables_second = driver.direct_get_tables()

    assert isinstance(tables_first, list)
    assert tables_first, "Expected at least one table"
    assert _coerce_str_set(tables_first) == _coerce_str_set(tables_second)

    # Basic sanity: we expect some core schema tables to exist in the test DBs.
    tables = _coerce_str_set(tables_first)
    assert "titles" in tables, "Missing expected compatibility relation: titles"
    assert "database_metadata" in tables, "Missing expected metadata relation: database_metadata"
    assert "creators" in tables or "agents" in tables, (
        "Missing expected creator/agent relation: expected creators compatibility "
        "view or canonical agents table"
    )


def test_direct_get_tables_and_columns_is_total_and_stable(driver) -> None:
    tac_first = driver.direct_get_tables_and_columns()
    tac_second = driver.direct_get_tables_and_columns()

    assert isinstance(tac_first, dict)
    assert tac_first, "Expected tables_and_columns to be non-empty"
    assert tac_first.keys() == tac_second.keys()

    exercised_id_helpers = 0
    exercised_datestamp_helpers = 0

    for table, headings in tac_first.items():
        assert isinstance(table, str)
        assert isinstance(headings, list)
        assert headings, f"Expected at least one column in table {table}"
        assert all(isinstance(h, str) for h in headings)

        # direct_get_column_headings should agree with the snapshot
        assert driver.direct_get_column_headings(table) == headings

        # Every table should have an id column and a datestamp column
        if any(h == "id" or h.endswith("_id") for h in headings):
            id_col = driver.direct_get_id_column(table)
            assert id_col in headings
            assert id_col == "id" or id_col.endswith("_id")
            exercised_id_helpers += 1

        if any(
            h == "datestamp"
            or h.endswith("_datestamp")
            or h.endswith("_datestamp_ep_k")
            or h.endswith("_timestamp")
            or h.endswith("_timestamp_ep_k")
            for h in headings
        ):
            ds_col = driver.direct_get_datestamp_column(table)
            assert ds_col in headings
            assert (
                ds_col == "datestamp"
                or ds_col.endswith("_datestamp")
                or ds_col.endswith("_datestamp_ep_k")
                or ds_col.endswith("_timestamp")
                or ds_col.endswith("_timestamp_ep_k")
            )
            exercised_datestamp_helpers += 1

    assert exercised_id_helpers > 0
    assert exercised_datestamp_helpers > 0


def test_declared_column_datatype_is_available_and_strict(driver) -> None:
    assert (
        driver.direct_get_declared_column_datatype(
            "database_metadata",
            "database_metadata_id",
        )
        == "INTEGER"
    )
    assert (
        driver.direct_get_declared_column_datatype(
            "database_metadata",
            "database_metadata_unique_id",
        )
        == "TEXT"
    )

    with pytest.raises(InputIntegrityError, match="column"):
        driver.direct_get_declared_column_datatype(
            "database_metadata",
            "__definitely_not_a_real_column__",
        )

    with pytest.raises(InputIntegrityError, match="table"):
        driver.direct_get_declared_column_datatype(
            "__definitely_not_a_real_table__",
            "id",
        )


def test_declared_column_datatype_cache_is_invalidated_with_schema_cache(driver) -> None:
    driver.direct_get_declared_column_datatype(
        "database_metadata",
        "database_metadata_unique_id",
    )
    declared_types_cache = getattr(driver, "_declared_types_cache")
    assert declared_types_cache

    driver._invalidate_schema_caches()

    assert declared_types_cache == {}


@pytest.mark.parametrize(
    ("table1", "table2", "expected_kind"),
    (
        ("agents", "labels", LinkKind.PLAIN),
        ("languages", "manifestations", LinkKind.TYPED),
        ("tags", "works", LinkKind.PRIORITY),
        ("agents", "works", LinkKind.TYPED_PRIORITY),
    ),
)
def test_direct_link_capabilities_classify_physical_link_columns(
    driver,
    table1: str,
    table2: str,
    expected_kind: LinkKind,
) -> None:
    capabilities = driver.direct_get_link_capabilities(table1, table2)

    assert capabilities is not None
    assert capabilities.kind is expected_kind
    assert capabilities.typed is (
        expected_kind in {LinkKind.TYPED, LinkKind.TYPED_PRIORITY}
    )
    assert capabilities.priority is (
        expected_kind in {LinkKind.PRIORITY, LinkKind.TYPED_PRIORITY}
    )
    assert capabilities.both is (expected_kind is LinkKind.TYPED_PRIORITY)
    assert driver.direct_is_link_typed(table1, table2) is capabilities.typed
    assert (
        driver.direct_is_link_priority(table1, table2)
        is capabilities.priority
    )
    if capabilities.typed:
        assert capabilities.type_column in driver.direct_get_column_headings(
            capabilities.link_table
        )
    if capabilities.priority:
        assert capabilities.priority_column in driver.direct_get_column_headings(
            capabilities.link_table
        )


def test_direct_link_capabilities_distinguish_absent_and_invalid_links(driver) -> None:
    assert (
        driver.direct_get_link_capabilities("agents", "database_metadata")
        is None
    )
    assert driver.direct_is_link_typed("agents", "database_metadata") is False
    assert driver.direct_is_link_priority("agents", "database_metadata") is False

    with pytest.raises(InputIntegrityError, match="table"):
        driver.direct_get_link_capabilities("__missing_table__", "works")
    with pytest.raises(InputIntegrityError, match="table"):
        driver.direct_is_link_typed("__missing_table__", "works")
    with pytest.raises(InputIntegrityError, match="table"):
        driver.direct_is_link_priority("__missing_table__", "works")


def test_direct_link_capabilities_support_intralinks(driver) -> None:
    capabilities = driver.direct_get_link_capabilities("works", "works")

    assert capabilities is not None
    assert capabilities.link_table == "work_work_intralinks"
    assert capabilities.kind is LinkKind.TYPED
    assert capabilities.type_column == "work_work_intralink_type"
    assert capabilities.priority_column is None


def test_column_case_sensitivity_is_persisted_and_strict(driver) -> None:
    assert driver.direct_get_case_sensitivity("tags", "tag") is False
    assert driver.direct_get_case_sensitivity("works", "work_title") is False
    assert driver.direct_get_case_sensitivity("notes", "note") is True
    assert driver.direct_is_column_case_sensitive("tags", "tag") is False

    original = driver.direct_get_case_sensitivity("works", "work_title")
    try:
        driver.direct_set_case_sensitivity("works", "work_title", not original)
        assert driver.direct_get_case_sensitivity("works", "work_title") is not original
    finally:
        driver.direct_set_case_sensitivity("works", "work_title", original)

    with pytest.raises(InputIntegrityError, match="bool"):
        driver.direct_set_case_sensitivity("works", "work_title", 1)
    with pytest.raises(InputIntegrityError, match="column"):
        driver.direct_get_case_sensitivity("works", "__missing_column__")
    with pytest.raises(InputIntegrityError, match="table"):
        driver.direct_get_case_sensitivity("__missing_table__", "title")


def test_column_metadata_policy_is_complete_and_persisted(driver) -> None:
    metadata = driver.direct_get_column_metadata("tags", "tag")

    assert metadata.semantic_role is ColumnSemanticRole.TAXONOMY_TERM
    assert metadata.normalization_profile is ColumnNormalizationProfile.TAG_SEARCH_TERM
    assert metadata.comparison_column == "tag_phash"
    assert metadata.empty_value_policy is ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING
    assert metadata.merge_policy is ColumnMergePolicy.SET_UNION
    assert metadata.validation_profile is ColumnValidationProfile.TAXONOMY_TERM

    identifier_metadata = driver.direct_get_column_metadata("works", "work_id")
    assert identifier_metadata.semantic_role is ColumnSemanticRole.IDENTIFIER
    assert identifier_metadata.validation_profile is ColumnValidationProfile.IDENTIFIER
    assert identifier_metadata.merge_policy is ColumnMergePolicy.PRESERVE_EXISTING

    foreign_key_metadata = driver.direct_get_column_metadata(
        "works",
        "work_original_language_id",
    )
    assert foreign_key_metadata.semantic_role is ColumnSemanticRole.RELATIONSHIP_KEY
    assert foreign_key_metadata.validation_profile is ColumnValidationProfile.IDENTIFIER

    scratch_metadata = driver.direct_get_column_metadata("works", "work_scratch")
    assert scratch_metadata.semantic_role is ColumnSemanticRole.SCRATCH
    assert scratch_metadata.empty_value_policy is ColumnEmptyValuePolicy.PRESERVE
    assert scratch_metadata.validation_profile is ColumnValidationProfile.NONE

    title_metadata = driver.direct_get_column_metadata("works", "work_title")
    changed = replace(
        title_metadata,
        merge_policy=ColumnMergePolicy.PRESERVE_EXISTING,
    )
    try:
        driver.direct_set_column_metadata(changed)
        assert driver.direct_get_column_metadata("works", "work_title") == changed
    finally:
        driver.direct_set_column_metadata(title_metadata)

    with pytest.raises(InputIntegrityError, match="comparison column"):
        driver.direct_set_column_metadata(
            replace(title_metadata, comparison_column="__missing_column__")
        )


def test_normalized_identity_declarations_are_database_backed(driver) -> None:
    tag_spec = driver.direct_get_normalized_identity_spec("tags", "tag")
    assert tag_spec is not None
    assert tag_spec.identity_column == "tag_phash"
    assert tag_spec.scope_columns == ()

    genre_spec = driver.direct_get_normalized_identity_spec("genres", "genre")
    assert genre_spec is not None
    assert genre_spec.identity_column == "genre_phash"
    assert genre_spec.scope_columns == ("genre_parent_id",)

    declarations = tuple(driver.direct_iter_normalized_identity_specs())
    assert tag_spec in declarations
    assert genre_spec in declarations
    assert driver.direct_get_normalized_identity_spec("works", "work_title") is None

    tag_metadata = driver.direct_get_column_metadata("tags", "tag")
    with pytest.raises(InputIntegrityError, match="normalized identity"):
        driver.direct_set_column_metadata(
            replace(
                tag_metadata,
                normalization_profile=ColumnNormalizationProfile.UNICODE_NFC,
            )
        )
    with pytest.raises(InputIntegrityError, match="normalized identity"):
        driver.direct_set_column_metadata(
            replace(tag_metadata, comparison_column=None)
        )
    with pytest.raises(InputIntegrityError, match="normalized identity"):
        driver.direct_set_case_sensitivity("tags", "tag", True)


def test_column_metadata_field_accessors_are_typed_and_persisted(driver) -> None:
    table = "works"
    column = "work_title"
    original = driver.direct_get_column_metadata(table, column)

    assert driver.direct_get_semantic_role(table, column) is original.semantic_role
    assert (
        driver.direct_get_normalization_profile(table, column)
        is original.normalization_profile
    )
    assert driver.direct_get_comparison_column(table, column) == original.comparison_column
    assert (
        driver.direct_get_empty_value_policy(table, column)
        is original.empty_value_policy
    )
    assert driver.direct_get_merge_policy(table, column) is original.merge_policy
    assert (
        driver.direct_get_validation_profile(table, column)
        is original.validation_profile
    )

    expected = replace(
        original,
        semantic_role=ColumnSemanticRole.LABEL,
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC,
        comparison_column="work_sort_title",
        empty_value_policy=ColumnEmptyValuePolicy.PRESERVE,
        merge_policy=ColumnMergePolicy.PRESERVE_EXISTING,
        validation_profile=ColumnValidationProfile.VERBATIM_TEXT,
    )
    try:
        driver.direct_set_semantic_role(table, column, expected.semantic_role)
        driver.direct_set_normalization_profile(
            table,
            column,
            expected.normalization_profile,
        )
        driver.direct_set_comparison_column(
            table,
            column,
            expected.comparison_column,
        )
        driver.direct_set_empty_value_policy(
            table,
            column,
            expected.empty_value_policy,
        )
        driver.direct_set_merge_policy(table, column, expected.merge_policy)
        driver.direct_set_validation_profile(
            table,
            column,
            expected.validation_profile,
        )

        assert driver.direct_get_semantic_role(table, column) is expected.semantic_role
        assert (
            driver.direct_get_normalization_profile(table, column)
            is expected.normalization_profile
        )
        assert (
            driver.direct_get_comparison_column(table, column)
            == expected.comparison_column
        )
        assert (
            driver.direct_get_empty_value_policy(table, column)
            is expected.empty_value_policy
        )
        assert driver.direct_get_merge_policy(table, column) is expected.merge_policy
        assert (
            driver.direct_get_validation_profile(table, column)
            is expected.validation_profile
        )
        assert driver.direct_get_column_metadata(table, column) == expected
    finally:
        driver.direct_set_column_metadata(original)

    with pytest.raises(InputIntegrityError, match="comparison column"):
        driver.direct_set_comparison_column(table, column, "__missing_column__")


def test_column_naming_helpers_match_pluralizer(driver) -> None:
    tables = _coerce_str_set(driver.direct_get_tables(force_refresh=True))

    # Representative sample: test common tables if present.
    sample = [
        "titles",
        "creators",
        "books",
        "series",
        "tags",
        "languages",
        "publishers",
        "notes",
        "identifiers",
    ]

    for table in sample:
        if table not in tables:
            continue

        expected = plural_singular_mapper(table)
        assert driver.direct_get_column_name(table) == expected

        # Some drivers implement this helper via a mixin staticmethod.
        assert driver.direct_get_column_base(table) == expected


def test_validate_existing_table_name_accepts_real_and_rejects_controls(driver) -> None:
    tables = driver.direct_get_tables(force_refresh=True)
    assert tables

    table = str(tables[0])

    # Exact name should validate
    assert driver.direct_validate_existing_table_name(table) is True

    # Whitespace is stripped by the implementation.
    assert driver.direct_validate_existing_table_name(f"  {table}  ") is True

    # Control characters that are explicitly forbidden should fail.
    assert driver.direct_validate_existing_table_name(f"{table};") is False
    assert driver.direct_validate_existing_table_name(f"{table}:") is False
    assert driver.direct_validate_existing_table_name(f"{table}&") is False


def test_unknown_table_raises_input_integrity(driver) -> None:
    with pytest.raises(InputIntegrityError):
        driver.direct_get_column_headings("__definitely_not_a_real_table__")

    with pytest.raises(InputIntegrityError):
        driver.direct_get_id_column("__definitely_not_a_real_table__")


def test_view_introspection_if_views_exist(driver) -> None:
    views = _discover_views(driver)
    if not views:
        pytest.skip("No SQL views found in this test database")

    conn = getattr(driver, "conn", None)
    assert conn is not None

    # Only exercise views that have an 'id' column because the helper hardcodes it.
    exercised = 0
    for view in views:
        headings = driver.direct_get_view_column_headings(view)
        assert isinstance(headings, list)
        assert headings, f"View {view} returned no headings"

        if "id" not in headings:
            continue

        try:
            row = conn.execute(f"SELECT id FROM {view} LIMIT 1").fetchone()
        except Exception:
            row = None

        if not row:
            continue

        row_id = row[0]
        result = driver.direct_get_view_row_dict_from_id(view, row_id)
        if result is False:
            continue

        assert isinstance(result, dict)
        assert "id" in result
        # Keys should be a subset of headings.
        assert set(result.keys()).issubset(set(headings))
        exercised += 1

    if exercised == 0:
        pytest.skip("Views exist, but none with usable 'id' rows were found")
