"""Driver contract: schema introspection.

This module exercises the driver's table/column discovery helpers. These are
core building blocks used throughout the higher-level Database APIs.

The tests are intentionally backend-agnostic and run for every selected driver.
"""

from __future__ import annotations

from typing import Iterable

import pytest

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
