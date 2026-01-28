"""Driver contract: row conversion + sentinel/null-row helpers.

These tests lock down a few conventions that make higher-level behaviours
predictable across different SQLite backends (sqlite3 vs sqlite3+APSW wrapper).

Contract points:
- Row dicts should preserve numeric types where possible (INTEGER stays int).
- Low-level helper queries should use binding for *values* (not string concat).
- Tables may optionally contain a sentinel/null row at id=0; helpers should
  expose it explicitly without forcing general iterators to include it.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import InputIntegrityError


def test_iterator_return_preserves_numeric_types_without_table_context(driver):
    # iterator_return is used for ad-hoc statements; when no table context is
    # provided we still want conservative, sensible typing.
    stmt = "SELECT 1 AS id, 42 AS n, 3.5 AS f, 'x' AS t;"
    headings = ["id", "n", "f", "t"]

    rows = list(driver.iterator_return(stmt, headings=headings, table=None))
    assert rows and isinstance(rows[0], dict)

    r = rows[0]
    assert r["id"] == 1 and isinstance(r["id"], int)
    assert r["n"] == 42 and isinstance(r["n"], int)
    assert r["f"] == 3.5 and isinstance(r["f"], float)
    assert r["t"] == "x" and isinstance(r["t"], str)


def test_get_table_sqlite_is_binding_safe_for_injection_shaped_names(driver):
    # Historically get_table_sqlite used string formatting and would throw
    # sqlite OperationalError if passed a quote. It should now be inert.
    with pytest.raises(InputIntegrityError):
        driver.get_table_sqlite("titles' OR 1=1 --")


def test_null_row_helpers_on_series_are_explicit_and_non_destructive(driver):
    # Not all schemas guarantee sentinel rows, but LiuXin/Calibre series does.
    if not driver.direct_has_null_row("series"):
        pytest.skip("This schema has no series sentinel row")

    null_row = driver.direct_get_null_row("series")
    assert null_row is not False
    id_col = driver._get_id_column("series")
    assert null_row[id_col] == 0

    # Update a low-risk column (prefer *_scratch) and restore it.
    headings = driver.direct_get_column_headings("series")
    scratch_col = next((c for c in headings if c.endswith("_scratch")), None)
    if scratch_col is None:
        pytest.skip("No scratch column available to safely mutate")

    old = null_row.get(scratch_col)
    marker = "__contract_null_row_marker__"
    assert driver.direct_update_null_row("series", **{scratch_col: marker}) is True

    updated = driver.direct_get_null_row("series")
    assert updated.get(scratch_col) == marker

    # Restore original value to avoid coupling between tests.
    driver.direct_update_null_row("series", **{scratch_col: old})


def test_null_row_helpers_on_tables_without_sentinel_row(driver, pick_payload):
    table = "contract_no_null_row"

    driver.direct_executescript(
        f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT
        );
        """
    )

    driver.direct_execute(f"INSERT INTO {table} (payload) VALUES (?);", (pick_payload(1),))

    assert driver.direct_has_null_row(table) is False
    assert driver.direct_get_null_row(table) is False
    with pytest.raises(InputIntegrityError):
        driver.direct_update_null_row(table, payload="x")
