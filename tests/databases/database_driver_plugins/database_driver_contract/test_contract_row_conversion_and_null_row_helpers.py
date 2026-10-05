"""
Check ad-hoc row types, rejection of SQL-shaped table names, and explicit sentinel-row behavior.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_row_conversion_and_null_row_helpers.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import InputIntegrityError


def test_iterator_return_preserves_numeric_types_without_table_context(driver):
    # iterator_return is used for ad-hoc statements; when no table context is
    # provided we still want conservative, sensible typing.
    """
    Require ad-hoc selected integer, float, and text literals to retain their exact values and Python types.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_row_conversion_and_null_row_helpers.py::test_iterator_return_preserves_numeric_types_without_table_context


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    stmt = "SELECT 1 AS id, 42 AS n, 3.5 AS f, 'x' AS t;"
    headings = ["id", "n", "f", "t"]

    rows = list(driver.direct_iterator_return(stmt, headings=headings, table=None))
    assert rows and isinstance(rows[0], dict)

    r = rows[0]
    assert r["id"] == 1 and isinstance(r["id"], int)
    assert r["n"] == 42 and isinstance(r["n"], int)
    assert r["f"] == 3.5 and isinstance(r["f"], float)
    assert r["t"] == "x" and isinstance(r["t"], str)


def test_direct_get_table_sqlite_is_binding_safe_for_injection_shaped_names(driver):
    # Historically get_table_sqlite used string formatting and would throw
    # sqlite OperationalError if passed a quote. It should now be inert.
    """
    Require InputIntegrityError when table lookup receives an SQL-shaped quoted name.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_row_conversion_and_null_row_helpers.py::test_direct_get_table_sqlite_is_binding_safe_for_injection_shaped_names


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        driver.direct_get_table_sqlite("titles' OR 1=1 --")


def test_null_row_helpers_on_series_are_explicit_and_non_destructive(driver):
    # Not all schemas guarantee sentinel rows, but LiuXin/Calibre series does.
    """
    Require the series sentinel ID to be zero, update its scratch field, and restore the old value after successful assertions.

    Skip when the sentinel or scratch column is absent. Restoration is on the success
    path, not in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_row_conversion_and_null_row_helpers.py::test_null_row_helpers_on_series_are_explicit_and_non_destructive


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    if not driver.direct_has_null_row("series"):
        pytest.skip("This schema has no series sentinel row")

    null_row = driver.direct_get_null_row("series")
    assert null_row is not False
    id_col = driver.direct_get_id_column("series")
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
    """
    Create a table containing only a nonzero row and require False sentinel reads plus rejection of sentinel updates.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_row_conversion_and_null_row_helpers.py::test_null_row_helpers_on_tables_without_sentinel_row


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
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
