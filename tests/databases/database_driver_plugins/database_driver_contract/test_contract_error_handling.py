"""Driver contract: error handling and invalid-input behaviour.

This module is intentionally strict ("fail" mode). It ensures that:

* Invalid table/column names raise *project* exceptions (not raw sqlite exceptions)
* Missing-row reads return False where the driver contract expects it
* Update calls missing an id fail loudly
* SQL-injection-shaped strings are treated as inert input and do not mutate schema
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import (
    InputIntegrityError,
    DatabaseIntegrityError,
    RowIntegrityError,
)


def _refresh_driver_caches(driver) -> None:
    # Many drivers cache table/column metadata; after creating contract tables we must
    # force a refresh so table identification works reliably.
    for attr in ("tables_and_columns",):
        if hasattr(driver, attr):
            setattr(driver, attr, None)


def _ensure_contract_table(driver) -> str:
    # A minimal "main-ish" table: has an *_id column so _get_id_column() works.
    driver.direct_executescript(
        """
        CREATE TABLE IF NOT EXISTS contract_errors (
            contract_error_id INTEGER PRIMARY KEY,
            contract_error_value TEXT
        );
        """
    )
    _refresh_driver_caches(driver)
    return "contract_errors"


def _ensure_noid_table(driver) -> str:
    driver.direct_executescript(
        """
        CREATE TABLE IF NOT EXISTS contract_noid (
            value TEXT
        );
        """
    )
    _refresh_driver_caches(driver)
    return "contract_noid"


def test_validate_existing_table_name_rejects_sql_control_chars(driver):
    assert driver.direct_validate_existing_table_name("titles") is True
    # validate_existing_table_name() strips whitespace: this should still be accepted.
    assert driver.direct_validate_existing_table_name("titles\n") is True

    assert driver.direct_validate_existing_table_name("titles;") is False
    assert driver.direct_validate_existing_table_name("titles:") is False
    assert driver.direct_validate_existing_table_name("titles&") is False


def test_direct_get_column_headings_unknown_table_raises_input_integrity(driver):
    with pytest.raises(InputIntegrityError):
        driver.direct_get_column_headings("definitely_not_a_table")


def test_direct_delete_row_by_id_rejects_injection_shaped_table_name(driver, assert_integrity):
    # Should be rejected at the validation layer, not executed.
    with pytest.raises(InputIntegrityError):
        driver.direct_delete_row_by_id("titles; DROP TABLE titles; --", 1)

    # And the schema should remain intact.
    assert driver.direct_validate_existing_table_name("titles") is True
    assert_integrity(driver)


def test_direct_add_simple_row_dict_with_unknown_columns_fails_loudly(driver):
    # This dict should not map to any known table -> DatabaseIntegrityError.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({"not_a_real_column": "x"})


def test_direct_update_row_dict_missing_id_raises_row_integrity(driver, pick_payload, assert_integrity):
    table = _ensure_contract_table(driver)
    # Create at least one row so the table definitely exists and is visible to the driver.
    driver.direct_add_simple_row_dict({"contract_error_value": pick_payload(0)})
    assert driver.direct_get_record_count(table) >= 1

    # Missing the id column should raise RowIntegrityError.
    with pytest.raises(RowIntegrityError):
        driver.direct_update_row_dict({"contract_error_value": "changed without id"})

    assert_integrity(driver)


def test_direct_search_table_bad_column_raises_input_integrity(driver):
    # Column is not parameterized in the driver; we expect an OperationalError mapped to InputIntegrityError.
    with pytest.raises(InputIntegrityError):
        driver.direct_search_table(table="titles", column="definitely_not_a_column", search_term="x")


def test_direct_get_row_dict_from_id_returns_false_when_missing(driver, pick_payload, assert_integrity):
    table = _ensure_contract_table(driver)
    driver.direct_add_simple_row_dict({"contract_error_value": pick_payload(1)})

    highest = int(driver.direct_get_highest_id(table))
    assert highest >= 1

    assert driver.direct_get_row_dict_from_id(table, highest) is not False
    assert driver.direct_get_row_dict_from_id(table, highest + 10_000) is False

    assert_integrity(driver)


def test_direct_get_id_column_raises_for_tables_without_id(driver):
    table = _ensure_noid_table(driver)
    with pytest.raises(InputIntegrityError):
        driver.direct_get_id_column(table)
