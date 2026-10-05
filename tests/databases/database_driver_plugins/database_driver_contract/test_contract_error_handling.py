"""
Check invalid-name and missing-ID exceptions, missing-row sentinels, and schema survival after rejected input.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py
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
    """
    Reset tables_and_columns to None when the driver exposes that attribute.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; mutates the selected metadata cache attribute.
    """
    for attr in ("tables_and_columns",):
        if hasattr(driver, attr):
            setattr(driver, attr, None)


def _ensure_contract_table(driver) -> str:
    # A minimal "main-ish" table: has an *_id column so _get_id_column() works.
    """
    Create the error-test table if absent and reset the driver’s table/column metadata cache.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: Fixed contract_errors table name; existing rows are retained.
    """
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
    """
    Create a value-only table if absent and reset the driver’s table/column metadata cache.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: Fixed contract_noid table name; it has no declared ID column.
    """
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
    """
    Accept titles with optional trailing newline and reject names ending in semicolon, colon, or ampersand.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_validate_existing_table_name_rejects_sql_control_chars


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    assert driver.direct_validate_existing_table_name("titles") is True
    # validate_existing_table_name() strips whitespace: this should still be accepted.
    assert driver.direct_validate_existing_table_name("titles\n") is True

    assert driver.direct_validate_existing_table_name("titles;") is False
    assert driver.direct_validate_existing_table_name("titles:") is False
    assert driver.direct_validate_existing_table_name("titles&") is False


def test_direct_get_column_headings_unknown_table_raises_input_integrity(driver):
    """
    Require InputIntegrityError when headings are requested for an unknown table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_get_column_headings_unknown_table_raises_input_integrity


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        driver.direct_get_column_headings("definitely_not_a_table")


def test_direct_delete_row_by_id_rejects_injection_shaped_table_name(driver, assert_integrity):
    # Should be rejected at the validation layer, not executed.
    """
    Reject an SQL-shaped table name with InputIntegrityError and require titles plus database integrity to survive.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_delete_row_by_id_rejects_injection_shaped_table_name


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        driver.direct_delete_row_by_id("titles; DROP TABLE titles; --", 1)

    # And the schema should remain intact.
    assert driver.direct_validate_existing_table_name("titles") is True
    assert_integrity(driver)


def test_direct_add_simple_row_dict_with_unknown_columns_fails_loudly(driver):
    # This dict should not map to any known table -> DatabaseIntegrityError.
    """
    Require DatabaseIntegrityError when an insert column cannot identify a known table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_add_simple_row_dict_with_unknown_columns_fails_loudly


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({"not_a_real_column": "x"})


def test_direct_update_row_dict_missing_id_raises_row_integrity(driver, pick_payload, assert_integrity):
    """
    Seed a controlled table and require RowIntegrityError for an update without its ID column.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_update_row_dict_missing_id_raises_row_integrity


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Require InputIntegrityError when searching an unknown column of titles.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_search_table_bad_column_raises_input_integrity


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        driver.direct_search_table(table="titles", column="definitely_not_a_column", search_term="x")


def test_direct_get_row_dict_from_id_returns_false_when_missing(driver, pick_payload, assert_integrity):
    """
    Require an inserted row to resolve and a far larger missing ID to return False, then check integrity.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_get_row_dict_from_id_returns_false_when_missing


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :return: None; failed expectations raise AssertionError.
    """
    table = _ensure_contract_table(driver)
    driver.direct_add_simple_row_dict({"contract_error_value": pick_payload(1)})

    highest = int(driver.direct_get_highest_id(table))
    assert highest >= 1

    assert driver.direct_get_row_dict_from_id(table, highest) is not False
    assert driver.direct_get_row_dict_from_id(table, highest + 10_000) is False

    assert_integrity(driver)


def test_direct_get_id_column_raises_for_tables_without_id(driver):
    """
    Require InputIntegrityError when ID discovery targets a table without an ID column.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py::test_direct_get_id_column_raises_for_tables_without_id


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    table = _ensure_noid_table(driver)
    with pytest.raises(InputIntegrityError):
        driver.direct_get_id_column(table)
