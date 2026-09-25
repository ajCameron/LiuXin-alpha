"""
Check sequential cross-connection visibility and resource release for each selected backend.

The two connections alternate operations in one thread; these probes do not
establish general thread safety.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py
"""

from __future__ import annotations

from typing import Tuple

import pytest


_CONTRACT_TABLE = "contract_concurrency_smoke"


def _cols(table: str) -> Tuple[str, str]:
    """
    Derive the ID and text column names for the trusted contract table.

    Example:
        >>> _cols('sample')
        ('sample_id', 'sample_text')


    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Two-tuple of ID and text names.
    """
    return (f"{table}_id", f"{table}_text")


def _create_contract_table(driver) -> None:
    """
    Create the concurrency table if absent and clear the driver property cache.

    Existing rows are retained.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; changes schema and cached properties.
    """
    table = _CONTRACT_TABLE
    id_col, text_col = _cols(table)

    driver.direct_executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {table}(
            {id_col} INTEGER PRIMARY KEY,
            {text_col} TEXT
        );
        """
    )
    driver.zero_prop_cache()


def _insert(driver, value: str) -> int:
    """
    Insert one text payload and infer its ID from the highest contract-table ID.

    Require a non-None ID and assume no competing inserts.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param value: Text payload bound through the row-dictionary helper.
    :return: Highest ID converted to int.
    """
    table = _CONTRACT_TABLE
    _, text_col = _cols(table)
    driver.direct_add_simple_row_dict({text_col: value})
    row_id = driver.direct_get_highest_id(table)
    assert row_id is not None
    return int(row_id)


def _read(driver, row_id: int) -> str:
    """
    Fetch a contract row, assert it exists with the requested ID, and extract its text field.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param row_id: ID requested from the contract table.
    :return: Stored text field value.
    """
    table = _CONTRACT_TABLE
    id_col, text_col = _cols(table)
    row = driver.direct_get_row_dict_from_id(table, row_id)
    assert row is not False
    assert int(row[id_col]) == int(row_id)
    return row[text_col]


def test_two_connections_can_interleave_writes_and_reads(driver_spec, db_metadata, pick_payload, assert_integrity):
    """
    Alternate twenty sequential writes between two Database instances and read each exact payload through the other.

    Run the integrity helper on both drivers and attempt to close both in finally,
    suppressing ordinary close errors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py::test_two_connections_can_interleave_writes_and_reads


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Metadata mapping containing the provisioned SQLite database
        path.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :param assert_integrity: Fixture callable requiring the first retained
        integrity_check result to be ok.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database import Database

    db1 = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    db2 = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        d1 = db1.driver
        d2 = db2.driver

        _create_contract_table(d1)

        # Ensure the second connection can validate the new table.
        assert d2.direct_validate_existing_table_name(_CONTRACT_TABLE) is True

        # Alternate inserts and immediate reads from the *other* connection.
        last_id = None
        for i in range(20):
            writer = d1 if i % 2 == 0 else d2
            reader = d2 if i % 2 == 0 else d1
            payload = pick_payload(i)

            row_id = _insert(writer, payload)
            last_id = row_id

            # The reader should see the inserted row right away (commit discipline).
            got = _read(reader, row_id)
            assert got == payload

        assert last_id is not None

        assert_integrity(d1)
        assert_integrity(d2)
    finally:
        try:
            db1.driver.close()
        except Exception:
            pass
        try:
            db2.driver.close()
        except Exception:
            pass


def test_close_releases_resources_for_other_connection(driver_spec, db_metadata, pick_payload):
    """
    Close the first of two drivers, then insert and read an exact payload through the second.

    Finally attempt both driver closes and suppress ordinary close errors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_concurrency_smoke.py::test_close_releases_resources_for_other_connection


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Metadata mapping containing the provisioned SQLite database
        path.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database import Database

    db1 = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    db2 = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        d1 = db1.driver
        d2 = db2.driver

        _create_contract_table(d1)

        _insert(d1, pick_payload(0))
        d1.close()

        # If the first connection left the DB in a locked/bad state, this write will fail.
        row_id = _insert(d2, pick_payload(1))
        got = _read(d2, row_id)
        assert got == pick_payload(1)
    finally:
        try:
            db1.driver.close()
        except Exception:
            pass
        try:
            db2.driver.close()
        except Exception:
            pass
