"""
Check dump output and restoration using sentinel rows, a marker table, and SQLite integrity.

Restoration assertions compare the seeded row count and exact marker value; they do
not establish file-replacement atomicity or compare every restored payload.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_dump_restore.py
"""

from __future__ import annotations

import pytest


_CONTRACT_TABLE = "contract_dump_restore"
_MARKER_TABLE = "contract_dump_restore_marker"


@pytest.fixture
def dump_table(driver, pick_payload) -> str:
    """
    Drop and recreate the dump contract table, insert three sentinel rows, and require a count of three.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_dump_restore.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: Trusted seeded table name; the database fixture owns cleanup.
    """
    table = _CONTRACT_TABLE

    sql = f"""
    DROP TABLE IF EXISTS `{table}`;
    CREATE TABLE `{table}` (
        `{table}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `dump_payload_text` TEXT,
        `dump_payload_note` TEXT,
        `created_datestamp` REAL
    );
    """
    driver.direct_executescript(sql)

    # Seed a few rows with deterministic-ish payloads
    rows = [
        {
            "dump_payload_text": "SENTINEL_ALPHA_" + pick_payload(0),
            "dump_payload_note": pick_payload(9),
            "created_datestamp": 1.0,
        },
        {
            "dump_payload_text": "SENTINEL_BETA_" + pick_payload(10),
            "dump_payload_note": pick_payload(12),
            "created_datestamp": 2.0,
        },
        {
            "dump_payload_text": "SENTINEL_GAMMA_" + pick_payload(18),
            "dump_payload_note": pick_payload(19),
            "created_datestamp": 3.0,
        },
    ]

    for r in rows:
        driver.direct_add_simple_row_dict(r)

    assert driver.direct_get_record_count(table) == 3
    return table


def test_sql_dump_contains_contract_table_and_sentinels(driver, dump_table: str):
    """
    Scan dump lines for the contract CREATE TABLE statement and any sentinel payload.

    Stop as soon as both appear, or after inspecting index 250001; the generator need
    not be exhausted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_dump_restore.py::test_sql_dump_contains_contract_table_and_sentinels


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param dump_table: Contract table seeded with three sentinel-bearing rows.
    :return: None; failed expectations raise AssertionError.
    """
    found_create = False
    found_sentinel = False

    # Scan until we find what we need; don't necessarily exhaust full dump.
    for i, line in enumerate(driver.sql_dump()):
        if not found_create:
            if f"CREATE TABLE `{dump_table}`" in line or f"CREATE TABLE {dump_table}" in line:
                found_create = True
        if not found_sentinel and "SENTINEL_" in line:
            found_sentinel = True

        if found_create and found_sentinel:
            break

        # Hard cap to avoid infinite loops if generator misbehaves
        if i > 250_000:
            break

    assert found_create, "sql_dump() did not include contract table CREATE statement"
    assert found_sentinel, "sql_dump() did not include any sentinel payload rows"


def test_dump_and_restore_roundtrip_preserves_data_and_applies_pre_sql(driver, dump_table: str, assert_integrity, pick_payload):
    """
    Restore with marker-creation SQL and require both tables, the original row count, and the exact marker value.

    Refresh table caches after restoration and run the shared integrity helper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_dump_restore.py::test_dump_and_restore_roundtrip_preserves_data_and_applies_pre_sql


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param dump_table: Contract table seeded with three sentinel-bearing rows.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    before_count = driver.direct_get_record_count(dump_table)
    assert before_count == 3

    marker_value = "MARKER_" + pick_payload(15)

    pre_sql = f"""
    CREATE TABLE IF NOT EXISTS `{_MARKER_TABLE}` (
        `{_MARKER_TABLE}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `marker_value` TEXT
    );
    INSERT INTO `{_MARKER_TABLE}` (`marker_value`) VALUES ('{marker_value.replace("'", "''")}');
    """

    # Should not raise, and should atomically replace + reopen the DB.
    driver.dump_and_restore(sql=pre_sql)

    # Force-refresh table list caches and verify both tables exist.
    tables = set(driver.direct_get_tables(force_refresh=True))
    assert dump_table in tables
    assert _MARKER_TABLE in tables

    after_count = driver.direct_get_record_count(dump_table)
    assert after_count == before_count

    # Marker table should have exactly one row, containing the value we inserted.
    assert driver.direct_get_record_count(_MARKER_TABLE) == 1
    marker_id = int(driver.direct_get_highest_id(_MARKER_TABLE))
    marker_row = driver.direct_get_row_dict_from_id(_MARKER_TABLE, marker_id)
    assert marker_row["marker_value"] == marker_value

    assert_integrity(driver)
