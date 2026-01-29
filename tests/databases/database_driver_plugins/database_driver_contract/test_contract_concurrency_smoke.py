"""Driver contract: concurrency smoke.

This module is intentionally light-weight and deterministic.

We do *not* attempt to prove full thread-safety; instead we check the most common
practical failure mode across driver backends: lingering write locks / missing
commits that prevent a second connection from seeing changes or writing.
"""

from __future__ import annotations

from typing import Tuple

import pytest


_CONTRACT_TABLE = "contract_concurrency_smoke"


def _cols(table: str) -> Tuple[str, str]:
    return (f"{table}_id", f"{table}_text")


def _create_contract_table(driver) -> None:
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
    table = _CONTRACT_TABLE
    _, text_col = _cols(table)
    driver.direct_add_simple_row_dict({text_col: value})
    row_id = driver.direct_get_highest_id(table)
    assert row_id is not None
    return int(row_id)


def _read(driver, row_id: int) -> str:
    table = _CONTRACT_TABLE
    id_col, text_col = _cols(table)
    row = driver.direct_get_row_dict_from_id(table, row_id)
    assert row is not False
    assert int(row[id_col]) == int(row_id)
    return row[text_col]


def test_two_connections_can_interleave_writes_and_reads(driver_spec, db_metadata, pick_payload, assert_integrity):
    """Alternate writes across two independent Database instances.

    Expectations:
      * each writer commits so the other connection can see the new row
      * no 'database is locked' errors in normal sequential usage
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
    """A close on one connection should never poison a second connection."""
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
