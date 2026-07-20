"""Driver contract: basic CRUD round-trips.

This module creates a small contract-specific table inside the provisioned test
DB and then exercises the core CRUD primitives using the driver's *own* helper
methods.

Why a contract table?
---------------------
The project schema contains many NOT NULL / FK / trigger interactions that can
make "minimal valid rows" drift over time. For round-trip semantics we want a
stable target where we fully control constraints, while still exercising:

* direct_executescript
* direct_add_simple_row_dict
* direct_get_highest_id
* direct_get_id_column / direct_get_datestamp_column
* direct_get_row_dict_from_id
* direct_update_row_dict
* direct_delete_row_by_id
* direct_get_record_count

Other modules will hammer schema-specific tables (links, custom columns, views,
book groups, etc.).
"""

from __future__ import annotations

from typing import Dict
import uuid

import pytest

from LiuXin_alpha.metadata.standardization import make_tag_search_term


_CONTRACT_TABLE = "contract_crud_roundtrips"


@pytest.fixture
def crud_table(driver) -> str:
    """Create (or recreate) the contract CRUD table."""

    table = _CONTRACT_TABLE

    # Use very distinctive column names so identify_table_from_row() cannot
    # accidentally match some other table.
    sql = f"""
    DROP TABLE IF EXISTS `{table}`;
    CREATE TABLE `{table}` (
        `{table}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `{table}_text` TEXT,
        `{table}_text2` TEXT,
        `{table}_num` INTEGER,
        `{table}_datestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    driver.direct_executescript(sql)

    # Sanity: ensure the driver's cache sees the new table.
    assert table in set(driver.direct_get_tables(force_refresh=True))

    return table


@pytest.fixture
def crud_cols(crud_table: str) -> Dict[str, str]:
    """Return the column names used by the contract CRUD table."""

    t = crud_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "text2": f"{t}_text2",
        "num": f"{t}_num",
        "datestamp": f"{t}_datestamp",
    }


def _coerce_datestamp(x) -> str:
    if x is None:
        return ""
    # sqlite3 commonly returns str; keep it tolerant.
    return str(x)


def test_insert_and_fetch_roundtrip(driver, crud_table: str, crud_cols: Dict[str, str], pick_payload, assert_integrity):
    """A single inserted row should read back byte-for-byte for TEXT fields."""

    payload_a = pick_payload(0)
    payload_b = pick_payload(9)

    row_dict = {
        crud_cols["text"]: payload_a,
        crud_cols["text2"]: payload_b,
        crud_cols["num"]: 42,
    }

    driver.direct_add_simple_row_dict(row_dict)

    row_id = driver.direct_get_highest_id(crud_table)
    assert row_id is not None

    got = driver.direct_get_row_dict_from_id(crud_table, row_id)
    assert got is not False

    assert got[crud_cols["id"]] == row_id
    assert got[crud_cols["text"]] == payload_a
    assert got[crud_cols["text2"]] == payload_b
    assert got[crud_cols["num"]] == 42

    # Datestamp should be set (even if only as a string)
    assert _coerce_datestamp(got.get(crud_cols["datestamp"])) != ""

    # Inserting injection-shaped values must not corrupt the DB.
    assert "titles" in set(driver.direct_get_tables(force_refresh=True))

    assert_integrity(driver)


def test_insert_many_and_record_count_roundtrip(driver, crud_table: str, crud_cols: Dict[str, str], all_torture_payloads):
    """Bulk-ish inserts via repeated add_simple_row_dict should match record_count."""

    # Insert a handful of rows with varied payloads (including long unicode).
    for i in range(10):
        row_dict = {
            crud_cols["text"]: all_torture_payloads[i],
            crud_cols["text2"]: all_torture_payloads[-(i + 1)],
            crud_cols["num"]: i,
        }
        driver.direct_add_simple_row_dict(row_dict)

    assert driver.direct_get_record_count(crud_table) == 10

    # Highest id should correspond to an existing row.
    row_id = driver.direct_get_highest_id(crud_table)
    assert row_id is not None
    assert driver.direct_get_row_dict_from_id(crud_table, row_id) is not False


def test_update_roundtrip(driver, crud_table: str, crud_cols: Dict[str, str], pick_payload):
    """Updating a row should persist, and unrelated columns should remain stable."""

    original_text = pick_payload(1)
    driver.direct_add_simple_row_dict({crud_cols["text"]: original_text, crud_cols["num"]: 1})

    row_id = driver.direct_get_highest_id(crud_table)
    assert row_id is not None

    before = driver.direct_get_row_dict_from_id(crud_table, row_id)
    assert before is not False

    new_text = pick_payload(2)
    update_dict = {
        crud_cols["id"]: row_id,
        crud_cols["text"]: new_text,
        crud_cols["num"]: 999,
    }

    driver.direct_update_row_dict(update_dict)

    after = driver.direct_get_row_dict_from_id(crud_table, row_id)
    assert after is not False

    assert after[crud_cols["text"]] == new_text
    assert after[crud_cols["num"]] == 999

    # text2 was never set; should remain None/empty (driver dependent). We only
    # assert it didn't spontaneously become the old text.
    assert after.get(crud_cols["text2"]) != original_text


def test_identity_key_is_derived_on_direct_insert_and_update(driver) -> None:
    original = f"Driver Identity {uuid.uuid4().hex}"
    row_id = driver.direct_add_simple_row_dict({"tag": original})
    row = driver.direct_get_row_dict_from_id("tags", row_id)
    assert row["tag_phash"] == make_tag_search_term(original)

    changed = f"Changed Identity {uuid.uuid4().hex}"
    driver.direct_update_row_dict({"tag_id": row_id, "tag": changed})
    row = driver.direct_get_row_dict_from_id("tags", row_id)
    assert row["tag"] == changed
    assert row["tag_phash"] == make_tag_search_term(changed)

    bulk_changed = f"Bulk Identity {uuid.uuid4().hex}"
    driver.direct_update_columns({row_id: bulk_changed}, field="tag")
    row = driver.direct_get_row_dict_from_id("tags", row_id)
    assert row["tag"] == bulk_changed
    assert row["tag_phash"] == make_tag_search_term(bulk_changed)


def test_delete_roundtrip(driver, crud_table: str, crud_cols: Dict[str, str], pick_payload) -> None:
    """Deleting by id should remove the row and not disturb other rows."""

    # Insert three rows.
    ids: list[int] = []
    for i in range(3):
        driver.direct_add_simple_row_dict({crud_cols["text"]: pick_payload(3 + i), crud_cols["num"]: i})
        ids.append(driver.direct_get_highest_id(crud_table))

    assert len(set(ids)) == 3
    assert driver.direct_get_record_count(crud_table) == 3

    # Delete the middle one.
    victim = sorted(ids)[1]
    driver.direct_delete_row_by_id(crud_table, victim)

    assert driver.direct_get_row_dict_from_id(crud_table, victim) is False
    assert driver.direct_get_record_count(crud_table) == 2

    # The remaining two should still exist.
    survivors = [i for i in ids if i != victim]
    for sid in survivors:
        assert driver.direct_get_row_dict_from_id(crud_table, sid) is not False


def test_id_and_datestamp_helpers_work_on_contract_table(driver, crud_table: str) -> None:
    """direct_get_id_column/direct_get_datestamp_column should handle new tables."""

    assert driver.direct_get_id_column(crud_table).endswith("_id")
    assert driver.direct_get_datestamp_column(crud_table).endswith("_datestamp")
