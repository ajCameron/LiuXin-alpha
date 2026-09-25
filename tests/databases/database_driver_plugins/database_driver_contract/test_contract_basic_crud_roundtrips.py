"""
Check driver CRUD helpers with a controlled table and verify tag identity-key derivation.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py
"""

from __future__ import annotations

from typing import Dict
import uuid

import pytest

from LiuXin_alpha.metadata.standardization import make_tag_search_term


_CONTRACT_TABLE = "contract_crud_roundtrips"


@pytest.fixture
def crud_table(driver) -> str:
    """
    Drop and recreate the isolated CRUD table with two text fields, a number, and a default timestamp.

    Refresh the driver table cache and assert the new table is visible. The database
    fixture owns cleanup.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: Trusted contract table name.
    """

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
    """
    Map CRUD field roles to columns prefixed by the supplied table name.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param crud_table: Freshly recreated contract CRUD table name.
    :return: Fresh id/text/text2/num/datestamp name mapping.
    """

    t = crud_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "text2": f"{t}_text2",
        "num": f"{t}_num",
        "datestamp": f"{t}_datestamp",
    }


def _coerce_datestamp(x) -> str:
    """
    Convert a non-None value to text without validating its timestamp format.

    Example:
        >>> (_coerce_datestamp(None), _coerce_datestamp(42))
        ('', '42')


    :param x: Value retrieved from the timestamp field.
    :return: Empty string for None; otherwise str(x).
    """
    if x is None:
        return ""
    # sqlite3 commonly returns str; keep it tolerant.
    return str(x)


def test_insert_and_fetch_roundtrip(driver, crud_table: str, crud_cols: Dict[str, str], pick_payload, assert_integrity):
    """
    Insert two payload fields and forty-two, then compare the fetched ID and values exactly.

    Select the inserted row through the highest ID in the isolated table; also require a
    nonempty timestamp, surviving titles relation, and passing integrity helper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_insert_and_fetch_roundtrip


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param crud_table: Freshly recreated contract CRUD table name.
    :param crud_cols: Mapping of CRUD field roles to concrete column names.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :param assert_integrity: Fixture callable requiring the first retained
        integrity_check result to be ok.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Insert ten mixed-payload rows, require a count of ten, and confirm the highest ID resolves to a row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_insert_many_and_record_count_roundtrip


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param crud_table: Freshly recreated contract CRUD table name.
    :param crud_cols: Mapping of CRUD field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Update text and numeric fields and require their new values on readback.

    Also require the other text field to differ from the original text; the test does
    not compare that field with its pre-update value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_update_roundtrip


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param crud_table: Freshly recreated contract CRUD table name.
    :param crud_cols: Mapping of CRUD field roles to concrete column names.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Require tag_phash to match the shared normalizer after insertion, row update, and bulk column update.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_identity_key_is_derived_on_direct_insert_and_update


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Delete the middle of three distinct inserted IDs and require its absence, count two, and surviving rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_delete_roundtrip


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param crud_table: Freshly recreated contract CRUD table name.
    :param crud_cols: Mapping of CRUD field roles to concrete column names.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Require discovered ID and timestamp column names to end with their expected suffixes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py::test_id_and_datestamp_helpers_work_on_contract_table


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param crud_table: Freshly recreated contract CRUD table name.
    :return: None; failed expectations raise AssertionError.
    """

    assert driver.direct_get_id_column(crud_table).endswith("_id")
    assert driver.direct_get_datestamp_column(crud_table).endswith("_datestamp")
