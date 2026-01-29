"""Driver contract: bulk operations.

This module focuses on methods that operate on *many* rows at once, exercising:

* direct_add_multiple_simple_row_dicts
* direct_delete_many_by_ids
* direct_delete_many
* direct_clear_table
* direct_executemany

We use a contract-specific table so constraints remain stable while still
exercising the driver's real SQL helpers.
"""

from __future__ import annotations

from typing import Dict, List, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError


_CONTRACT_TABLE = "contract_bulk_ops"


@pytest.fixture
def bulk_table(driver) -> str:
    """Create (or recreate) the contract bulk-ops table."""

    table = _CONTRACT_TABLE

    sql = f"""
    DROP TABLE IF EXISTS `{table}`;
    CREATE TABLE `{table}` (
        `{table}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `{table}_text` TEXT,
        `{table}_num` INTEGER,
        `{table}_datestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    driver.direct_executescript(sql)

    # Ensure the driver's schema cache sees the new table.
    assert table in set(driver.direct_get_tables(force_refresh=True))

    return table


@pytest.fixture
def bulk_cols(bulk_table: str) -> Dict[str, str]:
    t = bulk_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "num": f"{t}_num",
        "datestamp": f"{t}_datestamp",
    }


def _seed_rows(
    driver,
    table: str,
    cols: Dict[str, str],
    payloads: Sequence[str],
    n: int,
) -> List[int]:
    """Insert n rows via add_simple_row_dict and return their ids."""

    ids: List[int] = []
    for i in range(n):
        driver.direct_add_simple_row_dict(
            {
                cols["text"]: payloads[i % len(payloads)],
                cols["num"]: i,
            }
        )
        ids.append(driver.direct_get_highest_id(table))
    return ids


def test_add_multiple_empty_noop(driver, bulk_table: str):
    """Adding an empty list should succeed and be a no-op."""

    assert driver.direct_get_record_count(bulk_table) == 0
    assert driver.direct_add_multiple_simple_row_dicts([]) is True
    assert driver.direct_get_record_count(bulk_table) == 0


def test_add_multiple_simple_row_dicts_inserts_all(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """Bulk add should insert all rows and preserve their TEXT payloads."""

    rows = [
        {
            bulk_cols["text"]: all_torture_payloads[i],
            bulk_cols["num"]: i,
        }
        for i in range(10)
    ]

    driver.direct_add_multiple_simple_row_dicts(rows)

    assert driver.direct_get_record_count(bulk_table) == 10

    highest = driver.direct_get_highest_id(bulk_table)
    assert highest is not None

    got = driver.direct_get_row_dict_from_id(bulk_table, highest)
    assert got is not False


def test_add_multiple_rejects_mismatched_columns(driver, bulk_table: str, bulk_cols: Dict[str, str], pick_payload):
    """Rows with differing column sets should be rejected."""

    good = {bulk_cols["text"]: pick_payload(0), bulk_cols["num"]: 1}
    bad = {bulk_cols["text"]: pick_payload(1)}  # missing num

    with pytest.raises(InputIntegrityError):
        driver.direct_add_multiple_simple_row_dicts([good, bad])


def test_delete_many_by_ids_removes_specified_rows(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """direct_delete_many_by_ids should delete exactly the specified ids."""

    ids = _seed_rows(driver, bulk_table, bulk_cols, all_torture_payloads, 12)
    assert driver.direct_get_record_count(bulk_table) == 12

    victims = [ids[2], ids[5], ids[8], ids[9]]
    driver.direct_delete_many_by_ids(bulk_table, victims)

    assert driver.direct_get_record_count(bulk_table) == 8
    for vid in victims:
        assert driver.direct_get_row_dict_from_id(bulk_table, vid) is False


def test_delete_many_by_column_values(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """direct_delete_many should delete rows matching any of the provided values."""

    id_by_num: Dict[int, int] = {}
    for i in range(15):
        driver.direct_add_simple_row_dict({bulk_cols["text"]: all_torture_payloads[i], bulk_cols["num"]: i})
        id_by_num[i] = driver.direct_get_highest_id(bulk_table)

    assert driver.direct_get_record_count(bulk_table) == 15

    to_delete = [3, 7, 11]
    driver.direct_delete_many(bulk_table, bulk_cols["num"], to_delete)

    assert driver.direct_get_record_count(bulk_table) == 12
    for n in to_delete:
        assert driver.direct_get_row_dict_from_id(bulk_table, id_by_num[n]) is False


def test_clear_table_empties_everything(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """direct_clear_table should remove all rows and be idempotent."""

    _seed_rows(driver, bulk_table, bulk_cols, all_torture_payloads, 7)
    assert driver.direct_get_record_count(bulk_table) == 7

    driver.direct_clear_table(bulk_table)
    assert driver.direct_get_record_count(bulk_table) == 0

    # Idempotency check
    driver.direct_clear_table(bulk_table)
    assert driver.direct_get_record_count(bulk_table) == 0


def test_executemany_list_of_tuples_inserts_rows(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """direct_executemany should support a list of row-tuples."""

    stmt = f"INSERT INTO `{bulk_table}` (`{bulk_cols['text']}`, `{bulk_cols['num']}`) VALUES (?, ?);"
    values = [(all_torture_payloads[i], i) for i in range(5)]

    driver.direct_executemany(stmt, values)

    assert driver.direct_get_record_count(bulk_table) == 5


def test_executemany_tuple_of_scalars_is_supported(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """direct_executemany promises it can coerce a tuple of scalars.

    Example (from the driver's docstring): values=("a", "b") should be treated as
    (("a",), ("b",)) for single-placeholder statements.
    """

    stmt = f"INSERT INTO `{bulk_table}` (`{bulk_cols['text']}`) VALUES (?);"

    values = (
        all_torture_payloads[0],
        all_torture_payloads[1],
        all_torture_payloads[2],
    )

    driver.direct_executemany(stmt, values)

    assert driver.direct_get_record_count(bulk_table) == 3
