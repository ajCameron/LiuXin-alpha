"""
Exercise bulk insertion, deletion, clearing, and executemany through a controlled driver test table.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py
"""

from __future__ import annotations

from typing import Dict, List, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError


_CONTRACT_TABLE = "contract_bulk_ops"


@pytest.fixture
def bulk_table(driver) -> str:
    """
    Drop and recreate the bulk contract table and assert it appears after refreshing the schema cache.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: Trusted table name; the isolated database fixture owns cleanup.
    """

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
    """
    Build concrete column names for the bulk table’s ID, text, number, and timestamp fields.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py


    :param bulk_table: Freshly recreated bulk-operation table name.
    :return: Fresh mapping from field roles to prefixed column names.
    """
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
    """
    Insert numbered rows with cycling payloads and collect the highest ID after each insert.

    Assume no competing inserts. Nonpositive n yields an empty list; positive n with an
    empty payload sequence raises ZeroDivisionError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param cols: Mapping providing concrete text and num column names.
    :param payloads: Payload sequence to cycle through.
    :param n: Number of rows requested.
    :return: List of driver-reported highest IDs, without conversion or validation.
    """

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
    """
    Require adding an empty row list to return True and preserve a zero row count.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_add_multiple_empty_noop


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :return: None; failed expectations raise AssertionError.
    """

    assert driver.direct_get_record_count(bulk_table) == 0
    assert driver.direct_add_multiple_simple_row_dicts([]) is True
    assert driver.direct_get_record_count(bulk_table) == 0


def test_add_multiple_simple_row_dicts_inserts_all(
    driver,
    bulk_table: str,
    bulk_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """
    Insert ten row dictionaries, require count ten, and confirm the highest ID resolves to a row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_add_multiple_simple_row_dicts_inserts_all


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Require InputIntegrityError for a bulk row list with differing column sets.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_add_multiple_rejects_mismatched_columns


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Delete four selected IDs from twelve rows and require count eight plus each victim’s absence.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_delete_many_by_ids_removes_specified_rows


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Delete rows with numeric values three, seven, and eleven and require count twelve plus their absence.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_delete_many_by_column_values


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Clear seven seeded rows twice and require zero rows after each call.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_clear_table_empties_everything


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Execute a two-placeholder insert for five row tuples and require a row count of five.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_executemany_list_of_tuples_inserts_rows


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Pass three scalars to a one-placeholder executemany insert and require a row count of three.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_bulk_ops.py::test_executemany_tuple_of_scalars_is_supported


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param bulk_table: Freshly recreated bulk-operation table name.
    :param bulk_cols: Mapping of bulk-test field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """

    stmt = f"INSERT INTO `{bulk_table}` (`{bulk_cols['text']}`) VALUES (?);"

    values = (
        all_torture_payloads[0],
        all_torture_payloads[1],
        all_torture_payloads[2],
    )

    driver.direct_executemany(stmt, values)

    assert driver.direct_get_record_count(bulk_table) == 3
