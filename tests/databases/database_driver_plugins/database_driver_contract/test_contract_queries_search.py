"""
Check exact searches, unique values, random rows, scalar extrema, and multi-column request validation.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py
"""

from __future__ import annotations

from typing import Dict, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError


_CONTRACT_TABLE = "contract_queries_search"


@pytest.fixture
def query_table(driver) -> str:
    """
    Drop and recreate the controlled query table and assert it appears after refreshing the table cache.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
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

    # Force schema cache refresh so column-to-table identification works.
    assert table in set(driver.direct_get_tables(force_refresh=True))

    return table


@pytest.fixture
def query_cols(query_table: str) -> Dict[str, str]:
    """
    Derive prefixed ID, text, number, and timestamp column names from the query table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py


    :param query_table: Recreated isolated query-contract table name.
    :return: Fresh mapping of field roles to concrete names.
    """
    t = query_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "num": f"{t}_num",
        "datestamp": f"{t}_datestamp",
    }


def _insert_row(driver, table: str, cols: Dict[str, str], text: str, num: int) -> int:
    """
    Insert text and numeric values and infer the inserted ID from the current highest table ID.

    Assume no competing inserts; a missing highest ID is tolerated as zero.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param cols: Mapping supplying concrete text and num columns.
    :param text: Text field value forwarded unchanged.
    :param num: Numeric field value forwarded unchanged.
    :return: Highest ID converted to int, or zero when it is None.
    """
    driver.direct_add_simple_row_dict({cols["text"]: text, cols["num"]: num})
    highest = driver.direct_get_highest_id(table)
    return int(highest) if highest is not None else 0


def test_direct_search_table_returns_empty_list_when_no_match(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
    """
    Search the empty contract table and require an empty list.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_direct_search_table_returns_empty_list_when_no_match


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
    rows = driver.direct_search_table(query_table, query_cols["text"], pick_payload(0))
    assert isinstance(rows, list)
    assert rows == []


def test_direct_search_table_finds_exact_matches(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
    """
    Search for an inserted payload and require exactly one row with matching text.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_direct_search_table_finds_exact_matches


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
    payload = pick_payload(3)
    _insert_row(driver, query_table, query_cols, payload, 123)

    rows = driver.direct_search_table(query_table, query_cols["text"], payload)
    assert len(rows) == 1
    assert rows[0][query_cols["text"]] == payload


def test_direct_search_table_injection_shaped_value_is_inert(
    driver,
    query_table: str,
    query_cols: Dict[str, str],
    sql_injection_payloads: Sequence[str],
):
    """
    Search an inserted SQL-shaped value, require at least one exact match, and confirm the table remains populated.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_direct_search_table_injection_shaped_value_is_inert


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param sql_injection_payloads: Ordered corpus of SQL-shaped strings intended as test
        data.
    :return: None; failed expectations raise AssertionError.
    """
    inj = sql_injection_payloads[0]
    _insert_row(driver, query_table, query_cols, inj, 1)

    rows = driver.direct_search_table(query_table, query_cols["text"], inj)
    assert rows and any(r.get(query_cols["text"]) == inj for r in rows)

    # Schema should still exist and remain queryable.
    assert query_table in set(driver.direct_get_tables(force_refresh=True))
    assert driver.direct_get_record_count(query_table) >= 1


def test_direct_search_table_rejects_malformed_requests(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
    """
    Require InputIntegrityError when the table, column, or search value is None.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_direct_search_table_rejects_malformed_requests


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
    with pytest.raises(InputIntegrityError):
        driver.direct_search_table(query_table, None, pick_payload(0))

    with pytest.raises(InputIntegrityError):
        driver.direct_search_table(None, query_cols["text"], pick_payload(0))

    with pytest.raises(InputIntegrityError):
        driver.direct_search_table(query_table, query_cols["text"], None)


def test_unique_values_set_and_iterator_are_consistent(
    driver,
    query_table: str,
    query_cols: Dict[str, str],
    all_torture_payloads: Sequence[str],
):
    """
    Insert repeated payloads and require both unique-value helpers to agree and include every supplied value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_unique_values_set_and_iterator_are_consistent


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: None; failed expectations raise AssertionError.
    """
    values = [
        all_torture_payloads[0],
        all_torture_payloads[1],
        all_torture_payloads[0],
        all_torture_payloads[2],
        all_torture_payloads[2],
    ]

    for i, v in enumerate(values):
        _insert_row(driver, query_table, query_cols, v, i)

    uniq_set = set(driver.direct_get_unique_values_set(query_cols["text"]))
    assert set(values).issubset(uniq_set)

    uniq_iter = set(driver.direct_get_unique_values_iterator(query_cols["text"]))
    assert uniq_iter == uniq_set


def test_random_row_dict_returns_none_when_empty(driver, query_table: str):
    """
    Require both ordinary and direct random-row modes to return None for an empty table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_random_row_dict_returns_none_when_empty


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :return: None; failed expectations raise AssertionError.
    """
    assert driver.direct_get_random_row_dict(query_table) is None
    assert driver.direct_get_random_row_dict(query_table, direct=True) is None


def test_random_row_dict_returns_a_row_when_nonempty(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
    """
    Require both random-row modes to return a dictionary whose text belongs to the six inserted payloads.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_random_row_dict_returns_a_row_when_nonempty


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
    inserted = []
    for i in range(6):
        text = pick_payload(i)
        inserted.append(text)
        _insert_row(driver, query_table, query_cols, text, i)

    row = driver.direct_get_random_row_dict(query_table)
    assert isinstance(row, dict)
    assert row[query_cols["text"]] in inserted

    row2 = driver.direct_get_random_row_dict(query_table, direct=True)
    assert isinstance(row2, dict)
    assert row2[query_cols["text"]] in inserted


def test_get_max_min_return_scalar_values(driver, query_table: str, query_cols: Dict[str, str]):
    """
    Require extrema equal to nine and zero and reject list/tuple aggregate results.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_get_max_min_return_scalar_values


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :return: None; failed expectations raise AssertionError.
    """
    nums = [5, 2, 9, 0, 9]
    for i, n in enumerate(nums):
        _insert_row(driver, query_table, query_cols, f"n-{i}", n)

    max_v = driver.direct_get_max(query_cols["num"])
    min_v = driver.direct_get_min(query_cols["num"])

    # Contract: return scalars, not 1-tuples.
    assert not isinstance(max_v, (list, tuple)), f"expected scalar max, got {max_v!r}"
    assert not isinstance(min_v, (list, tuple)), f"expected scalar min, got {min_v!r}"

    assert max_v == max(nums)
    assert min_v == min(nums)


def test_multi_column_search_binds_parameters_and_accepts_raw_values(driver, query_table: str, query_cols: Dict[str, str]):
    """
    Search with raw text and integer conditions and require at least one row matching both values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_multi_column_search_binds_parameters_and_accepts_raw_values


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :return: None; failed expectations raise AssertionError.
    """
    text = "alpha"
    num = 42
    _insert_row(driver, query_table, query_cols, text, num)

    search_index = [
        (query_cols["text"], "=", text),
        (query_cols["num"], "=", num),
    ]

    rows = driver.direct_multi_column_search(search_index)
    assert rows, "expected at least one row"

    # Results are row_dicts keyed by column headings.
    assert any(
        (r.get(query_cols["text"]) == text) and (int(r.get(query_cols["num"])) == num)
        for r in rows
    )


def test_multi_column_search_rejects_injection_shaped_values(
    driver,
    query_table: str,
    query_cols: Dict[str, str],
    sql_injection_payloads: Sequence[str],
):
    # This payload is shaped like a multi-statement injection attempt.
    """
    Require InputIntegrityError for a multi-statement-shaped search value and confirm the contract table survives.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_queries_search.py::test_multi_column_search_rejects_injection_shaped_values


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param query_table: Recreated isolated query-contract table name.
    :param query_cols: Mapping from query field roles to concrete column names.
    :param sql_injection_payloads: Ordered corpus of SQL-shaped strings intended as test
        data.
    :return: None; failed expectations raise AssertionError.
    """
    inj = sql_injection_payloads[3]

    # Seed a safe row to ensure the table is non-empty.
    _insert_row(driver, query_table, query_cols, "safe", 1)

    with pytest.raises(InputIntegrityError):
        driver.direct_multi_column_search([(query_cols["text"], "=", inj)])

    # Contract: schema must remain intact.
    assert query_table in set(driver.direct_get_tables(force_refresh=True))
