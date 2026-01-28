"""Driver contract: query helpers and search semantics.

This module focuses on methods that *read* data and perform searches.

Covered:

* direct_search_table
* direct_multi_column_search
* direct_get_unique_values_set / direct_get_unique_values_iterator
* direct_get_random_row_dict
* direct_get_max / direct_get_min

Contract notes
--------------

* Unicode values and SQL-injection-shaped payloads must be treated as inert data.
* Search methods should use parameter binding (not unsafe SQL string interpolation).
* Helper methods returning aggregates (max/min) should return scalar values.

These tests are intentionally strict; failures indicate driver contract drift.
"""

from __future__ import annotations

from typing import Dict, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError


_CONTRACT_TABLE = "contract_queries_search"


@pytest.fixture
def query_table(driver) -> str:
    """Create (or recreate) the contract table used by these tests."""

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
    t = query_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "num": f"{t}_num",
        "datestamp": f"{t}_datestamp",
    }


def _insert_row(driver, table: str, cols: Dict[str, str], text: str, num: int) -> int:
    driver.direct_add_simple_row_dict({cols["text"]: text, cols["num"]: num})
    highest = driver.direct_get_highest_id(table)
    return int(highest) if highest is not None else 0


def test_direct_search_table_returns_empty_list_when_no_match(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
    rows = driver.direct_search_table(query_table, query_cols["text"], pick_payload(0))
    assert isinstance(rows, list)
    assert rows == []


def test_direct_search_table_finds_exact_matches(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
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
    inj = sql_injection_payloads[0]
    _insert_row(driver, query_table, query_cols, inj, 1)

    rows = driver.direct_search_table(query_table, query_cols["text"], inj)
    assert rows and any(r.get(query_cols["text"]) == inj for r in rows)

    # Schema should still exist and remain queryable.
    assert query_table in set(driver.direct_get_tables(force_refresh=True))
    assert driver.direct_get_record_count(query_table) >= 1


def test_direct_search_table_rejects_malformed_requests(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
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
    assert driver.direct_get_random_row_dict(query_table) is None
    assert driver.direct_get_random_row_dict(query_table, direct=True) is None


def test_random_row_dict_returns_a_row_when_nonempty(driver, query_table: str, query_cols: Dict[str, str], pick_payload):
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
    inj = sql_injection_payloads[3]

    # Seed a safe row to ensure the table is non-empty.
    _insert_row(driver, query_table, query_cols, "safe", 1)

    with pytest.raises(InputIntegrityError):
        driver.direct_multi_column_search([(query_cols["text"], "=", inj)])

    # Contract: schema must remain intact.
    assert query_table in set(driver.direct_get_tables(force_refresh=True))
