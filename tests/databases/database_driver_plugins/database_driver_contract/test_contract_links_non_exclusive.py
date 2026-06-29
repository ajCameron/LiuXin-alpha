"""Driver contract: many_many_non_exclusive link tables.

The intent of many_many_non_exclusive is to support role-style mappings where the
same (A,B) pair may appear multiple times as long as the `type` value differs.

SQLite UNIQUE semantics allow multiple NULLs, so repeated NULL `type` values are
also permitted.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError


def _create_contract_tables(driver) -> tuple[str, str, str, str]:
    left_table = "contract_link_ne_lefts"
    right_table = "contract_link_ne_rights"

    driver.direct_create_main_table(table_name=left_table)
    driver.direct_create_main_table(table_name=right_table)

    left_col = driver.direct_get_column_base(left_table)
    right_col = driver.direct_get_column_base(right_table)
    return left_table, right_table, left_col, right_col


def _insert_one(driver, table: str, value_col: str, value: str) -> int:
    driver.direct_add_simple_row_dict({value_col: value})
    return int(driver.direct_get_highest_id(table))


def _create_link_table(driver, driver_wrapper, left_table: str, right_table: str, *, requested_cols="all") -> str:
    link_table_name = driver.direct_link_main_tables(
        primary_table=left_table,
        secondary_table=right_table,
        link_type="many_many_non_exclusive",
        requested_cols=requested_cols,
    )

    wrapper_name = driver_wrapper.get_link_table_name(left_table, right_table)
    assert wrapper_name, "Wrapper failed to discover the link table"
    assert str(wrapper_name) == str(link_table_name)

    return str(link_table_name)


def _link_columns(driver_wrapper, left_table: str, right_table: str) -> dict[str, str]:
    left_id_col = driver_wrapper.get_id_column(left_table)
    right_id_col = driver_wrapper.get_id_column(right_table)

    return {
        "left_fk": driver_wrapper.get_link_column(left_table, right_table, left_id_col),
        "right_fk": driver_wrapper.get_link_column(left_table, right_table, right_id_col),
        "priority": driver_wrapper.get_link_column(left_table, right_table, "priority"),
        "type": driver_wrapper.get_link_column(left_table, right_table, "type"),
        "index": driver_wrapper.get_link_column(left_table, right_table, "index"),
    }


def test_many_many_non_exclusive_allows_multiple_types_and_nulls(driver, driver_wrapper, pick_payload) -> None:
    left_table, right_table, left_col, right_col = _create_contract_tables(driver)

    link_table = _create_link_table(driver, driver_wrapper, left_table, right_table, requested_cols="all")
    cols = _link_columns(driver_wrapper, left_table, right_table)

    left_id = _insert_one(driver, left_table, left_col, pick_payload(0))
    right_id = _insert_one(driver, right_table, right_col, pick_payload(1))

    # Same pair, different types => allowed.
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id, cols["priority"]: 1, cols["type"]: "author"}
    )
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id, cols["priority"]: 2, cols["type"]: "editor"}
    )

    # Same pair, same (non-null) type => rejected.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict(
            {cols["left_fk"]: left_id, cols["right_fk"]: right_id, cols["priority"]: 3, cols["type"]: "editor"}
        )

    # Same pair, NULL type => allowed multiple times.
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id, cols["priority"]: 4, cols["type"]: None}
    )
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id, cols["priority"]: 5, cols["type"]: None}
    )

    assert int(driver.direct_get_record_count(link_table)) == 4


def test_many_many_non_exclusive_priority_is_unique_per_type(driver, driver_wrapper, pick_payload) -> None:
    left_table, right_table, left_col, right_col = _create_contract_tables(driver)

    link_table = _create_link_table(driver, driver_wrapper, left_table, right_table, requested_cols="all")
    cols = _link_columns(driver_wrapper, left_table, right_table)

    left_id = _insert_one(driver, left_table, left_col, pick_payload(10))
    right_id_1 = _insert_one(driver, right_table, right_col, pick_payload(11))
    right_id_2 = _insert_one(driver, right_table, right_col, pick_payload(12))
    right_id_3 = _insert_one(driver, right_table, right_col, pick_payload(13))

    # Same priority across different types => allowed.
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id_1, cols["priority"]: 1, cols["type"]: "author"}
    )
    driver.direct_add_simple_row_dict(
        {cols["left_fk"]: left_id, cols["right_fk"]: right_id_2, cols["priority"]: 1, cols["type"]: "editor"}
    )

    # Same priority within the same type => rejected (ordering constraint).
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict(
            {cols["left_fk"]: left_id, cols["right_fk"]: right_id_3, cols["priority"]: 1, cols["type"]: "author"}
        )

    assert int(driver.direct_get_record_count(link_table)) == 2
