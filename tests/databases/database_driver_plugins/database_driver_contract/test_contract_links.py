"""
Check many-to-many link creation, endpoint uniqueness, cascade deletion, unlinking, and unsupported link types.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError


def _create_contract_tables(driver) -> tuple[str, str, str, str]:
    """
    Create fixed left and right main tables and discover their base value columns.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: Tuple of left table, right table, left value column, and right value
        column.
    """

    left_table = "contract_link_lefts"
    right_table = "contract_link_rights"

    driver.direct_create_main_table(table_name=left_table)
    driver.direct_create_main_table(table_name=right_table)

    left_col = driver.direct_get_column_base(left_table)
    right_col = driver.direct_get_column_base(right_table)
    return left_table, right_table, left_col, right_col


def _insert_one(driver, table: str, value_col: str, value: str) -> int:
    """
    Insert one value and infer the row ID from the highest ID in the supplied table.

    Assume column-based table identification and no competing insert.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param value_col: Concrete value column identifying the insertion target.
    :param value: Payload passed through the driver’s row-dictionary insert.
    :return: Highest ID converted to int.
    """

    driver.direct_add_simple_row_dict({value_col: value})
    return int(driver.direct_get_highest_id(table))


def _create_link_table(driver, driver_wrapper, left_table: str, right_table: str, *, requested_cols="all") -> str:
    """
    Create a many-to-many link with the requested optional columns and compare driver and wrapper names.

    Require a truthy wrapper result and equal string representations.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param driver_wrapper: Wrapper attached to the fixture database and selected driver.
    :param left_table: Trusted left main-table name.
    :param right_table: Trusted right main-table name.
    :param requested_cols: Optional-column selector forwarded unchanged; defaults to
        all.
    :return: Driver-reported link-table name converted to str.
    """

    link_table_name = driver.direct_link_main_tables(
        primary_table=left_table,
        secondary_table=right_table,
        link_type="many_many",
        requested_cols=requested_cols,
    )

    # Driver returns the table name; wrapper should also be able to discover it.
    wrapper_name = driver_wrapper.get_link_table_name(left_table, right_table)
    assert wrapper_name, "Wrapper failed to discover the link table"
    assert str(wrapper_name) == str(link_table_name)

    return str(link_table_name)


def _link_columns(driver_wrapper, left_table: str, right_table: str) -> dict[str, str]:
    """
    Resolve both endpoint foreign keys and the priority, type, and index metadata columns through the wrapper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py


    :param driver_wrapper: Wrapper attached to the fixture database and selected driver.
    :param left_table: Trusted left main-table name.
    :param right_table: Trusted right main-table name.
    :return: Mapping with left_fk, right_fk, priority, type, and index entries.
    """

    left_id_col = driver_wrapper.get_id_column(left_table)
    right_id_col = driver_wrapper.get_id_column(right_table)

    return {
        "left_fk": driver_wrapper.get_link_column(left_table, right_table, left_id_col),
        "right_fk": driver_wrapper.get_link_column(left_table, right_table, right_id_col),
        "priority": driver_wrapper.get_link_column(left_table, right_table, "priority"),
        "type": driver_wrapper.get_link_column(left_table, right_table, "type"),
        "index": driver_wrapper.get_link_column(left_table, right_table, "index"),
    }


def test_direct_link_main_tables_creates_link_table_and_enforces_uniqueness(
    driver,
    driver_wrapper,
    pick_payload,
) -> None:
    """
    Insert two distinct links, reject a duplicate endpoint pair, and require count two plus a valid fetched endpoint pair.

    Also require titles to survive an SQL-shaped type value; the fetched row’s metadata
    values are not compared.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py::test_direct_link_main_tables_creates_link_table_and_enforces_uniqueness


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param driver_wrapper: Wrapper attached to the fixture database and selected driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    left_table, right_table, left_col, right_col = _create_contract_tables(driver)

    link_table = _create_link_table(driver, driver_wrapper, left_table, right_table, requested_cols="all")
    cols = _link_columns(driver_wrapper, left_table, right_table)

    # Create 1 left row and 2 right rows (so we can test both uniqueness + ordering constraints).
    left_id = _insert_one(driver, left_table, left_col, pick_payload(0))
    right_id_1 = _insert_one(driver, right_table, right_col, pick_payload(1))
    right_id_2 = _insert_one(driver, right_table, right_col, pick_payload(2))

    inj = "x'); DROP TABLE titles; --"

    # Insert first link.
    driver.direct_add_simple_row_dict(
        {
            cols["left_fk"]: left_id,
            cols["right_fk"]: right_id_1,
            cols["priority"]: 1,
            cols["type"]: inj,
            cols["index"]: pick_payload(3),
        }
    )

    # Attempting to insert the same pair again should violate the many_many uniqueness constraint.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict(
            {
                cols["left_fk"]: left_id,
                cols["right_fk"]: right_id_1,
                cols["priority"]: 2,
                cols["type"]: pick_payload(4),
                cols["index"]: pick_payload(5),
            }
        )

    # Insert a second link with a distinct priority.
    driver.direct_add_simple_row_dict(
        {
            cols["left_fk"]: left_id,
            cols["right_fk"]: right_id_2,
            cols["priority"]: 2,
            cols["type"]: pick_payload(6),
            cols["index"]: pick_payload(7),
        }
    )

    assert int(driver.direct_get_record_count(link_table)) == 2

    # Fetch a representative link row and ensure our injection-shaped string is treated as data.
    link_row_id = driver.direct_get_highest_id(link_table)
    row = driver.direct_get_row_dict_from_id(link_table, link_row_id)

    assert row is not False
    assert str(row[cols["left_fk"]]) == str(left_id)
    assert str(row[cols["right_fk"]]) in {str(right_id_1), str(right_id_2)}

    # Ensure nothing actually dropped the titles table.
    assert "titles" in set(driver.direct_get_tables(force_refresh=True))


def test_link_rows_are_cascade_deleted_when_main_row_deleted(driver, driver_wrapper, pick_payload) -> None:
    """
    Delete a linked left-side main row and require its single link row to disappear.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py::test_link_rows_are_cascade_deleted_when_main_row_deleted


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param driver_wrapper: Wrapper attached to the fixture database and selected driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    left_table, right_table, left_col, right_col = _create_contract_tables(driver)

    link_table = _create_link_table(driver, driver_wrapper, left_table, right_table, requested_cols="all")
    cols = _link_columns(driver_wrapper, left_table, right_table)

    left_id = _insert_one(driver, left_table, left_col, pick_payload(10))
    right_id = _insert_one(driver, right_table, right_col, pick_payload(11))

    driver.direct_add_simple_row_dict(
        {
            cols["left_fk"]: left_id,
            cols["right_fk"]: right_id,
            cols["priority"]: 1,
            cols["type"]: pick_payload(12),
        }
    )

    assert int(driver.direct_get_record_count(link_table)) == 1

    # Deleting the left main-row should cascade delete link rows.
    driver.direct_delete_row_by_id(left_table, left_id)
    assert int(driver.direct_get_record_count(link_table)) == 0



def test_direct_unlink_main_tables_drops_link_table(driver, driver_wrapper) -> None:
    """
    Unlink two main tables and require the link relation to disappear and wrapper discovery to return False.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py::test_direct_unlink_main_tables_drops_link_table


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param driver_wrapper: Wrapper attached to the fixture database and selected driver.
    :return: None; failed expectations raise AssertionError.
    """
    left_table, right_table, _, _ = _create_contract_tables(driver)

    link_table = _create_link_table(driver, driver_wrapper, left_table, right_table, requested_cols="all")

    tables_before = set(driver.direct_get_tables(force_refresh=True))
    assert link_table in tables_before

    driver.direct_unlink_main_tables(left_table, right_table)

    tables_after = set(driver.direct_get_tables(force_refresh=True))
    assert link_table not in tables_after
    assert driver_wrapper.get_link_table_name(left_table, right_table) is False


def test_direct_link_main_tables_rejects_unknown_link_type(driver) -> None:
    """
    Require NotImplementedError when creating a link with an unrecognized link type.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py::test_direct_link_main_tables_rejects_unknown_link_type


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; failed expectations raise AssertionError.
    """
    left_table, right_table, _, _ = _create_contract_tables(driver)

    with pytest.raises(NotImplementedError):
        driver.direct_link_main_tables(
            primary_table=left_table,
            secondary_table=right_table,
            link_type="__not_a_real_link_type__",
            requested_cols="all",
        )
