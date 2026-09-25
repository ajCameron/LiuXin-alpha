"""
Check custom-column cardinality, uniqueness, cascade cleanup, and returned table names.

The tests insert Unicode and SQL-shaped values as data while exercising one-to-one,
one-to-many, many-to-one, and many-to-many mappings.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError


def _create_root_table(driver) -> tuple[str, str]:
    """
    Create the fixed contract custom-root main table and discover its value column.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: Pair of root table name and driver-reported base column.
    """

    root_table = "contract_custom_roots"
    driver.direct_create_main_table(table_name=root_table)
    root_col = driver.direct_get_column_base(root_table)
    return root_table, root_col


def _insert_one(driver, table: str, value_col: str, value: str) -> int:
    """
    Insert a single value and infer the row ID from the highest ID in the supplied table.

    Assume the value column identifies that table and no competing insert occurs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param value_col: Concrete value column identifying the insert target.
    :param value: Value passed through the row-dictionary helper.
    :return: Highest ID converted to int.
    """

    driver.direct_add_simple_row_dict({value_col: value})
    return int(driver.direct_get_highest_id(table))


def _one_one_columns(driver, custom_col_table: str) -> tuple[str, str, str]:
    """
    Find the ID, first other _id column, and first _value column in a custom storage table.

    Assert that both candidate lists are nonempty; this naming heuristic does not
    inspect foreign-key declarations.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param custom_col_table: One-to-one custom storage table to inspect.
    :return: Tuple of ID, inferred foreign-key, and value column names.
    """

    headings = list(driver.direct_get_column_headings(custom_col_table))
    id_col = str(driver.direct_get_id_column(custom_col_table))

    fk_candidates = [h for h in headings if h.endswith("_id") and h != id_col]
    assert fk_candidates, f"Could not find FK column in {custom_col_table}: {headings}"
    fk_col = str(fk_candidates[0])

    value_candidates = [h for h in headings if h.endswith("_value")]
    assert value_candidates, f"Could not find value column in {custom_col_table}: {headings}"
    value_col = str(value_candidates[0])

    return id_col, fk_col, value_col


def _link_columns(driver_wrapper, table1: str, table2: str) -> dict[str, str]:
    """
    Ask the wrapper for both foreign-key column names in the link between two tables.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py


    :param driver_wrapper: Wrapper associated with the fixture database and its selected
        driver.
    :param table1: Left table whose ID column is mapped into the link.
    :param table2: Right table whose ID column is mapped into the link.
    :return: Mapping with left_fk and right_fk entries; optional link metadata is not
        requested.
    """

    left_id_col = driver_wrapper.get_id_column(table1)
    right_id_col = driver_wrapper.get_id_column(table2)

    return {
        "left_fk": driver_wrapper.get_link_column(table1, table2, left_id_col),
        "right_fk": driver_wrapper.get_link_column(table1, table2, right_id_col),
    }


def test_one_to_one_custom_column_uniqueness_and_cascade(driver, pick_payload) -> None:
    """
    Require one value per root, cascade deletion of that value, and survival of titles after an SQL-shaped insert.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py::test_one_to_one_custom_column_uniqueness_and_cascade


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    root_table, root_col = _create_root_table(driver)

    custom_table = driver.direct_create_custom_column(
        in_table=root_table,
        column_name="cc_oneone",
        data_type="TEXT",
        multi=False,
    )
    assert custom_table

    root_id = _insert_one(driver, root_table, root_col, pick_payload(0))

    _, fk_col, value_col = _one_one_columns(driver, str(custom_table))

    inj = "x'); DROP TABLE titles; --"

    # Insert the one-to-one custom value.
    driver.direct_add_simple_row_dict({fk_col: root_id, value_col: inj})
    assert int(driver.direct_get_record_count(str(custom_table))) == 1

    # Attempting to insert a second value for the same root row must violate the UNIQUE constraint.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({fk_col: root_id, value_col: pick_payload(1)})

    # Deleting the root row should cascade delete the custom-column row.
    driver.direct_delete_row_by_id(root_table, root_id)
    assert int(driver.direct_get_record_count(str(custom_table))) == 0

    # Ensure dangerous-looking strings are treated as inert data.
    assert "titles" in set(driver.direct_get_tables(force_refresh=True))


def test_one_many_custom_column_enforces_secondary_exclusivity_and_cleanup(
    driver,
    driver_wrapper,
    pick_payload,
) -> None:
    """
    Reject sharing a secondary value between roots and require orphan cleanup after link and root deletion.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py::test_one_many_custom_column_enforces_secondary_exclusivity_and_cleanup


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param driver_wrapper: Wrapper associated with the fixture database and its selected
        driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    root_table, root_col = _create_root_table(driver)

    custom_table = driver.direct_create_custom_column(
        in_table=root_table,
        column_name="cc_onemany",
        data_type="TEXT",
        multi="one_many",
    )
    assert custom_table

    link_table = driver_wrapper.get_link_table_name(root_table, str(custom_table))
    assert link_table, "Wrapper failed to discover link table for one_many custom column"
    cols = _link_columns(driver_wrapper, root_table, str(custom_table))

    root_id_1 = _insert_one(driver, root_table, root_col, pick_payload(2))
    root_id_2 = _insert_one(driver, root_table, root_col, pick_payload(3))

    cc_col = driver.direct_get_column_base(str(custom_table))
    cc_id_1 = _insert_one(driver, str(custom_table), cc_col, pick_payload(4))
    cc_id_2 = _insert_one(driver, str(custom_table), cc_col, pick_payload(5))

    # Root 1 links to two custom values (allowed).
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: cc_id_1})
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: cc_id_2})

    # The same secondary custom value must be exclusive to one root in a one_many mapping.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_2, cols["right_fk"]: cc_id_1})

    assert int(driver.direct_get_record_count(str(link_table))) == 2

    # Deleting a link row should trigger cleanup of the now-orphaned custom value row.
    link_row_id = int(driver.direct_get_highest_id(str(link_table)))
    driver.direct_delete_row_by_id(str(link_table), link_row_id)
    assert int(driver.direct_get_record_count(str(link_table))) == 1

    # One custom value should have been deleted by the trigger.
    assert int(driver.direct_get_record_count(str(custom_table))) == 1

    # Deleting the root row should cascade delete remaining links and (via trigger) remaining custom values.
    driver.direct_delete_row_by_id(root_table, root_id_1)
    assert int(driver.direct_get_record_count(str(link_table))) == 0
    assert int(driver.direct_get_record_count(str(custom_table))) == 0


def test_many_one_custom_column_allows_shared_secondary_but_limits_primary(driver, driver_wrapper, pick_payload) -> None:
    """
    Allow two roots to share one value and reject a second value for an already-linked root.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py::test_many_one_custom_column_allows_shared_secondary_but_limits_primary


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param driver_wrapper: Wrapper associated with the fixture database and its selected
        driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    root_table, root_col = _create_root_table(driver)

    custom_table = driver.direct_create_custom_column(
        in_table=root_table,
        column_name="cc_manyone",
        data_type="TEXT",
        multi="many_one",
    )
    assert custom_table

    link_table = driver_wrapper.get_link_table_name(root_table, str(custom_table))
    assert link_table, "Wrapper failed to discover link table for many_one custom column"
    cols = _link_columns(driver_wrapper, root_table, str(custom_table))

    root_id_1 = _insert_one(driver, root_table, root_col, pick_payload(6))
    root_id_2 = _insert_one(driver, root_table, root_col, pick_payload(7))

    cc_col = driver.direct_get_column_base(str(custom_table))
    shared_cc_id = _insert_one(driver, str(custom_table), cc_col, pick_payload(8))
    other_cc_id = _insert_one(driver, str(custom_table), cc_col, pick_payload(9))

    # Many roots can point to the same custom value (allowed).
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: shared_cc_id})
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_2, cols["right_fk"]: shared_cc_id})

    # But each root may only point to a single custom value.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: other_cc_id})


def test_many_many_custom_column_via_direct_method_roundtrips(driver, driver_wrapper, pick_payload) -> None:
    """
    Create three many-to-many links with shared endpoints and reject a duplicate endpoint pair.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py::test_many_many_custom_column_via_direct_method_roundtrips


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param driver_wrapper: Wrapper associated with the fixture database and its selected
        driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    root_table, root_col = _create_root_table(driver)

    custom_table = driver.direct_create_many_many_custom_column(
        target_table=root_table,
        custom_column_name="cc_manymany_direct",
    )
    assert custom_table

    link_table = driver_wrapper.get_link_table_name(root_table, str(custom_table))
    assert link_table, "Wrapper failed to discover link table for many_many custom column"
    cols = _link_columns(driver_wrapper, root_table, str(custom_table))

    root_id_1 = _insert_one(driver, root_table, root_col, pick_payload(10))
    root_id_2 = _insert_one(driver, root_table, root_col, pick_payload(11))

    cc_col = driver.direct_get_column_base(str(custom_table))
    cc_id_1 = _insert_one(driver, str(custom_table), cc_col, pick_payload(12))
    cc_id_2 = _insert_one(driver, str(custom_table), cc_col, pick_payload(13))

    # Root 1 links to two custom values; Root 2 links to one of them.
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: cc_id_1})
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_1, cols["right_fk"]: cc_id_2})
    driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_2, cols["right_fk"]: cc_id_1})

    assert int(driver.direct_get_record_count(str(link_table))) == 3

    # Duplicate pair should violate the many_many uniqueness constraint.
    with pytest.raises(DatabaseIntegrityError):
        driver.direct_add_simple_row_dict({cols["left_fk"]: root_id_2, cols["right_fk"]: cc_id_1})


def test_direct_create_custom_column_many_many_returns_table_name(driver) -> None:
    """
    Require the front-door many_many custom-column API to return a nonempty table-name string.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py::test_direct_create_custom_column_many_many_returns_table_name


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; failed expectations raise AssertionError.
    """

    root_table, _ = _create_root_table(driver)
    created = driver.direct_create_custom_column(
        in_table=root_table,
        column_name="cc_manymany_frontdoor",
        data_type="TEXT",
        multi="many_many",
    )

    assert isinstance(created, str) and created, f"Expected table name string; got: {created!r}"
