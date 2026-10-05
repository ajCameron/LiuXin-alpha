"""
Check smallest-group selection and targeted deletion using isolated new_books rows.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py
"""

from __future__ import annotations

from typing import Iterable


def _wipe_new_books(driver) -> None:
    """
    Clear every new_books row through the driver before a group contract test.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; mutates the fixture database.
    """
    driver.direct_clear_table("new_books")


def _insert_new_book(driver, *, name: str, group_id: int, size: int, path: int = 123, ext: int = 0) -> int:
    """
    Insert a new_books row with supplied group metadata and fixed hash/cache defaults.

    Infer the inserted ID from the current highest ID, assuming no competing inserts.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param name: Stored book-name payload.
    :param group_id: Group identifier stored on the row.
    :param size: Stored new_book_size value.
    :param path: Stored path value; defaults to 123.
    :param ext: Stored extension value; defaults to zero.
    :return: Highest new_books ID converted to int.
    """
    driver.direct_add_simple_row_dict(
        {
            "new_book_name": name,
            "new_book_extension": ext,
            "new_book_path": path,
            "new_book_hash_1": None,
            "new_book_hash_2": 42,
            "new_book_size": size,
            "new_book_group_id": group_id,
            "new_book_cached": 0,
            "new_book_cache_attempted": 0,
        }
    )
    return int(driver.direct_get_highest_id("new_books"))


def _group_ids(rows: Iterable[dict]) -> set[int]:
    """
    Collect integer group identifiers from rows with a present, non-None new_book_group_id.

    Example:
        >>> _group_ids([{}, {'new_book_group_id': None}, {'new_book_group_id': '2'}])
        {2}


    :param rows: Iterable of row mappings; missing and None group fields are ignored.
    :return: Distinct integer IDs; malformed values propagate conversion errors.
    """
    return {int(r["new_book_group_id"]) for r in rows if r.get("new_book_group_id") is not None}


def test_direct_get_next_book_group_empty_returns_empty_and_none(driver):
    """
    Require an empty row list and None group ID after clearing new_books.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py::test_direct_get_next_book_group_empty_returns_empty_and_none


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; failed expectations raise AssertionError.
    """
    _wipe_new_books(driver)
    group, gid = driver.direct_get_next_book_group()
    assert group == []
    assert gid is None


def test_direct_get_next_book_group_returns_smallest_group_and_all_rows(driver, pick_payload):
    """
    Insert group two first, then require both exact group-one IDs and dictionary-shaped returned rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py::test_direct_get_next_book_group_returns_smallest_group_and_all_rows


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    _wipe_new_books(driver)

    # Insert group 2 first, then group 1, so min(group_id) is exercised.
    _insert_new_book(driver, name=pick_payload(0), group_id=2, size=200)
    g1_a = _insert_new_book(driver, name=pick_payload(10), group_id=1, size=100)
    g1_b = _insert_new_book(driver, name=pick_payload(18), group_id=1, size=101)

    group, gid = driver.direct_get_next_book_group()

    assert gid == 1
    assert isinstance(group, list)
    assert len(group) == 2

    for row in group:
        assert isinstance(row, dict)
        assert int(row["new_book_group_id"]) == 1
        assert "new_book_id" in row

    returned_ids = {int(r["new_book_id"]) for r in group}
    assert returned_ids == {g1_a, g1_b}
    assert _group_ids(group) == {1}


def test_direct_delete_book_group_removes_only_target_group(driver, pick_payload):
    """
    Delete group one, check its rows are absent, and require both group-two rows as the next group.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py::test_direct_delete_book_group_removes_only_target_group


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
    _wipe_new_books(driver)

    g1_ids = [
        _insert_new_book(driver, name=pick_payload(11), group_id=1, size=10),
        _insert_new_book(driver, name=pick_payload(12), group_id=1, size=11),
    ]
    g2_ids = [
        _insert_new_book(driver, name=pick_payload(13), group_id=2, size=20),
        _insert_new_book(driver, name=pick_payload(14), group_id=2, size=21),
    ]

    group, gid = driver.direct_get_next_book_group()
    assert gid == 1
    assert {int(r["new_book_id"]) for r in group} == set(g1_ids)

    driver.direct_delete_book_group(1)

    for row_id in g1_ids:
        assert driver.direct_get_row_dict_from_id("new_books", row_id) is False

    for row_id in g2_ids:
        row = driver.direct_get_row_dict_from_id("new_books", row_id)
        assert row is not False
        assert int(row["new_book_group_id"]) == 2

    group2, gid2 = driver.direct_get_next_book_group()
    assert gid2 == 2
    assert {int(r["new_book_id"]) for r in group2} == set(g2_ids)


def test_direct_delete_book_group_is_idempotent_for_missing_group(driver):
    """
    Delete one absent group from a cleared table and require the empty-group result.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_book_groups_new_books.py::test_direct_delete_book_group_is_idempotent_for_missing_group


    :param driver: Backend driver supplied by the isolated database fixture; its
        teardown attempts to close the driver.
    :return: None; failed expectations raise AssertionError.
    """
    _wipe_new_books(driver)
    driver.direct_delete_book_group(123456)
    group, gid = driver.direct_get_next_book_group()
    assert group == []
    assert gid is None
