"""
Driver contract: tree helpers (series/subjects).

This module exercises the driver's generic "tree" helper behaviour:

* ``direct_get_root_series(start_row)``
* ``direct_set_tree_ids(table)``

Despite the method name referencing "series", the implementation is table-
agnostic and works for any table that has exactly one ``*_parent`` or
``*_parent_id`` column.

We test against two real schema tables:

* subjects: TEXT display column -> good unicode coverage
* series: historically used as a tree, plus it has a required id=0 null row

These tests are intentionally strict ("fail" mode): if these helpers behave
differently across drivers, we want loud, actionable failures.
"""

from __future__ import annotations

from typing import Tuple

import pytest

from LiuXin_alpha.errors import InputIntegrityError


def _insert_and_get_id(driver, table: str, row_dict: dict) -> int:
    driver.direct_add_simple_row_dict(row_dict)
    row_id = driver.direct_get_highest_id(table)
    assert row_id is not None
    return int(row_id)


def _fetch_row(driver, table: str, row_id: int) -> dict:
    row = driver.direct_get_row_dict_from_id(table, row_id)
    assert row is not False
    assert isinstance(row, dict)
    return row


def _make_subjects_chain(
    driver, *, root_value: str, child_value: str, grand_value: str
) -> Tuple[int, int, int]:
    parent_col = driver.direct_get_parent_column_name("subjects")
    root_id = _insert_and_get_id(driver, "subjects", {"subject": root_value, parent_col: None})
    child_id = _insert_and_get_id(driver, "subjects", {"subject": child_value, parent_col: root_id})
    grand_id = _insert_and_get_id(driver, "subjects", {"subject": grand_value, parent_col: child_id})
    return root_id, child_id, grand_id


def _make_series_chain(driver, *, root_value, child_value, grand_value) -> Tuple[int, int, int]:
    parent_col = driver.direct_get_parent_column_name("series")
    root_id = _insert_and_get_id(driver, "series", {"series": root_value, parent_col: None})
    child_id = _insert_and_get_id(driver, "series", {"series": child_value, parent_col: root_id})
    grand_id = _insert_and_get_id(driver, "series", {"series": grand_value, parent_col: child_id})
    return root_id, child_id, grand_id


def test_direct_get_root_series_finds_root_in_subjects(driver, pick_payload):
    root_val = pick_payload(10)   # emoji 😀🤖🧠
    child_val = pick_payload(12)  # rtl עברית العربية
    grand_val = pick_payload(18)  # injection-shaped is fine as inert data

    root_id, child_id, grand_id = _make_subjects_chain(
        driver, root_value=root_val, child_value=child_val, grand_value=grand_val
    )

    root_row = _fetch_row(driver, "subjects", root_id)
    child_row = _fetch_row(driver, "subjects", child_id)
    grand_row = _fetch_row(driver, "subjects", grand_id)

    # Root of root is itself.
    got_root_from_root = driver.direct_get_root_series(root_row)
    assert got_root_from_root["subject_id"] == root_id

    # Root of child and grandchild should be the root row.
    got_root_from_child = driver.direct_get_root_series(child_row)
    got_root_from_grand = driver.direct_get_root_series(grand_row)

    assert got_root_from_child["subject_id"] == root_id
    assert got_root_from_grand["subject_id"] == root_id

    # Ensure the display value is preserved.
    assert got_root_from_grand["subject"] == root_val


def test_direct_set_tree_ids_subjects_is_deterministic(driver, pick_payload, assert_integrity):
    # Build two separate trees so we can assert tree_id differs by root.
    r1, c1, g1 = _make_subjects_chain(
        driver,
        root_value=pick_payload(10),
        child_value=pick_payload(11),
        grand_value=pick_payload(13),
    )
    r2, c2, g2 = _make_subjects_chain(
        driver,
        root_value=pick_payload(14),
        child_value=pick_payload(15),
        grand_value=pick_payload(19),
    )

    root1 = _fetch_row(driver, "subjects", r1)
    root2 = _fetch_row(driver, "subjects", r2)

    expected_tree1 = f"{r1}_{root1['subject']}"
    expected_tree2 = f"{r2}_{root2['subject']}"

    # Run twice to ensure idempotence.
    assert driver.direct_set_tree_ids("subjects") is True
    assert driver.direct_set_tree_ids("subjects") is True

    for row_id in (r1, c1, g1):
        row = _fetch_row(driver, "subjects", row_id)
        assert row["subject_tree_id"] == expected_tree1

    for row_id in (r2, c2, g2):
        row = _fetch_row(driver, "subjects", row_id)
        assert row["subject_tree_id"] == expected_tree2

    assert expected_tree1 != expected_tree2
    assert_integrity(driver)


def test_direct_get_root_series_and_set_tree_ids_on_series(driver, pick_payload, assert_integrity):
    # series has a required null row at id=0 in test_db_13; our inserts should not collide.
    root_value = pick_payload(0)   # plain-ascii
    child_value = pick_payload(1)
    grand_value = pick_payload(2)

    r, c, g = _make_series_chain(driver, root_value=root_value, child_value=child_value, grand_value=grand_value)

    grand_row = _fetch_row(driver, "series", g)

    got_root = driver.direct_get_root_series(grand_row)
    assert got_root["series_id"] == r
    assert got_root.get("series") == root_value

    assert driver.direct_set_tree_ids("series") is True

    expected_tree = f"{r}_{root_value}"
    for row_id in (r, c, g):
        row = _fetch_row(driver, "series", row_id)
        assert row["series_tree_id"] == expected_tree

    # The required null row (id=0) should also receive a deterministic tree id.
    null_row = _fetch_row(driver, "series", 0)
    assert null_row["series_tree_id"] == f"0_{null_row.get('series')}"  # usually '0_None'

    assert_integrity(driver)


def test_direct_set_tree_ids_rejects_non_tree_table(driver):
    # titles has no *_tree_id column in the default schema.
    with pytest.raises(InputIntegrityError):
        driver.direct_set_tree_ids("titles")
