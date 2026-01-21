"""Driver contract: views and triggers.

This module exercises the driver's view and trigger helpers.

We create a contract table with highly distinctive column names so that
__identify_table_from_row() cannot accidentally resolve to some other table.
A contract view is then created with an explicit `id` column so the view helper
(which assumes an `id` column) can round-trip data.

A contract trigger is also created, verified via direct_get_triggers(), and then
dropped via direct_drop_triggers().
"""

from __future__ import annotations

from typing import Any, Iterable

import pytest


_CONTRACT_TABLE = "contract_views_triggers"
_CONTRACT_VIEW = "contract_view_contract_views_triggers"


def _safe_close(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _fetchall(conn: Any, stmt: str, params: Iterable[Any] | None = None) -> list[tuple]:
    """Execute a statement and return rows for sqlite3/apsw-like connections."""

    params_tuple = tuple(params) if params is not None else None

    try:
        cur = conn.execute(stmt, params_tuple or ())
    except TypeError:
        # Some wrappers may not accept params when empty.
        cur = conn.execute(stmt) if params_tuple is None else conn.execute(stmt, params_tuple)

    if hasattr(cur, "fetchall"):
        return list(cur.fetchall())

    # APSW: cursor is iterable
    return [tuple(r) for r in cur]


@pytest.fixture
def vt_table(driver) -> str:
    """Create (or recreate) the contract table used for view/trigger testing."""

    t = _CONTRACT_TABLE

    sql = f"""
    DROP VIEW IF EXISTS `{_CONTRACT_VIEW}`;
    DROP TABLE IF EXISTS `{t}`;

    CREATE TABLE `{t}` (
        `{t}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `{t}_text` TEXT,
        `{t}_shadow` TEXT,
        `{t}_datestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    driver.direct_executescript(sql)

    # Ensure driver's cache sees the new table.
    assert t in set(driver.direct_get_tables(force_refresh=True))
    return t


@pytest.fixture
def vt_cols(vt_table: str) -> dict[str, str]:
    t = vt_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "shadow": f"{t}_shadow",
        "datestamp": f"{t}_datestamp",
    }


@pytest.fixture
def vt_view(driver, vt_table: str, vt_cols: dict[str, str]) -> str:
    """Create (or recreate) the contract view with an explicit `id` column."""

    view = _CONTRACT_VIEW

    sql = f"""
    DROP VIEW IF EXISTS `{view}`;
    CREATE VIEW `{view}` AS
        SELECT
            `{vt_cols['id']}` AS id,
            `{vt_cols['text']}` AS text,
            `{vt_cols['shadow']}` AS shadow
        FROM `{vt_table}`;
    """

    driver.direct_executescript(sql)
    return view


def _sqlite_master_has(conn: Any, obj_type: str, name: str) -> bool:
    rows = _fetchall(
        conn,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?;",
        (obj_type, name),
    )
    return bool(rows and int(rows[0][0]) > 0)


def test_view_can_roundtrip_row_dict(driver, vt_table: str, vt_cols: dict[str, str], vt_view: str, pick_payload):
    payload = pick_payload(10)  # avoid the explicit NUL payload

    # Insert a row via driver's helper (it identifies the table from the unique columns).
    driver.direct_add_simple_row_dict({vt_cols["text"]: payload, vt_cols["shadow"]: None})

    row_id = driver.direct_get_highest_id(vt_table)
    assert row_id is not None

    headings = driver.direct_get_view_column_headings(vt_view)
    assert isinstance(headings, list)
    assert headings
    assert "id" in headings

    got = driver.direct_get_view_row_dict_from_id(vt_view, row_id)
    assert got is not False
    assert got["id"] == row_id
    assert got["text"] == payload


def test_view_is_listed_in_sqlite_master(driver, vt_view: str):
    conn = driver.get_connection()
    try:
        assert _sqlite_master_has(conn, "view", vt_view)
        # A view should not also appear as a table.
        assert not _sqlite_master_has(conn, "table", vt_view)
    finally:
        _safe_close(conn)


def test_triggers_can_be_listed_and_dropped(driver, vt_table: str, vt_cols: dict[str, str], pick_payload, assert_integrity):
    baseline = set(driver.direct_get_triggers())

    trigger_name = f"trg_{vt_table}_shadow"  # safe characters only

    sql = f"""
    DROP TRIGGER IF EXISTS `{trigger_name}`;

    CREATE TRIGGER `{trigger_name}`
    AFTER INSERT ON `{vt_table}`
    BEGIN
        UPDATE `{vt_table}`
        SET `{vt_cols['shadow']}` = 'shadow:' || NEW.`{vt_cols['text']}`
        WHERE `{vt_cols['id']}` = NEW.`{vt_cols['id']}`;
    END;
    """

    driver.direct_executescript(sql)

    after_create = set(driver.direct_get_triggers())
    assert trigger_name in after_create

    # Trigger should fire: insert row with NULL shadow, then confirm it's set.
    payload = pick_payload(11)
    driver.direct_add_simple_row_dict({vt_cols["text"]: payload, vt_cols["shadow"]: None})
    row_id = driver.direct_get_highest_id(vt_table)

    row = driver.direct_get_row_dict_from_id(vt_table, row_id)
    assert row is not False
    assert row[vt_cols["shadow"]] == f"shadow:{payload}"

    assert driver.direct_drop_triggers([trigger_name]) is True

    after_drop = set(driver.direct_get_triggers())
    assert trigger_name not in after_drop
    assert after_drop == baseline

    # With trigger gone, shadow should remain None.
    payload2 = pick_payload(12)
    driver.direct_add_simple_row_dict({vt_cols["text"]: payload2, vt_cols["shadow"]: None})
    row_id2 = driver.direct_get_highest_id(vt_table)

    row2 = driver.direct_get_row_dict_from_id(vt_table, row_id2)
    assert row2 is not False
    assert row2[vt_cols["shadow"]] is None

    assert_integrity(driver)
