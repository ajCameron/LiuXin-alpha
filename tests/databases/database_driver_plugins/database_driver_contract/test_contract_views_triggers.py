"""
Check view aliases and row readback, plus trigger listing, execution, removal, and baseline restoration.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py
"""

from __future__ import annotations

from typing import Any, Iterable

import pytest


_CONTRACT_TABLE = "contract_views_triggers"
_CONTRACT_VIEW = "contract_view_contract_views_triggers"


def _safe_close(conn: Any) -> None:
    """
    Attempt to close the supplied object and suppress any ordinary exception.

    Example:
        >>> _safe_close(None)


    :param conn: Object whose close method is attempted.
    :return: None; the object may already be closed or lack a usable close method.
    """
    try:
        conn.close()
    except Exception:
        pass


def _fetchall(conn: Any, stmt: str, params: Iterable[Any] | None = None) -> list[tuple]:
    """
    Execute SQL with materialized bindings and collect rows from a cursor or iterable.

    On TypeError, retry without bindings only when params was None; otherwise retry with
    the same tuple. A fetchall attribute is called without a callable check and its rows
    are only wrapped in list. The iterable branch converts each row to tuple.

    Example:
        >>> import sqlite3
        >>> connection = sqlite3.connect(':memory:')
        >>> _fetchall(connection, 'SELECT ?', [42])
        [(42,)]
        >>> connection.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param stmt: SQL statement sent to the caller-owned connection.
    :param params: Optional binding iterable consumed once into a tuple.
    :return: Materialized rows; no commit or close occurs.
    """

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
    """
    Drop the contract view and table, recreate the backing table, and assert it appears after refreshing the cache.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: Trusted table name; the isolated database fixture owns cleanup.
    """

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
    """
    Derive ID, text, shadow, and timestamp column names from the backing table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py


    :param vt_table: Recreated isolated table backing the view and trigger contracts.
    :return: Fresh role-to-column mapping.
    """
    t = vt_table
    return {
        "id": f"{t}_id",
        "text": f"{t}_text",
        "shadow": f"{t}_shadow",
        "datestamp": f"{t}_datestamp",
    }


@pytest.fixture
def vt_view(driver, vt_table: str, vt_cols: dict[str, str]) -> str:
    """
    Drop and recreate the contract view with id, text, and shadow aliases over the backing table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param vt_table: Recreated isolated table backing the view and trigger contracts.
    :param vt_cols: Mapping from view/trigger field roles to concrete base-table
        columns.
    :return: Trusted view name; existing backing rows are retained.
    """

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
    """
    Count SQLite schema objects using bound type and name values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param obj_type: Object type bound to the sqlite_master query.
    :param name: Exact object name bound to the query.
    :return: True when the first count is positive; False for no rows or a zero count.
    """
    rows = _fetchall(
        conn,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?;",
        (obj_type, name),
    )
    return bool(rows and int(rows[0][0]) > 0)


def test_view_can_roundtrip_row_dict(driver, vt_table: str, vt_cols: dict[str, str], vt_view: str, pick_payload):
    """
    Insert a payload and require nonempty view headings containing id plus exact ID/text readback.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py::test_view_can_roundtrip_row_dict


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param vt_table: Recreated isolated table backing the view and trigger contracts.
    :param vt_cols: Mapping from view/trigger field roles to concrete base-table
        columns.
    :param vt_view: Contract view exposing id, text, and shadow aliases.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Require the contract view to appear as a view and not a table, then attempt to close the acquired connection in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py::test_view_is_listed_in_sqlite_master


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param vt_view: Contract view exposing id, text, and shadow aliases.
    :return: None; failed expectations raise AssertionError.
    """
    conn = driver.get_connection()
    try:
        assert _sqlite_master_has(conn, "view", vt_view)
        # A view should not also appear as a table.
        assert not _sqlite_master_has(conn, "table", vt_view)
    finally:
        _safe_close(conn)


def test_triggers_can_be_listed_and_dropped(driver, vt_table: str, vt_cols: dict[str, str], pick_payload, assert_integrity):
    """
    Create a shadow-populating trigger, verify its effect, drop it, and require the original trigger-name set.

    A subsequent insertion must retain a None shadow, followed by the shared integrity
    check.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_views_triggers.py::test_triggers_can_be_listed_and_dropped


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param vt_table: Recreated isolated table backing the view and trigger contracts.
    :param vt_cols: Mapping from view/trigger field roles to concrete base-table
        columns.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :param assert_integrity: Fixture callable requiring the first retained
        integrity_check result to be ok.
    :return: None; failed expectations raise AssertionError.
    """
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
