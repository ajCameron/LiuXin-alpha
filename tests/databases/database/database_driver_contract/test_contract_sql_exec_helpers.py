"""Driver contract: direct SQL execution helper methods.

This module exercises the lowest-level SQL plumbing exposed by the driver.

Goals:
- Ensure parameter binding works with hostile/unusual unicode payloads.
- Ensure scripts and executemany variants behave consistently.
- Ensure obvious SQL mistakes raise DatabaseDriverError (not raw sqlite errors).

These tests intentionally create their own small contract tables to avoid
coupling to any particular Calibre/LiuXin schema version.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import DatabaseDriverError


def _fetch_all(cursor_or_iter):
    """Fetch rows from either a DB-API cursor or an iterable."""
    if cursor_or_iter is None:
        return []
    fetchall = getattr(cursor_or_iter, "fetchall", None)
    if callable(fetchall):
        return fetchall()
    return list(cursor_or_iter)


def _table_exists(driver, table_name: str) -> bool:
    cur = driver.direct_execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?;",
        (table_name,),
    )
    rows = _fetch_all(cur)
    return bool(rows)


def test_direct_execute_binds_values_and_is_injection_inert(driver, pick_payload, assert_integrity):
    table = "contract_sql_exec"

    # Fresh table per test. Drop if it exists from a previous run.
    driver.direct_executescript(
        f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
        );
        """
    )

    payload = pick_payload(0)
    injection_shaped = pick_payload(9999)  # wraps by modulo

    # Insert two rows using proper binding (NOT string formatting).
    driver.direct_execute(f"INSERT INTO {table} (payload) VALUES (?);", (payload,))
    driver.direct_execute(f"INSERT INTO {table} (payload) VALUES (?);", (injection_shaped,))

    # The value must round-trip exactly.
    cur = driver.direct_execute(f"SELECT payload FROM {table} ORDER BY id ASC;")
    rows = _fetch_all(cur)
    got = [r[0] if isinstance(r, (tuple, list)) else r for r in rows]
    assert got == [payload, injection_shaped]

    # Ensure the injection-looking value did not mutate schema.
    assert _table_exists(driver, "titles"), "Core table 'titles' vanished; possible SQL injection vulnerability"

    # Basic integrity check after modifications.
    assert_integrity(driver)


def test_direct_execute_coerces_int_single_value_binding(driver):
    # When values is an int, drivers currently coerce to (force_unicode(values),)
    cur = driver.direct_execute("SELECT ?;", 123)
    rows = _fetch_all(cur)
    assert rows, "Expected a row from SELECT ?"

    first = rows[0][0] if isinstance(rows[0], (tuple, list)) else rows[0]
    assert str(first) == "123"


def test_direct_execute_raises_database_driver_error_on_invalid_sql(driver):
    with pytest.raises(DatabaseDriverError):
        driver.direct_execute("SELEC 1;")


def test_direct_executescript_runs_multiple_statements(driver, pick_payload):
    table = "contract_sql_script"
    payload = pick_payload(3)

    driver.direct_executescript(
        f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            n INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO {table} (payload, n) VALUES ('seed', 1);
        """
    )

    # Update using binding.
    driver.direct_execute(f"UPDATE {table} SET payload=?, n=n+1 WHERE id=1;", (payload,))

    cur = driver.direct_execute(f"SELECT payload, n FROM {table} WHERE id=1;")
    rows = _fetch_all(cur)
    assert rows and rows[0][0] == payload
    assert int(rows[0][1]) == 2


def test_direct_executemany_accepts_list_of_tuples_and_tuple_of_scalars(driver, pick_payload):
    table = "contract_sql_many"

    driver.direct_executescript(
        f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
        );
        """
    )

    p1 = pick_payload(10)
    p2 = pick_payload(11)
    p3 = pick_payload(12)
    p4 = pick_payload(13)

    # Variant A: list of 1-tuples.
    driver.direct_executemany(
        f"INSERT INTO {table} (payload) VALUES (?);",
        [(p1,), (p2,)],
    )

    # Variant B: tuple of scalars triggers the driver's "preflight" coercion.
    driver.direct_executemany(
        f"INSERT INTO {table} (payload) VALUES (?);",
        (p3, p4),
    )

    cur = driver.direct_execute(f"SELECT COUNT(*) FROM {table};")
    rows = _fetch_all(cur)
    count = int(rows[0][0] if isinstance(rows[0], (tuple, list)) else rows[0])
    assert count == 4
