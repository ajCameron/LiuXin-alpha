"""Chunk 12: Triggers + direct SQL helpers exposed via DriverWrapper (and a few via Database).

This slice covers the (intentionally "sharp") escape hatches for prototyping and low-level
maintenance work:

* DriverWrapper.execute / executemany / executescript
* DriverWrapper.get convenience helper
* Database.get_triggers / drop_triggers / drop_all_triggers

The goal is twofold:

1) Provide regression coverage for "escape hatch" behaviors that other tests depend on.
2) Act as proxy tests for backends by stressing locking, commits, and error propagation.

Notes:

* For DDL/DML, these tests prefer a fresh connection via driver.get_connection() when
  cross-connection visibility matters.
* Some behaviors are currently imperfect by design/legacy (e.g. DriverWrapper.get(all=False)
  calling cursor.next()). We mark those as xfail to keep signal without forcing a refactor.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from LiuXin_alpha.errors import DatabaseDriverError


def _mk_ident(prefix: str) -> str:
    # Safe identifier: ASCII + underscores only.
    return f"{prefix}__{uuid.uuid4().hex}"


@dataclass(frozen=True)
class ContractTable:
    name: str
    id_col: str
    scratch_col: str
    payload_col: str


def _fresh_conn(db) -> sqlite3.Connection:
    """Get a fresh backend connection (sqlite-compatible)."""

    conn = db.driver.get_connection()
    # Ensure consistent behavior for foreign keys on external sqlite3.
    try:
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    return conn


def _exec(conn, sql: str, params=None):
    if params is None:
        return conn.execute(sql)
    return conn.execute(sql, params)


def _commit(conn) -> None:
    try:
        conn.commit()
    except Exception:
        try:
            conn.execute("COMMIT")
        except Exception:
            pass


@pytest.fixture
def contract_table(open_db) -> ContractTable:
    t = ContractTable(
        name=_mk_ident("db_contract_l12"),
        id_col="id",
        scratch_col="scratch",
        payload_col="payload",
    )

    conn = _fresh_conn(open_db)
    try:
        _exec(
            conn,
            f"CREATE TABLE {t.name} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "scratch TEXT NOT NULL DEFAULT '',"
            "payload TEXT"
            ");",
        )
        _commit(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Let caches notice the new table.
    try:
        open_db.driver.call_after_table_changes()
    except Exception:
        pass
    try:
        open_db.refresh_db_metadata()
    except Exception:
        pass

    return t


# -------------------------------------------------------------------------------------------------
# DriverWrapper.execute
# -------------------------------------------------------------------------------------------------


def test_driver_wrapper_execute_select_basic(open_db):
    cur = open_db.driver_wrapper.execute("SELECT 1")
    rows = cur.fetchall()
    assert rows == [(1,)]


@pytest.mark.parametrize(
    "payload",
    [
        "plain-ascii",
        "fran\u00e7ais \u2014 na\u00efve caf\u00e9",
        "rtl \u05e2\u05d1\u05e8\u05d9\u05ea \u0627\u0644\u0639\u0631\u0628\u064a\u0629",
        "cjk \u6f22\u5b57 \u304b\u306a \u30ab\u30ca",
        "emoji \U0001f600\U0001f916\U0001f9e0",
        "combining e\u0301cole",
        "zero\u200bwidth\u200djoiner",
        "sql-ish '; DROP TABLE books; --",
    ],
)
def test_driver_wrapper_execute_param_roundtrip(open_db, contract_table: ContractTable, payload: str):
    t = contract_table

    open_db.driver_wrapper.execute(f"INSERT INTO {t.name} (payload) VALUES (?)", (payload,))
    cur = open_db.driver_wrapper.execute(
        f"SELECT payload FROM {t.name} WHERE payload = ?",
        (payload,),
    )
    got = cur.fetchone()
    assert got is not None
    assert got[0] == payload


def test_driver_wrapper_execute_values_none_is_allowed(open_db, contract_table: ContractTable):
    t = contract_table
    open_db.driver_wrapper.execute(f"INSERT INTO {t.name} (payload) VALUES ('x')")
    cur = open_db.driver_wrapper.execute(f"SELECT COUNT(*) FROM {t.name}")
    assert int(cur.fetchone()[0]) == 1


def test_driver_wrapper_execute_invalid_sql_raises_database_driver_error(open_db):
    with pytest.raises(DatabaseDriverError):
        open_db.driver_wrapper.execute("SELEC 1")


def test_driver_wrapper_execute_rejects_multiple_statements(open_db):
    # sqlite3 disallows multiple statements per execute() call.
    with pytest.raises(DatabaseDriverError):
        open_db.driver_wrapper.execute("SELECT 1; SELECT 2;")


# -------------------------------------------------------------------------------------------------
# DriverWrapper.executemany
# -------------------------------------------------------------------------------------------------


def test_driver_wrapper_executemany_inserts_many(open_db, contract_table: ContractTable):
    t = contract_table
    rows = [("a",), ("b",), ("c",)]
    open_db.driver_wrapper.executemany(f"INSERT INTO {t.name} (payload) VALUES (?)", rows)
    cur = open_db.driver_wrapper.execute(f"SELECT COUNT(*) FROM {t.name}")
    assert int(cur.fetchone()[0]) == 3


def test_driver_wrapper_executemany_preflights_tuple_of_scalars(open_db, contract_table: ContractTable):
    t = contract_table
    # The driver layer preflights tuple-of-scalars into per-row singletons.
    open_db.driver_wrapper.executemany(f"INSERT INTO {t.name} (payload) VALUES (?)", ("x", "y", "z"))
    cur = open_db.driver_wrapper.execute(
        f"SELECT payload FROM {t.name} ORDER BY {t.id_col}"
    )
    got = [r[0] for r in cur.fetchall()]
    assert got == ["x", "y", "z"]


def test_driver_wrapper_executemany_propagates_driver_error(open_db, contract_table: ContractTable):
    t = contract_table
    with pytest.raises(DatabaseDriverError):
        # Wrong column name -> should surface as a driver error.
        open_db.driver_wrapper.executemany(f"INSERT INTO {t.name} (nope) VALUES (?)", [("a",)])


def test_driver_wrapper_executemany_decorates_valueerror(monkeypatch, open_db, contract_table: ContractTable):
    t = contract_table

    def boom(sql, values=None):  # noqa: ARG001
        raise ValueError("kaboom")

    monkeypatch.setattr(open_db.driver, "direct_executemany", boom)

    with pytest.raises(ValueError) as e:
        open_db.driver_wrapper.executemany(f"INSERT INTO {t.name} (payload) VALUES (?)", [("a",)])

    assert "ValueError while trying to executemany" in str(e.value)


# -------------------------------------------------------------------------------------------------
# DriverWrapper.executescript
# -------------------------------------------------------------------------------------------------


def test_driver_wrapper_executescript_creates_and_inserts(open_db):
    table = _mk_ident("db_contract_l12_script")
    open_db.driver_wrapper.executescript(
        f"""
        CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT);
        INSERT INTO {table} (payload) VALUES ('one');
        INSERT INTO {table} (payload) VALUES ('two');
        """
    )

    cur = open_db.driver_wrapper.execute(f"SELECT payload FROM {table} ORDER BY id")
    assert [r[0] for r in cur.fetchall()] == ["one", "two"]


def test_driver_wrapper_executescript_syntax_error_raises(open_db):
    with pytest.raises(DatabaseDriverError):
        open_db.driver_wrapper.executescript("CREATE TABL nope (x);")


# -------------------------------------------------------------------------------------------------
# DriverWrapper.get
# -------------------------------------------------------------------------------------------------


def test_driver_wrapper_get_default_all_true_returns_fetchall(open_db):
    rows = open_db.driver_wrapper.get("SELECT 42")
    assert rows == [(42,)]


@pytest.mark.xfail(reason="DriverWrapper.get(all=False) uses cursor.next(); sqlite cursors use fetchone()/__next__")
def test_driver_wrapper_get_all_false_returns_scalar(open_db):
    val = open_db.driver_wrapper.get("SELECT 7", all=False)
    assert val == 7


# -------------------------------------------------------------------------------------------------
# Database trigger helpers
# -------------------------------------------------------------------------------------------------


def test_database_get_triggers_returns_list(open_db):
    triggers = open_db.get_triggers()
    assert isinstance(triggers, list)


def test_database_create_trigger_visible_then_drop(open_db, contract_table: ContractTable):
    t = contract_table
    trig = _mk_ident("trg_l12")

    before = set(open_db.get_triggers())

    # Create a trigger that writes a predictable marker.
    open_db.driver_wrapper.executescript(
        f"""
        CREATE TRIGGER {trig}
        AFTER INSERT ON {t.name}
        BEGIN
            UPDATE {t.name} SET scratch='touched' WHERE id = NEW.id;
        END;
        """
    )

    after = set(open_db.get_triggers())
    assert trig in after
    assert before.issubset(after)

    open_db.driver_wrapper.execute(f"INSERT INTO {t.name} (payload) VALUES ('p')")
    cur = open_db.driver_wrapper.execute(f"SELECT scratch FROM {t.name} ORDER BY id DESC LIMIT 1")
    assert cur.fetchone()[0] == "touched"

    # Drop it and verify it is gone.
    open_db.drop_triggers([trig])
    final = set(open_db.get_triggers())
    assert trig not in final


def test_database_drop_triggers_removes_only_target(open_db, contract_table: ContractTable):
    t = contract_table
    trig1 = _mk_ident("trg_l12_a")
    trig2 = _mk_ident("trg_l12_b")

    open_db.driver_wrapper.executescript(
        f"""
        CREATE TRIGGER {trig1} AFTER INSERT ON {t.name} BEGIN UPDATE {t.name} SET scratch='a' WHERE id=NEW.id; END;
        CREATE TRIGGER {trig2} AFTER INSERT ON {t.name} BEGIN UPDATE {t.name} SET scratch='b' WHERE id=NEW.id; END;
        """
    )

    got = set(open_db.get_triggers())
    assert trig1 in got and trig2 in got

    open_db.drop_triggers([trig1])

    got2 = set(open_db.get_triggers())
    assert trig1 not in got2
    assert trig2 in got2


def test_database_drop_triggers_empty_list_is_true(open_db):
    assert open_db.drop_triggers([]) is True


def test_database_drop_triggers_injection_like_name_does_not_drop_table(open_db, contract_table: ContractTable):
    t = contract_table
    trig = _mk_ident("trg_l12_safe")

    open_db.driver_wrapper.executescript(
        f"""
        CREATE TRIGGER {trig}
        AFTER INSERT ON {t.name}
        BEGIN
            UPDATE {t.name} SET scratch='ok' WHERE id = NEW.id;
        END;
        """
    )

    evil = f"{trig}; DROP TABLE {t.name}; --"

    # direct_drop_triggers does naive string formatting. sqlite3 will reject multi-statement executes.
    with pytest.raises(Exception):
        open_db.drop_triggers([evil])

    # Table should still exist.
    cur = open_db.driver_wrapper.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t.name,))
    assert cur.fetchone()[0] == t.name


def test_database_drop_all_triggers_on_fresh_db_removes_all(driver_spec, tmp_path: Path):
    from LiuXin_alpha.databases.database import Database

    db_path = tmp_path / "chunk12_triggers.db"
    meta = {"database_path": str(db_path)}

    with Database(metadata=meta, db_type=driver_spec.db_type, create=True, backup=False) as db:
        # Create a trivial trigger on a simple table.
        table = _mk_ident("t12")
        trig = _mk_ident("trg12")

        db.driver_wrapper.executescript(
            f"""
            CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT);
            CREATE TRIGGER {trig} AFTER INSERT ON {table} BEGIN UPDATE {table} SET payload='x' WHERE id=NEW.id; END;
            """
        )

        assert trig in set(db.get_triggers())

        db.drop_all_triggers()
        assert db.get_triggers() == []
