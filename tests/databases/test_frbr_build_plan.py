"""Prove compilation boundaries, resource completeness and endpoint policy."""

import itertools
import sqlite3
from contextlib import closing
from dataclasses import FrozenInstanceError

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator as frbr,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.sql_resources import (
    execute_sql_resources,
    load_sql_resources,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import (
    SQLiteTableLinkingMixin,
)


def test_resource_snapshot_keeps_triggers_and_unpaired_markers(tmp_path):
    path = tmp_path / "bundle.sql"
    path.write_text("""
-- BREAK
CREATE TABLE events(value TEXT);
CREATE TABLE audit(value TEXT);
-- BREAK
CREATE TRIGGER copied AFTER INSERT ON events BEGIN
  INSERT INTO audit VALUES('first; -- BREAK');
  INSERT INTO audit VALUES(NEW.value);
END;
INSERT INTO events VALUES('last; value');
-- BREAK
CREATE INDEX final_index ON events(value);
-- trailing comment
""")
    alias = tmp_path / "alias.sql"
    alias.symlink_to(path)
    plan = load_sql_resources([path, path, alias])
    assert len(plan) == 1
    path.write_text("DROP TABLE events;")
    with pytest.raises(FrozenInstanceError):
        plan[0].statements = ()
    with closing(sqlite3.connect(":memory:")) as conn:
        execute_sql_resources(conn, plan)
        assert conn.execute("SELECT value FROM audit").fetchall() == [
            ("first; -- BREAK",),
            ("last; value",),
        ]
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name='final_index'"
        ).fetchone()


@pytest.mark.parametrize(
    "tail",
    [
        "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1;",
        "SELECT 'unfinished",
        "/* unfinished",
    ],
)
def test_incomplete_resource_fails_before_any_stage_ddl(tmp_path, tail):
    first, last = tmp_path / "first.sql", tmp_path / "last.sql"
    first.write_text("CREATE TABLE a(id INTEGER);")
    last.write_text(tail)
    with closing(sqlite3.connect(":memory:")) as conn:
        builder = frbr.SQLiteDatabaseGenerator(conn)
        builder.main_tables_sql_files = [first, last]
        with pytest.raises(ValueError, match=r"last.sql, statement 1"):
            builder.create_main_tables()
        assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_invalid_sql_reports_resource_and_statement(tmp_path):
    path = tmp_path / "invalid.sql"
    path.write_text("CREATE TABLE a(id INTEGER); CREATE TABLE broken(;")
    with closing(sqlite3.connect(":memory:")) as conn:
        with pytest.raises(TypeError, match=r"invalid.sql, statement 2"):
            execute_sql_resources(conn, load_sql_resources([path]))
        # Execution remains staged and the handle belongs to the caller.
        assert conn.execute("SELECT count(*) FROM a").fetchone() == (0,)


# Independent expectations: changing the column catalog cannot silently weaken this test.
OPTIONAL = {
    "priority": ("INTEGER", "0"),
    "primary": ("INTEGER", "0"),
    "type": ("TEXT", None),
    "origin": ("TEXT", None),
    "source": ("TEXT", None),
    "policy": ("TEXT", None),
    "data": ("TEXT", None),
    "index": ("TEXT", None),
    "sequence_number": ("INTEGER", None),
    "is_required": ("INTEGER", "1"),
}


@pytest.mark.parametrize("kind", ["interlink", "intralink"])
def test_every_optional_column_subset_executes_once_with_expected_type(kind):
    emitter = SQLiteTableLinkingMixin()
    for flags in itertools.product((False, True), repeat=len(OPTIONAL)):
        selected = {
            name for name, enabled in zip(OPTIONAL, flags, strict=True) if enabled
        }
        with closing(sqlite3.connect(":memory:")) as conn:
            conn.executescript(
                "CREATE TABLE works(work_id INTEGER PRIMARY KEY); CREATE TABLE agents(agent_id INTEGER PRIMARY KEY);"
            )
            if kind == "intralink":
                statements = emitter.direct_build_intralink_table_sql(
                    "works", requested_cols=selected
                )
                base = "work_work_intralink"
            else:
                statements, table = emitter.direct_get_direct_link_main_tables_sql(
                    "works",
                    "agents",
                    "many_many",
                    requested_cols=selected,
                )
                base = table[:-1]
            for statement in statements:
                conn.execute(statement)
            rows = conn.execute(f"PRAGMA table_info({base}s)").fetchall()
            columns = {r[1]: r for r in rows}
            assert len(columns) == len(rows)
            for suffix, (sql_type, default) in OPTIONAL.items():
                name = f"{base}_{suffix}"
                if suffix in selected | {"source"}:
                    assert (columns[name][2], columns[name][4]) == (sql_type, default)
                else:
                    assert name not in columns


def configure(tmp_path, *, placeholders=True, nullable=False):
    policy = f"allow_placeholders={str(placeholders).lower()}\n"
    (tmp_path / "interlink_table_requests.toml").write_text(
        policy
        + '[[interlinks]]\nleft="works"\nright="agents"\ntypes=["author"]\n'
        + f"nullable={str(nullable).lower()}\n"
    )
    (tmp_path / "intralink_table_requests.toml").write_text(
        policy
        + '[[intralinks]]\ntable="works"\ntypes=["related"]\n'
        + f"nullable={str(nullable).lower()}\n"
    )


@pytest.mark.parametrize(
    "placeholders,nullable", [(True, False), (False, False), (False, True)]
)
def test_compiled_link_policy_is_explicit_and_immutable(
    tmp_path, monkeypatch, placeholders, nullable
):
    configure(tmp_path, placeholders=placeholders, nullable=nullable)
    monkeypatch.setattr(frbr, "__folder__", str(tmp_path))
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.executescript(
            "CREATE TABLE works(work_id INTEGER PRIMARY KEY); CREATE TABLE agents(agent_id INTEGER PRIMARY KEY);"
        )
        builder = frbr.SQLiteDatabaseGenerator(conn)
        plan = builder.build_link_plan()
        assert len(plan) == 2
        assert conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone() == (2,)
        builder.interlink_allowed_types_by_table.clear()
        builder.intralink_allowed_types_by_table.clear()
        for step in plan:
            assert step.effective_nullable is (placeholders or nullable)
            builder._execute_link_step(step, conn)
            assert conn.execute(f"SELECT type FROM {step.table}__types").fetchall()
            if step.effective_nullable:
                conn.execute(f"INSERT INTO {step.table} DEFAULT VALUES")
            else:
                with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
                    conn.execute(f"INSERT INTO {step.table} DEFAULT VALUES")


def test_invalid_self_link_configuration_cannot_leave_interlinks(tmp_path, monkeypatch):
    configure(tmp_path)
    (tmp_path / "intralink_table_requests.toml").write_text(
        '[[intralinks]]\ntable="missing"\n'
    )
    monkeypatch.setattr(frbr, "__folder__", str(tmp_path))
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.executescript(
            "CREATE TABLE works(work_id INTEGER PRIMARY KEY); CREATE TABLE agents(agent_id INTEGER PRIMARY KEY);"
        )
        with pytest.raises((TypeError, ValueError), match="[Uu]nknown"):
            frbr.SQLiteDatabaseGenerator(conn).build_link_plan()
        assert conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone() == (2,)


@pytest.mark.parametrize("kind", ["interlink", "intralink"])
def test_custom_columns_cannot_redeclare_endpoints(kind):
    emitter = SQLiteTableLinkingMixin()
    with pytest.raises(TypeError, match="reserved"):
        if kind == "interlink":
            emitter.direct_get_direct_link_main_tables_sql(
                "works", "agents", requested_cols={"work_id"}
            )
        else:
            emitter.direct_build_intralink_table_sql(
                "works", requested_cols={"primary_id"}
            )
