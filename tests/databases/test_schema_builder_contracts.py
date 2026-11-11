"""Check schema-builder error boundaries and single-pass column requests."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import (
    SQLiteTableLinkingMixin,
)


class _Builder(SQLiteTableLinkingMixin):
    INTERLINK_TABLE_CONSTRAINTS = {
        "agent_work_links": {
            "primary": "agents",
            "secondary": "works",
            "link_type": "many_many",
        },
    }
    ALLOWED_INTERLINK_TYPE_VAL_DICT = {"agent_work_links": ["author"]}


def test_link_script_preserves_backend_error():
    failure = sqlite3.OperationalError("injected link DDL failure")

    class FailingBuilder(_Builder):
        def direct_executescript(self, script):
            raise failure

        def _zero_prop_cache(self):
            pytest.fail("Failed DDL must not be reported as a successful cache refresh")

    with pytest.raises(sqlite3.OperationalError) as caught:
        FailingBuilder().direct_link_main_tables("agents", "works")
    assert caught.value is failure


def test_link_builder_preserves_one_pass_column_requests():
    builder = _Builder()
    expected = builder.build_interlink_table_sqlite(
        "agents", "works", requested_cols=("priority", "type")
    )
    generated = builder.build_interlink_table_sqlite(
        "agents", "works", requested_cols=(column for column in ("priority", "type"))
    )
    assert generated == expected
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.executescript(
            "CREATE TABLE agents (agent_id INTEGER PRIMARY KEY); CREATE TABLE works (work_id INTEGER PRIMARY KEY);"
        )
        connection.executescript("\n".join(generated))
        headings = {
            row[1] for row in connection.execute("PRAGMA table_info(agent_work_links)")
        }
        assert {"agent_work_link_priority", "agent_work_link_type"} <= headings


def test_generator_exposes_its_declared_link_execution_operation():
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        SQLiteDatabaseGenerator,
    )

    with closing(sqlite3.connect(":memory:")) as connection:
        connection.executescript(
            "CREATE TABLE agents (agent_id INTEGER PRIMARY KEY); CREATE TABLE works (work_id INTEGER PRIMARY KEY);"
        )
        generator = SQLiteDatabaseGenerator(connection)
        assert (
            generator.direct_link_main_tables(
                "agents", "works", requested_cols=("priority", "type")
            )
            == "agent_work_links"
        )
        assert "agent_work_links" in generator.direct_get_tables()


def test_duplicate_column_checker_accepts_mappings_and_one_pass_names():
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_checker import (
        check_for_duplicate_column_names,
    )

    columns = {"works": {"work_id": object(), "work_title": object()}}
    assert not check_for_duplicate_column_names(columns)
    assert check_for_duplicate_column_names(columns, iter(("work_title",)))
    assert check_for_duplicate_column_names(
        {"left": ["shared"], "right": iter(("shared",))}
    )


def test_legacy_link_request_identifies_unresolved_endpoints():
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        SQLiteDatabaseGenerator,
    )

    with closing(sqlite3.connect(":memory:")) as connection:
        generator = SQLiteDatabaseGenerator(connection)
        generator.main_tables = {"agents", "works"}
        assert generator.extract_main_tables("works-agents_link") == ["agents", "works"]
        assert generator.extract_main_tables("not a link") is None
        with pytest.raises(TypeError, match="missing-works_link"):
            generator.extract_main_tables("missing-works_link")


def test_calibre_resource_paths_decode_filesystem_bytes(monkeypatch, tmp_path):
    from LiuXin_alpha.utils.calibre_compat.database_generator import (
        database_generator,
    )

    path = tmp_path / "schema.sql"
    monkeypatch.setattr(database_generator, "get_path", lambda resource: bytes(path))
    resolved = database_generator.calibre_sql_paths()
    assert resolved
    assert set(resolved.values()) == {str(path)}
