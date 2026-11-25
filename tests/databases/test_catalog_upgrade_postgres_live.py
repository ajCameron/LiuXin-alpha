"""Repair old native Catalog shapes atomically in disposable PostgreSQL schemas."""

from contextlib import closing
from dataclasses import replace

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.catalog_upgrade import (
    _changes,
    apply_catalog_schema_upgrade,
    plan_catalog_schema_upgrade,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.link_ddl import _q
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


def old_shape(db):
    """Restore the known pre-fix generated constraints, preserving every data row."""
    with db.macros.transaction() as connection:
        connection.execute(
            "ALTER TABLE series DROP COLUMN series_parent_position"
        ).close()
        for change in _changes():
            table = _q(change.table)
            connection.execute(
                f"ALTER TABLE {table} DROP CONSTRAINT {_q(change.new_name)}"
            ).close()
            columns = ", ".join(_q(column) for column in change.old_columns)
            connection.execute(
                f"ALTER TABLE {table} ADD CONSTRAINT {_q(change.old_name)} UNIQUE ({columns})"
            ).close()
    db.refresh_db_metadata()


def rows(db, table):
    with db.macros.read_transaction() as connection:
        with closing(
            connection.execute(f"SELECT * FROM {_q(table)} ORDER BY 1")
        ) as cursor:
            return tuple(tuple(row) for row in cursor.fetchall())


def test_postgres_catalog_upgrade_preserves_rows_and_enables_roles(pg_database):
    db, _, _ = pg_database
    catalog = Catalog(db)
    work = catalog.works.create({"title": "Retained work"})
    agent = catalog.agents.create_person({"name": "Retained author"})
    catalog.agents.link_to_wemi(
        agent_id=agent, level="work", entity_id=work, role="aut"
    )
    series = catalog.series.create({"name": "Retained series"})
    old_shape(db)
    original = {
        table: rows(db, table)
        for table in ("works", "agents", "agent_work_links", "series")
    }
    preview = plan_catalog_schema_upgrade(db)
    assert not preview.blockers and preview.statements
    assert any(
        'ADD COLUMN "series_parent_position"' in sql for sql in preview.statements
    )
    assert original == {table: rows(db, table) for table in original}
    assert apply_catalog_schema_upgrade(db, preview) == preview
    for table in ("works", "agents", "agent_work_links"):
        assert rows(db, table) == original[table]
    with db.macros.read_transaction():
        assert catalog.series.require(series)["series"] == "Retained series"
        assert catalog.series.require(series)["series_parent_position"] is None
    catalog.agents.link_to_wemi(
        agent_id=agent, level="work", entity_id=work, role="edt"
    )
    credits = rows(db, "agent_work_links")
    assert len(credits) == 2
    assert original["agent_work_links"][0] in credits
    current = plan_catalog_schema_upgrade(db)
    assert current.statements == current.blockers == ()
    assert apply_catalog_schema_upgrade(db, current) == current
    with pytest.raises(ValueError, match="stale"):
        apply_catalog_schema_upgrade(db, preview)


@pytest.mark.parametrize(
    "conflict", ("column", "constraint", "index", "foreign_key", "null_semantics")
)
def test_postgres_catalog_upgrade_refuses_customized_old_shape(pg_database, conflict):
    db, _, _ = pg_database
    old_shape(db)
    with db.macros.transaction() as connection:
        if conflict == "column":
            sql = "ALTER TABLE series ADD COLUMN series_parent_position text"
        elif conflict == "constraint":
            connection.execute(
                "ALTER TABLE agent_work_links DROP CONSTRAINT agent_work_link_unique_pair"
            ).close()
            sql = "ALTER TABLE agent_work_links ADD CONSTRAINT agent_work_link_unique_pair UNIQUE (agent_work_link_agent_id)"
        elif conflict == "index":
            sql = "CREATE UNIQUE INDEX custom_pair ON agent_work_links (agent_work_link_agent_id, agent_work_link_work_id)"
        elif conflict == "foreign_key":
            sql = "CREATE TABLE custom_reference (agent bigint, work bigint, FOREIGN KEY (work, agent) REFERENCES agent_work_links (agent_work_link_work_id, agent_work_link_agent_id))"
        else:
            connection.execute(
                "ALTER TABLE agent_work_links DROP CONSTRAINT agent_work_link_unique_pair"
            ).close()
            sql = "ALTER TABLE agent_work_links ADD CONSTRAINT agent_work_link_unique_pair UNIQUE NULLS NOT DISTINCT (agent_work_link_work_id, agent_work_link_agent_id)"
        connection.execute(sql).close()
    plan = plan_catalog_schema_upgrade(db)
    assert plan.blockers
    with pytest.raises(ValueError, match="blocked"):
        apply_catalog_schema_upgrade(db, plan)
    assert plan_catalog_schema_upgrade(db) == plan


def test_postgres_catalog_upgrade_rechecks_plan_and_rolls_back_ddl(
    pg_database, monkeypatch
):
    db, _, _ = pg_database
    old_shape(db)
    plan = plan_catalog_schema_upgrade(db)
    with pytest.raises(ValueError, match="stale"):
        apply_catalog_schema_upgrade(db, replace(plan, statements=("SELECT 1",)))
    assert plan_catalog_schema_upgrade(db) == plan

    # A real late DDL failure must undo the earlier constraint replacements.
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import (
        catalog_upgrade,
    )

    inspect = catalog_upgrade._inspect
    bad = replace(
        plan, statements=(*plan.statements, "SELECT no_such_upgrade_function()")
    )
    monkeypatch.setattr(catalog_upgrade, "_inspect", lambda connection, schema: bad)
    with pytest.raises(Exception, match="no_such_upgrade_function"):
        apply_catalog_schema_upgrade(db, bad)
    monkeypatch.setattr(catalog_upgrade, "_inspect", inspect)
    assert plan_catalog_schema_upgrade(db) == plan
    with db.macros.transaction():
        with pytest.raises(RuntimeError, match="outer|transaction"):
            apply_catalog_schema_upgrade(db, plan)


def test_postgres_catalog_upgrade_rejects_pending_driver_and_changed_identity(
    pg_database,
):
    db, _, _ = pg_database
    old_shape(db)
    plan = plan_catalog_schema_upgrade(db)
    connection = db.driver.conn
    connection.execute("SELECT 1").close()
    try:
        with pytest.raises(RuntimeError, match="pending transaction"):
            apply_catalog_schema_upgrade(db, plan)
    finally:
        connection.rollback()
    assert plan_catalog_schema_upgrade(db) == plan
    with db.macros.transaction() as connection:
        connection.execute(
            "UPDATE database_metadata SET database_metadata_unique_id='different-catalogue'"
        ).close()
    with pytest.raises(ValueError, match="stale"):
        apply_catalog_schema_upgrade(db, plan)


def test_postgres_catalog_upgrade_cli_previews_then_applies_saved_plan(
    pg_database, tmp_path, monkeypatch, capsys
):
    import json

    from scripts.upgrade_postgres_catalog import main

    db, metadata, _ = pg_database
    old_shape(db)
    monkeypatch.setenv("LIUXIN_POSTGRES_URL", metadata["postgres_url"])
    args = ["--schema", metadata["schema"]]
    capsys.readouterr()
    assert main(args) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["statements"] and not preview["blockers"]
    path = tmp_path / "upgrade.json"
    path.write_text(json.dumps(preview))
    assert main([*args, "--apply-plan", str(path)]) == 0
    assert json.loads(capsys.readouterr().out) == preview
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["statements"] == []
