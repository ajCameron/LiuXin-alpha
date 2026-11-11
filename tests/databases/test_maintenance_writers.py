"""Exercise installed maintenance through production database and Catalog writers."""

from __future__ import annotations

import os
from contextlib import closing
from uuid import uuid4

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.maintenance import WorkRevisionPlugin, work_summary
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.library.library import Library


@pytest.fixture(params=["SQLite", "SQLite_apsw", "PostgreSQL"])
def maintained_database(tmp_path, request):
    backend = request.param
    cleanup = None
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    if backend == "PostgreSQL":
        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
        from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
            connect_postgres,
        )

        metadata = {"postgres_url": url, "schema": "maintenance_writers_" + uuid4().hex}
        cleanup = connect_postgres(metadata, prompt_for_password=False)
    else:
        metadata = {"database_path": str(tmp_path / "catalog.sqlite")}
    database = Database(
        metadata=metadata,
        db_type=backend,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    library = Library(database=database)
    runtime = library.maintenance_runtime
    runtime.install()
    try:
        yield database, runtime
    finally:
        database.close()
        if cleanup:
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(f'DROP SCHEMA "{metadata["schema"]}" CASCADE')
            cleanup.close()


def test_production_write_routes_finish_prompt_work_and_roll_back_failures(
    maintained_database, monkeypatch
):
    database, runtime = maintained_database
    catalog = Catalog(database)
    plugin = runtime._plugins[WorkRevisionPlugin.name]
    original = plugin.handle_events

    def fail(context, events):
        original(context, events)
        raise RuntimeError("required Work revision failed")

    def cte_writer(title):
        with closing(database.driver.get_connection()) as connection, connection:
            return connection.execute(
                "WITH source(title) AS (VALUES (?)) "
                "INSERT INTO works(work_title) SELECT title FROM source RETURNING work_id",
                (title,),
            ).fetchone()[0]

    writers = (
        lambda title: database.driver.direct_add_simple_row_dict({"work_title": title}),
        lambda title: catalog.works.create({"title": title}),
        cte_writer,
    )
    for index, writer in enumerate(writers):
        row_id = writer(f"kept {index}")
        assert work_summary(database, row_id)["source_revision"] is not None
        monkeypatch.setattr(plugin, "handle_events", fail)
        with pytest.raises(Exception, match="required Work revision failed"):
            writer(f"discarded {index}")
        monkeypatch.setattr(plugin, "handle_events", original)

    row_id = catalog.works.create({"title": "before"})
    before = work_summary(database, row_id)["source_revision"]
    monkeypatch.setattr(plugin, "handle_events", fail)
    with pytest.raises(Exception, match="required Work revision failed"):
        database.driver.direct_update_row_dict(
            {"work_id": row_id, "work_title": "discarded update"}
        )
    row = database.get_row_from_id("works", row_id)
    row["work_title"] = "discarded Row sync"
    with pytest.raises(Exception, match="required Work revision failed"):
        row.sync()
    with pytest.raises(Exception, match="required Work revision failed"):
        with database.macros.transaction() as connection:
            connection.execute(
                "UPDATE works SET work_title=? WHERE work_id=?",
                ("discarded macro", row_id),
            )
    monkeypatch.setattr(plugin, "handle_events", original)
    with closing(database.driver.get_connection()) as connection:
        titles = [
            entry[0]
            for entry in connection.execute("SELECT work_title FROM works").fetchall()
        ]
    assert sorted(titles) == ["before", "kept 0", "kept 1", "kept 2"]
    assert work_summary(database, row_id)["source_revision"] == before


def test_installed_readonly_queries_do_not_repair_or_create_jobs(
    maintained_database, tmp_path
):
    database, runtime = maintained_database
    row_id = Catalog(database).works.create({"title": "unprocessed"})
    before = runtime.status()
    assert not work_summary(database, row_id)["current"]
    after = runtime.status()
    assert [(row["obligation_id"], row["attempts"]) for row in after["pending"]] == [
        (row["obligation_id"], row["attempts"]) for row in before["pending"]
    ]
    assert list(tmp_path.glob("*.jobs.sqlite")) == []


@pytest.mark.parametrize("failure", ["second_link", "source_delete", "prompt"])
def test_physical_merge_rolls_back_all_transfers_on_failure(
    maintained_database, monkeypatch, failure
):
    from LiuXin_alpha.databases.maintenance.service import Maintainer

    database, runtime = maintained_database
    macros, catalog = database.macros, Catalog(database)
    kept = catalog.works.create({"title": "kept"})
    removed = catalog.works.create({"title": "removed"})
    expressions = [catalog.expressions.create({"title": str(i)}) for i in range(2)]
    links = [
        macros.insert_row(
            "expression_work_links",
            {
                "expression_work_link_work_id": removed,
                "expression_work_link_expression_id": expression,
                "expression_work_link_priority": priority,
            },
        )
        for priority, expression in enumerate(expressions, 1)
    ]
    before = macros.get_rows("expression_work_links")
    journal_before = macros.get_rows("maintenance_changes")
    original_update, original_delete = macros.update_row, macros.delete_row

    def update(table, row_id, values, **kwargs):
        if (
            failure == "second_link"
            and table == "expression_work_links"
            and row_id == links[1]
        ):
            raise RuntimeError("non-duplicate write failure")
        return original_update(table, row_id, values, **kwargs)

    def delete(table, row_id, **kwargs):
        if failure == "source_delete" and table == "works":
            raise RuntimeError("non-duplicate write failure")
        return original_delete(table, row_id, **kwargs)

    monkeypatch.setattr(macros, "update_row", update)
    monkeypatch.setattr(macros, "delete_row", delete)
    if failure == "prompt":

        def fail(*args):
            raise RuntimeError("non-duplicate write failure")

        monkeypatch.setattr(
            runtime._plugins[WorkRevisionPlugin.name], "handle_events", fail
        )
    # Exercise the public physical repair without starting a legacy queue thread.
    maintainer = Maintainer.__new__(Maintainer)
    maintainer.db = database
    with pytest.raises(RuntimeError, match="non-duplicate write failure"):
        maintainer.merge("works", kept, removed)
    assert macros.get_rows("expression_work_links") == before
    assert macros.get_row("works", removed) is not None
    assert macros.get_rows("maintenance_changes") == journal_before


def test_physical_merge_only_removes_proven_duplicate_links(maintained_database):
    from LiuXin_alpha.databases.maintenance.service import Maintainer

    database, _ = maintained_database
    macros, catalog = database.macros, Catalog(database)
    kept = catalog.works.create({"title": "kept"})
    removed = catalog.works.create({"title": "removed"})
    expression = catalog.expressions.create({"title": "shared"})
    datestamp = database.driver_wrapper.get_datestamp_column("expression_work_links")
    for work in (kept, removed):
        macros.insert_row(
            "expression_work_links",
            {
                "expression_work_link_work_id": work,
                "expression_work_link_expression_id": expression,
                datestamp: f"2020-01-{work:02d} 00:00:00+00:00",
            },
        )
    maintainer = Maintainer.__new__(Maintainer)
    maintainer.db = database
    with pytest.raises(ValueError, match="distinct"):
        maintainer.merge("works", kept, kept)
    with pytest.raises(ValueError, match="must exist"):
        maintainer.merge("works", kept + removed + 1000, removed)
    maintainer.merge("works", kept, removed)
    links = macros.get_rows("expression_work_links")
    assert len(links) == 1
    assert links[0]["expression_work_link_work_id"] == kept
    assert macros.get_row("works", removed) is None


def test_physical_merge_refuses_conflicting_duplicate_payloads(maintained_database):
    from LiuXin_alpha.databases.maintenance.service import Maintainer

    database, _ = maintained_database
    macros, catalog = database.macros, Catalog(database)
    kept = catalog.works.create({"title": "kept"})
    removed = catalog.works.create({"title": "removed"})
    expression = catalog.expressions.create({"title": "shared"})
    spec = database.driver_wrapper.get_link_spec("works", "expressions")
    assert spec.priority_link_col is not None
    for work, priority in ((kept, 1), (removed, 2)):
        macros.insert_row(
            "expression_work_links",
            {
                spec.primary_link_col: work,
                spec.secondary_link_col: expression,
                spec.priority_link_col: priority,
            },
        )
    before = macros.get_rows("expression_work_links")
    maintainer = Maintainer.__new__(Maintainer)
    maintainer.db = database
    with pytest.raises(ValueError, match="Conflicting duplicate link payload"):
        maintainer.merge("works", kept, removed)
    assert macros.get_rows("expression_work_links") == before
    assert macros.get_row("works", removed) is not None


def test_physical_merge_does_not_delete_links_after_unique_priority_failure(
    maintained_database,
):
    from LiuXin_alpha.databases.maintenance.service import Maintainer

    database, _ = maintained_database
    macros, catalog = database.macros, Catalog(database)
    kept = catalog.works.create({"title": "kept"})
    removed = catalog.works.create({"title": "removed"})
    for work in (kept, removed):
        expression = catalog.expressions.create({"title": str(work)})
        macros.insert_row(
            "expression_work_links",
            {
                "expression_work_link_work_id": work,
                "expression_work_link_expression_id": expression,
                "expression_work_link_priority": 1,
            },
        )
    before = macros.get_rows("expression_work_links")
    maintainer = Maintainer.__new__(Maintainer)
    maintainer.db = database
    with pytest.raises(Exception, match="(?i)unique"):
        maintainer.merge("works", kept, removed)
    assert macros.get_rows("expression_work_links") == before
    assert macros.get_row("works", removed) is not None
