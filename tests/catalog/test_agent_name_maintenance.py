"""Restore Agent rename/sort behavior through current Catalog/Core write owners."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.metadata.author_names import agent_name_to_sort
from tests.catalog.test_write_boundary_parity import write_runtime as write_runtime


@pytest.mark.parametrize(
    "name,kind,method,expected",
    [
        ("Mary Shelley", "person", "comma", "Shelley, Mary"),
        ("Dr Arthur C Clarke Jr", "person", "nocomma", "Clarke Arthur C Jr"),
        ("Shelley, Mary", "person", "comma", "Shelley, Mary"),
        ("王 小明", "person", "copy", "王 小明"),
        ("李白", "person", "comma", "李白"),
        (
            "  Example University Press  ",
            "organisation",
            "comma",
            "Example University Press",
        ),
        ("The Science Collective", "group", "comma", "The Science Collective"),
        ("James S. A. Corey", "pseudonym", "comma", "James S. A. Corey"),
    ],
)
def test_agent_sort_policy_is_explicit_about_identity_type_and_order(
    name, kind, method, expected
):
    assert agent_name_to_sort(name, kind, person_method=method) == expected


def test_rename_preserves_overrides_and_related_identity_rows(write_runtime):
    runtime, catalog, db = write_runtime
    agent = catalog.agents.create_person(
        {
            "name": "Mary Wollstonecraft",
            "sort_name": "Manual sort",
            "aliases": ["M. W."],
        },
        details={
            "human_agent_given_name": "Mary",
            "human_agent_family_name": "Wollstonecraft",
        },
    )
    work = catalog.works.create({"title": "Frankenstein"})
    catalog.agents.link_to_wemi(
        agent_id=agent, level="work", entity_id=work, role="aut"
    )
    old = catalog.agents.require(agent)
    details = db.macros.get_rows("human_agents")
    credits = catalog.agents.list_for_wemi(level="work", entity_id=work)
    preview = runtime.command(
        "catalog.agent.rename", {"agent_id": agent, "name": " Mary Shelley "}
    )
    assert preview["dry_run"] and not preview["updated"]
    assert preview["after"] == {"name": "Mary Shelley", "sort_name": "Manual sort"}
    assert catalog.agents.require(agent) == old
    applied = runtime.command(
        "catalog.agent.rename",
        {"agent_id": agent, "name": "Mary Shelley", "dry_run": False},
    )
    assert applied["updated"] and applied["sort_source"] == "preserved"
    assert catalog.agents.require(agent)["agent_sort_name"] == "Manual sort"
    regenerated = catalog.agents.rename(agent, "Mary Shelley", sort_mode="derive")
    assert regenerated["after"]["sort_name"] == "Shelley, Mary"
    explicit = catalog.agents.rename(
        agent, "Mary Shelley", sort_name=" Shelley, Mary W. "
    )
    assert explicit["after"]["sort_name"] == "Shelley, Mary W."
    assert catalog.agents.require(agent)["agent_aliases"] == old["agent_aliases"]
    assert db.macros.get_rows("human_agents") == details
    new_credits = catalog.agents.list_for_wemi(level="work", entity_id=work)
    assert [(row["agent_id"], row["_catalog_link"]) for row in new_credits] == [
        (row["agent_id"], row["_catalog_link"]) for row in credits
    ]
    assert new_credits[0]["agent_canonical_name"] == "Mary Shelley"
    # Repeating an already satisfied request performs no row update.
    assert not catalog.agents.rename(agent, "Mary Shelley")["updated"]


def test_repair_fills_missing_sorts_and_only_overwrites_explicitly(write_runtime):
    runtime, catalog, _db = write_runtime
    ids = [
        catalog.agents.create({"name": " Mary Shelley ", "sort_name": None}),
        catalog.agents.create(
            {"name": "Example Press", "type": "organisation", "sort_name": "  "}
        ),
        catalog.agents.create({"name": "Literal None", "sort_name": "none"}),
        catalog.agents.create({"name": "James S. A. Corey", "type": "pseudonym"}),
    ]
    old = [catalog.agents.require(i) for i in ids]
    preview = runtime.command("catalog.agent.sort.repair", {"agent_ids": ids + ids})
    assert (preview["selected"], preview["changed"], preview["updated"]) == (4, 3, 0)
    assert [catalog.agents.require(i) for i in ids] == old
    applied = runtime.command(
        "catalog.agent.sort.repair", {"agent_ids": ids, "dry_run": False}
    )
    assert applied["updated"] == 3
    assert [catalog.agents.require(i)["agent_sort_name"] for i in ids] == [
        "Shelley, Mary",
        "Example Press",
        "none",
        "James S. A. Corey",
    ]
    assert [catalog.agents.require(i)["agent_canonical_name"] for i in ids] == [
        r["agent_canonical_name"] for r in old
    ]
    # Apply replans; it must not overwrite an override entered after preview.
    catalog.agents.update(ids[0], {"sort_name": "New manual override"})
    assert catalog.agents.repair_sort_names(ids)["updated"] == 0
    regenerated = catalog.agents.repair_sort_names(
        ids, sort_mode="derive", person_sort_method="copy"
    )
    assert regenerated["updated"] == 2
    assert catalog.agents.require(ids[0])["agent_sort_name"] == "Mary Shelley"
    assert catalog.agents.require(ids[2])["agent_sort_name"] == "Literal None"


def test_agent_name_preflight_rejects_invalid_and_incomplete_batches(write_runtime):
    runtime, catalog, _db = write_runtime
    agent = catalog.agents.create({"name": "Kept Name"})
    blank = catalog.agents.create({"name": ""})
    before = catalog.agents.list()
    for options in (
        {"name": " "},
        {"name": 15},
        {"name": None},
        {"name": "Valid", "sort_name": ""},
        {"name": "Valid", "sort_mode": "auto"},
        {"name": "Valid", "person_sort_method": "unknown"},
        {"name": "Valid", "sort_name": "Explicit", "sort_mode": "derive"},
        {"name": "Valid", "dry_run": "false"},
    ):
        with pytest.raises(ValueError):
            catalog.agents.rename(agent, **options)
    for ids in ([], [True], [1.5], [-1], [0], "1", [agent] * 501, None):
        with pytest.raises(ValueError):
            catalog.agents.repair_sort_names(ids)
    with pytest.raises(CatalogNotFoundError):
        catalog.agents.repair_sort_names([agent, 999999])
    with pytest.raises(ValueError, match="nonblank"):
        catalog.agents.repair_sort_names([agent, blank])
    for bad_id in (True, 1.5, "1", 0):
        with pytest.raises(CoreHandlerError):
            runtime.command(
                "catalog.agent.rename",
                {"agent_id": bad_id, "name": "Wrong ID", "dry_run": False},
            )
    assert catalog.agents.list() == before


def test_repair_late_write_failure_rolls_back_every_selected_agent(
    write_runtime, monkeypatch
):
    _runtime, catalog, db = write_runtime
    ids = [
        catalog.agents.create({"name": name})
        for name in ("Mary Shelley", "Ada Lovelace")
    ]
    before = catalog.agents.list()
    update = db.macros.update_row
    writes = []

    def fail_second(table, row_id, values, **kwargs):
        if table == "agents":
            writes.append(row_id)
            if len(writes) == 2:
                raise RuntimeError("second Agent write failed")
        return update(table, row_id, values, **kwargs)

    monkeypatch.setattr(db.macros, "update_row", fail_second)
    with pytest.raises(RuntimeError, match="second Agent write"):
        catalog.agents.repair_sort_names(ids)
    assert writes == ids
    assert catalog.agents.list() == before


def test_rename_and_repair_honor_outer_transactions_and_readonly_preview(
    write_runtime, monkeypatch
):
    _runtime, catalog, db = write_runtime
    agent = catalog.agents.create({"name": "Mary Wollstonecraft"})
    before = catalog.agents.require(agent)
    with pytest.raises(RuntimeError, match="outer failure"):
        with db.macros.transaction(serialized=True):
            catalog.agents.rename(agent, "Mary Shelley", sort_mode="derive")
            raise RuntimeError("outer failure")
    assert catalog.agents.require(agent) == before

    def no_writer(*args, **kwargs):
        raise AssertionError("Preview opened a write unit")

    monkeypatch.setattr(db.macros, "transaction", no_writer)
    assert catalog.agents.rename(agent, "Mary Shelley", dry_run=True)["changed"]
    assert catalog.agents.repair_sort_names([agent], dry_run=True)["changed"] == 1
    assert catalog.agents.require(agent) == before


def test_installed_prompt_failure_rolls_back_name_sort_and_projection(write_runtime):
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
    from LiuXin_alpha.databases.maintenance.plugins import (
        MaintenancePluginBase,
        MaintenancePluginResult,
    )
    from LiuXin_alpha.databases.maintenance.projection_sql import put_projection
    from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime

    _core, catalog, db = write_runtime
    agent = catalog.agents.create({"name": "Mary Wollstonecraft"})
    before = catalog.agents.require(agent)

    class RequiredName(MaintenancePluginBase):
        name = "test.agent-name-receipt"
        mode = MaintenanceMode.PROMPT
        tables = ("agents",)
        fail = True

        def handle_events(self, context, events):
            for event in events:
                put_projection(
                    context.connection,
                    self.name,
                    str(event.row_id),
                    "{}",
                    event.change_id,
                )
            if self.fail:
                raise RuntimeError("required name receipt failed")
            return MaintenancePluginResult(handled=len(events))

    with Database(
        metadata=db.metadata,
        db_type=db.type,
        create=False,
        backup=False,
        enable_maintenance=False,
        enable_storage_manager=False,
    ) as writer:
        plugin = RequiredName()
        maintenance = MaintenanceRuntime(writer, (plugin,))
        maintenance.install()
        names = Catalog(writer).agents
        with pytest.raises(RuntimeError, match="required name receipt"):
            names.rename(agent, "Mary Shelley", sort_mode="derive")
        assert names.require(agent) == before
        assert not writer.macros.get_rows("maintenance_projections")
        assert not writer.macros.get_rows("maintenance_changes")
        plugin.fail = False
        assert names.rename(agent, "Mary Shelley", sort_mode="derive")["updated"]
        assert len(writer.macros.get_rows("maintenance_projections")) == 1


def test_cli_previews_then_applies_through_real_core(
    write_runtime, monkeypatch, capsys
):
    from LiuXin_alpha.surfaces.cli import catalogue as cli_catalog
    from LiuXin_alpha.surfaces.cli.app import main

    runtime, catalog, _db = write_runtime
    agent = catalog.agents.create({"name": "Mary Wollstonecraft"})

    @contextmanager
    def open_catalogue(args, **options):
        assert options == {"enable_storage_manager": False, "enable_maintenance": False}
        yield runtime

    monkeypatch.setattr(cli_catalog, "open_cli_core", open_catalogue)
    args = [
        "catalog",
        "agents",
        "rename",
        str(agent),
        "Mary Shelley",
        "--sort-mode",
        "derive",
    ]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["dry_run"]
    assert (
        catalog.agents.require(agent)["agent_canonical_name"] == "Mary Wollstonecraft"
    )
    assert main(args + ["--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["updated"]
    assert catalog.agents.require(agent)["agent_sort_name"] == "Shelley, Mary"
    catalog.agents.update(agent, {"sort_name": None})
    repair = [
        "catalog",
        "agents",
        "repair-sort",
        str(agent),
        "--person-sort-method",
        "copy",
    ]
    assert main(repair) == 0
    assert json.loads(capsys.readouterr().out)["updated"] == 0
    assert catalog.agents.require(agent)["agent_sort_name"] is None
    assert main(repair + ["--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["updated"] == 1
    assert catalog.agents.require(agent)["agent_sort_name"] == "Mary Shelley"


def test_postgres_rename_reads_a_concurrent_override_after_locking(
    write_runtime, monkeypatch
):
    _runtime, catalog, db = write_runtime
    if db.type != "PostgreSQL":
        pytest.skip("PostgreSQL row locks protect against non-advisory writers")
    agent = catalog.agents.create({"name": "Mary Wollstonecraft"})
    writer = db.driver.get_connection()
    observer = db.driver.get_connection()
    table = db.driver._table_sql("agents")
    writer.execute(
        f"UPDATE {table} SET agent_sort_name=? WHERE agent_id=?",
        ("Concurrent override", agent),
    )
    opened = threading.Event()
    main_thread = threading.get_ident()
    worker_pid = []
    connect = db.driver.get_connection

    def observe_connection():
        connection = connect()
        if threading.get_ident() != main_thread:
            worker_pid.append(connection.raw_connection.get_backend_pid())
            opened.set()
        return connection

    monkeypatch.setattr(db.driver, "get_connection", observe_connection)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(catalog.agents.rename, agent, "Mary Shelley")
            try:
                assert opened.wait(10)
                deadline = time.monotonic() + 10
                blocked = False
                while time.monotonic() < deadline:
                    # Clear PostgreSQL's statistics snapshot between observations.
                    observer.rollback()
                    row = observer.execute(
                        "SELECT wait_event_type, query FROM pg_stat_activity WHERE pid=?",
                        (worker_pid[0],),
                    ).fetchone()
                    if row and row[0] == "Lock" and "FOR UPDATE" in row[1]:
                        blocked = True
                        break
                    if future.done():
                        break
                    time.sleep(0.02)
                assert blocked, "Rename did not wait for the existing row writer"
                writer.commit()
            finally:
                writer.rollback()
            assert future.result(timeout=10)["after"] == {
                "name": "Mary Shelley",
                "sort_name": "Concurrent override",
            }
    finally:
        writer.close()
        observer.close()
