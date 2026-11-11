"""Exercise explicit version migrations, rollback and stale-process fences."""

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.plugins import MaintenancePluginUpgrade
from LiuXin_alpha.databases.maintenance.projection_sql import put_projection
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceFailure
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


class OldLazy(Projection):
    mode = MaintenanceMode.LAZY


class NewLazy(OldLazy):
    version = "2"
    upgrades = (MaintenancePluginUpgrade("1", replay_pending=True),)


def _write(runtime, value):
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (?,?)", (value, f"work {value}"))


def _pending_ids(runtime):
    return [row["obligation_id"] for row in runtime.status()["pending"]]


def test_upgrade_replays_pending_preserves_history_and_fences_old_jobs(runtime_factory):
    old = runtime_factory([OldLazy()])
    old.install()
    _write(old, 1)
    old.deliver("projection", _pending_ids(old))
    _write(old, 2)
    identities = _pending_ids(old)
    with old.unit() as connection, old._internal():
        connection.execute(
            "UPDATE maintenance_obligations SET job_run_id=123,job_ledger_uuid='old-ledger' "
            "WHERE completed_ms IS NULL"
        )
        put_projection(connection, "projection", "1", '{"old":true}', 1)
    new = runtime_factory([NewLazy()])
    preview = new.upgrade("projection", expected_version="1")
    assert preview["dry_run"] and preview["changed"]
    assert preview["pending"] == {"projection": 1}
    assert preview["invalidated_projections"] == {"projection": 1}
    assert not preview["blockers"]
    assert new.status()["plugins"][0]["version"] == "1"
    result = new.upgrade(
        "projection",
        expected_version="1",
        expected_definition=preview["expected_definition"],
        dry_run=False,
    )
    assert result["rebuild_required"]
    assert new.status()["upgrades"] == [result]
    pending = new.status()["pending"]
    assert pending[0]["plugin_version"] == "2"
    assert pending[0]["job_run_id"] is None
    assert pending[0]["retry_generation"] == 1
    with pytest.raises(MaintenanceFailure, match="superseded"):
        old.deliver(
            "projection", identities, job_run_id=123, job_ledger_uuid="old-ledger"
        )
    with pytest.raises(MaintenanceFailure, match="version"):
        new.deliver("projection", identities, expected_version="1")
    assert new.status()["pending"][0]["attempts"] == 0
    assert new.status()["pending"][0]["last_error"] is None
    new.deliver("projection", identities, expected_version="2")
    with new.unit() as connection:
        assert connection.execute(
            "SELECT row_id FROM effects ORDER BY row_id"
        ).fetchall() == [(1,), (2,)]
        assert connection.execute(
            "SELECT plugin_version FROM maintenance_obligations ORDER BY obligation_id"
        ).fetchall() == [("1",), ("2",)]
        assert (
            connection.execute("SELECT * FROM maintenance_projections").fetchall() == []
        )
    _write(new, 3)
    assert new.status()["pending"][0]["plugin_version"] == "2"


def test_default_upgrade_requires_old_worker_to_drain(runtime_factory):
    class NeedsDrain(NewLazy):
        upgrades = (MaintenancePluginUpgrade("1"),)

    old = runtime_factory([OldLazy()])
    old.install()
    _write(old, 1)
    new = runtime_factory([NeedsDrain()])
    assert new.upgrade("projection", expected_version="1")["blockers"]
    with pytest.raises(MaintenanceFailure, match="Drain pending"):
        new.upgrade("projection", expected_version="1", dry_run=False)
    old.deliver("projection", _pending_ids(old))
    assert new.upgrade("projection", expected_version="1", dry_run=False)["changed"]


def test_failed_migration_rolls_back_data_declarations_and_pending_generation(
    runtime_factory,
):
    old = runtime_factory([OldLazy()])
    old.install()
    _write(old, 1)
    with old.unit() as connection, old._internal():
        put_projection(connection, "projection", "1", '{"old":true}', 1)
    captured = []

    def fail(context, previous):
        assert context.db is None
        assert previous.version == "1"
        captured.append(context.connection)
        context.connection.execute("INSERT INTO effects VALUES (99,99)")
        raise RuntimeError("upgrade migration failed")

    plugin = NewLazy()
    plugin.upgrades = (
        MaintenancePluginUpgrade("1", replay_pending=True, migrate=fail),
    )
    new = runtime_factory([plugin])
    with pytest.raises(RuntimeError, match="upgrade migration failed"):
        new.upgrade("projection", expected_version="1", dry_run=False)
    with pytest.raises(RuntimeError, match="expired|active|closed"):
        captured[0].execute("SELECT 1")
    state = new.status()
    assert state["plugins"][0]["version"] == "1"
    assert state["pending"][0]["plugin_version"] == "1"
    assert state["pending"][0]["retry_generation"] == 0
    assert state["upgrades"] == []
    with old.unit() as connection:
        assert connection.execute("SELECT * FROM effects").fetchall() == []
        assert (
            connection.execute(
                "SELECT value_json FROM maintenance_projections"
            ).fetchone()[0]
            == '{"old":true}'
        )
    old.deliver("projection", _pending_ids(old))


def test_migration_callback_is_atomic_and_receives_new_bound_context(runtime_factory):
    old = runtime_factory([OldLazy()])
    old.install()

    def migrate(context, previous):
        assert previous.version == "1"
        assert context.plugin_id == "projection"
        context.connection.execute("INSERT INTO effects VALUES (99,2)")

    plugin = NewLazy()
    plugin.upgrades = (MaintenancePluginUpgrade("1", migrate=migrate),)
    new = runtime_factory([plugin])
    new.upgrade("projection", expected_version="1", dry_run=False)
    with new.unit() as connection:
        assert connection.execute("SELECT * FROM effects").fetchall() == [(99, 2)]


def test_prompt_upgrade_refuses_old_writers_and_invalidates_dependents(runtime_factory):
    class Dependent(OldLazy):
        name = "dependent"
        dependencies = ("projection",)

    class NewPrompt(Projection):
        version = "2"
        upgrades = (MaintenancePluginUpgrade("1"),)

    old = runtime_factory([Projection(), Dependent()])
    old.install()
    _write(old, 1)
    old.deliver("dependent", _pending_ids(old))
    with old.unit() as connection, old._internal():
        for name in ("projection", "dependent"):
            put_projection(connection, name, "1", "{}", 1)
    new = runtime_factory([NewPrompt(), Dependent()])
    plan = new.upgrade("projection", expected_version="1")
    assert plan["affected_plugins"] == ["dependent", "projection"]
    new.upgrade("projection", expected_version="1", dry_run=False)
    with pytest.raises(Exception, match="prompt maintenance|unavailable"):
        _write(old, 2)
    _write(new, 2)
    assert new.status()["pending"][0]["plugin_id"] == "dependent"
    with new.unit() as connection:
        assert (
            connection.execute("SELECT * FROM maintenance_projections").fetchall() == []
        )
        assert connection.execute(
            "SELECT work_id FROM works ORDER BY work_id"
        ).fetchall() == [(1,), (2,)]


def test_upgrade_refuses_stale_preview_undeclared_versions_and_changed_mode_pending(
    runtime_factory,
):
    old = runtime_factory([OldLazy()])
    old.install()
    _write(old, 1)
    new = runtime_factory([NewLazy()])
    with pytest.raises(MaintenanceFailure, match="changed since preview"):
        new.upgrade(
            "projection",
            expected_version="1",
            expected_definition="stale",
            dry_run=False,
        )
    with pytest.raises(MaintenanceFailure, match="version changed"):
        new.upgrade("projection", expected_version="0", dry_run=False)
    plugin = NewLazy()
    plugin.mode = MaintenanceMode.PROMPT
    changed_mode = runtime_factory([plugin])
    with pytest.raises(MaintenanceFailure, match="Drain pending"):
        changed_mode.upgrade("projection", expected_version="1", dry_run=False)
    plugin = NewLazy()
    plugin.upgrades = ()
    undeclared = runtime_factory([plugin])
    with pytest.raises(MaintenanceFailure, match="upgrade policy"):
        undeclared.upgrade("projection", expected_version="1", dry_run=False)


def test_upgrade_changes_subscriptions_only_after_atomic_apply(runtime_factory):
    old = runtime_factory([OldLazy()])
    old.install()
    plugin = NewLazy()
    plugin.tables = ("expression_work_links",)
    new = runtime_factory([plugin])
    preview = new.upgrade("projection", expected_version="1")
    assert preview["old_tables"] == ["works"]
    assert preview["new_tables"] == ["expression_work_links"]
    new.upgrade("projection", expected_version="1", dry_run=False)
    _write(new, 1)
    assert _pending_ids(new) == []
    with new.unit() as connection:
        connection.execute("INSERT INTO expression_work_links VALUES (1,1,10)")
    pending = new.status()["pending"]
    assert len(pending) == 1
    assert pending[0]["table_name"] == "expression_work_links"
    assert pending[0]["plugin_version"] == "2"
    with new.unit() as connection, new._internal():
        if new.schema:
            guards = connection.execute(
                "SELECT trigger_name FROM information_schema.triggers "
                "WHERE trigger_schema=? AND event_object_table='works' AND trigger_name LIKE ?",
                (new.schema, "liuxin_m_%"),
            ).fetchall()
        else:
            guards = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='works' AND name LIKE 'liuxin_m_%'"
            ).fetchall()
        assert guards == []


def test_work_projection_upgrade_requires_new_rebuild_and_restores_freshness(
    runtime_factory,
):
    from LiuXin_alpha.catalog.maintenance import (
        WorkRevisionPlugin,
        WorkSummaryPlugin,
        rebuild_work_projections,
        work_summary,
    )
    from tests.databases.test_maintenance_rebuild import _drain, _schedule

    old = runtime_factory([WorkRevisionPlugin(), WorkSummaryPlugin()])
    _write(old, 1)
    old.install()
    _schedule(old)
    _drain(old)
    assert work_summary(old.db, 1)["current"]

    class NewSummary(WorkSummaryPlugin):
        version = "2"
        upgrades = (MaintenancePluginUpgrade("1"),)

    new = runtime_factory([WorkRevisionPlugin(), NewSummary()])
    new.upgrade(NewSummary.name, expected_version="1", dry_run=False)
    assert not work_summary(new.db, 1)["current"]
    assert new.status()["rebuilds"][0]["phase"] == "obsolete"
    assert new.status()["rebuilds"][0]["needs_restart"]
    with pytest.raises(RuntimeError, match="declaration changed"):
        rebuild_work_projections(new.db)
    rebuild_work_projections(new.db, restart=True)
    _schedule(new)
    _drain(new)
    assert work_summary(new.db, 1)["current"]


def test_pending_dependents_block_upgrade_even_when_target_supports_replay(
    runtime_factory,
):
    class Child(OldLazy):
        name = "child"
        dependencies = ("projection",)

    old = runtime_factory([OldLazy(), Child()])
    old.install()
    _write(old, 1)
    new = runtime_factory([NewLazy(), Child()])
    plan = new.upgrade("projection", expected_version="1")
    assert any("child" in blocker for blocker in plan["blockers"])
    with pytest.raises(MaintenanceFailure, match="child"):
        new.upgrade("projection", expected_version="1", dry_run=False)


def test_upgrade_validates_persisted_dependents_before_changing_mode(runtime_factory):
    class PromptChild(Projection):
        name = "child"
        dependencies = ("projection",)

    old = runtime_factory([Projection(), PromptChild()])
    old.install()
    new = runtime_factory([NewLazy()])
    with pytest.raises(ValueError, match="Prompt maintenance cannot depend on lazy"):
        new.upgrade("projection", expected_version="1", dry_run=False)
    versions = {row["plugin_id"]: row["version"] for row in old.status()["plugins"]}
    assert versions == {"projection": "1", "child": "1"}
    _write(old, 1)


def test_upgrade_waits_for_writer_then_rechecks_pending_work(
    runtime_factory, monkeypatch
):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import contextmanager

    class NeedsDrain(NewLazy):
        upgrades = (MaintenancePluginUpgrade("1"),)

    old = runtime_factory([OldLazy()])
    old.install()
    new = runtime_factory([NeedsDrain()])
    preview = new.upgrade("projection", expected_version="1")
    assert not preview["blockers"]
    source = old.driver.get_connection()
    source.execute("INSERT INTO works VALUES (1,'concurrent source')")
    waiting = threading.Event()
    unit = new.unit

    @contextmanager
    def observed_unit():
        waiting.set()
        with unit() as connection:
            yield connection

    monkeypatch.setattr(new, "unit", observed_unit)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            new.upgrade, "projection", expected_version="1", dry_run=False
        )
        try:
            assert waiting.wait(10)
            source.commit()
            with pytest.raises(MaintenanceFailure, match="Drain pending"):
                future.result(timeout=10)
        finally:
            source.close()
    assert new.status()["plugins"][0]["version"] == "1"
    assert len(old.status()["pending"]) == 1
