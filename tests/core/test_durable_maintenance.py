"""Exercise maintenance installation and jobs delivery through real Core services."""

from LiuXin_alpha.catalog.maintenance import WorkSummaryPlugin, work_summary
from LiuXin_alpha.core.factory import create_core


def test_core_rebuilds_historical_works_through_existing_jobs(tmp_path):
    path = tmp_path / "rebuild.sqlite"
    with create_core(
        database_path=path,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        identities = [
            core.catalog.works.create({"title": f"Historical {i}"}) for i in range(3)
        ]
        core.command("maintenance.run", {"action": "install"})
        first = core.command("maintenance.run", {"action": "rebuild", "max_events": 1})
        assert first["scheduled_this_call"] == 1
        assert not list(tmp_path.glob("*.jobs.sqlite"))
    with create_core(
        database_path=path,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        result = core.command(
            "maintenance.run",
            {
                "action": "rebuild",
                "max_events": 2,
                "max_batches": 10,
            },
        )
        assert result["run_uuid"] == first["run_uuid"]
        assert result["phase"] == "waiting"
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        core.command("maintenance.run", {"action": "drain", "max_batches": 20})
        assert core.query("maintenance.status")["rebuilds"][0]["phase"] == "complete"
        assert all(
            work_summary(core.database, value)["current"] for value in identities
        )


def test_core_retires_completed_job_history_without_starting_polling(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.core import maintenance_history

    with create_core(
        database_path=tmp_path / "history.sqlite",
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        identity = core.catalog.works.create({"title": "Retain the projection"})
        core.command("maintenance.run", {"action": "drain", "max_batches": 10})
        assert core.durable_jobs.repository.list_runs()
        monkeypatch.setattr(maintenance_history, "now_ms", lambda: 2**60)
        result = core.command(
            "maintenance.run", {"action": "prune-jobs", "older_than_days": 7}
        )
        assert result["removed_runs"] >= 1
        assert not core.durable_jobs.repository.list_runs()
        assert core.durable_jobs._thread is None
        assert work_summary(core.database, identity)["current"]
        assert core.query("maintenance.status")["pending"] == []


def test_core_installs_pauses_retries_and_drains_without_storage_manager(
    tmp_path, monkeypatch
):
    path = tmp_path / "catalog.sqlite"
    with create_core(
        database_path=path,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        assert core.query("maintenance.status")["installed"] is False
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        assert core.command("maintenance.run", {"action": "install"})["current"]
        custom = core.command(
            "custom-fields.create",
            {
                "name": "Maintained field",
                "label": "maintained_field",
                "datatype": "text",
                "table": "works",
            },
        )
        assert custom["schema_changed"]
        work_id = core.catalog.works.create({"title": "maintained"})
        assert not work_summary(core.database, work_id)["current"]
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        core.command(
            "maintenance.run", {"action": "pause", "plugin_id": WorkSummaryPlugin.name}
        )
        assert (
            core.command("maintenance.run", {"action": "drain", "max_batches": 1})[
                "job_passes"
            ]
            == 0
        )
        core.command(
            "maintenance.run", {"action": "resume", "plugin_id": WorkSummaryPlugin.name}
        )
        original = WorkSummaryPlugin.handle_events

        def fail(self, context, events):
            original(self, context, events)
            raise RuntimeError("summary unavailable")

        monkeypatch.setattr(WorkSummaryPlugin, "handle_events", fail)
        core.command("maintenance.run", {"action": "drain", "max_batches": 1})
        assert any(
            "summary unavailable" in (row["last_error"] or "")
            for row in core.query("maintenance.status")["pending"]
        )
        monkeypatch.setattr(WorkSummaryPlugin, "handle_events", original)
        assert (
            core.command(
                "maintenance.run",
                {"action": "retry", "plugin_id": WorkSummaryPlugin.name},
            )["reset"]
            >= 1
        )
        core.command("maintenance.run", {"action": "drain", "max_batches": 20})
        assert core.query("maintenance.status")["pending"] == []
        assert work_summary(core.database, work_id)["current"]
        assert core.command("maintenance.run", {"action": "prune"})["removed"] >= 1


def test_core_previews_upgrades_and_republishes_pending_work(tmp_path, monkeypatch):
    from LiuXin_alpha.catalog import maintenance
    from LiuXin_alpha.databases.maintenance.plugins import MaintenancePluginUpgrade

    path = tmp_path / "upgrade.sqlite"
    with create_core(
        database_path=path,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        work_id = core.catalog.works.create({"title": "Before upgrade"})

    class NewSummary(maintenance.WorkSummaryPlugin):
        version = "2"
        upgrades = (MaintenancePluginUpgrade("1", replay_pending=True),)

    monkeypatch.setattr(maintenance, "WorkSummaryPlugin", NewSummary)
    with create_core(
        database_path=path,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        payload = {
            "action": "upgrade",
            "plugin_id": NewSummary.name,
            "expected_version": "1",
        }
        preview = core.command("maintenance.run", payload)
        assert preview["dry_run"] and not preview["blockers"]
        assert core.query("maintenance.status")["pending"][0]["plugin_version"] == "1"
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        result = core.command(
            "maintenance.run",
            {
                **payload,
                "dry_run": False,
                "expected_definition": preview["expected_definition"],
            },
        )
        assert result["to_version"] == "2"
        core.command("maintenance.run", {"action": "drain", "max_batches": 20})
        assert work_summary(core.database, work_id)["current"]
        assert core.query("maintenance.status")["pending"] == []


def test_drain_refills_after_dependency_layer_without_republishing_each_job(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.catalog import maintenance
    from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime

    class ChildSummary(maintenance.WorkSummaryPlugin):
        name = "catalog.child-summary"
        dependencies = (maintenance.WorkSummaryPlugin.name,)

    def attach(database):
        existing = vars(database.driver).get("_maintenance_runtime")
        return (
            existing
            if existing is not None
            else MaintenanceRuntime(
                database,
                (
                    maintenance.WorkRevisionPlugin(),
                    maintenance.WorkSummaryPlugin(),
                    ChildSummary(),
                ),
            )
        )

    monkeypatch.setattr(maintenance, "catalog_maintenance", attach)
    with create_core(
        database_path=tmp_path / "pages.sqlite",
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        identities = [
            core.catalog.works.create({"title": f"Layer {i}"}) for i in range(5)
        ]
        original = core.durable_jobs.publish_maintenance
        publications = []

        def publish(**kwargs):
            publications.append(True)
            return original(**kwargs)

        monkeypatch.setattr(core.durable_jobs, "publish_maintenance", publish)
        result = core.command(
            "maintenance.run",
            {
                "action": "drain",
                "max_events": 10,
                "max_batches": 20,
            },
        )
        assert result["job_passes"] == 10
        assert core.query("maintenance.status")["pending"] == []
        assert (
            len(publications) == 3
        )  # Parent page, ready children, final idle inspection.
        assert all(
            work_summary(core.database, value)["current"] for value in identities
        )


def test_readonly_core_status_after_restart_does_not_start_worker(tmp_path):
    path = tmp_path / "catalog.sqlite"
    with create_core(
        database_path=path,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        core.catalog.works.create({"title": "waiting"})
    with create_core(
        database_path=path, enable_storage_manager=False, enable_maintenance=False
    ) as reopened:
        assert reopened.query("maintenance.status")["pending"]
        assert reopened.durable_jobs._thread is None
        assert reopened.durable_jobs._repository is None
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        reopened.command("maintenance.run", {"action": "drain", "max_batches": 20})
        assert reopened.query("maintenance.status")["pending"] == []


def test_long_lived_polling_picks_up_later_commits(tmp_path):
    import time

    path = tmp_path / "catalog.sqlite"
    with create_core(
        database_path=path,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        core.command("maintenance.run", {"action": "start"})
        work_id = core.catalog.works.create({"title": "committed after worker start"})
        deadline = time.monotonic() + 15
        while not work_summary(core.database, work_id)["current"]:
            assert time.monotonic() < deadline, core.query("maintenance.status")
            time.sleep(0.05)
        core.durable_jobs.stop(wait=True)


def test_live_postgres_core_reopens_and_delivers_through_the_jobs_service(tmp_path):
    import os
    from uuid import uuid4

    import pytest

    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        connect_postgres,
    )

    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
    metadata = {"postgres_url": url, "schema": "maintenance_core_" + uuid4().hex}
    cleanup = connect_postgres(metadata, prompt_for_password=False)
    try:
        with create_core(
            database_path=url,
            db_type="PostgreSQL",
            database_metadata=metadata,
            create=True,
            enable_storage_manager=False,
            enable_maintenance=False,
            durable_job_path=tmp_path / "jobs.sqlite",
        ) as core:
            core.command("maintenance.run", {"action": "install"})
            work_id = core.catalog.works.create({"title": "PostgreSQL maintenance"})
            core.command("maintenance.run", {"action": "drain", "max_batches": 20})
            assert core.query("maintenance.status")["pending"] == []
            assert work_summary(core.database, work_id)["current"]
    finally:
        with cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE')
        cleanup.close()


def test_maintenance_drain_leaves_older_storage_audit_queued(tmp_path):
    import json

    from LiuXin_alpha.core.storage_audit_jobs import AUDIT_JOB_KIND
    from LiuXin_alpha.jobs.models import JobDefinition, JobRunState

    with create_core(
        database_path=tmp_path / "catalog.sqlite",
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        core.command("maintenance.run", {"action": "install"})
        repository = core.durable_jobs.repository
        audit = repository.submit_job(
            JobDefinition(
                job_kind=AUDIT_JOB_KIND,
                job_name="Earlier storage audit",
                payload_json=json.dumps(
                    {
                        "library_uuid": str(core.database.uuid),
                        "limit": 1,
                        "offset": 0,
                        "calculate_digests": True,
                    }
                ),
            ),
            request_key="audit",
        )
        work_id = core.catalog.works.create({"title": "maintenance only"})
        core.command("maintenance.run", {"action": "drain", "max_batches": 20})
        assert work_summary(core.database, work_id)["current"]
        assert repository.get_run(audit.job_run_id).state is JobRunState.QUEUED
        assert core.command("maintenance.run", {"action": "drain"})["job_passes"] == 0
        assert repository.get_run(audit.job_run_id).state is JobRunState.QUEUED


def test_selective_cleanup_reports_unavailable_and_preserves_rows(tmp_path):
    import pytest

    from LiuXin_alpha.core.errors import CoreHandlerError

    with create_core(
        database_path=tmp_path / "catalog.sqlite",
        create=True,
        enable_storage_manager=False,
        enable_maintenance=True,
    ) as core:
        work_id = core.catalog.works.create({"title": "must remain"})
        with pytest.raises(CoreHandlerError) as failure:
            core.command("maintenance.clean", {"table": "works", "row_ids": [work_id]})
        assert failure.value.code == "capability_unavailable"
        assert "no rows were changed" in str(failure.value)
        assert core.catalog.works.get(work_id) is not None
