"""Exercise durable storage acceptance, restart and verified streaming completion."""

import base64
from pathlib import Path
from uuid import UUID

import pytest

from LiuXin_alpha.configuration import CapturedPreferences, preferences_snapshot
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.connections import capture_database_connection
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.core.workflow_delivery import WorkflowHandler, publish
from LiuXin_alpha.ingest.native_disk import register_native_disk
from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobRunState
from LiuXin_alpha.jobs.worker import JobWorker
from LiuXin_alpha.storage.api import ReplicaMode, ReplicaState
from tests.core.test_native_metadata_postgres_live import pg_database as pg_database
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_restore_after_restart_retains_source_and_deduplicates_command(
    metadata_db, tmp_path
):
    exercise_restore(metadata_db, tmp_path)


def test_postgres_native_ingest_and_durable_restore(pg_database, tmp_path):
    exercise_restore(pg_database[0], tmp_path)


def exercise_restore(database, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    original = source / "original.epub"
    content = b"verified native bytes" * 100
    original.write_bytes(content)
    target = tmp_path / "target"
    target.mkdir()
    options = dict(
        **capture_database_connection(database),
        durable_job_path=tmp_path / "jobs.sqlite",
        start_jobs=False,
        enable_maintenance=False,
        repair_bootstrap_rows=False,
    )
    with create_core(**options) as core:
        first = register_native_disk(core.library, source)
        again = register_native_disk(core.library, source)
        assert first.inserted_assets == first.inserted_replicas == 1
        assert again.reused_replicas == 1
        asset = next(iter(core.library.storage.iter_digital_asset_records()))
        destination = core.library.storage.add_store(
            "restored",
            "filesystem",
            target,
            read_only=False,
            modes=(ReplicaMode.ACTIVE,),
        )
        command = CoreCommand(
            "storage.asset.restore",
            {"asset_id": asset.digital_asset_id, "store": str(destination.store_uuid)},
        )
        accepted = core.execute_command(command).result
        assert accepted["state"] == "accepted"
        assert not any(target.rglob("*.epub"))
    # The catalogue intent survives closing the submitting graph before delivery.
    with create_core(**options) as core:
        assert core.execute_command(command).result == accepted
        service = core._durable_jobs
        identities = publish(service)
        assert len(identities) == 1
        registry = JobHandlerRegistry()
        registry.register(WorkflowHandler(str(database.uuid)))
        assert JobWorker(repository=service.repository, handlers=registry).run_once()
        run = service.repository.get_run(service._run_id(identities[0]))
        assert run.state is JobRunState.SUCCEEDED, run.error_text
        outcome = core.query("commands.outcome", {"command_id": command.command_id})
        assert outcome["current_job_id"] == identities[0]
        restored = core.library.storage.select_replica(
            asset.digital_asset_id,
            preferred_store_ref=UUID(str(destination.store_uuid)),
        )
        assert restored.state is ReplicaState.VERIFIED
        with core.library.storage.get(restored.location) as stream:
            assert stream.read() == content
        acquired = core.query("storage.file.read", {"asset_id": asset.digital_asset_id})
        assert base64.b64decode(acquired["content"]["base64"]) == content
        assert original.read_bytes() == content
        core.execute_command(command)
        assert publish(service) == []
        assert len(service.repository.list_runs()) == 1


def test_captured_settings_preserve_literal_types_and_detach_defaults(tmp_path):
    class Settings(dict):
        defaults = {"nested": (b"default", {"a", "b"})}

    values = Settings(
        nested={"key": ({1, 2}, frozenset({3}), b"bytes", Path(tmp_path))}
    )
    snapshot = preferences_snapshot(values)
    values["nested"]["key"][0].add(99)
    restored = CapturedPreferences(**snapshot)
    assert restored["nested"]["key"] == ({1, 2}, frozenset({3}), b"bytes", tmp_path)
    defaults = restored.defaults
    defaults["nested"][1].add("changed")
    assert "changed" not in restored.defaults["nested"][1]
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match="cycle"):
        preferences_snapshot({"cyclic": cycle})
