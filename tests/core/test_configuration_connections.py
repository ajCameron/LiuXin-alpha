"""Prove Core reopens the selected backend/namespace with independent options."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.connections import (
    capture_database_connection,
    resolve_database_connection,
)
from LiuXin_alpha.core.services import CoreServices


@pytest.mark.parametrize(
    "key", ["postgres_url", "database_url", "dsn", "url", "database_path"]
)
@pytest.mark.parametrize("alias", ["PostgreSQL", "postgres", "pg"])
def test_postgres_url_metadata_forms_keep_target_and_schema(monkeypatch, key, alias):
    monkeypatch.setenv("LIUXIN_POSTGRES_URL", "postgresql:///wrong")
    monkeypatch.setenv("LIUXIN_POSTGRES_SCHEMA", "wrong")
    metadata = {
        key: "postgresql:///selected",
        "schema": "selected",
        "options": {"nested": [1]},
    }
    original = deepcopy(metadata)
    connection = resolve_database_connection(
        database_path=None, db_type=alias, database_metadata=metadata
    )
    assert connection["database_path"] == "postgresql:///selected"
    assert connection["database_metadata"]["schema"] == "selected"
    metadata["options"]["nested"].append(2)
    assert connection["database_metadata"]["options"] == original["options"]


@pytest.mark.parametrize("key", ["postgres_service", "database_service", "service"])
def test_postgres_service_forms_survive_conflicting_environment(monkeypatch, key):
    monkeypatch.setenv("LIUXIN_POSTGRES_URL", "postgresql:///wrong")
    connection = resolve_database_connection(
        database_path=None,
        db_type="PostgreSQL",
        database_metadata={key: "selected", "schema": "books"},
    )
    assert connection["database_path"] == "service=selected"
    assert connection["database_metadata"]["postgres_service"] == "selected"
    assert connection["database_metadata"]["schema"] == "books"
    assert resolve_database_connection(**connection) == connection


@pytest.mark.parametrize(
    "selector", ["selected", "service=selected", "postgresql:///selected"]
)
def test_explicit_selector_wins_without_retaining_competing_targets(selector):
    connection = resolve_database_connection(
        database_path=selector,
        db_type="pg",
        database_metadata={
            "dsn": "postgresql:///wrong",
            "service": "wrong",
            "schema": "books",
        },
    )
    metadata = connection["database_metadata"]
    assert "wrong" not in repr(connection)
    assert metadata["schema"] == "books"
    assert resolve_database_connection(**connection) == connection


def test_capture_uses_driver_resolved_target_and_schema_after_environment_changes(
    monkeypatch,
):
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
        PostgresConnectionTarget,
    )

    database = SimpleNamespace(
        type="PostgreSQL",
        metadata={"postgres_url": "postgresql:///stale"},
        _driver=SimpleNamespace(
            connection_target=PostgresConnectionTarget("service", "original"),
            schema="original_schema",
        ),
    )
    monkeypatch.setenv("LIUXIN_POSTGRES_URL", "postgresql:///wrong")
    monkeypatch.setenv("LIUXIN_POSTGRES_SCHEMA", "wrong")
    connection = capture_database_connection(database)
    assert connection["database_path"] == "service=original"
    assert connection["database_metadata"]["schema"] == "original_schema"
    assert "stale" not in repr(connection)


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw", "apsw"])
def test_relative_path_and_nested_options_captured_for_service_lifetime(
    tmp_path, monkeypatch, backend
):
    monkeypatch.chdir(tmp_path)
    metadata = {"database_path": "selected.sqlite", "options": {"values": [1]}}
    database = SimpleNamespace(type=backend, metadata=metadata)
    services = CoreServices(library=SimpleNamespace(db=database), preferences={})
    try:
        metadata["database_path"] = "wrong.sqlite"
        metadata["options"]["values"].append(2)
        monkeypatch.chdir(tmp_path.parent)
        options = services.database_connection
        assert options["database_path"] == str(tmp_path / "selected.sqlite")
        assert options["database_metadata"]["options"]["values"] == [1]
        options["database_metadata"]["options"]["values"].append(3)
        assert services.database_connection["database_metadata"]["options"][
            "values"
        ] == [1]
    finally:
        services.close()


def test_workflow_submission_carries_captured_metadata_without_sharing_input(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.core import command_recovery
    from LiuXin_alpha.core.program_services.payloads import _job_submit

    requests = []
    monkeypatch.setattr(
        command_recovery,
        "submit_workflow",
        lambda owner, **intent: requests.append(intent) or {"state": "accepted"},
    )
    database = SimpleNamespace(
        type="PostgreSQL", metadata={"postgres_service": "selected", "schema": "books"}
    )
    services = CoreServices(library=SimpleNamespace(db=database), preferences={})
    runtime = SimpleNamespace(
        _durable_jobs=object(),
        database=database,
        services=services,
        job_manager=SimpleNamespace(
            submit=lambda request, **opts: requests.append(request) or "job"
        ),
    )
    try:
        _job_submit(
            runtime,
            {},
            function_name="run_ingest_disk_job",
            kwargs={"database_path": "ignored", "db_type": "PostgreSQL"},
            default_label="ingest",
        )
        database.metadata["schema"] = "wrong"
        assert requests[0]["kwargs"]["database_metadata"]["schema"] == "books"
        assert requests[0]["kwargs"]["database_path"] == "service=selected"
    finally:
        services.close()


@pytest.mark.parametrize(
    "overrides,expected_path,expected_schema",
    [
        ({}, "postgresql:///selected", "books"),
        ({"db_type": "pg"}, "postgresql:///selected", "books"),
        ({"database_path": "postgresql:///other"}, "postgresql:///other", "public"),
        (
            {"database_metadata": {"schema": "explicit"}},
            "postgresql:///selected",
            "explicit",
        ),
    ],
)
def test_sync_submission_keeps_connection_defaults_with_safe_alternate_targets(
    monkeypatch, overrides, expected_path, expected_schema
):
    from LiuXin_alpha.core.runtime import CoreRuntime

    monkeypatch.delenv("LIUXIN_POSTGRES_SCHEMA", raising=False)
    from LiuXin_alpha.core import command_recovery
    from LiuXin_alpha.core.errors import CoreError

    requests = []
    monkeypatch.setattr(
        command_recovery,
        "submit_workflow",
        lambda owner, **intent: requests.append(intent) or {"state": "accepted"},
    )
    database = SimpleNamespace(
        type="PostgreSQL",
        metadata={
            "postgres_url": "postgresql:///selected",
            "schema": "books",
            "private_token": "retain-only-for-original",
        },
    )
    manager = SimpleNamespace(
        submit=lambda request, **options: requests.append(request) or "job"
    )
    core = CoreRuntime(
        library=SimpleNamespace(db=database), preferences={}, job_manager=manager
    )
    core._durable_jobs = SimpleNamespace(stop=lambda **kwargs: None)
    try:
        if expected_path != "postgresql:///selected" or expected_schema != "books":
            with pytest.raises(CoreError):
                core.command("sync.store.start", {"sync_kwargs": overrides})
            assert not requests
            return
        core.command("sync.store.start", {"sync_kwargs": overrides})
        selected = requests[0]["kwargs"]
        assert selected["database_path"] == expected_path
        assert selected["database_metadata"]["schema"] == expected_schema
        if expected_schema != "books":
            assert "private_token" not in selected["database_metadata"]
        assert "database_metadata" not in overrides or overrides[
            "database_metadata"
        ] == {"schema": "explicit"}
        # The public status projection must never serialize connection values.
        from LiuXin_alpha.utils.jobs.manager import ManagedJob

        public = core._serialize_managed_job(
            ManagedJob(
                job_id="job",
                request=__import__(
                    "LiuXin_alpha.utils.jobs", fromlist=["JobRequest"]
                ).JobRequest(
                    module_name="LiuXin_alpha.core.workflow_jobs",
                    function_name="run_sync_store_job",
                    kwargs=selected,
                ),
                state="pending",
                submitted_at=0,
            )
        )
        assert "retain-only-for-original" not in repr(public)
    finally:
        core.shutdown()


@pytest.mark.parametrize(
    "field,value",
    [
        (field, value)
        for field in ("database_path", "db_type")
        for value in (False, 0, [], {})
    ]
    + [("database_metadata", value) for value in (False, "", [], 0)],
)
def test_invalid_sync_connection_fields_cannot_fall_back_to_current_library(
    field, value
):
    from LiuXin_alpha.core.errors import CoreHandlerError
    from LiuXin_alpha.core.runtime import CoreRuntime

    requests = []
    database = SimpleNamespace(
        type="SQLite", metadata={"database_path": "/tmp/never-opened.sqlite"}
    )
    manager = SimpleNamespace(submit=lambda *args, **kwargs: requests.append(args))
    core = CoreRuntime(
        library=SimpleNamespace(db=database), preferences={}, job_manager=manager
    )
    try:
        with pytest.raises(CoreHandlerError) as caught:
            core.command("sync.store.start", {"sync_kwargs": {field: value}})
        assert caught.value.code == "invalid_payload"
        assert requests == []
    finally:
        core.shutdown()


@pytest.mark.parametrize("kind", ["SQLite", "SQLite_apsw", "OtherDriver"])
def test_non_postgres_drivers_do_not_use_postgres_target_introspection(kind, tmp_path):
    database = SimpleNamespace(
        type=kind,
        metadata={"database_path": str(tmp_path / "selected.db")},
        _driver=SimpleNamespace(connection_target=object()),
    )
    connection = capture_database_connection(database)
    assert connection["database_path"] == str(tmp_path / "selected.db")
    assert "schema" not in connection["database_metadata"]
