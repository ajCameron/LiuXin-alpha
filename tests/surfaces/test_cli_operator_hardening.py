"""Operator-contract coverage for profiles, diagnosis, recovery, and integrity."""

from __future__ import annotations

import argparse
import json
import sqlite3
import stat

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.surfaces.cli import catalogue as catalogue_cli
from LiuXin_alpha.surfaces.cli import diagnostics as diagnostics_cli
from LiuXin_alpha.surfaces.cli import ingest_runs as ingest_runs_cli
from LiuXin_alpha.surfaces.cli import storage as storage_cli
from LiuXin_alpha.surfaces.cli.storage_commands import (
    administration, core_access, integrity, store_add, store_wizard,
)
from LiuXin_alpha.surfaces.cli import workflows as workflows_cli
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.system_profile import (
    PROFILE_POINTER_FORMAT,
    apply_system_profile,
)


class _OperatorCore:
    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def query(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.queries.append((name, values))
        if name == "health":
            return {"ok": True, "shutdown": False}
        if name == "database.info":
            return {
                "type": "SQLite",
                "exists": True,
                "target": "postgresql://reader:swordfish@example.invalid/books",
                "password": "swordfish",
            }
        if name == "storage.stores.list":
            return {"stores": [], "count": 0}
        if name == "storage.backends.list":
            return {
                "count": 2,
                "backends": [
                    {
                        "kind": "filesystem",
                        "label": "Local folder (read/write)",
                        "aliases": ["file"],
                        "location_type": "dir",
                        "access_protocol": "file",
                        "read_only_default": False,
                        "user_selectable": True,
                        "policy_section": None,
                        "capabilities": {
                            "folders": True,
                            "hierarchical_list": True,
                            "random_read": True,
                            "random_write": True,
                            "delete": True,
                            "checksums": True,
                            "immutable_objects": False,
                        },
                        "limitations": [],
                    },
                    {
                        "kind": "s3",
                        "label": "Native S3-compatible bucket",
                        "aliases": ["s3_compatible"],
                        "location_type": "remote",
                        "access_protocol": "s3",
                        "read_only_default": False,
                        "user_selectable": True,
                        "policy_section": "s3",
                        "capabilities": {
                            "folders": True,
                            "hierarchical_list": True,
                            "random_read": True,
                            "random_write": True,
                            "delete": True,
                            "checksums": True,
                            "immutable_objects": False,
                        },
                        "limitations": [
                            {
                                "code": "s3_service_limits_apply",
                                "message": "The selected S3 service sets limits.",
                            }
                        ],
                    },
                ],
                "credentials": "not persisted",
            }
        if name == "storage.status":
            return {
                "healthy": True,
                "summary": {
                    "configured_stores": 1,
                    "folder_stores": 1,
                    "available_stores": 1,
                    "live_replicas": 2,
                    "replica_bytes": 4096,
                },
                "stores": [
                    {
                        "name": "primary",
                        "kind": "filesystem",
                        "root": "/srv/liuxin/store",
                        "supports_folders": True,
                        "available": True,
                        "writable": True,
                        "replicas": 2,
                    }
                ],
                "status": {"issues": []},
            }
        if name == "storage.reconcile.plan":
            return {"healthy": False, "automatic_actions": [], "deferred_actions": []}
        if name == "storage.repair.plan":
            return {"blocked": False, "actions": [], "deletes_bytes": False}
        if name == "storage.store.evacuate.plan":
            return {
                "blocked": False,
                "entries": [],
                "source_store_ref": values["store"],
            }
        if name == "storage.recovery.list":
            return {"operations": [], "total": 0, **values}
        if name == "capabilities.list":
            return {"families": {}}
        if name == "jobs.list":
            return {"jobs": [], "total": 0}
        if name == "custom-fields.list":
            return {
                "fields": [{"num": 7, "label": "source", "name": "Source"}],
                "count": 1,
            }
        if name.startswith("database.migrations."):
            return {"ok": True, "actions": [], "operation": name}
        raise AssertionError("Unexpected query: {}".format(name))

    def command(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.commands.append((name, values))
        if name == "storage.store.save":
            return {"saved": True, "store": values["store"]}
        if name == "storage.store.update":
            return {"updated": True, **values}
        if name == "storage.refresh":
            return {"refreshed": True, "report": {"loaded_stores": 1}}
        if name == "storage.default.set":
            return {"selected": True, "store_name": values["store"]}
        if name == "storage.store.probe":
            return {
                "store": values["store"],
                "status": {"available": True, "writable": True},
                "live_status": {"available": True, "writable": True},
            }
        if name == "storage.source.register":
            return {"registered": True, **values}
        if name in {"storage.replica.verify", "storage.asset.verify"}:
            return {"healthy": True, "report": values}
        if name == "storage.audit":
            return {"ok": True, "checked": 1, "results": []}
        if name == "storage.reconcile.apply":
            return {"ok": True, "actions": []}
        if name == "storage.repair.apply":
            return {"ok": True, "actions": [], "deletes_bytes": False}
        if name == "storage.store.evacuate.apply":
            return {"ok": True, "actions": [], **values}
        if name.startswith("storage.recovery."):
            return {"ok": True, "operation": name, **values}
        if name.startswith("custom-fields."):
            return {"operation": name, **values}
        if name == "database.migrations.apply":
            return {"migrated": True}
        raise AssertionError("Unexpected command: {}".format(name))


@contextmanager
def _session(core: _OperatorCore, *_args: object, **_kwargs: object):
    yield core


@pytest.fixture
def operator_core(monkeypatch: pytest.MonkeyPatch) -> _OperatorCore:
    core = _OperatorCore()
    opener = lambda *args, **kwargs: _session(core, *args, **kwargs)
    for owner in (administration, core_access, integrity, store_add, store_wizard):
        monkeypatch.setattr(owner, "open_cli_core", opener)
    monkeypatch.setattr(catalogue_cli, "open_cli_core", opener)
    monkeypatch.setattr(diagnostics_cli, "open_cli_core", opener)
    monkeypatch.setattr(workflows_cli, "open_cli_core", opener)
    return core


def _sqlite(path: Path, table: str = "sample") -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE {} (id INTEGER PRIMARY KEY)".format(table))
        connection.commit()
    finally:
        connection.close()


def _manifest(root: Path, database: Path) -> Path:
    root.mkdir()
    (root / "logs" / "ingest").mkdir(parents=True)
    (root / "ingest-materialized").mkdir()
    path = root / "liuxin-system.json"
    path.write_text(
        json.dumps(
            {
                "format": "liuxin.system",
                "version": 1,
                "system_root": str(root),
                "database": str(database),
                "db_type": "SQLite",
                "store_root": None,
                "materialization_root": str(root / "ingest-materialized"),
                "log_directory": str(root / "logs" / "ingest"),
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def test_global_system_profile_show_validate_and_argument_resolution(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)
    root = tmp_path / "system"
    manifest = _manifest(root, database)

    assert cli_main(["--system-root", str(root), "config", "show"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["path"] == str(manifest)
    assert shown["manifest"]["database"] == str(database)

    assert cli_main(["config", "validate", "--system-root", str(root)]) == 0
    validated = json.loads(capsys.readouterr().out)
    assert validated["ok"] is True

    args = argparse.Namespace(
        database=None,
        core_endpoint=None,
        db_type="SQLite",
        system_root=str(root),
        profile=None,
    )
    resolved = apply_system_profile(args)
    assert resolved is not None
    assert args.database == str(database)
    assert args.resolved_system_manifest == str(manifest)

    postgres_root = tmp_path / "postgres-system"
    postgres_root.mkdir()
    postgres_manifest = postgres_root / "liuxin-system.json"
    postgres_manifest.write_text(
        json.dumps(
            {
                "format": "liuxin.system",
                "version": 1,
                "database": (
                    "postgresql://reader:swordfish@example.invalid/catalogue"
                ),
                "db_type": "PostgreSQL",
                "database_metadata": {"schema": "liuxin"},
            }
        ),
        encoding="utf-8",
    )
    postgres_manifest.chmod(0o600)
    postgres_args = argparse.Namespace(
        database=None,
        core_endpoint=None,
        db_type="SQLite",
        system_root=str(postgres_root),
        profile=None,
    )
    apply_system_profile(postgres_args)
    assert postgres_args.db_type == "PostgreSQL"
    assert postgres_args.database.startswith("postgresql://reader:swordfish@")
    assert postgres_args.database_metadata == {"schema": "liuxin"}

    assert cli_main(["config", "show", "--system-root", str(postgres_root)]) == 0
    redacted = json.loads(capsys.readouterr().out)
    assert "swordfish" not in json.dumps(redacted)
    assert "<redacted>" in redacted["manifest"]["database"]


def test_named_profiles_are_credential_free_selectors_and_can_be_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT", raising=False)
    monkeypatch.delenv("LIUXIN_PROFILE", raising=False)
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)
    root = tmp_path / "named-system"
    manifest = _manifest(root, database)

    assert cli_main(["config", "profiles", "add", "reading", str(root)]) == 0
    created = json.loads(capsys.readouterr().out)
    pointer_path = Path(created["profile"])
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert pointer == {
        "format": PROFILE_POINTER_FORMAT,
        "manifest": str(manifest),
        "version": 1,
    }
    assert stat.S_IMODE(pointer_path.stat().st_mode) == 0o600

    assert cli_main(["config", "profiles", "list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["count"] == 1
    assert listed["profiles"][0]["name"] == "reading"
    assert listed["profiles"][0]["valid"] is True

    args = argparse.Namespace(
        database=None,
        core_endpoint=None,
        db_type="SQLite",
        system_root=None,
        profile="reading",
    )
    selected = apply_system_profile(args)
    assert selected is not None
    assert selected.path == manifest
    assert args.database == str(database)

    assert cli_main(["config", "profiles", "remove", "reading"]) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert pointer_path.is_file()
    assert cli_main(
        ["config", "profiles", "remove", "reading", "--yes"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["systems_modified"] is False
    assert not pointer_path.exists()


def test_status_projection_and_completion_scripts_are_operator_facing(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)
    assert cli_main(["status", "--database", str(database)]) == 0
    status_report = json.loads(capsys.readouterr().out)
    assert status_report["ok"] is True
    assert status_report["database"]["type"] == "SQLite"
    assert status_report["storage"] == {
        "healthy": True,
        "issues": 0,
        "stores": 0,
    }
    assert "sections" not in status_report

    markers = {
        "bash": "complete -F _liuxin_complete liuxin",
        "zsh": "compdef _liuxin liuxin",
        "fish": "complete -c liuxin",
    }
    for shell, marker in markers.items():
        assert cli_main(["completion", shell]) == 0
        script = capsys.readouterr().out
        assert marker in script
        assert "storage" in script


def test_storage_status_prints_the_store_overview_and_can_refresh(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)

    assert cli_main(
        [
            "storage",
            "status",
            "--database",
            str(database),
            "--refresh",
        ]
    ) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["summary"]["folder_stores"] == 1
    assert report["summary"]["live_replicas"] == 2
    assert report["stores"] == [
        {
            "available": True,
            "kind": "filesystem",
            "name": "primary",
            "replicas": 2,
            "root": "/srv/liuxin/store",
            "supports_folders": True,
            "writable": True,
        }
    ]
    assert operator_core.queries[-1] == (
        "storage.status",
        {"refresh_stores": True},
    )


def test_storage_add_has_provider_discovery_and_rclone_style_automation(
    operator_core: _OperatorCore,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = ["--database", "catalogue.sqlite"]
    assert cli_main(["storage", "backends", *connection]) == 0
    providers = json.loads(capsys.readouterr().out)
    assert providers["count"] == 2
    assert [item["kind"] for item in providers["backends"]] == [
        "filesystem",
        "s3",
    ]

    assert cli_main(
        [
            "storage",
            "add",
            *connection,
            "offsite-books",
            "s3",
            "s3://book-archive/library",
            'region_name="eu-west-2"',
            "multipart_threshold=16777216",
            "--tag",
            "offsite",
            "--failure-domain",
            "cloud-eu-west-2",
            "--default",
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True
    assert result["backend"]["kind"] == "s3"
    assert result["probe"]["ok"] is True
    store = result["store"]
    assert store["store_name"] == "offsite-books"
    assert store["store_kind"] == "s3"
    assert store["store_root_uri"] == "s3://book-archive/library"
    assert store["store_access_protocol"] == "s3"
    assert store["store_supports_folders"] == 1
    assert store["store_supports_random_write"] == 1
    assert store["store_failure_domain"] == "cloud-eu-west-2"
    assert json.loads(store["store_tags_json"]) == ["offsite"]
    policy = json.loads(store["store_policy_json"])
    assert policy == {
        "backend": "s3",
        "s3": {
            "multipart_threshold": 16777216,
            "region_name": "eu-west-2",
        },
    }
    assert [name for name, _payload in operator_core.commands[-4:]] == [
        "storage.store.save",
        "storage.refresh",
        "storage.store.probe",
        "storage.default.set",
    ]


def test_storage_add_wizard_confirms_a_registry_backed_folder_store(
    operator_core: _OperatorCore,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    answers = iter(
        [
            "1",                 # filesystem backend
            "/srv/Library Books",
            "",                  # generated name
            "",                  # live role
            "",                  # writable
            "",                  # online
            "",                  # no advanced configuration
            "y",                 # default Store
            "",                  # probe after save
            "y",                 # final confirmation
        ]
    )
    monkeypatch.setattr(store_wizard, "_storage_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert cli_main(
        [
            "storage",
            "add",
            "--database",
            "catalogue.sqlite",
            "--compact",
        ]
    ) == 0
    output = capsys.readouterr().out
    assert "LiuXin storage configuration" in output
    assert "Store configuration plan" in output
    result = json.loads(output.splitlines()[-1])
    assert result["store"]["store_name"] == "library_books"
    assert result["store"]["store_kind"] == "filesystem"
    assert result["store"]["store_root_uri"] == "/srv/Library Books"
    assert result["store"]["store_operational_role"] == "live"
    assert result["default"]["selected"] is True
    assert result["probe"]["ok"] is True
    assert operator_core.queries[-1] == (
        "storage.backends.list",
        {"include_internal": False},
    )


def test_storage_add_wizard_preserves_advanced_backend_configuration(
    operator_core: _OperatorCore,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    answers = iter(
        [
            "2",                         # S3 backend
            "s3://archive/books",
            "",                          # generated name
            "",                          # live role
            "",                          # writable
            "",                          # online
            "y",                         # advanced configuration
            "cloud-eu-west-2",
            "eu-west-2",
            "offsite, archive",
            "multipart_threshold=16777216",
            "",                          # backend options complete
            "y",                         # default Store
            "",                          # probe after save
            "y",                         # final confirmation
        ]
    )
    monkeypatch.setattr(store_wizard, "_storage_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert cli_main(
        [
            "storage",
            "add",
            "--database",
            "catalogue.sqlite",
            "--compact",
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    store = result["store"]
    assert store["store_name"] == "books"
    assert store["store_failure_domain"] == "cloud-eu-west-2"
    assert store["store_region"] == "eu-west-2"
    assert json.loads(store["store_tags_json"]) == ["archive", "offsite"]
    assert json.loads(store["store_policy_json"]) == {
        "backend": "s3",
        "s3": {
            "multipart_threshold": 16777216,
        },
    }
    assert result["default"]["selected"] is True
    assert result["probe"]["ok"] is True


def test_storage_add_rejects_persisted_credentials_before_writing(
    operator_core: _OperatorCore,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = len(operator_core.commands)
    assert cli_main(
        [
            "storage",
            "add",
            "--database",
            "catalogue.sqlite",
            "private-bucket",
            "s3",
            "s3://private/books",
            "access_key=do-not-store-this",
        ]
    ) == 2
    assert len(operator_core.commands) == before
    assert "looks secret-bearing" in capsys.readouterr().err


def test_global_system_root_opens_a_real_initialized_core(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "real-system"
    assert cli_main(["init", str(root), "--no-store"]) == 0
    initialized = json.loads(capsys.readouterr().out)
    assert initialized["ok"] is True

    assert cli_main(["--system-root", str(root), "core", "health"]) == 0
    health = json.loads(capsys.readouterr().out)
    assert health["shutdown"] is False

    assert cli_main(["--system-root", str(root), "storage", "status"]) == 0
    storage_status = json.loads(capsys.readouterr().out)
    assert storage_status["healthy"] is True
    assert storage_status["summary"]["configured_stores"] == 0
    assert storage_status["summary"]["folder_stores"] == 0
    assert storage_status["stores"] == []

    added_root = root / "added-store"
    assert cli_main(
        [
            "storage",
            "add",
            "primary",
            "filesystem",
            str(added_root),
            "--system-root",
            str(root),
            "--default",
        ]
    ) == 0
    added = json.loads(capsys.readouterr().out)
    assert added["ok"] is True
    assert added["probe"]["ok"] is True
    assert added["default"]["selected"] is True
    assert added_root.is_dir()

    assert cli_main(["--system-root", str(root), "storage", "status"]) == 0
    storage_status = json.loads(capsys.readouterr().out)
    assert storage_status["summary"]["configured_stores"] == 1
    assert storage_status["summary"]["folder_stores"] == 1
    assert storage_status["stores"][0]["root"] == str(added_root)

    assert cli_main(["doctor", "--system-root", str(root)]) == 0
    doctor = json.loads(capsys.readouterr().out)
    assert doctor["ok"] is True
    assert "database" in doctor["sections"]
    assert "storage_status" in doctor["sections"]


def test_real_initialized_folder_store_has_an_operator_status_overview(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "folder-store-system"
    assert cli_main(["init", str(root)]) == 0
    _ = capsys.readouterr()

    assert cli_main(["storage", "status", "--system-root", str(root)]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["healthy"] is True
    assert report["summary"]["configured_stores"] == 1
    assert report["summary"]["folder_stores"] == 1
    assert report["summary"]["available_stores"] == 1
    assert report["summary"]["writable_stores"] == 1
    assert len(report["stores"]) == 1
    store = report["stores"][0]
    assert store["name"] == "primary"
    assert store["kind"] == "filesystem"
    assert store["root"] == str(root / "store")
    assert store["supports_folders"] is True
    assert store["available"] is True
    assert store["writable"] is True
    assert store["is_default"] is True


def test_persistent_connect_selects_later_commands_and_disconnects_safely(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT", raising=False)
    monkeypatch.delenv("LIUXIN_PROFILE", raising=False)
    root = tmp_path / "connected-system"
    assert cli_main(["init", str(root), "--no-store"]) == 0
    _ = capsys.readouterr()

    assert cli_main(["connect", str(root)]) == 0
    connected = json.loads(capsys.readouterr().out)
    assert connected["connected"] is True
    assert connected["effective_now"] is True
    connection_file = Path(connected["connection_file"])
    assert stat.S_IMODE(connection_file.stat().st_mode) == 0o600
    pointer = json.loads(connection_file.read_text(encoding="utf-8"))
    assert set(pointer) == {"format", "version", "manifest"}
    assert pointer["manifest"] == str(root / "liuxin-system.json")
    assert "database" not in pointer

    assert cli_main(["connect", "status"]) == 0
    connection_status = json.loads(capsys.readouterr().out)
    assert connection_status["connected"] is True
    assert connection_status["effective_source"] == "active-connection"
    assert connection_status["persisted_manifest_exists"] is True
    assert cli_main(["connect"]) == 0
    assert json.loads(capsys.readouterr().out)["connected"] is True

    assert cli_main(["core", "health"]) == 0
    assert json.loads(capsys.readouterr().out)["shutdown"] is False
    assert cli_main(["config", "path"]) == 0
    selected = json.loads(capsys.readouterr().out)
    assert selected["source"] == "active-connection"
    assert selected["path"] == str(root / "liuxin-system.json")
    assert cli_main(["doctor"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    environment_root = tmp_path / "environment-system"
    _manifest(environment_root, root / "catalogue.sqlite")
    assert cli_main(["config", "path", "--system-root", str(environment_root)]) == 0
    explicit_selected = json.loads(capsys.readouterr().out)
    assert explicit_selected["source"] == "system-root"
    assert explicit_selected["path"] == str(
        environment_root / "liuxin-system.json"
    )
    monkeypatch.setenv("LIUXIN_SYSTEM_ROOT", str(environment_root))
    assert cli_main(["config", "path"]) == 0
    environment_selected = json.loads(capsys.readouterr().out)
    assert environment_selected["source"] == "LIUXIN_SYSTEM_ROOT"
    assert environment_selected["path"] == str(
        environment_root / "liuxin-system.json"
    )
    assert cli_main(["connect", str(root), "--no-health-check"]) == 0
    overridden_connect = json.loads(capsys.readouterr().out)
    assert overridden_connect["effective_now"] is False
    assert "currently overrides" in overridden_connect["warning"]

    assert cli_main(["disconnect"]) == 0
    disconnected = json.loads(capsys.readouterr().out)
    assert disconnected["disconnected"] is True
    assert disconnected["systems_modified"] is False
    assert disconnected["environment_selection_remains"] is True
    assert not connection_file.exists()
    assert (root / "catalogue.sqlite").is_file()

    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT")
    assert cli_main(["core", "health"]) == 2
    assert "liuxin connect" in capsys.readouterr().err

    assert cli_main(
        [
            "connect",
            "--profile",
            str(root / "liuxin-system.json"),
            "--no-health-check",
        ]
    ) == 0
    profile_connected = json.loads(capsys.readouterr().out)
    assert profile_connected["effective_now"] is True
    assert cli_main(["disconnect"]) == 0
    _ = capsys.readouterr()

    connection_file.write_text("{broken", encoding="utf-8")
    connection_file.chmod(0o600)
    assert cli_main(["core", "health"]) == 2
    assert "liuxin disconnect" in capsys.readouterr().err
    assert cli_main(["disconnect"]) == 0
    assert json.loads(capsys.readouterr().out)["disconnected"] is True
    assert cli_main(["connect", "status"]) == 1
    assert json.loads(capsys.readouterr().out)["connected"] is False


def test_doctor_and_diagnostics_are_aggregated_and_redacted(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)
    connection = ["--database", str(database)]

    assert cli_main(["doctor", *connection]) == 0
    doctor = json.loads(capsys.readouterr().out)
    assert doctor["ok"] is True
    assert doctor["sections"]["core_health"]["ok"] is True
    assert "swordfish" not in json.dumps(doctor)
    assert doctor["sections"]["database"]["password"] == "<redacted>"

    assert cli_main(["diagnostics", "collect", *connection]) == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["format"] == "liuxin.diagnostics"
    assert "credential fields" in bundle["redaction"]
    assert "swordfish" not in json.dumps(bundle)
    assert operator_core.queries


def test_typed_storage_setup_integrity_and_reconcile_commands(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    store_root = tmp_path / "store"

    assert cli_main(
        [
            "storage",
            "store",
            "add",
            *connection,
            "filesystem",
            str(store_root),
            "--name",
            "primary",
            "--default",
            "--tag",
            "fast",
        ]
    ) == 0
    saved = json.loads(capsys.readouterr().out)
    assert saved["store"]["store_name"] == "primary"
    assert operator_core.commands[-1][0] == "storage.default.set"

    assert cli_main(
        [
            "storage",
            "sources",
            "add",
            *connection,
            "unmanaged-disk",
            str(tmp_path / "incoming"),
            "--name",
            "drive-1",
            "--no-hash",
        ]
    ) == 0
    _ = capsys.readouterr()
    name, payload = operator_core.commands[-1]
    assert name == "storage.source.register"
    assert payload["options"]["compute_hash"] is False

    assert cli_main(["storage", "replica", "verify", *connection, "4"]) == 0
    assert json.loads(capsys.readouterr().out)["healthy"] is True
    assert cli_main(["storage", "asset", "verify", *connection, "8", "--all-replicas"]) == 0
    _ = capsys.readouterr()
    assert cli_main(["storage", "audit", *connection, "--limit", "1"]) == 0
    _ = capsys.readouterr()
    assert cli_main(["storage", "reconcile", "apply", *connection]) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert cli_main(["storage", "reconcile", "apply", *connection, "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert cli_main(
        [
            "storage",
            "store",
            "update",
            *connection,
            "primary",
            "--add-tag",
            "offsite",
            "--read-only",
        ]
    ) == 0
    _ = capsys.readouterr()
    assert operator_core.commands[-1] == (
        "storage.store.update",
        {
            "store": "primary",
            "changes": {"read_only": True, "add_tags": ["offsite"]},
        },
    )

    assert cli_main(
        ["storage", "repair", "plan", *connection, "--asset-id", "8"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["deletes_bytes"] is False
    assert cli_main(["storage", "repair", "apply", *connection]) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert cli_main(
        ["storage", "repair", "apply", *connection, "--yes"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    before = len(operator_core.commands)
    assert cli_main(
        [
            "storage",
            "store",
            "evacuate",
            *connection,
            "primary",
            "--destination-store",
            "archive",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["blocked"] is False
    assert len(operator_core.commands) == before
    assert cli_main(
        [
            "storage",
            "store",
            "evacuate",
            *connection,
            "primary",
            "--destination-store",
            "archive",
            "--yes",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert cli_main(
        ["storage", "recovery", "list", *connection, "--state", "failed"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "failed"
    operation_id = "12345678-1234-5678-9234-567812345678"
    assert cli_main(
        ["storage", "recovery", "retry-ingest", *connection, operation_id]
    ) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert cli_main(
        [
            "storage",
            "recovery",
            "retry-ingest",
            *connection,
            operation_id,
            "--yes",
        ]
    ) == 0
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["operation"] == "storage.recovery.retry-ingest"


def test_custom_fields_are_semantic_and_deletion_previews(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    assert cli_main(["catalog", "custom-fields", "show", *connection, "source"]) == 0
    assert json.loads(capsys.readouterr().out)["field"]["num"] == 7

    assert cli_main(
        [
            "catalog",
            "custom-fields",
            "create",
            *connection,
            "Ingest source",
            "--label",
            "ingest_source",
            "--datatype",
            "text",
        ]
    ) == 0
    _ = capsys.readouterr()
    assert operator_core.commands[-1][0] == "custom-fields.create"

    before = len(operator_core.commands)
    assert cli_main(
        ["catalog", "custom-fields", "delete", *connection, "--num", "7"]
    ) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["preview"] is True
    assert len(operator_core.commands) == before
    assert cli_main(
        ["catalog", "custom-fields", "delete", *connection, "--num", "7", "--yes"]
    ) == 0
    _ = capsys.readouterr()
    assert operator_core.commands[-1][0] == "custom-fields.delete"


def test_ingest_runs_list_show_issues_and_refuse_discovery_resume(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")
    logs = tmp_path / "logs"
    run_id = "12345678-1234-5678-9234-567812345678"
    assert cli_main(
        [
            "storage",
            "ingest",
            "--source-root",
            str(source),
            "--discover-only",
            "--log-directory",
            str(logs),
            "--run-id",
            run_id,
            "--no-console-progress",
        ]
    ) == 0
    _ = capsys.readouterr()

    assert cli_main(["ingest", "runs", "list", "--log-directory", str(logs)]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["runs"][0]["run_id"] == run_id
    assert cli_main(
        ["ingest", "runs", "show", "--log-directory", str(logs), run_id]
    ) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["report"]["mode"] == "discovery"
    assert cli_main(
        ["ingest", "runs", "issues", "--log-directory", str(logs), run_id]
    ) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0
    assert cli_main(
        ["ingest", "runs", "resume", "--log-directory", str(logs), run_id]
    ) == 2
    assert "Only real ingest attempts" in capsys.readouterr().err


def test_ingest_run_resume_reconstructs_an_operational_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT", raising=False)
    monkeypatch.delenv("LIUXIN_PROFILE", raising=False)
    system_root = tmp_path / "system"
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")
    run_id = "22345678-1234-5678-9234-567812345678"
    assert cli_main(["init", str(system_root)]) == 0
    _ = capsys.readouterr()
    assert cli_main(["connect", str(system_root), "--no-health-check"]) == 0
    _ = capsys.readouterr()
    first_exit = cli_main(
        [
            "storage",
            "ingest",
            "--source-root",
            str(source),
            "--run-id",
            run_id,
            "--no-console-progress",
        ]
    )
    assert first_exit in {0, 1}
    _ = capsys.readouterr()

    resumed: list[argparse.Namespace] = []

    def capture_resume(args: argparse.Namespace) -> int:
        resumed.append(args)
        return 0

    monkeypatch.setattr(ingest_runs_cli, "cmd_storage_ingest", capture_resume)
    assert cli_main(
        [
            "ingest",
            "runs",
            "resume",
            run_id,
            "--yes",
        ]
    ) == 0
    assert resumed
    resumed_args = resumed[0]
    assert str(resumed_args.run_id) == run_id
    assert resumed_args.source_root == str(source.resolve())
    assert resumed_args.database == str(system_root / "catalogue.sqlite")
    assert resumed_args.materialization_root == str(
        system_root / "ingest-materialized"
    )
    assert resumed_args.discover_only is False
    assert resumed_args.preflight_only is False


def test_database_backup_verification_and_atomic_offline_restore(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "catalogue Ω.sqlite"
    backup = tmp_path / "backup # naïve.sqlite"
    _sqlite(target, "old_data")
    _sqlite(backup, "restored_data")

    assert cli_main(["database", "verify-backup", str(backup)]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["ok"] is True
    assert len(verified["sha256"]) == 64

    assert cli_main(["backup", "verify", str(backup)]) == 0
    alias_verified = json.loads(capsys.readouterr().out)
    assert alias_verified["sha256"] == verified["sha256"]

    assert cli_main(
        [
            "database",
            "restore",
            "--database",
            str(target),
            str(backup),
            "--yes",
        ]
    ) == 0
    restored = json.loads(capsys.readouterr().out)
    assert restored["ok"] is True
    assert Path(restored["safety_backup"]).is_file()
    connection = sqlite3.connect(target)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        connection.close()
    assert "restored_data" in tables
    assert "old_data" not in tables

    second_target = tmp_path / "second-catalogue.sqlite"
    _sqlite(second_target, "second_old_data")
    assert cli_main(
        [
            "backup",
            "restore",
            "--database",
            str(second_target),
            str(backup),
            "--yes",
        ]
    ) == 0
    alias_restored = json.loads(capsys.readouterr().out)
    assert alias_restored["ok"] is True


def test_migration_apply_previews_until_confirmed(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    assert cli_main(["database", "migrations", "apply", *connection]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["preview"] is True
    assert cli_main(
        ["database", "migrations", "apply", *connection, "--yes"]
    ) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["applied"] is True
