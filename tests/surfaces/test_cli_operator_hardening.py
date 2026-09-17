"""
Exercise operator-facing selection, storage setup, diagnosis, and recovery contracts.

Tests requesting operator_core use a recording fake with canned service results;
they establish CLI routing and presentation, not real S3, integrity, or repair
behavior. Other cases initialize temporary SQLite systems, persist local profile
pointers, discover files, or restore small SQLite fixtures. Wizard input and
interactive detection are patched. Resume captures the reconstructed namespace
after an actual first attempt; it does not execute a second ingest. Shell
completion is checked as text, not installed or executed by a shell.
"""

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
from LiuXin_alpha.surfaces.cli import workflows as workflows_cli
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.cli.storage_commands import (
    administration,
    core_access,
    integrity,
    store_add,
    store_wizard,
)
from LiuXin_alpha.surfaces.system_profile import (
    PROFILE_POINTER_FORMAT,
    apply_system_profile,
)


class _OperatorCore:
    """
    Record CLI requests and return fixed operator-oriented service projections.

    Payload histories are shallow copies, not immutable snapshots. Responses
    need not agree with each other: the Store list is empty while storage status
    describes one folder Store. Commands do not mutate a backing catalogue or
    influence later queries. The database-info fixture deliberately contains
    a fake credential so diagnostic redaction can be asserted.

    Example:
        >>> core = _OperatorCore()
        >>> core.query('health')
        {'ok': True, 'shutdown': False}
        >>> core.queries
        [('health', {})]


    :ivar queries: Ordered operation names and shallow query payload copies.
    :ivar commands: Ordered operation names and shallow command payload copies.
    """

    def __init__(self) -> None:
        """
        Start a fake session with independent empty request histories.

        Example:
            >>> (_OperatorCore().queries, _OperatorCore().commands)
            ([], [])


        :return: None; initialize mutable query and command lists.
        """
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def query(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        """
        Record a query before selecting its canned response or rejecting its name.

        Falsey payloads become empty mappings. Recovery-list fields override
        the canned fields when echoed; evacuation requires a store key. Backend
        descriptors describe fixture capabilities, not installed provider checks.

        Example:
            >>> _OperatorCore().query('storage.recovery.list', {'state': 'failed'})
            {'operations': [], 'total': 0, 'state': 'failed'}


        :param name: Exact supported operation or a database.migrations. prefix.
        :param payload: Optional request fields copied shallowly into the history.
        :return: Branch-specific fixture mapping, with selected caller fields echoed.
        :raises AssertionError: No supported query branch matches, after recording it.
        :raises KeyError: A supported branch requires a missing payload field.
        """
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
        """
        Record a command and synthesize its receipt without executing a mutation.

        Some branches require store; several merge caller fields after receipt
        defaults, allowing those fields to override ok or operation. Reported
        probe, verification, refresh, and migration success are canned values.

        Example:
            >>> _OperatorCore().command('storage.default.set', {'store': 'primary'})
            {'selected': True, 'store_name': 'primary'}


        :param name: Exact command or a supported recovery/custom-field prefix.
        :param payload: Optional request fields shallow-copied before branch selection.
        :return: Synthetic receipt, not proof of storage or catalogue changes.
        :raises AssertionError: No command branch matches, after recording the call.
        :raises KeyError: A selected branch requires a missing payload field.
        """
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
    """
    Yield the supplied fake through the CLI opener's context-manager shape.

    No connection is opened, validated, or closed. Body exceptions propagate;
    extra arguments are accepted only so the real opener can be replaced.

    Example:
        >>> core = _OperatorCore()
        >>> with _session(core, storage=True) as opened:
        ...     opened is core
        True


    :param core: Recording fake exposed as the session value.
    :param _args: Ignored positional opener arguments.
    :param _kwargs: Ignored keyword opener arguments.
    :return: Context manager yielding the same fake without lifecycle effects.
    """
    yield core


@pytest.fixture
def operator_core(monkeypatch: pytest.MonkeyPatch) -> _OperatorCore:
    """
    Share one recording fake among storage, catalogue, diagnostic, and workflow owners.

    Patch the implementation modules' imported openers, not compatibility aliases.
    Other CLI owners retain real sessions unless the test patches them separately.
    pytest restores these replacements after the requesting test.

    Example:
        >>> operator_core.query('health')['ok']  # doctest: +SKIP
        True


    :param monkeypatch: Fixture managing scoped opener replacements.
    :return: New fake whose histories capture calls through the patched owners.
    """
    core = _OperatorCore()
    opener = lambda *args, **kwargs: _session(core, *args, **kwargs)
    for owner in (administration, core_access, integrity, store_add, store_wizard):
        monkeypatch.setattr(owner, "open_cli_core", opener)
    monkeypatch.setattr(catalogue_cli, "open_cli_core", opener)
    monkeypatch.setattr(diagnostics_cli, "open_cli_core", opener)
    monkeypatch.setattr(workflows_cli, "open_cli_core", opener)
    return core


def _sqlite(path: Path, table: str = "sample") -> None:
    """
    Commit one integer-primary-key table in a minimal SQLite fixture database.

    The table identifier is interpolated without quoting and must be controlled
    fixture text. This is not a LiuXin schema or a parent-directory creator.
    The connection closes even when table creation fails; a file may remain.

    Example:
        >>> _sqlite(tmp_path / 'backup.sqlite', 'restored_data')  # doctest: +SKIP


    :param path: Target SQLite file whose parent already exists.
    :param table: Trusted SQL identifier for the new fixture table.
    :return: None after committing and closing the fixture connection.
    """
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE {} (id INTEGER PRIMARY KEY)".format(table))
        connection.commit()
    finally:
        connection.close()


def _manifest(root: Path, database: Path) -> Path:
    """
    Create a private SQLite system manifest and its empty ingest directories.

    The root must not already exist. The database path is recorded as supplied,
    without opening or validating it; store_root is None. Directory creation,
    JSON publication, and chmod are separate effects with no rollback.

    Example:
        >>> _manifest(tmp_path / 'system', tmp_path / 'catalogue.sqlite')  # doctest: +SKIP


    :param root: New system directory beneath an existing parent.
    :param database: SQLite target to record without initializing a catalogue.
    :return: Path to liuxin-system.json after setting its mode to 0600.
    """
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
    """
    Resolve explicit system profiles while redacting a displayed PostgreSQL URL.

    SQLite validation uses a small real file. PostgreSQL coverage only resolves
    manifest fields and checks display redaction; no PostgreSQL server is opened.
    The operational namespace retains the fake password while display removes it.

    Example:
        >>> test_global_system_profile_show_validate_and_argument_resolution(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary location for SQLite and two system-manifest fixtures.
    :param capsys: Capture of config output used to inspect resolution and redaction.
    :return: None; assert selected paths, database type/metadata, and password filtering.
    """
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
                "database": ("postgresql://reader:swordfish@example.invalid/catalogue"),
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
    """
    Persist a named manifest pointer and require confirmation before removing it.

    Check exact pointer keys and private permissions, then resolve the named
    profile into a database selection. Unconfirmed removal preserves the pointer;
    confirmed removal reports that systems were not modified.

    Example:
        >>> test_named_profiles_are_credential_free_selectors_and_can_be_removed(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated configuration, manifest, and minimal SQLite locations.
    :param monkeypatch: Select temporary XDG configuration and clear ambient selectors.
    :param capsys: Capture of profile receipts and confirmation diagnostics.
    :return: None; assert pointer contents, selection, permissions, and deletion guard.
    """
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
    assert cli_main(["config", "profiles", "remove", "reading", "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["systems_modified"] is False
    assert not pointer_path.exists()


def test_status_projection_and_completion_scripts_are_operator_facing(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Project canned Core status compactly and render all three completion dialects.

    Completion assertions check shell-specific registration markers and the
    storage token; they do not source the scripts or exercise shell completion.

    Example:
        >>> test_status_projection_and_completion_scripts_are_operator_facing(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Recording fake supplying database and Store summaries.
    :param tmp_path: Location for a real minimal database selected by the CLI.
    :param capsys: Capture of status JSON and generated shell-script text.
    :return: None; assert compact status shape and bash/zsh/fish registration markers.
    """
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
    """
    Forward refresh intent to the storage-status query and retain its Store overview.

    The returned replica counts and writable/available flags come from the fake;
    this case does not refresh a real Store or inspect its bytes.

    Example:
        >>> test_storage_status_prints_the_store_overview_and_can_refresh(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Fake recording the final storage.status query and payload.
    :param tmp_path: Minimal SQLite fixture location used for CLI database selection.
    :param capsys: Capture for parsing the displayed status receipt.
    :return: None; assert Store projection, summary counts, and refresh_stores=True.
    """
    database = tmp_path / "catalogue.sqlite"
    _sqlite(database)

    assert (
        cli_main(
            [
                "storage",
                "status",
                "--database",
                str(database),
                "--refresh",
            ]
        )
        == 0
    )
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
    """
    Build an automated S3 Store declaration from descriptors and typed assignments.

    Verify policy/tag serialization and save-refresh-probe-default command order.
    The provider list, probe, and save are faked; no bucket or credentials are used.

    Example:
        >>> test_storage_add_has_provider_discovery_and_rclone_style_automation(operator_core, capsys)  # doctest: +SKIP


    :param operator_core: Recording fake advertising filesystem and S3 providers.
    :param capsys: Capture for parsing provider discovery and the final add receipt.
    :return: None; assert descriptor-derived fields, typed options, and command sequence.
    """
    connection = ["--database", "catalogue.sqlite"]
    assert cli_main(["storage", "backends", *connection]) == 0
    providers = json.loads(capsys.readouterr().out)
    assert providers["count"] == 2
    assert [item["kind"] for item in providers["backends"]] == [
        "filesystem",
        "s3",
    ]

    assert (
        cli_main(
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
        )
        == 0
    )
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
    """
    Drive the folder wizard through defaults, confirmation, probing, and selection.

    Synthetic input and forced interactivity replace a real terminal. Assert
    the generated name and final compact receipt after the textual plan; the
    recording Core does not create the selected folder.

    Example:
        >>> test_storage_add_wizard_confirms_a_registry_backed_folder_store(operator_core, monkeypatch, capsys)  # doctest: +SKIP


    :param operator_core: Fake provider registry and storage command recorder.
    :param monkeypatch: Supply ordered answers and force the interactive-input branch.
    :param capsys: Capture for plan headings and the last-line JSON receipt.
    :return: None; assert folder defaults, provider query, probe, and default selection.
    """
    answers = iter(
        [
            "1",  # filesystem backend
            "/srv/Library Books",
            "",  # generated name
            "",  # live role
            "",  # writable
            "",  # online
            "",  # no advanced configuration
            "y",  # default Store
            "",  # probe after save
            "y",  # final confirmation
        ]
    )
    monkeypatch.setattr(store_wizard, "_storage_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert (
        cli_main(
            [
                "storage",
                "add",
                "--database",
                "catalogue.sqlite",
                "--compact",
            ]
        )
        == 0
    )
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
    """
    Carry advanced wizard answers into an S3 Store declaration and typed policy.

    Exercise failure domain, region, sorted tags, and a numeric multipart option
    with canned save/probe/default receipts rather than a live S3 connection.

    Example:
        >>> test_storage_add_wizard_preserves_advanced_backend_configuration(operator_core, monkeypatch, capsys)  # doctest: +SKIP


    :param operator_core: Fake registry exposing the policy-bearing S3 descriptor.
    :param monkeypatch: Force interactive mode and provide the complete answer stream.
    :param capsys: Capture for parsing the final compact JSON after wizard output.
    :return: None; assert advanced declaration fields, numeric policy, and post-save receipts.
    """
    answers = iter(
        [
            "2",  # S3 backend
            "s3://archive/books",
            "",  # generated name
            "",  # live role
            "",  # writable
            "",  # online
            "y",  # advanced configuration
            "cloud-eu-west-2",
            "eu-west-2",
            "offsite, archive",
            "multipart_threshold=16777216",
            "",  # backend options complete
            "y",  # default Store
            "",  # probe after save
            "y",  # final confirmation
        ]
    )
    monkeypatch.setattr(store_wizard, "_storage_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert (
        cli_main(
            [
                "storage",
                "add",
                "--database",
                "catalogue.sqlite",
                "--compact",
            ]
        )
        == 0
    )
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
    """
    Reject a secret-bearing option key before any storage command is dispatched.

    This covers the access_key spelling, not exhaustive secret detection or
    absence of provider queries before the refusal.

    Example:
        >>> test_storage_add_rejects_persisted_credentials_before_writing(operator_core, capsys)  # doctest: +SKIP


    :param operator_core: Fake whose command-history length is checked around rejection.
    :param capsys: Capture for the option-policy usage diagnostic.
    :return: None; assert usage status, diagnostic text, and no added commands.
    """
    before = len(operator_core.commands)
    assert (
        cli_main(
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
        )
        == 2
    )
    assert len(operator_core.commands) == before
    assert "looks secret-bearing" in capsys.readouterr().err


def test_global_system_root_opens_a_real_initialized_core(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Initialize an empty SQLite system, add its first folder Store, and inspect it.

    Unlike recording-fake cases, use real local Core sessions and verify the
    added directory exists, the Store count changes, and doctor reports success.
    No remote service or external database is involved.

    Example:
        >>> test_global_system_root_opens_a_real_initialized_core(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Parent for the initialized system and subsequently created Store.
    :param capsys: Capture for initialization, health, storage, add, and doctor receipts.
    :return: None; assert real local initialization and Store visibility through the profile.
    """
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
    assert (
        cli_main(
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
        )
        == 0
    )
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
    """
    Expose the default folder Store created by real local system initialization.

    Check the single Store's identity, root, capabilities, availability,
    writability, and default designation in the operator status projection.

    Example:
        >>> test_real_initialized_folder_store_has_an_operator_status_overview(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for a SQLite system with its default local Store.
    :param capsys: Capture for initialization output and the storage status receipt.
    :return: None; assert healthy summary counts and the primary Store's projected facts.
    """
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
    """
    Persist a private active-system pointer, test selection precedence, and disconnect.

    Use a real initialized system. Explicit and environment roots override the
    saved pointer; disconnect removes it without removing the catalogue and
    reports a remaining environment selection. Also exercise an explicit profile
    and recovery from a malformed pointer before checking disconnected status.

    Example:
        >>> test_persistent_connect_selects_later_commands_and_disconnects_safely(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated configuration directory and local system roots.
    :param monkeypatch: Control XDG configuration and environment selector precedence.
    :param capsys: Capture for pointer paths, status projections, and usage diagnostics.
    :return: None; assert pointer-only persistence, precedence, and safe disconnection results.
    """
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
    assert explicit_selected["path"] == str(environment_root / "liuxin-system.json")
    monkeypatch.setenv("LIUXIN_SYSTEM_ROOT", str(environment_root))
    assert cli_main(["config", "path"]) == 0
    environment_selected = json.loads(capsys.readouterr().out)
    assert environment_selected["source"] == "LIUXIN_SYSTEM_ROOT"
    assert environment_selected["path"] == str(environment_root / "liuxin-system.json")
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

    assert (
        cli_main(
            [
                "connect",
                "--profile",
                str(root / "liuxin-system.json"),
                "--no-health-check",
            ]
        )
        == 0
    )
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
    """
    Aggregate canned Core diagnostics and remove the fixture password from output.

    Assert both the explicit password field and embedded URL credential are
    absent from serialized results. This is a known-fixture redaction contract,
    not proof that arbitrary strings or future credential forms are scrubbed.

    Example:
        >>> test_doctor_and_diagnostics_are_aggregated_and_redacted(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Recording fake supplying intentionally credential-bearing data.
    :param tmp_path: Location for the selected minimal SQLite fixture.
    :param capsys: Capture for doctor and diagnostics JSON projections.
    :return: None; assert aggregate shape, redaction markers, and service-query activity.
    """
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
    """
    Route typed Store/source setup and guarded integrity/recovery operations to Core.

    Verify selected payloads and require --yes for reconciliation, repair, and
    retry. Evacuation without --yes remains a query preview with no command.
    All operation outcomes are canned; no bytes are verified, repaired, moved,
    or recovered by this test.

    Example:
        >>> test_typed_storage_setup_integrity_and_reconcile_commands(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Fake recording queries and commands across the operation sequence.
    :param tmp_path: Synthetic catalogue, Store, and incoming-source path values.
    :param capsys: Capture for success receipts, previews, and confirmation errors.
    :return: None; assert routing, selected semantic payloads, and confirmation boundaries.
    """
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    store_root = tmp_path / "store"

    assert (
        cli_main(
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
        )
        == 0
    )
    saved = json.loads(capsys.readouterr().out)
    assert saved["store"]["store_name"] == "primary"
    assert operator_core.commands[-1][0] == "storage.default.set"

    assert (
        cli_main(
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
        )
        == 0
    )
    _ = capsys.readouterr()
    name, payload = operator_core.commands[-1]
    assert name == "storage.source.register"
    assert payload["options"]["compute_hash"] is False

    assert cli_main(["storage", "replica", "verify", *connection, "4"]) == 0
    assert json.loads(capsys.readouterr().out)["healthy"] is True
    assert (
        cli_main(["storage", "asset", "verify", *connection, "8", "--all-replicas"])
        == 0
    )
    _ = capsys.readouterr()
    assert cli_main(["storage", "audit", *connection, "--limit", "1"]) == 0
    _ = capsys.readouterr()
    assert cli_main(["storage", "reconcile", "apply", *connection]) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert cli_main(["storage", "reconcile", "apply", *connection, "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert (
        cli_main(
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
        )
        == 0
    )
    _ = capsys.readouterr()
    assert operator_core.commands[-1] == (
        "storage.store.update",
        {
            "store": "primary",
            "changes": {"read_only": True, "add_tags": ["offsite"]},
        },
    )

    assert cli_main(["storage", "repair", "plan", *connection, "--asset-id", "8"]) == 0
    assert json.loads(capsys.readouterr().out)["deletes_bytes"] is False
    assert cli_main(["storage", "repair", "apply", *connection]) == 2
    assert "requires --yes" in capsys.readouterr().err
    assert cli_main(["storage", "repair", "apply", *connection, "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    before = len(operator_core.commands)
    assert (
        cli_main(
            [
                "storage",
                "store",
                "evacuate",
                *connection,
                "primary",
                "--destination-store",
                "archive",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["blocked"] is False
    assert len(operator_core.commands) == before
    assert (
        cli_main(
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
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert (
        cli_main(["storage", "recovery", "list", *connection, "--state", "failed"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["state"] == "failed"
    operation_id = "12345678-1234-5678-9234-567812345678"
    assert (
        cli_main(["storage", "recovery", "retry-ingest", *connection, operation_id])
        == 2
    )
    assert "requires --yes" in capsys.readouterr().err
    assert (
        cli_main(
            [
                "storage",
                "recovery",
                "retry-ingest",
                *connection,
                operation_id,
                "--yes",
            ]
        )
        == 0
    )
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["operation"] == "storage.recovery.retry-ingest"


def test_custom_fields_are_semantic_and_deletion_previews(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Resolve a named custom field and preview deletion until confirmation is supplied.

    A create request is routed to Core, but subsequent results are fixed fake
    data. The preview must add no command; --yes dispatches the delete operation.

    Example:
        >>> test_custom_fields_are_semantic_and_deletion_previews(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Fake exposing field seven and recording semantic commands.
    :param tmp_path: Parent of the synthetic catalogue path passed to the CLI.
    :param capsys: Capture for field selection, create output, and delete preview.
    :return: None; assert field resolution, create routing, and preview-versus-delete behavior.
    """
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    assert cli_main(["catalog", "custom-fields", "show", *connection, "source"]) == 0
    assert json.loads(capsys.readouterr().out)["field"]["num"] == 7

    assert (
        cli_main(
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
        )
        == 0
    )
    _ = capsys.readouterr()
    assert operator_core.commands[-1][0] == "custom-fields.create"

    before = len(operator_core.commands)
    assert (
        cli_main(["catalog", "custom-fields", "delete", *connection, "--num", "7"]) == 0
    )
    preview = json.loads(capsys.readouterr().out)
    assert preview["preview"] is True
    assert len(operator_core.commands) == before
    assert (
        cli_main(
            ["catalog", "custom-fields", "delete", *connection, "--num", "7", "--yes"]
        )
        == 0
    )
    _ = capsys.readouterr()
    assert operator_core.commands[-1][0] == "custom-fields.delete"


def test_ingest_runs_list_show_issues_and_refuse_discovery_resume(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Inspect a real discovery run's persisted history and reject its operational resume.

    The .epub fixture contains arbitrary bytes; the discovery pass need not
    parse a valid book. History is read from generated logs/reports, and the
    empty issue projection must not turn discovery into a resumable ingest.

    Example:
        >>> test_ingest_runs_list_show_issues_and_refuse_discovery_resume(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Parent for the source directory and generated run artifacts.
    :param capsys: Capture for discovery, history-list/show/issues, and refusal output.
    :return: None; assert retained run identity, discovery mode, no issues, and resume refusal.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")
    logs = tmp_path / "logs"
    run_id = "12345678-1234-5678-9234-567812345678"
    assert (
        cli_main(
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
        )
        == 0
    )
    _ = capsys.readouterr()

    assert cli_main(["ingest", "runs", "list", "--log-directory", str(logs)]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["runs"][0]["run_id"] == run_id
    assert (
        cli_main(["ingest", "runs", "show", "--log-directory", str(logs), run_id]) == 0
    )
    shown = json.loads(capsys.readouterr().out)
    assert shown["report"]["mode"] == "discovery"
    assert (
        cli_main(["ingest", "runs", "issues", "--log-directory", str(logs), run_id])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["count"] == 0
    assert (
        cli_main(["ingest", "runs", "resume", "--log-directory", str(logs), run_id])
        == 2
    )
    assert "Only real ingest attempts" in capsys.readouterr().err


def test_ingest_run_resume_reconstructs_an_operational_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Reconstruct resume arguments from a real operational attempt and active profile.

    The first ingest may finish cleanly or with issues; both statuses are accepted.
    Patch only the subsequent resume dispatch to capture its namespace, so this
    verifies reconstruction rather than a second ingest or successful recovery.

    Example:
        >>> test_ingest_run_resume_reconstructs_an_operational_attempt(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated configuration, initialized system, and source file tree.
    :param monkeypatch: Control profile environment and capture the later resume dispatch.
    :param capsys: Capture used to drain first-run and profile command output.
    :return: None; assert retained identity, effective paths, and non-preview resume flags.
    """
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
        """
        Retain the reconstructed namespace by reference without executing ingestion.

        Example:
            >>> capture_resume(parsed_resume_args)  # doctest: +SKIP
            0


        :param args: Effective resume settings supplied by the history command.
        :return: Zero so the test can inspect a successful dispatch without a second run.
        """
        resumed.append(args)
        return 0

    monkeypatch.setattr(ingest_runs_cli, "cmd_storage_ingest", capture_resume)
    assert (
        cli_main(
            [
                "ingest",
                "runs",
                "resume",
                run_id,
                "--yes",
            ]
        )
        == 0
    )
    assert resumed
    resumed_args = resumed[0]
    assert str(resumed_args.run_id) == run_id
    assert resumed_args.source_root == str(source.resolve())
    assert resumed_args.database == str(system_root / "catalogue.sqlite")
    assert resumed_args.materialization_root == str(system_root / "ingest-materialized")
    assert resumed_args.discover_only is False
    assert resumed_args.preflight_only is False


def test_database_backup_verification_and_atomic_offline_restore(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Verify a SQLite backup and restore its schema through both command spellings.

    Unicode paths, matching alias digests, a safety-backup file, and replacement
    table contents are checked. Despite the test name, no crash or concurrent
    reader is injected to establish atomicity under interruption.

    Example:
        >>> test_database_backup_verification_and_atomic_offline_restore(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Parent of the independent source-backup and target SQLite files.
    :param capsys: Capture for database/backup verification and restore receipts.
    :return: None; assert digest parity, safety-copy existence, and restored schema visibility.
    """
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

    assert (
        cli_main(
            [
                "database",
                "restore",
                "--database",
                str(target),
                str(backup),
                "--yes",
            ]
        )
        == 0
    )
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
    assert (
        cli_main(
            [
                "backup",
                "restore",
                "--database",
                str(second_target),
                str(backup),
                "--yes",
            ]
        )
        == 0
    )
    alias_restored = json.loads(capsys.readouterr().out)
    assert alias_restored["ok"] is True


def test_migration_apply_previews_until_confirmed(
    operator_core: _OperatorCore,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Distinguish a migration preview receipt from the confirmed apply projection.

    The Core fake reports success without changing a schema. This case checks
    displayed preview/applied flags, not real database migration or rollback.

    Example:
        >>> test_migration_apply_previews_until_confirmed(operator_core, tmp_path, capsys)  # doctest: +SKIP


    :param operator_core: Fake serving migration-query and apply-command responses.
    :param tmp_path: Parent for the synthetic CLI catalogue target.
    :param capsys: Capture for the unconfirmed and confirmed migration receipts.
    :return: None; assert preview without --yes and applied=True with confirmation.
    """
    connection = ["--database", str(tmp_path / "catalogue.sqlite")]
    assert cli_main(["database", "migrations", "apply", *connection]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["preview"] is True
    assert cli_main(["database", "migrations", "apply", *connection, "--yes"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["applied"] is True
