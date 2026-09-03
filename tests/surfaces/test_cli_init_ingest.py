"""First-run ``init`` and concise local ``ingest`` contracts."""

from __future__ import annotations

import json
import stat

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.surfaces.cli import initialize as init_cli
from LiuXin_alpha.surfaces.cli import storage as storage_cli
from LiuXin_alpha.surfaces.cli.app import main as cli_main


class _Core:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def query(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        if name == "health":
            return {
                "core_uuid": "core-1",
                "core_version": "2.0.0",
                "api_version": "2.0",
                "shutdown": False,
            }
        if name == "storage.stores.list":
            return {"stores": [], "count": 1}
        raise AssertionError(name)

    def command(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        values = dict(payload)
        self.commands.append((name, values))
        if name == "storage.store.save":
            return {"store": values["store"]}
        if name == "storage.refresh":
            return {"refreshed": True, "report": {"loaded_stores": 1, "issues": []}}
        if name == "storage.default.set":
            return {
                "selected": True,
                "store_uuid": "store-1",
                "store_name": values["store"],
            }
        raise AssertionError(name)


@contextmanager
def _open_fake(args: Any, **_kwargs: Any):
    Path(args.database).touch(exist_ok=True)
    yield args._test_core


def _answers(*values: str):
    remaining = iter(values)

    def answer(_prompt: str) -> str:
        return next(remaining)

    return answer


def test_init_creates_idempotent_system_layout_and_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    core = _Core()

    def opener(args: Any, **kwargs: Any):
        args._test_core = core
        return _open_fake(args, **kwargs)

    monkeypatch.setattr(init_cli, "open_cli_core", opener)
    root = tmp_path / "system"

    assert cli_main(["init", str(root)]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["ok"] is True
    assert result["database_created"] is True
    assert result["store"] == {
        "kind": "filesystem",
        "name": "primary",
        "root": str(root / "store"),
        "saved": True,
    }
    assert (root / "catalogue.sqlite").is_file()
    assert (root / "store").is_dir()
    assert (root / "ingest-materialized").is_dir()
    assert (root / "logs" / "ingest").is_dir()
    manifest_path = root / init_cli.SYSTEM_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["format"] == init_cli.SYSTEM_MANIFEST_FORMAT
    assert manifest["database"] == str(root / "catalogue.sqlite")
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600
    assert [name for name, _payload in core.commands] == [
        "storage.store.save",
        "storage.refresh",
        "storage.default.set",
    ]
    assert result["next"]["ingest"] == [
        "liuxin",
        "ingest",
        "/path/to/source",
        "--system-root",
        str(root),
    ]
    assert result["next"]["connect"] == ["liuxin", "connect", str(root)]


def test_init_wizard_can_choose_apsw_system_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    core = _Core()

    def opener(args: Any, **kwargs: Any):
        args._test_core = core
        return _open_fake(args, **kwargs)

    root = tmp_path / "guided-system"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "open_cli_core", opener)
    monkeypatch.setattr(
        "builtins.input",
        _answers("2", str(root), "yes", "yes"),
    )

    # An attended bare `liuxin init` enters the same wizard automatically.
    assert cli_main(["init", "--compact"]) == 0

    output = capsys.readouterr().out
    result = json.loads(output.splitlines()[-1])
    assert "Initialization plan" in output
    assert result["db_type"] == "APSW"
    assert result["database"] == str(root / "catalogue.sqlite")
    assert result["default_store"]["selected"] is True
    assert (root / init_cli.SYSTEM_MANIFEST_NAME).is_file()


def test_init_wizard_postgres_initializes_checks_and_redacts_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    received: dict[str, Any] = {}

    def fake_postgres_init(args: Any) -> int:
        received.update(vars(args))
        print("PostgreSQL readiness checks passed")
        return 0

    environment_file = tmp_path / "liuxin-postgres.env"
    system_root = tmp_path / "liuxin-postgres-system"
    target = "postgresql://owner:very-secret@example.invalid/liuxin"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "postgres_driver_is_available", lambda: True)
    monkeypatch.setattr(init_cli, "cmd_postgres_init", fake_postgres_init)
    monkeypatch.setattr(
        "builtins.input",
        _answers(
            "3",
            "1",
            target,
            "liuxin_catalogue",
            str(system_root),
            "yes",
            str(environment_file),
            "yes",
        ),
    )

    assert cli_main(["init", "--wizard"]) == 0

    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert "PostgreSQL initialization plan" in captured.out
    assert "PostgreSQL readiness checks passed" in captured.out
    assert "very-secret" not in combined_output
    assert "owner:***@example.invalid" in captured.out
    assert received["url"] == target
    assert received["service"] is None
    assert received["schema"] == "liuxin_catalogue"
    assert received["system_root"] == str(system_root)
    assert received["check"] is True
    assert received["no_password_prompt"] is False
    assert environment_file.is_file()
    assert stat.S_IMODE(environment_file.stat().st_mode) == 0o600
    assert "LIUXIN_POSTGRES_PASSWORD" not in environment_file.read_text(
        encoding="utf-8"
    )


def test_init_wizard_hints_when_postgres_support_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "postgres_driver_is_available", lambda: False)
    monkeypatch.setattr("builtins.input", _answers("3"))

    assert cli_main(["init", "--wizard"]) == 2

    captured = capsys.readouterr()
    assert ".[postgres]" in captured.err
    assert "PostgreSQL Python support" in captured.err


def test_init_wizard_cancellation_makes_no_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "cancelled-system"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(
        "builtins.input",
        _answers("1", str(root), "yes", "no"),
    )

    assert cli_main(["init", "--wizard"]) == 1
    assert not root.exists()
    assert "cancelled" in capsys.readouterr().out.casefold()


def test_init_wizard_requires_an_interactive_terminal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: False)

    assert cli_main(["init", "--wizard"]) == 2
    assert "interactive terminal" in capsys.readouterr().err


def test_system_manifest_populates_safe_mixed_ingest_defaults(tmp_path: Path) -> None:
    root = tmp_path / "system"
    root.mkdir()
    database = root / "catalogue.sqlite"
    database.touch()
    manifest = {
        "format": "liuxin.system",
        "version": 1,
        "database": str(database),
        "db_type": "SQLite",
        "materialization_root": str(root / "materialized"),
        "log_directory": str(root / "logs"),
    }
    (root / init_cli.SYSTEM_MANIFEST_NAME).write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    args = type(
        "Args",
        (),
        {
            "system_root": str(root),
            "database": None,
            "materialization_root": None,
            "log_directory": None,
            "require_existing_database": False,
        },
    )()

    storage_cli._apply_system_root_defaults(args)

    assert args.database == str(database)
    assert args.materialization_root == str(root / "materialized")
    assert args.log_directory == str(root / "logs")
    assert args.require_existing_database is True


def test_concise_ingest_source_expands_to_mixed_ingest_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, Any] = {}

    def handler(args: Any) -> int:
        received.update(vars(args))
        return 0

    monkeypatch.setattr(storage_cli, "cmd_storage_ingest", handler)
    source = tmp_path / "mess"
    root = tmp_path / "system"

    assert (
        cli_main(
            ["ingest", str(source), "--system-root", str(root), "--strict"]
        )
        == 0
    )
    assert received["source_root"] == str(source)
    assert received["system_root"] == str(root)
    assert received["strict"] is True
    assert received["storage_command"] == "ingest"

    received.clear()
    assert (
        cli_main(
            [
                "ingest",
                "--system-root",
                str(root),
                "--source",
                str(source),
            ]
        )
        == 0
    )
    assert received["source_root"] == str(source)
    assert received["system_root"] == str(root)


def test_init_rejects_database_inside_managed_store(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store = tmp_path / "store"
    rc = cli_main(
        [
            "init",
            "--database",
            str(store / "catalogue.sqlite"),
            "--store-root",
            str(store),
        ]
    )
    assert rc == 2
    assert "database must not be inside" in capsys.readouterr().err


def test_real_init_then_concise_ingest_round_trip(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "system"
    source = tmp_path / "incoming"
    source.mkdir()
    (source / "book.epub").write_bytes(b"first-run-book")

    assert cli_main(["init", str(root), "--compact"]) == 0
    initialized = json.loads(capsys.readouterr().out)
    assert initialized["database_created"] is True
    assert initialized["store_count"] == 1
    assert initialized["default_store"]["selected"] is True

    assert (
        cli_main(
            [
                "ingest",
                str(source),
                "--system-root",
                str(root),
                "--no-nested-containers",
                "--no-console-progress",
                "--compact-json",
            ]
        )
        == 0
    )
    ingested = json.loads(capsys.readouterr().out)
    assert ingested["mode"] == "ingest"
    assert ingested["ok"] is True
    assert ingested["report"]["files_examined"] == 1
    assert ingested["report"]["files_adopted"] == 1
    assert ingested["report"]["assets_created"] == 1
    assert Path(ingested["report_file"]).is_file()
    assert Path(ingested["event_log"]).is_file()

    assert cli_main(["init", str(root), "--compact"]) == 0
    reopened = json.loads(capsys.readouterr().out)
    assert reopened["database_created"] is False
    assert reopened["store_count"] >= 2
    assert reopened["default_store"]["selected"] is True
