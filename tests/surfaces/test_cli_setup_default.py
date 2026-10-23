"""Verify setup saves the existing per-user selector only after successful initialization."""

from __future__ import annotations

import json
import stat
from contextlib import contextmanager
from pathlib import Path

import pytest

from LiuXin_alpha.core.settings import active_connection_path, persist_manifest_path
from LiuXin_alpha.surfaces.cli import initialize
from LiuXin_alpha.surfaces.cli.app import main
from tests.surfaces.test_cli_init_ingest import _answers, _Core, _open_fake


@pytest.fixture(autouse=True)
def isolated_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("LIUXIN_SYSTEM_ROOT", raising=False)
    monkeypatch.delenv("LIUXIN_PROFILE", raising=False)


@pytest.fixture
def fake_core(monkeypatch):
    core = _Core()

    def open_core(args, **kwargs):
        args._test_core = core
        return _open_fake(args, **kwargs)

    monkeypatch.setattr(initialize, "open_cli_core", open_core)
    return core


def manifest(root, *, database=None, db_type="SQLite"):
    root.mkdir(parents=True, exist_ok=True)
    path = root / "liuxin-system.json"
    path.write_text(
        json.dumps(
            {
                "format": "liuxin.system",
                "version": 1,
                "system_root": str(root),
                "database": str(database or root / "catalogue.sqlite"),
                "db_type": db_type,
            }
        )
    )
    return path


def old_default(tmp_path):
    persist_manifest_path(manifest(tmp_path / "old"))
    return active_connection_path().read_bytes()


def test_real_setup_default_is_used_by_core_ingest_and_watcher(tmp_path, capsys):
    root = tmp_path / "system"
    assert main(["init", str(root), "--make-default", "--no-store"]) == 0
    receipt = json.loads(capsys.readouterr().out)
    pointer = Path(receipt["default_connection"]["connection_file"])
    assert stat.S_IMODE(pointer.stat().st_mode) == 0o600
    assert set(json.loads(pointer.read_text())) == {"format", "version", "manifest"}
    assert receipt["default_connection"]["effective_now"] is True
    assert main(["core", "health"]) == 0
    assert json.loads(capsys.readouterr().out)["shutdown"] is False
    assert main(["config", "path"]) == 0
    assert json.loads(capsys.readouterr().out)["path"] == str(
        root / "liuxin-system.json"
    )

    source = tmp_path / "books"
    source.mkdir()
    (source / "book.epub").write_bytes(b"catalogue this source")
    assert (
        main(
            [
                "storage",
                "ingest",
                "--source-root",
                str(source),
                "--host-space-path",
                str(tmp_path),
                "--min-free-mib",
                "1",
                "--no-console-progress",
            ]
        )
        == 0
    )
    imported = json.loads(capsys.readouterr().out)
    assert imported["database"] == str(root / "catalogue.sqlite")
    assert (
        main(
            [
                "ingest",
                "runs",
                "watch",
                imported["run_id"],
                "--once",
                "--progress",
                "plain",
            ]
        )
        == 0
    )
    assert "Status: complete" in capsys.readouterr().err
    assert main(["disconnect"]) == 0
    capsys.readouterr()
    assert not pointer.exists()
    assert (root / "catalogue.sqlite").exists()


@pytest.mark.parametrize("answer", ["no", "cancel", "eof"])
def test_declined_or_cancelled_wizard_preserves_previous_default(
    tmp_path, monkeypatch, capsys, fake_core, answer
):
    before = old_default(tmp_path)
    root = tmp_path / "new"
    monkeypatch.setattr(initialize, "_stdin_is_interactive", lambda: True)
    answers = iter(["1", str(root), "no", "no" if answer == "no" else "yes"])

    def reply(prompt):
        try:
            return next(answers)
        except StopIteration:
            if answer == "eof":
                raise EOFError from None
            return "yes" if answer == "no" else "no"

    monkeypatch.setattr("builtins.input", reply)
    assert main(["init"]) == (0 if answer == "no" else 1)
    assert active_connection_path().read_bytes() == before
    assert root.exists() == (answer == "no")


@pytest.mark.parametrize(
    "options", [["--no-manifest"], ["--database", "catalogue.sqlite"]]
)
def test_make_default_requires_manifest_before_any_writes(tmp_path, capsys, options):
    root = tmp_path / "not-created"
    arguments = options if "--database" in options else [str(root), *options]
    assert main(["init", *arguments, "--make-default"]) == 2
    assert "requires a system manifest" in capsys.readouterr().err
    assert not root.exists() and not active_connection_path().exists()


@pytest.mark.parametrize("failure", ["open", "health", "publish", "persist"])
def test_failed_setup_does_not_replace_default(
    tmp_path, monkeypatch, capsys, fake_core, failure
):
    before = old_default(tmp_path)

    def fail(*args, **kwargs):
        raise OSError("simulated failure")

    @contextmanager
    def failed_open(*args, **kwargs):
        raise OSError("simulated open failure")
        yield

    if failure == "open":
        monkeypatch.setattr(initialize, "open_cli_core", failed_open)
    elif failure == "health":
        original = fake_core.query
        monkeypatch.setattr(
            fake_core,
            "query",
            lambda name, payload: (
                {"ok": False} if name == "health" else original(name, payload)
            ),
        )
    elif failure == "publish":
        monkeypatch.setattr(initialize, "emit_bytes", fail)
    else:
        monkeypatch.setattr(initialize, "persist_manifest_path", fail)
    assert main(["init", str(tmp_path / "new"), "--make-default", "--no-store"]) == 2
    assert active_connection_path().read_bytes() == before


def test_environment_override_is_reported_and_explicit_selection_still_wins(
    tmp_path, monkeypatch, capsys, fake_core
):
    root = tmp_path / "saved"
    override = tmp_path / "environment"
    manifest(override)
    monkeypatch.setenv("LIUXIN_SYSTEM_ROOT", str(override))
    assert main(["init", str(root), "--make-default", "--no-store"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["default_connection"]["effective_now"] is False
    assert "overrides this saved default" in output.err
    assert main(["config", "path"]) == 0
    assert json.loads(capsys.readouterr().out)["path"] == str(
        override / "liuxin-system.json"
    )
    assert main(["config", "path", "--system-root", str(root)]) == 0
    assert json.loads(capsys.readouterr().out)["path"] == str(
        root / "liuxin-system.json"
    )


def test_setup_output_cannot_overwrite_connection_pointer(tmp_path, capsys):
    before = old_default(tmp_path)
    root = tmp_path / "new"
    assert (
        main(
            [
                "init",
                str(root),
                "--make-default",
                "--output",
                str(active_connection_path()),
                "--replace-output",
            ]
        )
        == 2
    )
    assert "saved-connection file" in capsys.readouterr().err
    assert active_connection_path().read_bytes() == before
    assert not root.exists()


@pytest.mark.parametrize("code", [0, 2])
def test_postgres_wizard_saves_only_after_success(tmp_path, monkeypatch, capsys, code):
    before = old_default(tmp_path)
    root = tmp_path / "postgres"
    monkeypatch.setattr(initialize, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(initialize, "postgres_driver_is_available", lambda: True)

    def initialize_postgres(args):
        if code == 0:
            manifest(
                root, database="postgresql://localhost/library", db_type="PostgreSQL"
            )
        return code

    monkeypatch.setattr(initialize, "cmd_postgres_init", initialize_postgres)
    monkeypatch.setattr(
        "builtins.input",
        _answers(
            "3",
            "1",
            "postgresql://localhost/library",
            "public",
            str(root),
            "no",
            "",
            "yes",
        ),
    )
    assert main(["init", "--wizard"]) == code
    if code:
        assert active_connection_path().read_bytes() == before
    else:
        assert json.loads(active_connection_path().read_text())["manifest"] == str(
            root / "liuxin-system.json"
        )


def test_postgres_environment_output_cannot_replace_default(
    tmp_path, monkeypatch, capsys
):
    before = old_default(tmp_path)
    root = tmp_path / "postgres"
    monkeypatch.setattr(initialize, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(initialize, "postgres_driver_is_available", lambda: True)
    monkeypatch.setattr(
        initialize,
        "cmd_postgres_init",
        lambda args: pytest.fail("Must reject the output before initialization"),
    )
    monkeypatch.setattr(
        "builtins.input",
        _answers(
            "3",
            "1",
            "postgresql://localhost/library",
            "public",
            str(root),
            "yes",
            str(active_connection_path()),
            "yes",
            "yes",
        ),
    )
    assert main(["init", "--wizard"]) == 2
    assert "saved-connection file" in capsys.readouterr().err
    assert active_connection_path().read_bytes() == before
    assert not root.exists()
