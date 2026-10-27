"""Verify real CLI receipts stay separate from foreground/attached status output."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.surfaces.cli.app import main
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_run


@pytest.fixture(autouse=True)
def interactive_terminal(monkeypatch):
    """Exercise real cursor restoration even when the test runner uses TERM=dumb."""
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("TTY_INTERACTIVE", "1")


def arguments(tmp_path):
    return [
        "storage",
        "ingest",
        "--source-root",
        str(tmp_path / "source"),
        "--database",
        str(tmp_path / "catalogue.sqlite"),
        "--materialization-root",
        str(tmp_path / "cache"),
        "--log-directory",
        str(tmp_path / "logs"),
        "--host-space-path",
        str(tmp_path),
        "--min-free-mib",
        "1",
        "--resource-interval-seconds",
        "0.1",
        "--log-level",
        "ERROR",
    ]


@pytest.mark.parametrize("mode", ["auto", "plain", "rich", "off"])
def test_real_import_and_completed_watcher_keep_json_stdout_and_source_bytes(
    tmp_path, capsys, monkeypatch, mode
):
    source = tmp_path / "source"
    source.mkdir()
    archive = source / "books.zip"
    with zipfile.ZipFile(archive, "w") as stream:
        stream.writestr("introduction.txt", b"introduction")
        stream.writestr("novel.txt", b"novel")
    original = archive.read_bytes()
    options = (
        ["--progress", mode]
        if mode != "off"
        else ["--progress", "rich", "--no-console-progress"]
    )
    assert main([*arguments(tmp_path), *options]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "complete"
    counts = payload["resources"]["latest"]["counts"]
    assert counts["files_adopted"] == 1
    assert counts["members_adopted"] == 2
    assert counts["containers_processed"] == 1
    assert ("LiuXin import" in captured.err) == (mode == "rich")
    assert ("\x1b[?25h" in captured.err) == (mode == "rich")
    assert archive.read_bytes() == original

    def forbidden(*args, **kwargs):
        raise AssertionError("Watching must not open a catalogue")

    monkeypatch.setattr(
        "LiuXin_alpha.core.library_services.LibraryServices.open", forbidden
    )
    assert (
        main(
            [
                "ingest",
                "runs",
                "watch",
                payload["run_id"],
                "--log-directory",
                str(tmp_path / "logs"),
                "--progress",
                "plain",
            ]
        )
        == 0
    )
    watched = capsys.readouterr()
    assert watched.out == ""
    assert "Status: complete" in watched.err
    assert "Members: 2 adopted" in watched.err
    assert archive.read_bytes() == original


@pytest.mark.parametrize(
    "error,status,code",
    [
        (RuntimeError("reader failed"), "failed", 1),
        (KeyboardInterrupt(), "interrupted", 130),
    ],
)
def test_rich_restores_terminal_on_failure_and_interrupt(
    tmp_path, capsys, monkeypatch, error, status, code
):
    (tmp_path / "source").mkdir()

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(ingest_run, "execute_mixed_ingest", fail)
    assert main([*arguments(tmp_path), "--discover-only", "--progress", "rich"]) == code
    captured = capsys.readouterr()
    assert json.loads(captured.out)["status"] == status
    assert status in captured.err
    assert "\x1b[?25h" in captured.err


def test_watch_invalid_uuid_and_refresh_are_usage_errors(tmp_path, capsys):
    assert (
        main(
            [
                "ingest",
                "runs",
                "watch",
                "bad",
                "--log-directory",
                str(tmp_path),
                "--once",
            ]
        )
        == 2
    )
    assert "RUN_ID" in capsys.readouterr().err


def test_rich_resource_stop_and_no_stdout_report(tmp_path, capsys, monkeypatch):
    (tmp_path / "source").mkdir()
    monkeypatch.setattr(
        "LiuXin_alpha.ingest.resource_monitor.shutil.disk_usage",
        lambda path: SimpleNamespace(free=1, total=100),
    )
    assert main([*arguments(tmp_path), "--progress", "rich", "--no-stdout-report"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "resource_stopped" in captured.err
    assert "low space" in captured.err
    assert "\x1b[?25h" in captured.err
    assert not (tmp_path / "catalogue.sqlite").exists()


def test_watch_selects_newest_attempt_for_resumed_run(tmp_path, capsys, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Watching must not load historical reports")

    monkeypatch.setattr("LiuXin_alpha.surfaces.cli.ingest_runs._load_report", forbidden)
    run = str(uuid4())
    for label, count in (("old", 3), ("new", 27)):
        path = tmp_path / f"mixed-ingest-{label}-{run}.jsonl"
        path.write_text(
            json.dumps(
                {
                    "context": {
                        "event": "resource_sample",
                        "details": {
                            "run_id": run,
                            "counts": {"members_adopted": count},
                        },
                    }
                }
            )
            + "\n"
        )
        if label == "old":
            os.utime(path, (1, 1))
    assert (
        main(
            [
                "ingest",
                "runs",
                "watch",
                run,
                "--log-directory",
                str(tmp_path),
                "--once",
                "--progress",
                "plain",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    assert output.out == ""
    assert "Members: 27 adopted" in output.err


def test_cli_help_does_not_import_rich():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from LiuXin_alpha.surfaces.cli.app import main\n"
            "import sys\n"
            "try: main(['ingest', 'runs', 'watch', '--help'])\n"
            "except SystemExit as error: assert error.code == 0\n"
            "assert 'rich' not in sys.modules\n",
        ],
        capture_output=True,
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
