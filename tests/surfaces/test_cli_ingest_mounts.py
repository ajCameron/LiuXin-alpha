"""Keep source mount opt-in, failure reporting, and saved-run replay explicit."""

from __future__ import annotations

import argparse
import json
import os
import signal
from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.surfaces.cli import ingest_runs
from LiuXin_alpha.surfaces.cli.storage_commands import ingest, ingest_mount
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_options import (
    add_storage_ingest_arguments,
)


def test_missing_mount_support_never_falls_back(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    """Do not start ordinary ingest when the requested Linux launcher is unavailable."""
    parser = argparse.ArgumentParser()
    add_storage_ingest_arguments(parser)
    args = parser.parse_args(
        ["--source-root", str(tmp_path), "--mount-source-read-only"]
    )
    monkeypatch.setattr(ingest_mount, "mount_namespace_id", lambda: 17)
    monkeypatch.setattr(ingest_mount.shutil, "which", lambda name: None)

    def forbidden(args):
        raise AssertionError("unprotected ingest started")

    monkeypatch.setattr(ingest, "_cmd_storage_ingest", forbidden)
    assert ingest.cmd_storage_ingest(args) == 2
    assert "unshare" in capsys.readouterr().err


@pytest.mark.parametrize(
    "options",
    [
        {"mount_source_read_only": True},
        {"source_image": "/archive/books.squashfs", "source_filesystem": "squashfs"},
        {"source_device": "/dev/source-partition", "source_filesystem": "ext4"},
    ],
)
def test_resume_retains_readonly_mount_request(tmp_path: Path, options) -> None:
    """A recorded mount request must establish fresh protection on resume."""
    event_log = tmp_path / "run.jsonl"
    event_log.write_text(
        json.dumps(
            {
                "context": {
                    "event": "cli_started",
                    "details": {
                        "mode": "ingest",
                        "source_root": str(tmp_path / "source"),
                        "database": str(tmp_path / "catalogue.sqlite"),
                        "arguments": options,
                    },
                },
            }
        )
        + "\n"
    )
    args = argparse.Namespace(run_id=str(uuid4()))
    resumed = ingest_runs._resume_namespace(args, {"event_log": str(event_log)})
    assert all(getattr(resumed, key) == value for key, value in options.items())


@pytest.mark.parametrize(
    "options",
    [
        ["--source-image", "books.img"],
        ["--source-filesystem", "ext4"],
    ],
)
def test_incomplete_attachment_options_never_start_ingest(
    monkeypatch, tmp_path: Path, options
) -> None:
    """Require a paired explicit source and filesystem without retrying ordinary ingest."""
    parser = argparse.ArgumentParser()
    add_storage_ingest_arguments(parser)
    args = parser.parse_args(["--source-root", str(tmp_path), *options])

    def forbidden(args):
        pytest.fail("ordinary ingest started")

    monkeypatch.setattr(ingest, "_cmd_storage_ingest", forbidden)
    assert ingest.cmd_storage_ingest(args) == 2


@pytest.mark.parametrize("alias", ["same", "symlink", "hardlink", "sqlite-sidecar"])
def test_backing_output_collision_fails_before_logging(tmp_path: Path, alias) -> None:
    """A report or catalogue sidecar must never overwrite the attached image."""
    from LiuXin_alpha.surfaces.cli.storage_commands.constants import CLIUsageError
    from LiuXin_alpha.surfaces.cli.storage_commands.ingest_paths import (
        _validate_backing_destinations,
    )

    source = tmp_path / "source.img"
    source.write_bytes(b"source bytes")
    output = source
    if alias in {"symlink", "hardlink"}:
        output = tmp_path / "output"
        if alias == "symlink":
            output.symlink_to(source)
        else:
            os.link(source, output)
    database = tmp_path / "catalogue"
    if alias == "sqlite-sidecar":
        os.link(source, str(database) + "-wal")
        output = tmp_path / "report.json"
    args = argparse.Namespace(
        source_image=str(source), database=str(database), report_file=str(output)
    )
    with pytest.raises(CLIUsageError, match="backing source"):
        _validate_backing_destinations(args, tmp_path / "logs")
    assert source.read_bytes() == b"source bytes"


def test_launcher_forwards_signals_and_preserves_exit_status(monkeypatch) -> None:
    """Relay cooperative cancellation to the child and restore the parent's handlers."""
    handlers = {signal.SIGINT: signal.SIG_DFL, signal.SIGTERM: signal.SIG_DFL}
    sent = []
    monkeypatch.setattr(signal, "getsignal", lambda number: handlers[number])
    monkeypatch.setattr(
        signal, "signal", lambda number, handler: handlers.__setitem__(number, handler)
    )

    class Process:
        returncode = None

        def poll(self):
            return self.returncode

        def send_signal(self, number):
            sent.append(number)

        def communicate(self, payload):
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            self.returncode = 143

    assert ingest_mount._wait_worker(Process(), b"{}") == 143
    assert sent == [signal.SIGTERM]
    assert handlers == {signal.SIGINT: signal.SIG_DFL, signal.SIGTERM: signal.SIG_DFL}
