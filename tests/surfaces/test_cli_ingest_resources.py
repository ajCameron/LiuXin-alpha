"""Exercise resource stops and resume through the real SQLite local-ingest CLI."""

from __future__ import annotations

import json
import shutil
import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.ingest import resource_monitor as resources
from LiuXin_alpha.ingest.resource_monitor import ResourceMonitor
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog


def arguments(tmp_path: Path) -> list[str]:
    """Use real isolated paths and an explicit host volume, independent of WSL interop."""
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
        "--no-console-progress",
    ]


def events(payload):
    rows = [
        json.loads(line)["context"]
        for line in Path(payload["event_log"]).read_text().splitlines()
    ]
    return [row for row in rows if "event" in row]


@pytest.mark.parametrize("preview", [[], ["--preflight-only"]])
def test_initial_low_space_refuses_catalogue_and_cache(
    tmp_path, monkeypatch, capsys, preview
):
    (tmp_path / "source").mkdir()
    monkeypatch.setattr(
        resources.shutil, "disk_usage", lambda path: SimpleNamespace(free=10, total=100)
    )
    assert cli_main([*arguments(tmp_path), *preview]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "resource_stopped"
    assert not (tmp_path / "catalogue.sqlite").exists()
    assert not (tmp_path / "cache").exists()
    assert payload["resources"]["stop_reason"].startswith("low space")
    roles = {disk["role"] for disk in payload["resources"]["latest"]["disks"]}
    assert {"catalogue", "cache", "temporary", "logs", "report", "host_0"} <= roles
    if not preview:
        assert "lock" in roles
    assert Path(payload["report_file"]).exists()
    assert events(payload)[0]["event"] == "cli_started"
    if preview:
        assert payload["preflight"]["ready"] is False
    else:
        assert (
            cli_main(
                ["ingest", "runs", "list", "--log-directory", str(tmp_path / "logs")]
            )
            == 0
        )
        assert payload["run_id"] in capsys.readouterr().out


@pytest.mark.parametrize(
    "format_name,log_level", [("zip", "INFO"), ("zip", "ERROR"), ("squashfs", "INFO")]
)
def test_mid_member_stop_and_exact_resume_deduplicate(
    tmp_path, monkeypatch, capsys, format_name, log_level
):
    """Persist partial adoption, stop before another member, then resume the same durable run."""
    source = tmp_path / "source"
    source.mkdir()
    archive_path = source / ("books." + format_name)
    if format_name == "zip":
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("a.epub", b"first")
            archive.writestr("b.pdf", b"second")
    else:
        if not shutil.which("mksquashfs") or not shutil.which("unsquashfs"):
            pytest.skip("squashfs-tools is required for the real SquashFS round trip")
        members = tmp_path / "members"
        members.mkdir()
        (members / "a.epub").write_bytes(b"first")
        (members / "b.pdf").write_bytes(b"second")
        subprocess.run(
            [
                "mksquashfs",
                str(members),
                str(archive_path),
                "-noappend",
                "-no-progress",
                "-processors",
                "1",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
    original_bytes = archive_path.read_bytes()
    pressured = []
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(
            free=10 if pressured else 20 * 1024**3, total=40 * 1024**3
        ),
    )
    original_progress = ResourceMonitor.progress

    def progress(self, event, details):
        original_progress(self, event, details)
        if event == "member_adopted":
            pressured.append(True)

    monkeypatch.setattr(ResourceMonitor, "progress", progress)
    assert cli_main([*arguments(tmp_path), "--log-level", log_level]) == 1
    stopped = json.loads(capsys.readouterr().out)
    assert stopped["status"] == "resource_stopped"
    assert stopped["report"]["members_adopted"] == 1
    assert stopped["report"]["halt_reason"].startswith("low space")
    assert any(
        issue["stage"] == "resource_limit" for issue in stopped["report"]["issues"]
    )
    assert any(event["event"] == "resource_stop" for event in events(stopped))
    assert not any(
        event["event"] == "member_adopted" for event in events(stopped)
    )  # INFO default
    pressured.clear()
    monkeypatch.setattr(ResourceMonitor, "progress", original_progress)
    assert (
        cli_main(
            [
                "ingest",
                "runs",
                "resume",
                stopped["run_id"],
                "--log-directory",
                str(tmp_path / "logs"),
            ]
        )
        == 0
    )
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["run_id"] == stopped["run_id"]
    assert resumed["report"]["members_adopted"] == 2
    assert resumed["report"]["assets_created"] == 1
    assert resumed["resources"]["min_free_bytes"] == 1024**2
    assert resumed["resources"]["interval_seconds"] == 0.1
    assert archive_path.read_bytes() == original_bytes


def test_member_spool_admission_prevents_first_read(tmp_path, monkeypatch, capsys):
    """A file can exceed remaining headroom even before the filesystem hits the reserve."""
    source = tmp_path / "source"
    source.mkdir()
    with zipfile.ZipFile(source / "books.zip", "w") as archive:
        archive.writestr("novel.epub", b"x" * 1024)
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=1024**2 + 512, total=10 * 1024**2),
    )
    assert cli_main(arguments(tmp_path)) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["report"]["files_adopted"] == 1
    assert payload["report"]["members_adopted"] == 0
    assert payload["status"] == "resource_stopped"
    snapshots = [
        event["details"]
        for event in events(payload)
        if event["event"] == "resource_stop"
    ]
    assert any(
        snapshot["activity"].get("member_path") == "novel.epub"
        for snapshot in snapshots
    )


def test_telemetry_persistence_failure_stops_before_catalogue(
    tmp_path, monkeypatch, capsys
):
    """Python logging normally swallows handler errors; required run telemetry must notice them."""
    (tmp_path / "source").mkdir()
    original = InMemoryEventLog.put_event

    def put_event(self, *args, **kwargs):
        if kwargs.get("context", {}).get("event") == "resource_sample":
            raise OSError("telemetry volume disappeared")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(InMemoryEventLog, "put_event", put_event)
    assert cli_main(arguments(tmp_path)) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "resource_stopped"
    assert "cannot persist resource telemetry" in payload["resources"]["stop_reason"]
    assert not (tmp_path / "catalogue.sqlite").exists()


def test_space_drop_after_initial_check_keeps_preflight_identity(
    tmp_path, monkeypatch, capsys
):
    """A changed filesystem between checks must not turn a preview into an ingest receipt."""
    (tmp_path / "source").mkdir()
    low = []
    calls = []
    original = ResourceMonitor.sample

    def sample(self, **kwargs):
        calls.append(True)
        if len(calls) >= 2:
            low.append(True)
        return original(self, **kwargs)

    monkeypatch.setattr(ResourceMonitor, "sample", sample)
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(
            free=1 if low else 10 * 1024**3, total=20 * 1024**3
        ),
    )
    assert cli_main([*arguments(tmp_path), "--preflight-only"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "preflight"
    assert payload["preflight"]["ready"] is False
    assert not (tmp_path / "catalogue.sqlite").exists()
