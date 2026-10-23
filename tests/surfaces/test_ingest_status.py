"""Exercise bounded log following and terminal rendering against durable event files."""

from __future__ import annotations

import argparse
import io
import json
import time
from datetime import UTC, datetime
from uuid import UUID

import pytest
from rich.console import Console

from LiuXin_alpha.surfaces.cli import ingest_watch
from LiuXin_alpha.surfaces.cli.ingest_display import render_panel, rich_enabled
from LiuXin_alpha.surfaces.cli.ingest_status import (
    MAX_LINE,
    READ_WINDOW,
    EventTail,
    IngestStatus,
)

RUN = str(UUID(int=42))


def event(name, **details):
    return (
        json.dumps(
            {
                "ts": datetime.now(UTC).isoformat(),
                "message": "An issue",
                "context": {"event": name, "details": {"run_id": RUN, **details}},
            },
            ensure_ascii=False,
        )
        + "\n"
    ).encode()


def test_partial_unicode_record_and_append_are_read_once(tmp_path):
    path = tmp_path / "run.jsonl"
    data = event("resource_sample", counts={"members_adopted": 7}, source_root="café")
    cut = data.index("é".encode()) + 1
    path.write_bytes(data[:cut])
    tail = EventTail(path, RUN)
    assert not tail.poll().resources
    with path.open("ab") as stream:
        stream.write(data[cut:])
    state = tail.poll()
    assert state.counts["members_adopted"] == 7
    assert state.source == "café"
    assert tail.poll() is state
    assert tail.offset == len(data)
    assert tail.pending == b""


def test_large_logs_and_oversized_records_have_bounded_buffers(tmp_path):
    path = tmp_path / "run.jsonl"
    path.write_bytes(
        b"x" * (READ_WINDOW * 3)
        + b"\n"
        + event("resource_sample", counts={"members_adopted": 999})
    )
    tail = EventTail(path, RUN)
    assert tail.poll().counts["members_adopted"] == 999
    assert "omitted" in tail.state.notice
    with path.open("ab") as stream:
        stream.write(b"x" * (MAX_LINE + 1))
    tail.poll()
    assert len(tail.pending) <= MAX_LINE
    with path.open("ab") as stream:
        stream.write(b"\nnot-json\n" + event("cli_complete", ok=True))
    assert tail.poll().done
    assert tail.state.status == "complete"


@pytest.mark.parametrize("replace", [False, True])
def test_truncation_and_replacement_reset_previous_terminal_state(tmp_path, replace):
    path = tmp_path / "run.jsonl"
    path.write_bytes(event("cli_complete", ok=True, padding="x" * 200))
    tail = EventTail(path, RUN)
    assert tail.poll().done
    if replace:
        replacement = tmp_path / "replacement"
        replacement.write_bytes(event("cli_started", source_root="new source"))
        replacement.replace(path)
    else:
        path.write_bytes(event("cli_started", source_root="new source"))
    assert not tail.poll().done
    assert tail.state.source == "new source"


def test_stale_sample_does_not_claim_failure_or_completion():
    state = IngestStatus(RUN)
    assert "unknown" in state.freshness(time.time())
    state.consume(json.loads(event("resource_sample")))
    assert "STALE" in state.freshness(time.time() + 100)
    assert not state.done
    assert state.status == "running"


@pytest.mark.parametrize(
    "name,status,details",
    [
        ("cli_complete", "complete", {"ok": True}),
        ("cli_complete", "issues", {"ok": False}),
        (
            "cli_complete",
            "resource_stopped",
            {"ok": False, "status": "resource_stopped"},
        ),
        ("cli_cancelled", "cancelled", {}),
        ("cli_failed", "failed", {"error_message": "catalogue error"}),
        ("cli_interrupted", "interrupted", {}),
        ("cli_configuration_error", "configuration_error", {}),
    ],
)
def test_terminal_events_are_authoritative(name, status, details):
    state = IngestStatus(RUN)
    if status == "resource_stopped":
        state.consume(json.loads(event("resource_stop", stop_reason="disk full")))
    state.consume(json.loads(event(name, **details)))
    state.consume(json.loads(event("resource_sample")))
    assert state.done and state.status == status


def test_foreign_run_events_and_malformed_shapes_are_ignored():
    state = IngestStatus(RUN)
    state.consume({"context": []})
    state.consume(json.loads(event("cli_complete", run_id=str(UUID(int=1)), ok=True)))
    assert not state.done


def test_rich_render_keeps_markup_literal_and_strips_terminal_controls():
    state = IngestStatus(RUN, source="[red]novel[/red]\x1b[2J\r\n", issue="bad\x07")
    stream = io.StringIO()
    Console(file=stream, width=100, color_system=None).print(
        render_panel(state, watching=True)
    )
    output = stream.getvalue()
    assert "[red]novel[/red]" in output
    assert "\x1b" not in output and "\x07" not in output and "\r" not in output
    assert "liveness unknown" in output
    assert "import continues" in output


def test_auto_and_explicit_progress_modes(monkeypatch):
    monkeypatch.delenv("TERM", raising=False)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)
    assert rich_enabled(argparse.Namespace(progress="auto"))
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    assert not rich_enabled(argparse.Namespace(progress="auto"))
    assert rich_enabled(argparse.Namespace(progress="rich"))
    assert not rich_enabled(
        argparse.Namespace(progress="rich", no_console_progress=True)
    )


def test_detaching_watcher_leaves_writer_and_artifacts_alone(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "run.jsonl"
    path.write_bytes(event("resource_sample", counts={"members_adopted": 2}))
    before = path.read_bytes()
    before_entries = set(tmp_path.iterdir())
    calls = []

    def interrupted(seconds):
        calls.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(ingest_watch.time, "sleep", interrupted)
    args = argparse.Namespace(progress="plain", refresh_seconds=0.1, once=False)
    assert (
        ingest_watch.watch_attempt(args, {"event_log": str(path), "run_id": RUN}) == 0
    )
    assert calls == [0.1]
    assert path.read_bytes() == before
    assert set(tmp_path.iterdir()) == before_entries
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "2 adopted" in captured.err
    assert "import was not signalled" in captured.err


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_watcher_rejects_invalid_refresh_interval(interval):
    with pytest.raises(ValueError, match="refresh-seconds"):
        ingest_watch.watch_attempt(argparse.Namespace(refresh_seconds=interval), {})


@pytest.mark.parametrize("mode", ["plain", "rich"])
def test_watcher_follows_new_samples_and_exits_on_terminal_event(
    tmp_path, monkeypatch, capsys, mode
):
    path = tmp_path / "run.jsonl"
    path.write_bytes(event("resource_sample", counts={"members_adopted": 2}))
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("TTY_INTERACTIVE", "1")
    updates = [
        event("resource_sample", counts={"members_adopted": 12}),
        event("cli_cancelled"),
    ]

    def advance(seconds):
        assert updates, "Watcher failed to stop after the terminal event"
        with path.open("ab") as stream:
            stream.write(updates.pop(0))

    monkeypatch.setattr(ingest_watch.time, "sleep", advance)
    args = argparse.Namespace(progress=mode, refresh_seconds=0.1, once=False)
    assert (
        ingest_watch.watch_attempt(args, {"event_log": str(path), "run_id": RUN}) == 0
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "2 adopted" in captured.err and "12 adopted" in captured.err
    assert "cancelled" in captured.err
    assert not updates
    if mode == "rich":
        assert "\x1b[?25h" in captured.err


def test_once_displays_incomplete_attempt_without_waiting(tmp_path, capsys):
    path = tmp_path / "run.jsonl"
    path.write_bytes(event("cli_started", source_root="source"))
    args = argparse.Namespace(progress="plain", refresh_seconds=1, once=True)
    attempt = {"event_log": str(path), "run_id": RUN}
    assert ingest_watch.watch_attempt(args, attempt) == 0
    assert "Status: starting" in capsys.readouterr().err
