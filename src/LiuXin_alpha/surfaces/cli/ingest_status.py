"""Read bounded, incremental ingest telemetry without opening a catalogue or source.

Foreground and attached displays consume the same snapshots. Counters describe
one attempt, including already-catalogued objects visited again during resume.
An absent terminal event never proves completion or process liveness.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import cast

READ_WINDOW = 1024 * 1024
MAX_LINE = 256 * 1024
TERMINAL_EVENTS = {
    "cli_complete": "complete",
    "cli_cancelled": "cancelled",
    "cli_failed": "failed",
    "cli_interrupted": "interrupted",
    "cli_configuration_error": "configuration_error",
}


def mapping(value: object) -> Mapping[str, object]:
    """Treat malformed or old optional objects as unavailable telemetry."""
    return cast(Mapping[str, object], value) if isinstance(value, dict) else {}


def number(value: object) -> float | None:
    """Accept finite numeric telemetry without coercing arbitrary strings or booleans."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            result = float(value)
            return result if math.isfinite(result) else None
        except OverflowError:
            pass
    return None


def timestamp(value: object) -> float | None:
    """Parse timezone-bearing JSONL timestamps; reject ambiguous legacy values."""
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


@dataclass
class IngestStatus:
    """Retain only the latest snapshot, issue, and terminal receipt for one attempt."""

    run_id: str
    source: str = "unknown"
    status: str = "awaiting telemetry"
    done: bool = False
    sample_time: float | None = None
    interval: float = 5.0
    resources: Mapping[str, object] = field(default_factory=dict)
    counts: Mapping[str, object] = field(default_factory=dict)
    activity: Mapping[str, object] = field(default_factory=dict)
    issue: str = ""
    notice: str = ""

    def consume(self, row: Mapping[str, object]) -> None:
        """Project correlated events, preserving failure/cancellation terminal states."""
        context = mapping(row.get("context"))
        details = mapping(context.get("details"))
        if str(details.get("run_id", "")) != self.run_id:
            return
        event = str(context.get("event", ""))
        if event == "cli_started":
            self.source = str(details.get("source_root", "unknown"))
            self.status = "starting"
            args = mapping(details.get("arguments"))
            self.interval = number(args.get("resource_interval_seconds")) or 5.0
        elif event in {"resource_sample", "resource_stop"}:
            self.resources = details
            self.source = str(details.get("source_root") or self.source)
            self.counts = mapping(details.get("counts"))
            self.activity = mapping(details.get("activity"))
            self.sample_time = timestamp(row.get("ts"))
            self.interval = number(details.get("interval_seconds")) or self.interval
            if not self.done:
                self.status = (
                    "stopping: resource limit"
                    if details.get("stop_reason")
                    else "running"
                )
        elif event == "ingest_issue":
            self.issue = str(row.get("message", ""))
        elif event in TERMINAL_EVENTS:
            self.done = True
            self.status = str(details.get("status") or TERMINAL_EVENTS[event])
            if (
                event == "cli_complete"
                and not details.get("status")
                and not details.get("ok", False)
            ):
                self.status = (
                    "resource_stopped"
                    if self.resources.get("stop_reason")
                    else "issues"
                )
            self.issue = str(details.get("error_message") or self.issue)

    def freshness(self, now: float) -> str:
        """Label silence explicitly; the watcher never infers death or success."""
        if self.done:
            return "final observation"
        if self.sample_time is None:
            return "No resource sample yet; liveness unknown"
        age = max(0.0, now - self.sample_time)
        if age > max(15.0, 3 * self.interval):
            return f"STALE: last sample {age:.0f}s ago; liveness unknown"
        return f"sample {age:.0f}s ago"


class EventTail:
    """Follow one selected attempt, bounding both reads and incomplete-line memory.

    A long existing log or an overrun starts at its last MiB. Full counter
    snapshots make historical replay unnecessary. Truncation/replacement resets
    state; malformed/oversized lines are skipped visibly. Partial UTF-8 records
    are held as bytes until their terminating newline arrives.
    """

    def __init__(self, path: Path, run_id: str) -> None:
        """Select a log without opening or mutating any run artifact."""
        self.path = path
        self.run_id = run_id
        self.state = IngestStatus(run_id)
        self.offset = 0
        self.identity: tuple[int, int] | None = None
        self.pending = b""
        self.discard_line = False

    def poll(self) -> IngestStatus:
        """Read at most one window; expose read errors without failing the import."""
        try:
            with self.path.open("rb") as stream:
                info = os.fstat(stream.fileno())
                identity = (info.st_dev, info.st_ino)
                if identity != self.identity or info.st_size < self.offset:
                    self.state = IngestStatus(self.run_id)
                    self.offset, self.pending, self.discard_line = 0, b"", False
                    self.identity = identity
                if info.st_size - self.offset > READ_WINDOW:
                    self.offset = info.st_size - READ_WINDOW
                    self.pending, self.discard_line = b"", True
                    self.state.notice = (
                        "Showing recent telemetry; earlier events omitted"
                    )
                stream.seek(self.offset)
                chunk = stream.read(READ_WINDOW)
                self.offset += len(chunk)
            self._consume(chunk)
        except OSError as error:
            self.state.notice = f"Cannot read telemetry: {error}"
        return self.state

    def _consume(self, chunk: bytes) -> None:
        """Decode complete bounded records only, recovering at the next newline."""
        lines = (self.pending + chunk).split(b"\n")
        self.pending = lines.pop()
        for line in lines:
            if self.discard_line:
                self.discard_line = False
                continue
            if len(line) > MAX_LINE:
                self.state.notice = "Skipped oversized telemetry record"
                continue
            try:
                self.state.consume(mapping(json.loads(line)))
            except (ValueError, UnicodeError, RecursionError):
                self.state.notice = "Skipped malformed telemetry record"
        if self.discard_line or len(self.pending) > MAX_LINE:
            self.pending = b""
            self.discard_line = True
            self.state.notice = "Skipped partial or oversized telemetry record"
