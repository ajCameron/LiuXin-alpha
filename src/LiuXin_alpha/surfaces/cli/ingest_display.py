"""Render shared ingest telemetry on stderr, with Rich loaded only on demand."""

from __future__ import annotations

import argparse
import os
import sys
import time
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from LiuXin_alpha.surfaces.cli.ingest_status import (
    EventTail,
    IngestStatus,
    mapping,
    number,
)

if TYPE_CHECKING:
    from rich.panel import Panel


def clean(value: object) -> str:
    """Keep literal filenames, removing terminal controls and bounding display text."""
    return "".join(
        char if not unicodedata.category(char).startswith("C") else "?"
        for char in str(value)[:1000]
    )


def size(value: object) -> str:
    """Format observed bytes; missing metrics remain visibly unknown."""
    amount = number(value)
    if amount is None:
        return "unknown"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(amount) < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return "unknown"


def rich_enabled(args: argparse.Namespace) -> bool:
    """Resolve explicit selection or interactive stderr, honoring the existing opt-out."""
    if getattr(args, "no_console_progress", False):
        return False
    mode = getattr(args, "progress", "auto")
    return mode == "rich" or (
        mode == "auto" and sys.stderr.isatty() and os.environ.get("TERM") != "dumb"
    )


def disk_rows(disks: object) -> list[tuple[str, str]]:
    """Combine identical observations on shared volumes to fit ordinary terminals."""
    grouped: dict[tuple[str, str], list[str]] = {}
    if not isinstance(disks, list):
        return []
    for value in disks:
        disk = mapping(value)
        detail = (
            str(disk["error"])
            if disk.get("error")
            else (
                f"{size(disk.get('free_bytes'))} free; {size(disk.get('required_free_bytes'))} required"
            )
        )
        key = (str(disk.get("device", disk.get("path"))), detail)
        grouped.setdefault(key, []).append(str(disk.get("role", "?")))
    return [
        ("Space", f"{detail} ({', '.join(roles)})")
        for (_, detail), roles in grouped.items()
    ]


def status_rows(state: IngestStatus, now: float) -> list[tuple[str, str]]:
    """Build common rich/plain content without an invented total, ETA, or percentage."""
    resources, counts = state.resources, state.counts
    elapsed = number(resources.get("elapsed_seconds"))
    members = number(counts.get("members_adopted"))
    rate = (
        f"{members / elapsed:.1f}/s average"
        if members is not None and elapsed
        else "rate unknown"
    )
    rows = [
        ("Run", state.run_id),
        ("Status", state.status),
        ("Telemetry", state.freshness(now)),
        ("Elapsed", f"{elapsed:.0f}s" if elapsed is not None else "unknown"),
        ("Source", state.source),
        (
            "Phase",
            str(state.activity.get("event", "awaiting progress")).replace("_", " "),
        ),
        (
            "Sources",
            f"{counts.get('files_adopted', '?')} adopted / {counts.get('files_examined', '?')} discovered",
        ),
        (
            "Containers",
            f"{counts.get('containers_processed', '?')} processed / {counts.get('containers_discovered', '?')} discovered so far",
        ),
        ("Members", f"{counts.get('members_adopted', '?')} adopted; {rate}"),
        ("Member bytes", size(counts.get("expanded_bytes"))),
        (
            "Cache",
            f"{size(resources.get('cache_materialized_bytes'))} written this attempt",
        ),
        (
            "Memory",
            f"{size(mapping(resources.get('memory')).get('process_rss_bytes'))} process RSS",
        ),
    ]
    for key, label in (
        ("path", "Path"),
        ("container_path", "Container"),
        ("member_path", "Member"),
    ):
        if state.activity.get(key):
            rows.append((label, str(state.activity[key])))
    rows.extend(disk_rows(resources.get("disks")))
    if resources.get("stop_reason"):
        rows.insert(3, ("Stop reason", str(resources["stop_reason"])))
    if state.issue:
        rows.insert(3, ("Latest issue", state.issue))
    if state.notice:
        rows.append(("Notice", state.notice))
    return [(label, clean(value)) for label, value in rows]


def render_panel(state: IngestStatus, *, watching: bool = False) -> Panel:
    """Render literal text so source filenames cannot inject Rich markup or controls."""
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    table = Table.grid(padding=(0, 1), expand=True)
    table.add_column(style="bold", no_wrap=True)
    table.add_column(overflow="fold", ratio=1)
    for label, value in status_rows(state, time.time()):
        table.add_row(Text(label), Text(value))
    footer = (
        "Ctrl-C detaches this watcher; import continues"
        if watching
        else "Ctrl-C requests a cooperative import stop"
    )
    return Panel(
        table, title="LiuXin import", subtitle=Text(footer), border_style="cyan"
    )


@contextmanager
def import_panel(
    args: argparse.Namespace, event_log: Path, run_id: str
) -> Iterator[None]:
    """Scope Rich to the whole invocation, restoring the terminal on every exit path."""
    if not rich_enabled(args):
        yield
        return
    from rich.console import Console
    from rich.live import Live

    tail = EventTail(event_log, run_id)
    console = Console(stderr=True, force_terminal=True)
    with Live(
        console=console,
        get_renderable=lambda: render_panel(tail.poll()),
        refresh_per_second=2,
        redirect_stdout=False,
        redirect_stderr=False,
    ):
        yield
