"""Attach a read-only terminal observer to the newest selected ingest attempt."""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections.abc import Mapping
from contextlib import nullcontext
from pathlib import Path

from LiuXin_alpha.surfaces.cli.ingest_display import (
    render_panel,
    rich_enabled,
    status_rows,
)
from LiuXin_alpha.surfaces.cli.ingest_status import EventTail


def watch_attempt(args: argparse.Namespace, attempt: Mapping[str, object]) -> int:
    """Follow an attempt until its terminal event, one snapshot, or a local Ctrl-C.

    Do not acquire run locks, open the catalogue, or signal the writer. A stale
    snapshot stays visible until detached; restarting/resuming is a separate
    command and requires attaching again to select that newer attempt.
    """
    interval = float(args.refresh_seconds)
    if not math.isfinite(interval) or interval < 0.1:
        raise ValueError("--refresh-seconds must be finite and at least 0.1")
    path = Path(str(attempt.get("event_log") or ""))
    if not path.is_file():
        raise FileNotFoundError(f"No event log for this attempt: {path}")
    tail = EventTail(path, str(attempt["run_id"]))
    live = None
    last_snapshot: tuple[object, ...] | None = None
    if rich_enabled(args):
        from rich.console import Console
        from rich.live import Live

        live = Live(
            console=Console(stderr=True, force_terminal=True),
            auto_refresh=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
    try:
        with live if live is not None else nullcontext():
            while True:
                state = tail.poll()
                if live is not None:
                    live.update(render_panel(state, watching=True), refresh=True)
                snapshot = (
                    state.sample_time,
                    state.status,
                    state.issue,
                    state.notice,
                    state.freshness(time.time()).startswith("STALE"),
                )
                if live is None and snapshot != last_snapshot:
                    print(
                        " | ".join(
                            f"{key}: {value}"
                            for key, value in status_rows(state, time.time())
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
                    last_snapshot = snapshot
                if args.once or state.done:
                    return 0
                time.sleep(interval)
    except KeyboardInterrupt:
        print("Detached watcher; import was not signalled.", file=sys.stderr)
        return 0
