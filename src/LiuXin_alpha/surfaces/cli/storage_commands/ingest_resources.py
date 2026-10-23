"""Translate local CLI output paths and resource controls into an ingest monitor."""

from __future__ import annotations

import argparse
import logging
import tempfile
from collections.abc import Mapping
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.ingest.resource_monitor import ResourceMonitor
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_config import resource_policy
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_reporting import _log
from LiuXin_alpha.utils.logging.run_logging import RunLoggingSession


def resource_monitor(
    args: argparse.Namespace,
    *,
    run_id: UUID,
    report_path: Path,
    event_log: Path,
    lock_path: Path | None,
    log_session: RunLoggingSession,
) -> ResourceMonitor:
    """Watch catalogue/WAL, cache, temp, logs, report and lock filesystems, plus WSL host."""
    paths = {
        "temporary": Path(tempfile.gettempdir()),
        "logs": event_log.parent,
        "report": report_path.parent,
    }
    if args.database:
        paths["catalogue"] = Path(args.database).expanduser().resolve().parent
    if args.materialization_root:
        paths["cache"] = Path(args.materialization_root).expanduser().resolve()
    if lock_path is not None:
        paths["lock"] = lock_path.parent

    def emit(event: str, details: Mapping[str, object]) -> None:
        """Keep telemetry in the existing correlated and flushed event stream."""
        _log(
            logging.ERROR if event == "resource_stop" else logging.INFO,
            event,
            "Mixed ingest resource observation",
            run_id=run_id,
            **dict(details),
        )
        log_session.check_health()

    return ResourceMonitor(resource_policy(args), paths, emit)
