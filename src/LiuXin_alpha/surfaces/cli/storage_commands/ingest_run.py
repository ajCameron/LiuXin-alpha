"""Storage CLI ingest run ownership."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.ingest.mixed_application import (
    MixedIngestApplicationRequest,
    execute_mixed_ingest,
)
from LiuXin_alpha.surfaces.cli.storage_commands.constants import EXIT_ISSUES, EXIT_OK
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_config import _budget
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_preflight import (
    _preflight_checks,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_reporting import _LOGGER, _log
from LiuXin_alpha.utils.logging.run_logging import LoggingTextStream


def _run_ingest(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    cancellation_callback: Callable[[], bool],
) -> tuple[int, dict[str, object]]:
    discovery_only = bool(args.discover_only) or bool(args.preflight_only)
    database_path = (
        None
        if discovery_only
        else Path(str(args.database)).expanduser().resolve(strict=False)
    )
    captured_stdout = (
        None
        if discovery_only
        else LoggingTextStream(
            _LOGGER,
            level=logging.DEBUG,
            stream_name="database_stdout",
        )
    )

    def application_event(
        level: int,
        event: str,
        message: str,
        details: Mapping[str, object],
    ) -> None:
        _log(level, event, message, run_id=run_id, **dict(details))

    result = execute_mixed_ingest(
        MixedIngestApplicationRequest(
            source_root=source_root,
            run_id=run_id,
            budget=_budget(args),
            discovery_only=discovery_only,
            database_path=database_path,
            recursive_filesystem=not bool(args.no_recursive_filesystem),
            recurse_containers=not bool(args.no_nested_containers),
            expand_ebook_containers=bool(args.expand_ebook_containers),
            continue_on_error=not bool(args.strict),
            verify=bool(args.verify),
            materialization_root=args.materialization_root,
            unsquashfs_exe=str(args.unsquashfs_exe),
            rar_extractor_exe=args.rar_extractor_exe,
            backend_timeout_s=float(args.backend_timeout_seconds),
            log_checkpoint_every=int(args.log_checkpoint_every),
            progress_callback=(
                None if bool(args.no_console_progress) else _console_progress
            ),
            cancellation_callback=cancellation_callback,
            event_callback=application_event,
            database_stdout=captured_stdout,
        )
    )
    report = result.report
    if discovery_only:
        payload: dict[str, object] = {
            "mode": "preflight" if args.preflight_only else "discovery",
            "ok": report.ok,
            "budget": result.budget,
            "report": report,
        }
        if args.preflight_only:
            checks = _preflight_checks(args, source_root, report.recognized_formats)
            ready = report.ok and all(
                bool(check["ok"]) for check in checks if check["severity"] == "error"
            )
            payload["ok"] = ready
            payload["preflight"] = {
                "ready": ready,
                "checks": checks,
            }
            _log(
                logging.INFO if ready else logging.ERROR,
                "preflight_complete",
                "Mixed ingest preflight complete",
                run_id=run_id,
                ready=ready,
                check_count=len(checks),
                failed_checks=sum(not bool(check["ok"]) for check in checks),
            )
        return (EXIT_OK if bool(payload["ok"]) else EXIT_ISSUES), payload
    assert result.database_path is not None
    payload = {
        "mode": result.mode,
        "database": str(result.database_path),
        "metadata_is_durable": result.metadata_is_durable,
        "budget": result.budget,
        "ok": result.ok,
        "report": report,
    }
    return (EXIT_OK if result.ok else EXIT_ISSUES), payload


def _console_progress(event: str, details: Mapping[str, object]) -> None:
    if event == "container_started":
        print(
            f"[depth {details['depth']}] {details['format']}: {details['path']}",
            file=sys.stderr,
            flush=True,
        )
    elif event == "container_complete":
        print(
            "  members={} issues={} ok={}".format(
                details["members_adopted"],
                details["issue_count"],
                details["ok"],
            ),
            file=sys.stderr,
            flush=True,
        )
    elif event == "source_checkpoint":
        print(
            "[source checkpoint] adopted={}/{} containers={} issues pending".format(
                details["files_adopted"],
                details["files_examined"],
                details["containers_discovered"],
            ),
            file=sys.stderr,
            flush=True,
        )
    elif event == "member_checkpoint":
        print(
            "[member checkpoint] adopted={} expanded_bytes={} queued={}".format(
                details["run_members_adopted"],
                details["run_expanded_bytes"],
                details["queued_containers"],
            ),
            file=sys.stderr,
            flush=True,
        )
