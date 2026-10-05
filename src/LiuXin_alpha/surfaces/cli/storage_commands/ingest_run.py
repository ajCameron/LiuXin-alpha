"""
Translate CLI ingest controls into the application request and project its result.

Discovery/preflight omit a durable database path; preflight adds observations
after discovery. The caller owns signal/lock scopes and terminal report publication.
Application results remain rich objects for the ingest JSON serializer rather
than being flattened or redacted at this boundary.
"""

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
    """
    Execute discovery or durable ingest and build a mode-specific receipt/status pair.

    Either preview flag enables discovery; preflight wins the receipt label when
    both are set by a direct caller. Real ingest resolves its database path and
    supplies a DEBUG logging sink for database-construction stdout. Forward budget,
    recursion/backend/verification controls and optional progress/cancellation callbacks.

    Preflight runs after discovery and requires report.ok plus every error-severity
    check; warnings do not make it unready. Real results must include a database path.
    No local exception handling, report publication, or rollback is added here.

    Example:
        >>> code, payload = _run_ingest(args, source_root=source, run_id=run_id, cancellation_callback=lambda: False)  # doctest: +SKIP


    :param args: Complete ingest policy, source/database/cache, backend, and progress settings.
    :param source_root: Prepared local source path forwarded unchanged to the application request.
    :param run_id: Run UUID forwarded to application events and the coordinator.
    :param cancellation_callback: Cooperative cancellation predicate supplied by the outer signal scope.
    :return: EXIT_OK/EXIT_ISSUES and a payload retaining the rich budget/report objects.
    :raises AssertionError: A non-discovery application result lacks its database path.
    """
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
        """
        Relay an application event with the enclosing run UUID and copied detail fields.

        Detail keys colliding with explicit _log arguments raise rather than being
        silently renamed; other values are passed to the logging adapter unchanged.

        Example:
            >>> application_event(logging.INFO, "database_open_complete", "Opened", {"created": False})  # doctest: +SKIP


        :param level: Logging severity supplied by the application.
        :param event: Structured event name forwarded to the ingest logger.
        :param message: Human-readable event text.
        :param details: Detail mapping copied and expanded as keyword context.
        :return: None after logging, without suppressing callback/logging failures.
        """
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
    """
    Print selected container/source/member progress events on stderr and flush immediately.

    Recognize container_started, container_complete, source_checkpoint, and
    member_checkpoint; ignore other names without inspecting their details.
    Required fields use direct indexing and values are not redacted.

    Example:
        >>> _console_progress("unhandled_event", {})


    :param event: Exact progress-event name controlling the rendered message.
    :param details: Event-specific depth/path/count/status fields required by the chosen branch.
    :return: None after printing one message or ignoring an unrecognized event.
    :raises KeyError: A recognized event omits a directly indexed detail field.
    """
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
