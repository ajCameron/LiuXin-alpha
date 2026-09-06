"""Own one ingest invocation from logging setup to its terminal receipt.

Signal and lock scopes surround the application call. Path validation,
preflight, and report publication live in their dedicated helper modules.
"""

from __future__ import annotations

import argparse
import logging
import sys
from contextlib import nullcontext
from pathlib import Path
from uuid import UUID, uuid4

from LiuXin_alpha.surfaces.cli.storage_commands.constants import (
    EXIT_INTERRUPTED,
    EXIT_ISSUES,
    EXIT_OK,
    EXIT_USAGE,
    CLIUsageError,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_config import (
    _apply_system_root_defaults,
    _validate_early_options,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_options import (
    add_storage_ingest_arguments,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_paths import (
    _acquire_run_lock,
    _lock_path,
    _log_directory,
    _report_path,
    _validate_paths,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_reporting import (
    _enrich_terminal_payload,
    _handle_failure,
    _log,
    _log_cli_start,
    _print_payload,
    _write_report,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_run import _run_ingest
from LiuXin_alpha.surfaces.cli.storage_commands.signals import SignalCancellation
from LiuXin_alpha.utils.logging.run_logging import RunLoggingSession


def ingest_main(argv: list[str] | None = None) -> int:
    """Standalone mixed-ingest parser retained for the executable example."""

    parser = argparse.ArgumentParser(
        prog="liuxin storage ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Catalogue a bounded mixed local file/container tree.\n\n"
            "Prefer the installed `liuxin storage ingest` command in operations."
        ),
    )
    add_storage_ingest_arguments(parser)
    args = parser.parse_args(argv)
    return cmd_storage_ingest(args)


def cmd_storage_ingest(args: argparse.Namespace) -> int:
    """Execute one logged, report-producing mixed-ingest invocation."""

    try:
        _apply_system_root_defaults(args)
        _validate_early_options(args)
        source_root = Path(args.source_root).expanduser().resolve(strict=False)
        run_id = args.run_id if args.run_id is not None else uuid4()
        log_directory = _log_directory(args, source_root)
    except (CLIUsageError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return EXIT_USAGE

    try:
        session_context = RunLoggingSession(
            log_directory,
            run_id=run_id,
            prefix="mixed-ingest",
            level=int(getattr(logging, str(args.log_level))),
            max_text_bytes=int(args.log_max_mib) * 1024 * 1024,
            text_backup_count=int(args.log_backup_count),
        )
        with session_context as log_session:
            assert log_session.paths is not None
            paths = log_session.paths
            report_path = _report_path(args, source_root, paths.human_log)
            lock_path = _lock_path(args, source_root, log_directory)
            print(f"Run ID: {run_id}", file=sys.stderr, flush=True)
            print(f"Human log: {paths.human_log}", file=sys.stderr, flush=True)
            print(f"Event log: {paths.event_log}", file=sys.stderr, flush=True)
            print(f"Report: {report_path}", file=sys.stderr, flush=True)
            if lock_path is not None:
                print(f"Run lock: {lock_path}", file=sys.stderr, flush=True)
            return _run_logged_command(
                args,
                source_root=source_root,
                run_id=run_id,
                report_path=report_path,
                human_log=paths.human_log,
                event_log=paths.event_log,
                lock_path=lock_path,
                log_session=log_session,
            )
    except CLIUsageError as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return EXIT_USAGE
    except (OSError, ValueError) as error:
        print(f"ERROR: could not initialize ingest logging: {error}", file=sys.stderr)
        return EXIT_USAGE


def _run_logged_command(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
    log_session: RunLoggingSession,
) -> int:
    controller = SignalCancellation()
    try:
        _validate_paths(
            args,
            source_root=source_root,
            report_path=report_path,
            lock_path=lock_path,
        )
        _log_cli_start(
            args,
            source_root=source_root,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
        )
        lock_context = (
            _acquire_run_lock(lock_path, run_id=run_id, args=args)
            if lock_path is not None
            else nullcontext()
        )
        with controller, lock_context:
            exit_code, payload = _run_ingest(
                args,
                source_root=source_root,
                run_id=run_id,
                cancellation_callback=controller.requested,
            )

        received_signal = controller.signal_number
        if received_signal is not None:
            exit_code = 128 + received_signal
            payload["ok"] = False
            payload["status"] = "cancelled"
            payload["signal"] = received_signal
        else:
            payload["status"] = "complete" if exit_code == EXIT_OK else "issues"
        _enrich_terminal_payload(
            payload,
            args=args,
            run_id=run_id,
            exit_code=exit_code,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
        )
        _write_report(
            report_path,
            payload,
            replace=bool(args.replace_report),
            compact=bool(args.compact_json),
        )
        terminal_event = (
            "cli_cancelled" if received_signal is not None else "cli_complete"
        )
        _log(
            logging.WARNING if exit_code else logging.INFO,
            terminal_event,
            "Mixed ingest command cancelled"
            if received_signal is not None
            else "Mixed ingest command complete",
            run_id=run_id,
            exit_code=exit_code,
            ok=bool(payload["ok"]),
            signal=received_signal,
            report_file=str(report_path),
            human_log=str(human_log),
            event_log=str(event_log),
        )
        log_session.flush()
        _print_payload(args, payload)
        return exit_code
    except KeyboardInterrupt as error:
        signal_number = controller.signal_number
        exit_code = (
            128 + signal_number if signal_number is not None else EXIT_INTERRUPTED
        )
        return _handle_failure(
            args,
            error,
            event="cli_interrupted",
            status="interrupted",
            exit_code=exit_code,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
            signal_number=signal_number,
        )
    except CLIUsageError as error:
        return _handle_failure(
            args,
            error,
            event="cli_configuration_error",
            status="configuration_error",
            exit_code=EXIT_USAGE,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
        )
    except Exception as error:
        return _handle_failure(
            args,
            error,
            event="cli_failed",
            status="failed",
            exit_code=EXIT_ISSUES,
            run_id=run_id,
            report_path=report_path,
            human_log=human_log,
            event_log=event_log,
            lock_path=lock_path,
            log_session=log_session,
        )
