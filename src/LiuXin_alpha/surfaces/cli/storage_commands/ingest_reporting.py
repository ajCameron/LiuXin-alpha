"""Correlated ingest logs, terminal receipts, and atomic report publication.

Failure and success paths use the same run identity and output conventions;
publishing a report never silently overwrites a competing writer's file.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import platform
import socket
import sys
import tempfile
import traceback
from collections.abc import Mapping
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.constants import __version__ as liuxin_version
from LiuXin_alpha.surfaces.cli.storage_commands.constants import (
    EXIT_USAGE,
    CLIUsageError,
)
from LiuXin_alpha.surfaces.cli.storage_commands.filesystem import _fsync_directory
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_config import _budget
from LiuXin_alpha.utils.logging import get_compat_logger
from LiuXin_alpha.utils.logging.run_logging import RunLoggingSession

_LOGGER = get_compat_logger("LiuXin_alpha.storage.ingest.mixed_cli")


def _log_cli_start(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
) -> None:
    excluded = {
        "database",
        "handler",
        "materialization_root",
        "report_file",
        "source_root",
    }
    _log(
        logging.INFO,
        "cli_started",
        "Mixed ingest command started",
        run_id=run_id,
        source_root=str(source_root),
        database=(
            None
            if args.database is None
            else str(Path(args.database).expanduser().resolve(strict=False))
        ),
        materialization_root=(
            None
            if args.materialization_root is None
            else str(Path(args.materialization_root).expanduser().resolve(strict=False))
        ),
        mode=(
            "preflight"
            if args.preflight_only
            else "discovery"
            if args.discover_only
            else "ingest"
        ),
        arguments={
            key: value for key, value in vars(args).items() if key not in excluded
        },
        budget=dataclasses.asdict(_budget(args)),
        liuxin_version=liuxin_version,
        python_version=sys.version,
        python_executable=sys.executable,
        platform=platform.platform(),
        hostname=socket.gethostname(),
        process_id=os.getpid(),
        working_directory=str(Path.cwd()),
        report_file=str(report_path),
        human_log=str(human_log),
        event_log=str(event_log),
        lock_file=None if lock_path is None else str(lock_path),
    )


def _enrich_terminal_payload(
    payload: dict[str, object],
    *,
    args: argparse.Namespace,
    run_id: UUID,
    exit_code: int,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
) -> None:
    payload.update(
        {
            "schema_version": 1,
            "command": "storage ingest",
            "run_id": str(run_id),
            "exit_code": int(exit_code),
            "report_file": str(report_path),
            "human_log": str(human_log),
            "event_log": str(event_log),
            "lock_file": None if lock_path is None else str(lock_path),
            "stdout_report": not bool(args.no_stdout_report),
        }
    )


def _handle_failure(
    args: argparse.Namespace,
    error: BaseException,
    *,
    event: str,
    status: str,
    exit_code: int,
    run_id: UUID,
    report_path: Path,
    human_log: Path,
    event_log: Path,
    lock_path: Path | None,
    log_session: RunLoggingSession,
    signal_number: int | None = None,
) -> int:
    level = logging.ERROR if exit_code == EXIT_USAGE else logging.CRITICAL
    _LOGGER.log(
        level,
        "Mixed ingest command failed",
        exc_info=(type(error), error, error.__traceback__),
        extra={
            "liuxin_event": event,
            "liuxin_context": {
                "run_id": str(run_id),
                "error_type": type(error).__name__,
                "error_message": str(error) or type(error).__name__,
                "signal": signal_number,
            },
        },
    )
    payload: dict[str, object] = {
        "ok": False,
        "status": status,
        "error": {
            "type": type(error).__name__,
            "message": str(error) or type(error).__name__,
            "traceback": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        },
    }
    if signal_number is not None:
        payload["signal"] = signal_number
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
    try:
        _write_report(
            report_path,
            payload,
            replace=bool(args.replace_report),
            compact=bool(args.compact_json),
        )
    except Exception as report_error:
        _LOGGER.error(
            "Could not write mixed ingest failure report",
            exc_info=(
                type(report_error),
                report_error,
                report_error.__traceback__,
            ),
            extra={
                "liuxin_event": "report_write_failed",
                "liuxin_context": {
                    "run_id": str(run_id),
                    "report_file": str(report_path),
                },
            },
        )
        print(f"ERROR: could not write report: {report_error}", file=sys.stderr)
    log_session.flush()
    print(
        f"Run {run_id} {status}; inspect {event_log}",
        file=sys.stderr,
        flush=True,
    )
    _print_payload(args, payload)
    return exit_code


def _write_report(
    path: Path,
    payload: Mapping[str, object],
    *,
    replace: bool,
    compact: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = _json_text(payload, compact=compact) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=False,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            _ = output.write(text.encode("utf-8", errors="backslashreplace"))
            output.flush()
            os.fsync(output.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            try:
                os.link(temporary, path)
            except FileExistsError as error:
                raise CLIUsageError(
                    f"report file already exists: {path}; pass --replace-report"
                ) from error
            temporary.unlink()
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _print_payload(args: argparse.Namespace, payload: Mapping[str, object]) -> None:
    if bool(args.no_stdout_report):
        return
    try:
        print(_json_text(payload, compact=bool(args.compact_json)), flush=True)
    except BrokenPipeError:
        pass


def _json_text(value: object, *, compact: bool) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        indent=None if compact else 2,
        separators=(",", ":") if compact else None,
        default=_json_default,
    )


def _json_default(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: getattr(value, field.name)
            for field in dataclasses.fields(value)
            if not field.name.startswith("_")
        }
    if isinstance(value, (UUID, Path)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot serialize {type(value).__name__} to JSON")


def _log(
    level: int,
    event: str,
    message: str,
    *,
    run_id: UUID,
    **details: object,
) -> None:
    context = dict(details)
    context["run_id"] = str(run_id)
    _LOGGER.log(
        level,
        message,
        extra={"liuxin_event": event, "liuxin_context": context},
    )
