"""
Correlate ingest events, serialize rich receipts, and publish individual report files.

Failure and success paths share run identity and output conventions. No-clobber
publication uses a hard link; explicit replacement uses atomic rename. Directory
creation, report publication, logging, and stdout are independent effects. Error
messages, tracebacks, argument metadata, and progress values are not comprehensively
redacted, and a failure receipt cannot retract an earlier successful publication.
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
    """
    Emit the start event with paths, effective options, budget, and host/process provenance.

    Resolve database/cache paths for separate fields and exclude only database,
    handler, materialization_root, report_file, and source_root from the raw argument
    dictionary. Other arguments remain visible; this is not a general secret filter.
    Reconstruct the budget for logging, with its normal validation failures.

    Example:
        >>> _log_cli_start(args, source_root=source, run_id=run_id, report_path=report, human_log=human_log, event_log=event_log, lock_path=None)  # doctest: +SKIP


    :param args: Complete effective ingest namespace, including mode and budget options.
    :param source_root: Prepared source path rendered into the event context.
    :param run_id: Correlation UUID serialized by the shared event helper.
    :param report_path: Selected terminal report location, not published by this function.
    :param human_log: Current rotating-text log path.
    :param event_log: Current authoritative JSONL path.
    :param lock_path: Optional advisory lock location included as text or None.
    :return: None after submitting one INFO cli_started logging record.
    """
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
    """
    Overwrite terminal receipt identity, schema, exit, and artifact-location fields in place.

    Existing mode/status/report/error fields are retained. This is a projection,
    not proof that the named files exist or that stdout will successfully publish.

    Example:
        >>> payload = {"ok": True}
        >>> _enrich_terminal_payload(payload, args=argparse.Namespace(no_stdout_report=True), run_id=UUID(int=1), exit_code=0, report_path=Path("report.json"), human_log=Path("run.log"), event_log=Path("run.jsonl"), lock_path=None)
        >>> payload["command"], payload["stdout_report"], payload["lock_file"]
        ('storage ingest', False, None)


    :param payload: Mutable result/error dictionary updated with fixed terminal metadata keys.
    :param args: Namespace whose no_stdout_report flag determines the declared stdout policy.
    :param run_id: Run UUID stringified into the receipt.
    :param exit_code: Status integer-converted for the exit_code field.
    :param report_path: Selected report destination rendered without filesystem inspection.
    :param human_log: Human-readable log path rendered into the receipt.
    :param event_log: JSONL event-log path rendered into the receipt.
    :param lock_path: Advisory lock path or None for runs without that lock.
    :return: None; mutate payload without publishing it.
    """
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
    """
    Log a failure, attempt a terminal error report, then flush and emit its stdout projection.

    Preserve the supplied error type/text/traceback without sanitization. Usage
    failures log at ERROR, others at CRITICAL. Report-write Exceptions are separately
    logged/printed and do not change the requested exit code or report_file field;
    the file may be absent or contain an earlier receipt. Other logger/flush/output
    failures can still escape. No prior catalogue/cache/report effect is rolled back.

    Example:
        >>> code = _handle_failure(args, error, event="cli_failed", status="failed", exit_code=1, run_id=run_id, report_path=report, human_log=human_log, event_log=event_log, lock_path=None, log_session=session)  # doctest: +SKIP


    :param args: Report replacement/format and stdout-publication controls.
    :param error: Original failure whose existing traceback is logged and serialized.
    :param event: Structured logging event name identifying the failure category.
    :param status: Terminal status text copied into the error receipt and stderr notice.
    :param exit_code: Requested return/report code, retained if report writing itself fails.
    :param run_id: Correlation UUID shared across failure artifacts and output.
    :param report_path: Destination attempted under the original replacement policy.
    :param human_log: Current human-readable log path recorded in the receipt.
    :param event_log: Current event-log path named in the receipt and stderr guidance.
    :param lock_path: Selected lock path or None, reported without reacquisition.
    :param log_session: Active logging session flushed after report publication is attempted.
    :param signal_number: Optional first signal included in context and the receipt.
    :return: The supplied exit_code if remaining failure reporting completes.
    """
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
    """
    Stage/fsync a serialized ingest report and publish by replacement or no-clobber link.

    Create parents before serialization, so even encoding failure may leave layout
    effects. Write ASCII-escaped JSON plus newline to a same-directory temporary file,
    then replace the destination or link without overwriting. Attempt directory fsync
    afterward. A later unlink/sync/close error can follow successful publication;
    final temporary cleanup ignores OSError and never rolls back the destination.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     path = Path(directory) / "report.json"
        ...     _write_report(path, {"ok": True}, replace=False, compact=True)
        ...     value = json.loads(path.read_text())
        >>> value
        {'ok': True}


    :param path: Report destination whose parents may be created recursively.
    :param payload: Rich ingest receipt serialized through _json_text before staging.
    :param replace: Permit replacing an existing destination; otherwise refuse atomically at linking.
    :param compact: Select compact separators rather than two-space JSON indentation.
    :return: None after publication and best-effort durability/cleanup.
    :raises CLIUsageError: No-clobber linking finds an occupied destination.
    """
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
    """
    Print and flush terminal JSON unless stdout reporting is disabled, ignoring a broken pipe.

    Serialization happens only when reporting is enabled. BrokenPipeError from
    rendering/printing is swallowed; other serialization/output failures propagate.

    Example:
        >>> _print_payload(argparse.Namespace(no_stdout_report=False, compact_json=True), {"ok": True})
        {"ok":true}


    :param args: no_stdout_report and compact_json controls.
    :param payload: Terminal receipt passed to the rich JSON serializer.
    :return: None after printing, opting out, or encountering a broken pipe.
    """
    if bool(args.no_stdout_report):
        return
    try:
        print(_json_text(payload, compact=bool(args.compact_json)), flush=True)
    except BrokenPipeError:
        pass


def _json_text(value: object, *, compact: bool) -> str:
    """
    Serialize rich ingest values as sorted-key, ASCII-escaped JSON without a trailing newline.

    Delegate unsupported standard types to _json_default. Default encoder behavior
    still permits NaN/infinity and can reject incompatible sortable mapping keys.

    Example:
        >>> _json_text({"path": Path("books"), "run": UUID(int=1)}, compact=True)
        '{"path":"books","run":"00000000-0000-0000-0000-000000000001"}'


    :param value: JSON-compatible value or supported rich record/path/date/enum object.
    :param compact: Use compact separators when true, otherwise two-space indentation.
    :return: JSON text with ASCII escapes and deterministic ordering for sortable keys.
    """
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        indent=None if compact else 2,
        separators=(",", ":") if compact else None,
        default=_json_default,
    )


def _json_default(value: object) -> object:
    """
    Project supported rich ingest values for the standard JSON encoder.

    Dataclass instances expose non-underscore fields without recursively copying
    them; dataclass classes are rejected. UUID/Path stringify, date/time objects use
    isoformat, and Enum values unwrap once for the encoder to process further.

    Example:
        >>> _json_default(date(2026, 9, 10))
        '2026-09-10'


    :param value: Object the standard encoder cannot serialize directly.
    :return: Public-field dictionary, path/UUID/date text, or the enum's underlying value.
    :raises TypeError: The object is not one of the supported rich value categories.
    """
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
    """
    Submit an ingest event with a fresh detail mapping and stringified run identity.

    The logger owns filtering, formatting, and persistence. No serialization,
    redaction, explicit flush, or exception suppression is performed here.

    Example:
        >>> _log(logging.INFO, "checkpoint", "Progress", run_id=UUID(int=1), adopted=3)  # doctest: +SKIP


    :param level: Logging severity passed to the compatibility logger.
    :param event: Structured event identifier stored in liuxin_event.
    :param message: Human-readable logging message.
    :param run_id: Correlation UUID stringified into liuxin_context.
    :param details: Additional context values shallow-copied into the event mapping.
    :return: None after the logging call returns, without promising durable delivery.
    """
    context = dict(details)
    context["run_id"] = str(run_id)
    _LOGGER.log(
        level,
        message,
        extra={"liuxin_event": event, "liuxin_context": context},
    )
