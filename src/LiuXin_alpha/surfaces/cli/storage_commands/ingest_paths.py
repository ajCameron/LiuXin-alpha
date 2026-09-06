"""Storage CLI ingest paths ownership."""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.surfaces.cli.storage_commands.constants import CLIUsageError
from LiuXin_alpha.surfaces.cli.storage_commands.filesystem import _path_is_within
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_reporting import _log
from LiuXin_alpha.utils.lock import ExclusiveFile, LockError


def _validate_paths(
    args: argparse.Namespace,
    *,
    source_root: Path,
    report_path: Path,
    lock_path: Path | None,
) -> None:
    _validate_source_root(source_root)
    _validate_run_control_paths(
        args,
        source_root=source_root,
        report_path=report_path,
        lock_path=lock_path,
    )
    _validate_database_path(args, source_root=source_root)
    _validate_materialization_path(args, source_root=source_root)


def _validate_source_root(source_root: Path) -> None:
    if not source_root.exists():
        raise CLIUsageError(f"source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise CLIUsageError(f"source root is not a directory: {source_root}")


def _validate_run_control_paths(
    args: argparse.Namespace,
    *,
    source_root: Path,
    report_path: Path,
    lock_path: Path | None,
) -> None:
    if _path_is_within(report_path, source_root):
        raise CLIUsageError("--report-file must be outside --source-root")
    if report_path.exists() and not bool(args.replace_report):
        raise CLIUsageError(
            f"report file already exists: {report_path}; pass --replace-report"
        )
    if lock_path is not None and _path_is_within(lock_path, source_root):
        raise CLIUsageError("--lock-file must be outside --source-root")


def _validate_database_path(
    args: argparse.Namespace,
    *,
    source_root: Path,
) -> None:
    if not args.database:
        return
    database_path = Path(args.database).expanduser().resolve(strict=False)
    if _path_is_within(database_path, source_root):
        raise CLIUsageError("--database must be outside --source-root")
    if database_path.exists() and not database_path.is_file():
        raise CLIUsageError(f"database path is not a file: {database_path}")
    if bool(args.require_existing_database) and not database_path.is_file():
        raise CLIUsageError(f"database does not exist: {database_path}")


def _validate_materialization_path(
    args: argparse.Namespace,
    *,
    source_root: Path,
) -> None:
    if not args.materialization_root:
        return
    materialization = Path(args.materialization_root).expanduser().resolve(strict=False)
    if _path_is_within(materialization, source_root):
        raise CLIUsageError("--materialization-root must be outside --source-root")


def _log_directory(args: argparse.Namespace, source_root: Path) -> Path:
    if args.log_directory:
        selected = Path(args.log_directory).expanduser().resolve(strict=False)
        if _path_is_within(selected, source_root):
            raise CLIUsageError("--log-directory must be outside --source-root")
    elif args.database:
        database_path = Path(args.database).expanduser().resolve(strict=False)
        selected = database_path.with_name(database_path.name + ".ingest-logs")
    else:
        selected = source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
    selected = selected.resolve(strict=False)
    if _path_is_within(selected, source_root):
        selected = (
            source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
        ).resolve(strict=False)
    if _path_is_within(selected, source_root):
        raise CLIUsageError("the log directory must be outside --source-root")
    return selected


def _report_path(
    args: argparse.Namespace,
    source_root: Path,
    human_log: Path,
) -> Path:
    if args.report_file:
        path = Path(args.report_file).expanduser().resolve(strict=False)
    else:
        path = human_log.with_suffix(".report.json")
    if _path_is_within(path, source_root):
        raise CLIUsageError("--report-file must be outside --source-root")
    return path


def _lock_path(
    args: argparse.Namespace,
    source_root: Path,
    log_directory: Path,
) -> Path | None:
    if bool(args.no_run_lock) or bool(args.discover_only) or bool(args.preflight_only):
        return None
    if args.lock_file:
        path = Path(args.lock_file).expanduser().resolve(strict=False)
    else:
        database_name = Path(str(args.database)).name or "catalogue"
        path = (log_directory / f".{database_name}.mixed-ingest.lock").resolve(
            strict=False
        )
    if _path_is_within(path, source_root):
        raise CLIUsageError("--lock-file must be outside --source-root")
    return path


@contextmanager
def _acquire_run_lock(
    path: Path,
    *,
    run_id: UUID,
    args: argparse.Namespace,
) -> Generator[object, None, None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with ExclusiveFile(
            str(path),
            timeout=int(args.lock_timeout_seconds),
        ) as lock_file:
            lock_file.seek(0)
            lock_file.truncate()
            record = {
                "run_id": str(run_id),
                "process_id": os.getpid(),
                "hostname": socket.gethostname(),
                "started_utc": datetime.now().astimezone().isoformat(),
                "source_root": str(
                    Path(args.source_root).expanduser().resolve(strict=False)
                ),
                "database": str(Path(args.database).expanduser().resolve(strict=False)),
            }
            _ = lock_file.write(
                (json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n").encode(
                    "utf-8"
                )
            )
            lock_file.flush()
            try:
                path.chmod(0o600)
            except OSError:
                pass
            _log(
                logging.INFO,
                "run_lock_acquired",
                "Mixed ingest run lock acquired",
                run_id=run_id,
                lock_file=str(path),
            )
            yield lock_file
    except LockError as error:
        raise CLIUsageError(
            f"another ingest owns run lock {path}; wait or use --no-run-lock"
        ) from error
