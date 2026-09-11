"""
Select ingest control paths, keep them outside discovery, and scope an advisory run lock.

Most selectors expand/resolve paths, but validation receives caller-prepared source,
report, and lock paths and uses lexical containment. Checks are observations, not
reservations or protection against every hard-link/concurrent alias. Log/report
publication and catalogue/materialization effects are not one filesystem transaction.
"""

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
    """
    Validate source, report/lock, database, and materialization paths in that order.

    Stop at the first error. Do not create anything, check all pairwise output
    collisions, or reserve a validated path against later changes.

    Example:
        >>> _validate_paths(args, source_root=source, report_path=report, lock_path=None)  # doctest: +SKIP


    :param args: Ingest options used for replacement, database, and cache validation.
    :param source_root: Caller-prepared source directory used as the exclusion prefix.
    :param report_path: Caller-prepared report destination, checked before database/cache paths.
    :param lock_path: Optional caller-prepared advisory lock path; None skips its check.
    :return: None when all ordered checks pass; later I/O can still fail.
    :raises CLIUsageError: A source/control/database/materialization check refuses the configuration.
    """
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
    """
    Require the source path to exist and currently refer to a directory.

    Example:
        >>> _validate_source_root(Path("."))


    :param source_root: Path checked with exists/is_dir; no resolution or symlink rejection is added.
    :return: None after both observations succeed, without proving read/traverse access.
    :raises CLIUsageError: Source is missing or is not observed as a directory.
    """
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
    """
    Refuse report/lock containment and an observed report collision without replacement.

    Caller paths are compared lexically, not resolved here. exists() follows links,
    so a dangling report symlink can pass this preflight; publication must still
    enforce no-clobber. Replacement permission does not prove destination usability.

    Example:
        >>> _validate_run_control_paths(args, source_root=source, report_path=report, lock_path=lock)  # doctest: +SKIP


    :param args: Namespace whose replace_report truthiness permits an existing report.
    :param source_root: Excluded directory prefix, normally already resolved by the caller.
    :param report_path: Report path checked for containment before existing-file ownership.
    :param lock_path: Optional lock path checked only for containment, not current lock ownership.
    :return: None after checks; no path is reserved and report/lock equality is not tested.
    :raises CLIUsageError: A control path is inside source or a non-replacing report already exists.
    """
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
    """
    Check a selected local catalogue lies outside source and satisfies existence/type policy.

    Expand/resolve the database selector, but do not rewrite args or inspect SQLite
    contents. A falsey selector is ignored. Permission and parent writability checks
    belong to preflight rather than this validation step.

    Example:
        >>> _validate_database_path(argparse.Namespace(database=None), source_root=Path("books"))


    :param args: database selector and require_existing_database flag.
    :param source_root: Caller-prepared exclusion prefix for the resolved catalogue path.
    :return: None when no database is selected or its observed path satisfies policy.
    :raises CLIUsageError: Catalogue lies in source, exists as a non-file, or a required file is absent.
    """
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
    """
    Reject a selected cache path whose resolved location falls within the source tree.

    This does not require an existing directory, test permissions, or reserve space.

    Example:
        >>> _validate_materialization_path(argparse.Namespace(materialization_root=None), source_root=Path("books"))


    :param args: Namespace containing an optional materialization_root selector.
    :param source_root: Caller-prepared source prefix compared with the expanded/resolved cache.
    :return: None for absent or outside-source cache selection.
    :raises CLIUsageError: The resolved cache is equal to or below the source prefix.
    """
    if not args.materialization_root:
        return
    materialization = Path(args.materialization_root).expanduser().resolve(strict=False)
    if _path_is_within(materialization, source_root):
        raise CLIUsageError("--materialization-root must be outside --source-root")


def _log_directory(args: argparse.Namespace, source_root: Path) -> Path:
    """
    Choose resolved logs from an explicit path, catalogue sibling, or hidden source sibling.

    An explicit inside-source path fails immediately. An inferred inside-source
    path falls back to the hidden source sibling, then fails if still contained.
    No directory is created and no permissions or collisions are checked here.

    Example:
        >>> path = _log_directory(argparse.Namespace(log_directory=None, database=None), Path("/books"))
        >>> path.name
        '.books.liuxin-ingest-logs'


    :param args: Optional log_directory and database selectors used in precedence order.
    :param source_root: Normally resolved source directory determining the final fallback.
    :return: Resolved log-directory path outside the supplied source prefix.
    :raises CLIUsageError: Explicit or fallback log selection remains within source.
    """
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
    """
    Select an explicit resolved report or replace the human-log suffix with .report.json.

    The default inherits the supplied log path's coordinate system. Only containment
    is checked here; existing-output refusal occurs during later path validation/publication.

    Example:
        >>> _report_path(argparse.Namespace(report_file=None), Path("books"), Path("logs/run.log")).name
        'run.report.json'


    :param args: Namespace with optional report_file override.
    :param source_root: Excluded source prefix for the selected report path.
    :param human_log: Run-specific human log used to derive the default report filename.
    :return: Selected report Path without creating or reserving it.
    :raises CLIUsageError: The report is equal to or below source_root.
    """
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
    """
    Disable locking for previews/opt-out, otherwise choose a resolved advisory lock path.

    Disabled/discovery/preflight modes ignore even an explicit lock selector.
    Real-run defaults combine the log directory with the database basename, not
    the database's full identity; separately selected log directories can yield
    different locks for the same catalogue. No lock is acquired here.

    Example:
        >>> args = argparse.Namespace(no_run_lock=True)
        >>> _lock_path(args, Path("books"), Path("logs")) is None
        True


    :param args: Lock opt-out/mode flags, optional lock_file, and database selector.
    :param source_root: Excluded source prefix for enabled lock selection.
    :param log_directory: Parent for the default .<database-name>.mixed-ingest.lock file.
    :return: Resolved lock path outside source, or None when locking is disabled.
    :raises CLIUsageError: An enabled lock path lies within the source tree.
    """
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
    """
    Hold an advisory file lock while recording this run's identity and yielding its handle.

    Create parents, acquire ExclusiveFile, truncate/write/flush the record, attempt
    chmod 0600, then log acquisition before yielding. The record's started_utc key
    actually uses a timezone-aware local timestamp; contents are flushed, not fsynced.
    Closing releases the lock but leaves its record on disk. Any LockError from the
    managed scope, including the body, is translated as a competing-ingest refusal.

    Example:
        >>> with _acquire_run_lock(lock_path, run_id=run_id, args=args):  # doctest: +SKIP
        ...     execute_ingest()


    :param path: Selected advisory lock path whose parent may be created.
    :param run_id: Correlation UUID serialized into the lock record and acquisition event.
    :param args: lock_timeout_seconds, source_root, and database values used in the record.
    :return: Context manager yielding the locked read/write file until its scope exits.
    :raises CLIUsageError: ExclusiveFile or the protected scope raises LockError.
    """
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
