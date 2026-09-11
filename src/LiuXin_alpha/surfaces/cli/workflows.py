"""
Adapt managed ingest, conversion, backup, and upkeep operations to the CLI.

Specifications/options are loaded on the CLI host; managed workflow paths refer
to the Core host. Shared adapters publish receipts after session exit, without
undoing accepted work on publication failure. Ordinary query/command receipts
do not determine exit status; jobs use the common execution-status policy.

SQLite backup verification and offline restore are exceptions to remote dispatch:
they access CLI-host files directly. Restore keeps a safety copy and atomically
replaces the target pathname, but its preliminary offline check is not a lock held
through replacement and failures after replacement do not automatically roll back.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sqlite3
import stat
import tempfile

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_job_execution_arguments,
    add_json_output,
    emit_json,
    execution_exit_code,
    load_json_object,
    open_cli_core,
    submit_job,
)
from LiuXin_alpha.surfaces.cli.ingest_runs import build_ingest_runs_parser
from LiuXin_alpha.surfaces.system_profile import apply_system_profile


def _core_json(parser: argparse.ArgumentParser) -> None:
    """
    Declare Core connection selectors and JSON output controls on a leaf parser.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _core_json(parser)
        >>> parser.parse_args(["--database", "db.sqlite"]).database
        'db.sqlite'


    :param parser: Mutable command parser receiving the shared arguments.
    :return: None; declaration performs no connection or publication.
    """
    add_connection_arguments(parser)
    add_json_output(parser)


def _query(
    args: argparse.Namespace,
    operation: str,
    payload: dict[str, Any],
    *,
    maintenance: bool = False,
) -> int:
    """
    Query Core with storage and optional maintenance composition, then emit JSON.

    Pass the payload unchanged and publish only after session exit. Receipt
    status is not interpreted; connection, query, cleanup, and output errors escape.

    Example:
        >>> _query(args, "maintenance.status", {}, maintenance=True)  # doctest: +SKIP


    :param args: Connection selectors and JSON output controls.
    :param operation: Exact Core query name to dispatch.
    :param payload: Request mapping passed by identity without validation.
    :param maintenance: Whether local Core composition should enable maintenance.
    :return: Zero after publication, regardless of receipt-level success flags.
    """
    with open_cli_core(
        args,
        enable_storage_manager=True,
        enable_maintenance=maintenance,
    ) as core:
        result = core.query(operation, payload)
    emit_json(result, args)
    return 0


def _command(
    args: argparse.Namespace,
    operation: str,
    payload: dict[str, Any],
    *,
    maintenance: bool = False,
) -> int:
    """
    Execute a Core command with optional maintenance and publish after exit.

    No generic confirmation or receipt-status check is supplied. Exceptions
    propagate, and output/cleanup failure does not undo an accepted mutation.

    Example:
        >>> _command(args, "database.backup", {"verify": True})  # doctest: +SKIP


    :param args: Connection selectors and JSON output controls.
    :param operation: Exact Core command name to dispatch.
    :param payload: Request dictionary passed without copying or validation.
    :param maintenance: Whether local Core composition should enable maintenance.
    :return: Zero after publication, without interpreting the command receipt.
    """
    with open_cli_core(
        args,
        enable_storage_manager=True,
        enable_maintenance=maintenance,
    ) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0


def _job(
    args: argparse.Namespace,
    operation: str,
    payload: dict[str, Any],
) -> int:
    """
    Submit a managed job using the shared wait/detach and exit-status policy.

    The submitter may augment payload in place and wait unless detached. Publish
    the result only after leaving the storage-enabled session, then project the
    narrow execution status. A zero exit is not a claim about every nested report.
    Accepted jobs are not cancelled merely because later publication fails.

    Example:
        >>> _job(args, "backup.workflow.start", {"workflow_id": 7})  # doctest: +SKIP


    :param args: Connection, JSON output, job tag, wait/detach, and polling options.
    :param operation: Core start command expected to return a managed job receipt.
    :param payload: Mutable job request passed to submit_job without copying.
    :return: The common execution_exit_code after JSON output, normally zero or one.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = submit_job(core, operation, payload, args)
    emit_json(result, args)
    return execution_exit_code(result)


def cmd_ingest_formats(args: argparse.Namespace) -> int:
    """
    Publish Core's supported ingest formats without starting an ingest job.

    Example:
        >>> cmd_ingest_formats(parsed_ingest_formats_args)  # doctest: +SKIP


    :param args: Common Core selection and JSON output controls.
    :return: Zero after publishing ingest.formats' receipt.
    """
    return _query(args, "ingest.formats", {})


def cmd_ingest_disk(args: argparse.Namespace) -> int:
    """
    Submit filesystem ingestion for a source path visible to the Core host.

    Translate negative hash/link/refresh flags into positive payload booleans.
    Include store name/source label only when truthy, and copy truthy extensions
    to a list without deduplication. The CLI neither scans nor validates the path.

    Example:
        >>> cmd_ingest_disk(parsed_ingest_disk_args)  # doctest: +SKIP


    :param args: Common job controls plus disk_path, no_hash, follow_symlinks,
        no_store_links, no_refresh, store_name, extension, and source_label.
    :return: Shared job status after publishing the ingest.disk.start result.
    """
    payload: dict[str, Any] = {
        "disk_path": args.disk_path,
        "compute_hash": not args.no_hash,
        "follow_symlinks": bool(args.follow_symlinks),
        "attach_store_links": not args.no_store_links,
        "refresh_storage_manager": not args.no_refresh,
    }
    if args.store_name:
        payload["store_name"] = args.store_name
    if args.extension:
        payload["ebook_extensions"] = list(args.extension)
    if args.source_label:
        payload["source_label"] = args.source_label
    return _job(args, "ingest.disk.start", payload)


def cmd_ingest_remote_html(args: argparse.Namespace) -> int:
    """
    Submit HTML-source ingestion using an options object loaded on the CLI host.

    Any paths inside the options are interpreted by Core; this adapter does not
    fetch the remote source. Parser choices constrain kind, not this handler.

    Example:
        >>> cmd_ingest_remote_html(parsed_remote_html_args)  # doctest: +SKIP


    :param args: Common job controls, kind, and options_file JSON selector.
    :return: Shared job status after publishing ingest.remote-html.start's result.
    """
    return _job(
        args,
        "ingest.remote-html.start",
        {"kind": args.kind, "options": load_json_object(args.options_file)},
    )


def cmd_conversion_formats(args: argparse.Namespace) -> int:
    """
    Publish Core's conversion format discovery without converting a document.

    Example:
        >>> cmd_conversion_formats(parsed_conversion_formats_args)  # doctest: +SKIP


    :param args: Common connection selectors and JSON output controls.
    :return: Zero after publishing conversion.formats' receipt.
    """
    return _query(args, "conversion.formats", {})


def cmd_conversion_options(args: argparse.Namespace) -> int:
    """
    Inspect conversion options for an input/output path pair on the Core host.

    The original path values pass through; no CLI-host existence check is made.

    Example:
        >>> cmd_conversion_options(parsed_conversion_options_args)  # doctest: +SKIP


    :param args: Common controls plus input_path and output_path selectors.
    :return: Zero after publishing conversion.options' receipt.
    """
    return _query(
        args,
        "conversion.options",
        {"input_path": args.input_path, "output_path": args.output_path},
    )


def cmd_conversion_run(args: argparse.Namespace) -> int:
    """
    Submit a conversion between Core-host paths with CLI-loaded option overrides.

    A None options_file supplies an empty object; any other selector is loaded
    before dispatch. Core owns file access, converter selection, and execution.

    Example:
        >>> cmd_conversion_run(parsed_conversion_run_args)  # doctest: +SKIP


    :param args: Common job controls, input_path, output_path, and options_file.
    :return: Shared job status after publishing conversion.start's result.
    """
    return _job(
        args,
        "conversion.start",
        {
            "input_path": args.input_path,
            "output_path": args.output_path,
            "options": (
                {} if args.options_file is None else load_json_object(args.options_file)
            ),
        },
    )


def cmd_backup_plan(args: argparse.Namespace) -> int:
    """
    Request a bounded backup-pack plan between two configured Stores.

    Convert target_pack_mib to bytes using 1,048,576 bytes per MiB and int
    truncation. Include non-None max-files, truthy workflow prefix, and a copied
    truthy extension list. No positivity, finite-number, or Store validation is
    done here; this query does not itself save or execute the plan.

    Example:
        >>> cmd_backup_plan(parsed_backup_plan_args)  # doctest: +SKIP


    :param args: Common controls, source_store, destination_store, target_pack_mib,
        output_key_prefix, workflow_name_prefix, max_files_per_pack, and extension.
    :return: Zero after publishing backup.plan's receipt.
    """
    payload: dict[str, Any] = {
        "source_store": args.source_store,
        "destination_store": args.destination_store,
        "target_pack_size_bytes": int(args.target_pack_mib * 1024 * 1024),
        "output_key_prefix": args.output_key_prefix,
    }
    if args.workflow_name_prefix:
        payload["workflow_name_prefix"] = args.workflow_name_prefix
    if args.max_files_per_pack is not None:
        payload["max_files_per_pack"] = int(args.max_files_per_pack)
    if args.extension:
        payload["allowed_extensions"] = list(args.extension)
    return _query(args, "backup.plan", payload)


def cmd_backup_workflows_list(args: argparse.Namespace) -> int:
    """
    Request a page of persisted backup workflows, without running them.

    Example:
        >>> cmd_backup_workflows_list(parsed_backup_workflows_args)  # doctest: +SKIP


    :param args: Common controls and integer-convertible limit/offset; values
        are not clamped to positive ranges locally.
    :return: Zero after publishing backup.workflows.list's receipt.
    """
    return _query(
        args,
        "backup.workflows.list",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_backup_workflow_show(args: argparse.Namespace) -> int:
    """
    Retrieve one persisted backup workflow by integer-converted identifier.

    Example:
        >>> cmd_backup_workflow_show(parsed_backup_show_args)  # doctest: +SKIP


    :param args: Common controls and workflow_id to resolve on Core.
    :return: Zero after publishing backup.workflow.get's receipt.
    """
    return _query(
        args, "backup.workflow.get", {"workflow_id": int(args.workflow_id)}
    )


def cmd_backup_workflow_save(args: argparse.Namespace) -> int:
    """
    Persist a backup workflow object loaded from CLI-host JSON.

    Wrap it as workflow_spec and include an integer workflow_id whenever non-None,
    including zero. Core owns validation and creation/update semantics.

    Example:
        >>> cmd_backup_workflow_save(parsed_backup_save_args)  # doctest: +SKIP


    :param args: Common controls, spec_file, and optional workflow_id.
    :return: Zero after publishing backup.workflow.save's receipt.
    """
    payload: dict[str, Any] = {"workflow_spec": load_json_object(args.spec_file)}
    if args.workflow_id is not None:
        payload["workflow_id"] = int(args.workflow_id)
    return _command(args, "backup.workflow.save", payload)


def cmd_backup_workflow_run(args: argparse.Namespace) -> int:
    """
    Submit execution of an already persisted backup workflow.

    Example:
        >>> cmd_backup_workflow_run(parsed_backup_run_args)  # doctest: +SKIP


    :param args: Common job controls and integer-convertible workflow_id.
    :return: Shared job status after publishing backup.workflow.start's result.
    """
    return _job(
        args,
        "backup.workflow.start",
        {"workflow_id": int(args.workflow_id)},
    )


def cmd_backup_squashfs_run(args: argparse.Namespace) -> int:
    """
    Submit an ad-hoc SquashFS backup declaration with verification/cleanup policy.

    Load spec_file on the CLI host; staging_root and paths within the declaration
    belong to Core. Verification defaults on; staging cleanup is opt-in and is
    requested only after success. The CLI does not build or remove packs itself.

    Example:
        >>> cmd_backup_squashfs_run(parsed_backup_squashfs_args)  # doctest: +SKIP


    :param args: Common job controls, spec_file, no_verify, cleanup_staging,
        and optional truthy staging_root.
    :return: Shared job status after publishing backup.squashfs.start's result.
    """
    payload: dict[str, Any] = {
        "workflow_spec": load_json_object(args.spec_file),
        "verify_after_build": not args.no_verify,
        "cleanup_staging_after_success": bool(args.cleanup_staging),
    }
    if args.staging_root:
        payload["staging_root"] = args.staging_root
    return _job(args, "backup.squashfs.start", payload)


def cmd_database_query(args: argparse.Namespace) -> int:
    """
    Publish a database inspection receipt selected by the database action token.

    The parser limits these leaves to info, summary, and telemetry; direct callers
    are not checked against that list before the 'database.' prefix is added.

    Example:
        >>> cmd_database_query(parsed_database_info_args)  # doctest: +SKIP


    :param args: Common controls and database_action naming the query suffix.
    :return: Zero after publishing the selected database query receipt.
    """
    return _query(args, "database." + args.database_action, {})


def cmd_database_backup(args: argparse.Namespace) -> int:
    """
    Request the configured Core database driver's backup operation.

    Verification is boolean-coerced; a truthy output_path is forwarded as a
    Core-host destination. This differs from local backup verification/restore.

    Example:
        >>> cmd_database_backup(parsed_database_backup_args)  # doctest: +SKIP


    :param args: Common controls, verify flag, and optional output_path.
    :return: Zero after publishing database.backup's receipt, regardless of its ok.
    """
    payload: dict[str, Any] = {"verify": bool(args.verify)}
    if args.output_path:
        payload["output_path"] = args.output_path
    return _command(args, "database.backup", payload)


def cmd_database_vacuum(args: argparse.Namespace) -> int:
    """
    Request database vacuum only after confirmation, with no preview fallback.

    Example:
        >>> cmd_database_vacuum(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: Database vacuum requires --yes.


    :param args: Common controls and a truthy yes flag to permit dispatch.
    :return: Zero after publishing the confirmed database.vacuum receipt.
    :raises ValueError: Confirmation is absent, before any Core connection.
    """
    if not args.yes:
        raise ValueError("Database vacuum requires --yes.")
    return _command(args, "database.vacuum", {})


def _sha256(path: Path) -> str:
    """
    Hash a local file incrementally using one-MiB binary reads.

    This is a digest of the bytes observed, not a lock or stable-file guarantee.
    Open/read errors propagate.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     path = Path(directory) / "empty"
        ...     _ = path.write_bytes(b"")
        ...     digest = _sha256(path)
        >>> digest == hashlib.sha256(b"").hexdigest()
        True


    :param path: CLI-host file to open for binary reading.
    :return: Lowercase 64-character SHA-256 hexadecimal digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            content = stream.read(1024 * 1024)
            if not content:
                break
            digest.update(content)
    return digest.hexdigest()


def verify_sqlite_backup(path: str | Path, *, full: bool = False) -> dict[str, Any]:
    """
    Check a local SQLite file read-only and report integrity, size, and digest.

    Expand/resolve the path, run quick_check or integrity_check, count tables, and
    read user_version. Success requires exactly ['ok'] messages and at least one
    table; it does not prove a LiuXin schema or application-level completeness.
    SQLite errors, including connection/close failures, become an ok=False error
    report. File/path/stat/hash errors propagate instead. Size and digest are read
    after closing SQLite, so all reported facts are not an atomic snapshot.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     path = Path(directory) / "backup.sqlite"
        ...     connection = sqlite3.connect(path)
        ...     _ = connection.execute("CREATE TABLE sample (id INTEGER)")
        ...     connection.close()
        ...     report = verify_sqlite_backup(path)
        >>> report["ok"], report["check"], report["table_count"]
        (True, 'quick_check', 1)


    :param path: CLI-host SQLite/APSW database-file path to inspect.
    :param full: Use integrity_check instead of the default quick_check.
    :return: Verification dictionary with ok/path/check and either SQLite error
        details or messages, table_count, user_version, size_bytes, and sha256.
    :raises FileNotFoundError: The resolved selector is not an existing file.
    """
    selected = Path(path).expanduser().resolve(strict=False)
    if not selected.is_file():
        raise FileNotFoundError("Backup file does not exist: {!s}".format(selected))
    pragma = "integrity_check" if full else "quick_check"
    try:
        connection = sqlite3.connect(selected.as_uri() + "?mode=ro", uri=True)
        try:
            messages = [str(row[0]) for row in connection.execute("PRAGMA " + pragma)]
            table_count = int(
                connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table'"
                ).fetchone()[0]
            )
            user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        finally:
            connection.close()
    except sqlite3.Error as error:
        return {
            "ok": False,
            "path": str(selected),
            "check": pragma,
            "error": str(error),
            "error_type": type(error).__name__,
        }
    ok = messages == ["ok"] and table_count > 0
    return {
        "ok": ok,
        "path": str(selected),
        "check": pragma,
        "messages": messages,
        "table_count": table_count,
        "user_version": user_version,
        "size_bytes": selected.stat().st_size,
        "sha256": _sha256(selected),
    }


def cmd_database_verify_backup(args: argparse.Namespace) -> int:
    """
    Publish verification of a CLI-host SQLite backup without opening Core.

    Example:
        >>> cmd_database_verify_backup(parsed_backup_verify_args)  # doctest: +SKIP


    :param args: backup_file, full integrity-check flag, and JSON output controls.
    :return: Zero if local verification reports ok, otherwise one, after output.
    """
    result = verify_sqlite_backup(args.backup_file, full=bool(args.full))
    emit_json(result, args)
    return 0 if result["ok"] else 1


def _ensure_offline_sqlite(path: Path) -> None:
    """
    Reject SQLite companion files and test a momentary exclusive transaction.

    Existing -wal/-shm paths cause refusal; otherwise connect with zero timeout,
    begin exclusive, roll back, and close. The lock is released before return:
    callers must independently keep all processes stopped through restoration.
    No process is stopped, persistent lock held, or rollback-journal check added.
    A direct call on a missing path may create a SQLite file through connect.

    Example:
        >>> _ensure_offline_sqlite(stopped_catalogue_path)  # doctest: +SKIP


    :param path: Existing local catalogue path, checked by the restore caller.
    :return: None when companion and exclusive-transaction checks complete.
    :raises ValueError: A companion exists or SQLite raises during the check.
    """
    for suffix in ("-wal", "-shm"):
        companion = Path(str(path) + suffix)
        if companion.exists():
            raise ValueError(
                "Refusing offline restore while SQLite companion file exists: {!s}. "
                "Stop every LiuXin process and checkpoint/close the database first."
                .format(companion)
            )
    try:
        connection = sqlite3.connect(str(path), timeout=0.0)
        try:
            connection.execute("BEGIN EXCLUSIVE")
            connection.rollback()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise ValueError(
            "Could not acquire an exclusive offline database check; stop every "
            "LiuXin process before restoring: {}".format(error)
        ) from error


def _fsync_parent(path: Path) -> None:
    """
    Sync a containing directory after replacement when it can be opened.

    Suppress OSError only from opening the directory. Errors from fsync or close
    propagate and can therefore be reported after the target has been replaced.

    Example:
        >>> _fsync_parent(restored_catalogue_path)  # doctest: +SKIP


    :param path: Published local file whose parent directory should be synced.
    :return: None after syncing, or when opening the directory is unsupported.
    """
    try:
        descriptor = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def cmd_database_restore(args: argparse.Namespace) -> int:
    """
    Restore an offline CLI-host SQLite catalogue while retaining a safety copy.

    Require yes before applying profile selection in place. Reject remote Core
    and non-SQLite/APSW drivers, equal resolved source/target paths, invalid backup
    integrity, a missing target, and the offline-check hazards. The caller must
    keep all catalogue users stopped: the exclusive check releases its lock before
    copying. Distinct hard links are not identified by the path-equality check.

    Copy the current target to the explicit safety path or a timestamped sibling,
    then compare hashes. The prior safety-existence check is not an exclusive-create
    guarantee against races. Stage source bytes beside the target, flush/fsync,
    preserve target permission bits, and require the verified source digest before
    atomic pathname replacement. Attempt parent sync and temporary-file cleanup,
    then verify the replacement and publish its report. Cleanup errors propagate.

    The safety copy remains on subsequent failure. There is no automatic rollback:
    parent sync, verification, or output can fail after replacement has succeeded.
    Staging preserves permission bits, not all ownership/metadata, and safety-copy
    hashing alone is not an explicit fsync durability guarantee.

    Example:
        >>> cmd_database_restore(argparse.Namespace(yes=False))
        Traceback (most recent call last):
        ...
        ValueError: Database restore requires --yes after reviewing the backup.


    :param args: Local connection/profile selectors, backup_file, safety_backup,
        full_verify, yes, and JSON output controls; profile resolution mutates args.
    :return: Zero after successful final verification/output, one if final
        verification returns ok=False; preflight, copy, and output failures raise.
    :raises ValueError: Confirmation, selection, source verification, or offline
        preflight rejects the request.
    :raises FileNotFoundError: A required source, target, or safety directory is absent.
    :raises FileExistsError: The selected safety path already exists at the check.
    :raises OSError: File operations or safety/staged digest verification fail.
    """
    if not args.yes:
        raise ValueError("Database restore requires --yes after reviewing the backup.")
    apply_system_profile(args)
    if getattr(args, "core_endpoint", None):
        raise ValueError(
            "Database restore is an offline host operation; use --database or a "
            "path-backed --system-root/profile, not --core-endpoint."
        )
    if str(getattr(args, "db_type", "SQLite")).casefold() not in {"sqlite", "apsw"}:
        raise ValueError(
            "Portable file restore currently supports SQLite/APSW only; use the "
            "PostgreSQL server's pg_restore/base-backup tooling for PostgreSQL."
        )
    target = Path(str(args.database)).expanduser().resolve(strict=False)
    source = Path(args.backup_file).expanduser().resolve(strict=False)
    if source == target:
        raise ValueError("Backup and target database paths must differ.")
    verification = verify_sqlite_backup(source, full=bool(args.full_verify))
    if not verification["ok"]:
        raise ValueError("Refusing to restore a backup that failed integrity checks.")
    if not target.is_file():
        raise FileNotFoundError(
            "Target catalogue does not exist: {!s}; use `liuxin init` for a new system."
            .format(target)
        )
    _ensure_offline_sqlite(target)
    if args.safety_backup:
        safety = Path(args.safety_backup).expanduser().resolve(strict=False)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safety = target.with_name(target.name + ".before-restore-" + stamp)
    if safety.exists():
        raise FileExistsError("Safety backup already exists: {!s}".format(safety))
    if safety.parent != target.parent and not safety.parent.is_dir():
        raise FileNotFoundError("Safety-backup directory does not exist: {!s}".format(safety.parent))
    shutil.copy2(target, safety)
    if _sha256(target) != _sha256(safety):
        raise OSError("Safety backup hash does not match the current catalogue.")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".{}-restore-".format(target.name), suffix=".tmp", dir=str(target.parent)
    )
    staged = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w+b") as output, source.open("rb") as input_stream:
            descriptor = -1
            shutil.copyfileobj(input_stream, output, length=1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(staged, stat.S_IMODE(target.stat().st_mode))
        if _sha256(staged) != verification["sha256"]:
            raise OSError("Staged restore hash does not match the verified backup.")
        os.replace(staged, target)
        _fsync_parent(target)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            staged.unlink()
        except FileNotFoundError:
            pass
    restored = verify_sqlite_backup(target, full=bool(args.full_verify))
    result = {
        "ok": bool(restored["ok"]),
        "restored": str(target),
        "from": str(source),
        "safety_backup": str(safety),
        "verification": restored,
    }
    emit_json(result, args)
    return 0 if result["ok"] else 1


def cmd_database_migrations(args: argparse.Namespace) -> int:
    """
    Inspect migrations, preview a Core plan, or apply a confirmed unblocked plan.

    Status/plan actions use ordinary queries. Other direct-call actions enter the
    apply path, which queries a fresh plan even without yes. Preview is emitted
    inside the session and returns zero even for a blocked plan. Confirmed plans
    require truthy plan.ok; blocked output returns one. A dispatched apply is
    reported as applied=True without inspecting its receipt's success flag.
    Planning and application are separate calls, not an adapter-owned transaction.

    Example:
        >>> cmd_database_migrations(parsed_migrations_apply_args)  # doctest: +SKIP


    :param args: Common controls, migrations_action, and yes for the apply path.
    :return: Zero for query/preview/dispatched apply; one for a confirmed blocked
        plan. Core and publication exceptions propagate instead of becoming codes.
    """
    if args.migrations_action in {"status", "plan"}:
        return _query(args, "database.migrations." + args.migrations_action, {})
    with open_cli_core(args, enable_storage_manager=True) as core:
        plan = core.query("database.migrations.plan", {})
        if not args.yes:
            emit_json(
                {
                    "preview": True,
                    "plan": plan,
                    "message": "No migrations were applied; pass --yes to execute.",
                },
                args,
            )
            return 0
        if not bool(plan.get("ok", False)):
            emit_json(
                {"applied": False, "plan": plan, "error": "Migration plan is blocked."},
                args,
            )
            return 1
        result = core.command("database.migrations.apply", {})
    emit_json({"applied": True, "plan": plan, "result": result}, args)
    return 0


def cmd_maintenance_status(args: argparse.Namespace) -> int:
    """
    Query maintenance readiness with the local maintenance service enabled.

    Example:
        >>> cmd_maintenance_status(parsed_maintenance_status_args)  # doctest: +SKIP


    :param args: Common Core connection and JSON output controls.
    :return: Zero after publishing maintenance.status' receipt.
    """
    return _query(args, "maintenance.status", {}, maintenance=True)


def cmd_maintenance_duplicates(args: argparse.Namespace) -> int:
    """
    Ask maintenance to find duplicate column values without requesting a repair.

    Table, column, and comparison tokens are forwarded unchanged for Core to
    validate; this adapter does not inspect or compare rows locally.

    Example:
        >>> cmd_maintenance_duplicates(parsed_duplicate_args)  # doctest: +SKIP


    :param args: Common controls plus table, column, and comparison policy.
    :return: Zero after publishing maintenance.duplicates.find's receipt.
    """
    return _query(
        args,
        "maintenance.duplicates.find",
        {"table": args.table, "column": args.column, "comparison": args.comparison},
        maintenance=True,
    )


def cmd_maintenance_run(args: argparse.Namespace) -> int:
    """
    Preview a maintenance run locally, or execute it after confirmation.

    Preview does not open Core or inspect its queue. max_events is integer-
    converted but not range-checked; only execution enables maintenance services.

    Example:
        >>> cmd_maintenance_run(parsed_maintenance_run_args)  # doctest: +SKIP


    :param args: Common controls, max_events count, and yes execution flag.
    :return: Zero after preview or maintenance.run receipt publication.
    """
    if not args.yes:
        emit_json(
            {
                "preview": True,
                "operation": "maintenance.run",
                "max_events": int(args.max_events),
                "message": "No maintenance plugins were run; pass --yes to execute.",
            },
            args,
        )
        return 0
    return _command(
        args,
        "maintenance.run",
        {"max_events": int(args.max_events)},
        maintenance=True,
    )


def cmd_maintenance_clean(args: argparse.Namespace) -> int:
    """
    Preview or execute cleaning for explicitly selected rows in one table.

    Copy row_id to a list, preserving order and duplicates; the parser normally
    supplies integers. Without yes, output a local preview without verifying rows
    or opening Core. Confirmed execution enables the maintenance service.

    Example:
        >>> cmd_maintenance_clean(parsed_maintenance_clean_args)  # doctest: +SKIP


    :param args: Common controls, table, row_id sequence, and yes execution flag.
    :return: Zero after preview or maintenance.clean receipt publication.
    """
    payload = {"table": args.table, "row_ids": list(args.row_id)}
    if not args.yes:
        emit_json(
            {
                "preview": True,
                "operation": "maintenance.clean",
                **payload,
                "message": "No rows were cleaned; pass --yes to execute.",
            },
            args,
        )
        return 0
    return _command(args, "maintenance.clean", payload, maintenance=True)


def cmd_maintenance_merge(args: argparse.Namespace) -> int:
    """
    Preview or execute merging one row into another within a named table.

    Integer-convert both IDs without checking equality/existence. Preview opens
    no Core; only confirmed execution enables maintenance and dispatches the merge.

    Example:
        >>> cmd_maintenance_merge(parsed_maintenance_merge_args)  # doctest: +SKIP


    :param args: Common controls, table, retained_id, merged_id, and yes flag.
    :return: Zero after preview or maintenance.merge receipt publication.
    """
    payload = {
        "table": args.table,
        "retained_id": int(args.retained_id),
        "merged_id": int(args.merged_id),
    }
    if not args.yes:
        emit_json(
            {
                "preview": True,
                "operation": "maintenance.merge",
                **payload,
                "message": "No rows were merged; pass --yes to execute.",
            },
            args,
        )
        return 0
    return _command(args, "maintenance.merge", payload, maintenance=True)


def _job_parser(parser: argparse.ArgumentParser) -> None:
    """
    Add connection, JSON, and managed-job execution controls to a command leaf.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _job_parser(parser)
        >>> parser.parse_args(["--detach"]).detach
        True


    :param parser: Mutable leaf parser receiving common job arguments.
    :return: None; no job is submitted while declaring the arguments.
    """
    _core_json(parser)
    add_job_execution_arguments(parser)


def build_ingest_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register managed ingest discovery, run-history, disk, and HTML-source leaves.

    Explicit disk/remote-html paths are Core-host paths; the options JSON itself
    is CLI-host input. Simple local-ingest shortcut routing is owned by the outer
    CLI, not implemented by this builder. Required actions and handler defaults
    are declared without scanning files, loading options, or submitting work.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_ingest_parser(root.add_subparsers())
        >>> args = root.parse_args(["ingest", "disk", "/srv/incoming"])
        >>> args.disk_path, args.no_hash, args.follow_symlinks
        ('/srv/incoming', False, False)


    :param subparsers: Root CLI collection receiving the required ingest family.
    :return: None; discovery/history and job leaves are registered in place.
    """
    parser = subparsers.add_parser(
        "ingest",
        help="Point LiuXin at local material or submit a managed Core-host ingest.",
        description=(
            "Point LiuXin at a local mess with `liuxin ingest SOURCE "
            "--system-root ROOT`. The explicit subcommands below submit "
            "managed workflows whose paths are interpreted on the Core host."
        ),
        epilog=(
            "Simple local form: liuxin ingest /media/books --system-root "
            "/srv/liuxin\nOptions-first form: liuxin ingest --system-root "
            "/srv/liuxin --source /media/books\nManaged Core-host form: liuxin ingest disk "
            "/srv/incoming --core-endpoint http://127.0.0.1:8765"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="ingest_command", required=True)
    build_ingest_runs_parser(commands)
    formats = commands.add_parser("formats")
    _core_json(formats)
    formats.set_defaults(handler=cmd_ingest_formats)
    disk = commands.add_parser("disk", help="Ingest a filesystem tree visible on the Core host.")
    _job_parser(disk)
    disk.add_argument("disk_path", help="Source path on the Core host.")
    disk.add_argument("--store-name")
    disk.add_argument("--extension", action="append")
    disk.add_argument("--source-label")
    disk.add_argument("--no-hash", action="store_true")
    disk.add_argument("--follow-symlinks", action="store_true")
    disk.add_argument("--no-store-links", action="store_true")
    disk.add_argument("--no-refresh", action="store_true")
    disk.set_defaults(handler=cmd_ingest_disk)
    remote = commands.add_parser("remote-html", help="Ingest a configured remote HTML source.")
    _job_parser(remote)
    remote.add_argument("kind", choices=("wget_html", "native_html"))
    remote.add_argument("options_file", help="CLI-host JSON options file; path values refer to the Core host.")
    remote.set_defaults(handler=cmd_ingest_remote_html)


def build_conversion_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register convert/conversion format, option-inspection, and execution leaves.

    Input/output selectors name Core-host paths; the optional JSON file is loaded
    on the CLI host. Only run receives job execution controls. No converter or
    filesystem capability is validated during parser construction.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_conversion_parser(root.add_subparsers())
        >>> args = root.parse_args(["convert", "run", "input.epub", "output.pdf"])
        >>> args.output_path, args.options_file
        ('output.pdf', None)


    :param subparsers: Root CLI collection receiving the conversion family/alias.
    :return: None; all three required action choices are installed.
    """
    parser = subparsers.add_parser(
        "convert", aliases=["conversion"], help="Inspect or submit ebook conversions on the Core host."
    )
    commands = parser.add_subparsers(dest="conversion_action", required=True)
    formats = commands.add_parser("formats")
    _core_json(formats)
    formats.set_defaults(handler=cmd_conversion_formats)
    options = commands.add_parser("options", help="Inspect options for Core-host input/output paths.")
    _core_json(options)
    options.add_argument("input_path", help="Input path on the Core host.")
    options.add_argument("output_path", help="Output path on the Core host.")
    options.set_defaults(handler=cmd_conversion_options)
    run = commands.add_parser("run", help="Submit a conversion using Core-host paths.")
    _job_parser(run)
    run.add_argument("input_path", help="Input path on the Core host.")
    run.add_argument("output_path", help="Output path on the Core host.")
    run.add_argument("--options-file", help="CLI-host JSON option object.")
    run.set_defaults(handler=cmd_conversion_run)


def build_backup_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register storage-workflow backup commands and CLI-host SQLite verification/restore.

    Plan/save/show/list/run/SquashFS leaves use Core; verify is a local JSON-report
    operation without connection selectors. Restore accepts selectors but its
    handler requires a local SQLite/APSW target. Defaults include 4096 MiB packs
    and 100-row workflow pages; no files, range checks, or backup work occur here.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_backup_parser(root.add_subparsers())
        >>> args = root.parse_args(["backup", "plan", "source", "archive"])
        >>> args.target_pack_mib, args.output_key_prefix
        (4096.0, 'backup-packs')


    :param subparsers: Root CLI collection receiving the backup action family.
    :return: None; parsers, defaults, and handlers are registered in place.
    """
    parser = subparsers.add_parser(
        "backup",
        help="Manage storage backups and verify or restore database backups.",
    )
    commands = parser.add_subparsers(dest="backup_command", required=True)
    plan = commands.add_parser(
        "plan", help="Plan bounded SquashFS packs between configured Stores."
    )
    _core_json(plan)
    plan.add_argument("source_store", help="Source Store UUID, id, or unique name.")
    plan.add_argument(
        "destination_store", help="Destination Store UUID, id, or unique name."
    )
    plan.add_argument("--target-pack-mib", type=float, default=4096.0)
    plan.add_argument("--output-key-prefix", default="backup-packs")
    plan.add_argument("--workflow-name-prefix")
    plan.add_argument("--max-files-per-pack", type=int)
    plan.add_argument("--extension", action="append")
    plan.set_defaults(handler=cmd_backup_plan)
    workflows = commands.add_parser("workflows", help="List persisted backup workflows.")
    _core_json(workflows)
    workflows.add_argument("--limit", type=int, default=100)
    workflows.add_argument("--offset", type=int, default=0)
    workflows.set_defaults(handler=cmd_backup_workflows_list)
    show = commands.add_parser("show", help="Show one persisted backup workflow.")
    _core_json(show)
    show.add_argument("workflow_id", type=int)
    show.set_defaults(handler=cmd_backup_workflow_show)
    save = commands.add_parser("save", help="Persist a workflow declaration from CLI-host JSON.")
    _core_json(save)
    save.add_argument("spec_file")
    save.add_argument("--workflow-id", type=int)
    save.set_defaults(handler=cmd_backup_workflow_save)
    run = commands.add_parser("run", help="Submit one persisted workflow.")
    _job_parser(run)
    run.add_argument("workflow_id", type=int)
    run.set_defaults(handler=cmd_backup_workflow_run)
    squashfs = commands.add_parser("squashfs", help="Submit an ad-hoc SquashFS workflow declaration.")
    _job_parser(squashfs)
    squashfs.add_argument("spec_file")
    squashfs.add_argument("--no-verify", action="store_true")
    squashfs.add_argument("--cleanup-staging", action="store_true")
    squashfs.add_argument("--staging-root", help="Staging path on the Core host.")
    squashfs.set_defaults(handler=cmd_backup_squashfs_run)
    verify = commands.add_parser(
        "verify", help="Verify a SQLite/APSW database backup on the CLI host."
    )
    verify.add_argument("backup_file")
    verify.add_argument("--full", action="store_true")
    add_json_output(verify)
    verify.set_defaults(handler=cmd_database_verify_backup)
    restore = commands.add_parser(
        "restore", help="Atomically restore an offline SQLite/APSW catalogue."
    )
    add_connection_arguments(restore)
    restore.add_argument("backup_file")
    restore.add_argument("--safety-backup")
    restore.add_argument("--full-verify", action="store_true")
    restore.add_argument("--yes", action="store_true")
    add_json_output(restore)
    restore.set_defaults(handler=cmd_database_restore)


def build_database_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register database/db inspection, backup, migration, vacuum, and local restore.

    Remote backup destinations belong to Core; verify-backup and restore operate
    on CLI-host files. Confirmation is enforced by handlers, not parsing; apply
    previews without yes while restore/vacuum refuse. Construction does no database
    work and does not validate file existence, offline state, or migration readiness.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_database_parser(root.add_subparsers())
        >>> args = root.parse_args(["db", "migrations", "apply"])
        >>> args.migrations_action, args.yes
        ('apply', False)


    :param subparsers: Root CLI collection receiving the database family/alias.
    :return: None; inspection, nested migration, and upkeep leaves are installed.
    """
    parser = subparsers.add_parser(
        "database", aliases=["db"], help="Inspect and perform bounded database upkeep."
    )
    commands = parser.add_subparsers(dest="database_action", required=True)
    for action in ("info", "summary", "telemetry"):
        command = commands.add_parser(action)
        _core_json(command)
        command.set_defaults(handler=cmd_database_query)
    backup = commands.add_parser("backup", help="Run the configured database driver's backup operation.")
    _core_json(backup)
    backup.add_argument("--output-path", help="Backup path on the Core host.")
    backup.add_argument("--verify", action="store_true", help="Verify a SQLite backup after writing it.")
    backup.set_defaults(handler=cmd_database_backup)
    verify_backup = commands.add_parser(
        "verify-backup", help="Verify a SQLite/APSW backup file on the CLI host."
    )
    verify_backup.add_argument("backup_file")
    verify_backup.add_argument("--full", action="store_true")
    add_json_output(verify_backup)
    verify_backup.set_defaults(handler=cmd_database_verify_backup)
    restore = commands.add_parser(
        "restore", help="Atomically restore an offline SQLite/APSW catalogue."
    )
    add_connection_arguments(restore)
    restore.add_argument("backup_file")
    restore.add_argument("--safety-backup")
    restore.add_argument("--full-verify", action="store_true")
    restore.add_argument("--yes", action="store_true")
    add_json_output(restore)
    restore.set_defaults(handler=cmd_database_restore)
    migrations = commands.add_parser(
        "migrations", help="Inspect, plan, or apply known additive migrations."
    )
    migration_commands = migrations.add_subparsers(dest="migrations_action", required=True)
    for action in ("status", "plan", "apply"):
        migration = migration_commands.add_parser(action)
        _core_json(migration)
        if action == "apply":
            migration.add_argument("--yes", action="store_true")
        migration.set_defaults(handler=cmd_database_migrations)
    vacuum = commands.add_parser("vacuum", help="Run the configured database driver's vacuum operation.")
    _core_json(vacuum)
    vacuum.add_argument("--yes", action="store_true")
    vacuum.set_defaults(handler=cmd_database_vacuum)


def build_maintenance_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register maintenance status, duplicate discovery, and previewable repair leaves.

    Run defaults to 128 events, duplicate comparison to nocase, and clean requires
    at least one integer row ID. Run/clean/merge receive yes flags but default to
    handler-produced previews. Construction does not enable services or inspect rows.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_maintenance_parser(root.add_subparsers())
        >>> args = root.parse_args(["maintenance", "clean", "titles", "7", "7"])
        >>> args.row_id, args.yes
        ([7, 7], False)


    :param subparsers: Root CLI collection receiving the maintenance family.
    :return: None; required action choices and their handlers are added in place.
    """
    parser = subparsers.add_parser(
        "maintenance", help="Inspect duplicates and explicitly run guarded repair work."
    )
    commands = parser.add_subparsers(dest="maintenance_action", required=True)
    status = commands.add_parser("status")
    _core_json(status)
    status.set_defaults(handler=cmd_maintenance_status)
    duplicates = commands.add_parser("duplicates", help="Find duplicate values without modifying rows.")
    _core_json(duplicates)
    duplicates.add_argument("table")
    duplicates.add_argument("column")
    duplicates.add_argument("--comparison", default="nocase")
    duplicates.set_defaults(handler=cmd_maintenance_duplicates)
    run = commands.add_parser("run", help="Preview or run queued maintenance plugins once.")
    _core_json(run)
    run.add_argument("--max-events", type=int, default=128)
    run.add_argument("--yes", action="store_true")
    run.set_defaults(handler=cmd_maintenance_run)
    clean = commands.add_parser("clean", help="Preview or clean explicit row ids.")
    _core_json(clean)
    clean.add_argument("table")
    clean.add_argument("row_id", type=int, nargs="+")
    clean.add_argument("--yes", action="store_true")
    clean.set_defaults(handler=cmd_maintenance_clean)
    merge = commands.add_parser("merge", help="Preview or merge one row into another.")
    _core_json(merge)
    merge.add_argument("table")
    merge.add_argument("retained_id", type=int)
    merge.add_argument("merged_id", type=int)
    merge.add_argument("--yes", action="store_true")
    merge.set_defaults(handler=cmd_maintenance_merge)


__all__ = [
    "build_backup_parser",
    "build_conversion_parser",
    "build_database_parser",
    "build_ingest_parser",
    "build_maintenance_parser",
]
