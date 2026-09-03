"""Managed ingest, conversion, backup, database, and maintenance commands."""

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
    add_connection_arguments(parser)
    add_json_output(parser)


def _query(
    args: argparse.Namespace,
    operation: str,
    payload: dict[str, Any],
    *,
    maintenance: bool = False,
) -> int:
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
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = submit_job(core, operation, payload, args)
    emit_json(result, args)
    return execution_exit_code(result)


def cmd_ingest_formats(args: argparse.Namespace) -> int:
    return _query(args, "ingest.formats", {})


def cmd_ingest_disk(args: argparse.Namespace) -> int:
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
    return _job(
        args,
        "ingest.remote-html.start",
        {"kind": args.kind, "options": load_json_object(args.options_file)},
    )


def cmd_conversion_formats(args: argparse.Namespace) -> int:
    return _query(args, "conversion.formats", {})


def cmd_conversion_options(args: argparse.Namespace) -> int:
    return _query(
        args,
        "conversion.options",
        {"input_path": args.input_path, "output_path": args.output_path},
    )


def cmd_conversion_run(args: argparse.Namespace) -> int:
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
    return _query(
        args,
        "backup.workflows.list",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_backup_workflow_show(args: argparse.Namespace) -> int:
    return _query(
        args, "backup.workflow.get", {"workflow_id": int(args.workflow_id)}
    )


def cmd_backup_workflow_save(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"workflow_spec": load_json_object(args.spec_file)}
    if args.workflow_id is not None:
        payload["workflow_id"] = int(args.workflow_id)
    return _command(args, "backup.workflow.save", payload)


def cmd_backup_workflow_run(args: argparse.Namespace) -> int:
    return _job(
        args,
        "backup.workflow.start",
        {"workflow_id": int(args.workflow_id)},
    )


def cmd_backup_squashfs_run(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "workflow_spec": load_json_object(args.spec_file),
        "verify_after_build": not args.no_verify,
        "cleanup_staging_after_success": bool(args.cleanup_staging),
    }
    if args.staging_root:
        payload["staging_root"] = args.staging_root
    return _job(args, "backup.squashfs.start", payload)


def cmd_database_query(args: argparse.Namespace) -> int:
    return _query(args, "database." + args.database_action, {})


def cmd_database_backup(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"verify": bool(args.verify)}
    if args.output_path:
        payload["output_path"] = args.output_path
    return _command(args, "database.backup", payload)


def cmd_database_vacuum(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Database vacuum requires --yes.")
    return _command(args, "database.vacuum", {})


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            content = stream.read(1024 * 1024)
            if not content:
                break
            digest.update(content)
    return digest.hexdigest()


def verify_sqlite_backup(path: str | Path, *, full: bool = False) -> dict[str, Any]:
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
    result = verify_sqlite_backup(args.backup_file, full=bool(args.full))
    emit_json(result, args)
    return 0 if result["ok"] else 1


def _ensure_offline_sqlite(path: Path) -> None:
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
    try:
        descriptor = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def cmd_database_restore(args: argparse.Namespace) -> int:
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
    return _query(args, "maintenance.status", {}, maintenance=True)


def cmd_maintenance_duplicates(args: argparse.Namespace) -> int:
    return _query(
        args,
        "maintenance.duplicates.find",
        {"table": args.table, "column": args.column, "comparison": args.comparison},
        maintenance=True,
    )


def cmd_maintenance_run(args: argparse.Namespace) -> int:
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
    _core_json(parser)
    add_job_execution_arguments(parser)


def build_ingest_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Build the `ingest` command-line parser.


    :param subparsers:
    :return:
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
    Build the `conversion` command-line parser.


    :param subparsers:
    :return:
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
    Build the `backup` command-line parser.


    :param subparsers:
    :return:
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
    Build the `database` command-line parser.


    :param subparsers:
    :return:
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
    Build the `maintenance` command-line parser.


    :param subparsers:
    :return:
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
