#!/usr/bin/env python3
"""Discover or durably catalogue a mixed local file/archive tree."""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import platform
import socket
import sys

from collections.abc import Mapping
from contextlib import redirect_stdout
from pathlib import Path
from uuid import UUID, uuid4


EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import (  # pyright: ignore[reportImplicitRelativeImport]
    bootstrap_src_path,
    dump_json,
)


bootstrap_src_path()

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.constants import __version__ as liuxin_version
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.ingest import (
    MixedFormatIngestCoordinator,
    MixedIngestBudget,
)
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.utils.logging import get_compat_logger
from LiuXin_alpha.utils.logging.run_logging import LoggingTextStream, RunLoggingSession


_LOGGER = get_compat_logger("LiuXin_alpha.storage.ingest.mixed_cli")


def parse_args() -> argparse.Namespace:
    defaults = MixedIngestBudget()
    parser = argparse.ArgumentParser(
        description=(
            "Classify or catalogue loose files plus nested SquashFS, ISO/UDF, "
            "ZIP, TAR, RAR, and 7z containers without extracting into the "
            "source tree"
        )
    )
    parser.add_argument("--source-root", required=True, help="Existing input tree")
    parser.add_argument(
        "--database",
        help="LiuXin catalogue path (required unless --discover-only)",
    )
    parser.add_argument(
        "--materialization-root",
        help=(
            "Managed cache directory outside source-root; required only if "
            "nested containers are encountered"
        ),
    )
    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="Classify top-level files without creating any Store or catalogue rows",
    )
    parser.add_argument(
        "--no-recursive-filesystem",
        action="store_true",
        help="Inspect only files immediately below source-root",
    )
    parser.add_argument(
        "--no-nested-containers",
        action="store_true",
        help="Inventory top-level containers but do not open containers inside them",
    )
    parser.add_argument(
        "--expand-ebook-containers",
        action="store_true",
        help="Treat EPUB/CBZ/CBR and other ZIP-like ebook formats as containers",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-read adopted bytes and mark their Replicas verified",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Raise at the first bad file/container/member instead of isolating it",
    )
    parser.add_argument("--max-source-files", type=int, default=defaults.max_source_files)
    parser.add_argument("--max-containers", type=int, default=defaults.max_containers)
    parser.add_argument(
        "--max-container-depth", type=int, default=defaults.max_container_depth
    )
    parser.add_argument("--max-members", type=int, default=defaults.max_members)
    parser.add_argument(
        "--max-members-per-container",
        type=int,
        default=defaults.max_members_per_container,
    )
    parser.add_argument(
        "--max-member-gib",
        type=float,
        default=defaults.max_member_bytes / (1024**3),
    )
    parser.add_argument(
        "--max-container-expanded-gib",
        type=float,
        default=defaults.max_container_expanded_bytes / (1024**3),
    )
    parser.add_argument(
        "--max-total-expanded-gib",
        type=float,
        default=defaults.max_total_expanded_bytes / (1024**3),
    )
    parser.add_argument(
        "--max-expansion-ratio",
        type=float,
        default=defaults.max_container_expansion_ratio,
    )
    parser.add_argument(
        "--max-materialized-gib",
        type=float,
        default=defaults.max_materialized_bytes / (1024**3),
    )
    parser.add_argument(
        "--max-temporary-gib",
        type=float,
        default=defaults.max_temporary_bytes / (1024**3),
    )
    parser.add_argument(
        "--max-wall-time-seconds",
        type=float,
        default=defaults.max_wall_time_s,
    )
    parser.add_argument(
        "--log-directory",
        help=(
            "Directory for this run's rotating text log and authoritative "
            "JSONL event log; defaults beside the catalogue (and always "
            "outside source-root)"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="DEBUG",
        help="Minimum durable log level (default: DEBUG, including every object)",
    )
    parser.add_argument(
        "--log-max-mib",
        type=int,
        default=100,
        help="Maximum size of each rotating human-readable log (default: 100)",
    )
    parser.add_argument(
        "--log-backup-count",
        type=int,
        default=10,
        help="Number of old human-readable logs to retain (default: 10)",
    )
    parser.add_argument(
        "--log-checkpoint-every",
        type=int,
        default=1_000,
        help="Emit aggregate source/member checkpoints at this interval",
    )
    parser.add_argument(
        "--no-console-progress",
        action="store_true",
        help="Suppress container summaries on stderr; durable logs are unaffected",
    )
    return parser.parse_args()


def _gib(value: float, option: str) -> int:
    if value <= 0:
        raise ValueError(f"{option} must be positive")
    return int(value * 1024**3)


def _budget(args: argparse.Namespace) -> MixedIngestBudget:
    return MixedIngestBudget(
        max_source_files=args.max_source_files,
        max_containers=args.max_containers,
        max_container_depth=args.max_container_depth,
        max_members=args.max_members,
        max_members_per_container=args.max_members_per_container,
        max_member_bytes=_gib(args.max_member_gib, "--max-member-gib"),
        max_container_expanded_bytes=_gib(
            args.max_container_expanded_gib, "--max-container-expanded-gib"
        ),
        max_total_expanded_bytes=_gib(
            args.max_total_expanded_gib, "--max-total-expanded-gib"
        ),
        max_container_expansion_ratio=args.max_expansion_ratio,
        max_materialized_bytes=_gib(
            args.max_materialized_gib, "--max-materialized-gib"
        ),
        max_temporary_bytes=_gib(args.max_temporary_gib, "--max-temporary-gib"),
        max_wall_time_s=args.max_wall_time_seconds,
    )


def _progress(event: str, details: Mapping[str, object]) -> None:
    if event == "container_started":
        print(
            f"[depth {details['depth']}] {details['format']}: {details['path']}",
            file=sys.stderr,
            flush=True,
        )
    elif event == "container_complete":
        print(
            "  members={} issues={} ok={}".format(
                details["members_adopted"], details["issue_count"], details["ok"]
            ),
            file=sys.stderr,
            flush=True,
        )


def _coordinator(
    manager: api.StorageManagerAPI, args: argparse.Namespace
) -> MixedFormatIngestCoordinator:
    return MixedFormatIngestCoordinator(
        manager,
        budget=_budget(args),
        recursive_filesystem=not args.no_recursive_filesystem,
        recurse_containers=not args.no_nested_containers,
        expand_ebook_containers=args.expand_ebook_containers,
        continue_on_error=not args.strict,
        verify_source_files=args.verify,
        verify_members=args.verify,
        materialization_root=args.materialization_root,
        progress_callback=(None if args.no_console_progress else _progress),
        log_checkpoint_every=args.log_checkpoint_every,
    )


def _path_is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def _log_directory(args: argparse.Namespace, source_root: Path) -> Path:
    if args.log_directory:
        selected = Path(args.log_directory).expanduser().resolve(strict=False)
        if _path_is_within(selected, source_root):
            raise ValueError("the log directory must be outside --source-root")
    elif args.database:
        database_path = Path(args.database).expanduser().resolve(strict=False)
        selected = database_path.with_name(database_path.name + ".ingest-logs")
    else:
        selected = source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
    selected = selected.resolve(strict=False)
    if _path_is_within(selected, source_root):
        selected = source_root.parent / f".{source_root.name}.liuxin-ingest-logs"
        selected = selected.resolve(strict=False)
    if _path_is_within(selected, source_root):
        raise ValueError("the log directory must be outside --source-root")
    return selected


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


def _run(
    args: argparse.Namespace,
    *,
    source_root: Path,
    run_id: UUID,
) -> tuple[int, dict[str, object]]:
    if args.discover_only:
        with StorageManager() as manager:
            report = _coordinator(manager, args).ingest(
                source_root,
                discovery_only=True,
                run_id=run_id,
            )
        payload: dict[str, object] = {
            "ok": report.ok,
            "report": report,
            "budget": _budget(args),
        }
        return (0 if report.ok else 1), payload

    assert args.database
    database_path = Path(args.database).expanduser().resolve(strict=False)
    create = not database_path.exists()
    _log(
        logging.INFO,
        "database_open_started",
        "Opening LiuXin catalogue",
        run_id=run_id,
        database=str(database_path),
        create=create,
    )
    # Legacy schema construction is chatty. Capture it as structured logging
    # while leaving stdout reserved for the final machine-readable report.
    captured_stdout = LoggingTextStream(
        _LOGGER,
        level=logging.DEBUG,
        stream_name="database_stdout",
    )
    try:
        with redirect_stdout(captured_stdout):
            database = Database(
                metadata={"database_path": str(database_path)},
                create=create,
                backup=False,
                enable_storage_manager=False,
            )
    finally:
        captured_stdout.flush()
    _log(
        logging.INFO,
        "database_open_complete",
        "LiuXin catalogue opened",
        run_id=run_id,
        database=str(database_path),
        created=create,
    )
    manager = StorageManager(db=database, startup_on_add=True)
    with database, manager:
        if not create:
            bootstrap = manager.load_from_database(startup=True)
            for issue in bootstrap.issues:
                _log(
                    logging.WARNING,
                    "store_bootstrap_issue",
                    "Store bootstrap warning",
                    run_id=run_id,
                    store_ref=(
                        None if issue.store_ref is None else str(issue.store_ref)
                    ),
                    store_name=issue.store_name,
                    reason=issue.reason,
                )
            _log(
                logging.INFO if bootstrap.ok else logging.WARNING,
                "store_bootstrap_complete",
                "Store bootstrap complete",
                run_id=run_id,
                discovered_configurations=bootstrap.discovered_configurations,
                loaded_stores=bootstrap.loaded_stores,
                skipped_configurations=bootstrap.skipped_configurations,
                failed_configurations=bootstrap.failed_configurations,
                issue_count=len(bootstrap.issues),
                ok=bootstrap.ok,
            )
        report = _coordinator(manager, args).ingest(source_root, run_id=run_id)
        payload = {
            "database": str(database_path),
            "metadata_is_durable": manager.metadata_is_durable,
            "budget": _budget(args),
            "ok": report.ok,
            "report": report,
        }
    return (0 if report.ok else 1), payload


def main() -> int:
    args = parse_args()
    if not args.discover_only and not args.database:
        print("--database is required for a real ingest run", file=sys.stderr)
        return 2
    if args.log_max_mib < 1:
        print("--log-max-mib must be positive", file=sys.stderr)
        return 2
    if args.log_backup_count < 0:
        print("--log-backup-count must not be negative", file=sys.stderr)
        return 2
    if args.log_checkpoint_every < 1:
        print("--log-checkpoint-every must be positive", file=sys.stderr)
        return 2

    source_root = Path(args.source_root).expanduser().resolve(strict=False)
    run_id = uuid4()
    try:
        log_directory = _log_directory(args, source_root)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    level = int(getattr(logging, args.log_level))
    with RunLoggingSession(
        log_directory,
        run_id=run_id,
        prefix="mixed-ingest",
        level=level,
        max_text_bytes=args.log_max_mib * 1024 * 1024,
        text_backup_count=args.log_backup_count,
    ) as log_session:
        assert log_session.paths is not None
        paths = log_session.paths
        print(f"Run ID: {run_id}", file=sys.stderr, flush=True)
        print(f"Human log: {paths.human_log}", file=sys.stderr, flush=True)
        print(f"Event log: {paths.event_log}", file=sys.stderr, flush=True)
        try:
            _log(
                logging.INFO,
                "cli_started",
                "Mixed ingest command started",
                run_id=run_id,
                source_root=str(source_root),
                database=(
                    None
                    if args.database is None
                    else str(
                        Path(args.database).expanduser().resolve(strict=False)
                    )
                ),
                materialization_root=(
                    None
                    if args.materialization_root is None
                    else str(
                        Path(args.materialization_root)
                        .expanduser()
                        .resolve(strict=False)
                    )
                ),
                discovery_only=args.discover_only,
                arguments={
                    key: value
                    for key, value in vars(args).items()
                    if key
                    not in {"database", "source_root", "materialization_root"}
                },
                budget=dataclasses.asdict(_budget(args)),
                liuxin_version=liuxin_version,
                python_version=sys.version,
                python_executable=sys.executable,
                platform=platform.platform(),
                hostname=socket.gethostname(),
                process_id=os.getpid(),
                working_directory=str(Path.cwd()),
                human_log=str(paths.human_log),
                event_log=str(paths.event_log),
            )
            exit_code, payload = _run(
                args,
                source_root=source_root,
                run_id=run_id,
            )
        except KeyboardInterrupt as error:
            _LOGGER.error(
                "Mixed ingest command interrupted",
                exc_info=(type(error), error, error.__traceback__),
                extra={
                    "liuxin_event": "cli_interrupted",
                    "liuxin_context": {"run_id": str(run_id)},
                },
            )
            log_session.flush()
            print(f"Run {run_id} interrupted; inspect {paths.event_log}", file=sys.stderr)
            return 130
        except Exception as error:
            _LOGGER.critical(
                "Mixed ingest command failed",
                exc_info=(type(error), error, error.__traceback__),
                extra={
                    "liuxin_event": "cli_failed",
                    "liuxin_context": {
                        "run_id": str(run_id),
                        "error_type": type(error).__name__,
                        "error_message": str(error) or type(error).__name__,
                    },
                },
            )
            log_session.flush()
            print(
                f"Run {run_id} failed; inspect {paths.event_log}",
                file=sys.stderr,
                flush=True,
            )
            return 1

        payload.update(
            {
                "run_id": str(run_id),
                "human_log": str(paths.human_log),
                "event_log": str(paths.event_log),
            }
        )
        _log(
            logging.INFO if exit_code == 0 else logging.WARNING,
            "cli_complete",
            "Mixed ingest command complete",
            run_id=run_id,
            exit_code=exit_code,
            ok=bool(payload["ok"]),
            human_log=str(paths.human_log),
            event_log=str(paths.event_log),
        )
        log_session.flush()
    print(dump_json(payload))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
