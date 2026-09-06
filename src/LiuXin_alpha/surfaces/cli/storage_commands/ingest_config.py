"""Storage CLI ingest config ownership."""

from __future__ import annotations

import argparse

from LiuXin_alpha.ingest.mixed_application import MixedIngestBudget
from LiuXin_alpha.surfaces.cli.storage_commands.constants import _GIB, CLIUsageError
from LiuXin_alpha.surfaces.system_profile import load_system_profile


def _apply_system_root_defaults(args: argparse.Namespace) -> None:
    raw_root = getattr(args, "system_root", None)
    raw_profile = getattr(args, "profile", None)
    if args.database and not raw_root and not raw_profile:
        return
    try:
        resolved = load_system_profile(
            system_root=raw_root,
            profile=raw_profile,
            use_environment=True,
            use_persisted=True,
            required=False,
        )
    except (FileNotFoundError, ValueError) as error:
        raise CLIUsageError(str(error)) from error
    if resolved is None:
        return
    manifest_path = resolved.path
    manifest = resolved.values
    if str(manifest.get("db_type") or "SQLite").strip().lower() not in {
        "sqlite",
        "apsw",
    }:
        raise CLIUsageError(
            "mixed local ingest currently requires a SQLite/APSW system manifest"
        )
    if not args.database:
        args.database = str(manifest.get("database") or "") or None
    if not args.materialization_root:
        value = manifest.get("materialization_root")
        args.materialization_root = None if value in (None, "") else str(value)
    if not args.log_directory:
        value = manifest.get("log_directory")
        args.log_directory = None if value in (None, "") else str(value)
    if not args.database:
        raise CLIUsageError(f"system manifest has no catalogue path: {manifest_path!s}")
    args.require_existing_database = True


def _budget(args: argparse.Namespace) -> MixedIngestBudget:
    return MixedIngestBudget(
        max_source_files=int(args.max_source_files),
        max_containers=int(args.max_containers),
        max_container_depth=int(args.max_container_depth),
        max_members=int(args.max_members),
        max_members_per_container=int(args.max_members_per_container),
        max_member_bytes=_gib(args.max_member_gib, "--max-member-gib"),
        max_container_expanded_bytes=_gib(
            args.max_container_expanded_gib,
            "--max-container-expanded-gib",
        ),
        max_total_expanded_bytes=_gib(
            args.max_total_expanded_gib,
            "--max-total-expanded-gib",
        ),
        max_container_expansion_ratio=float(args.max_expansion_ratio),
        max_materialized_bytes=_gib(
            args.max_materialized_gib,
            "--max-materialized-gib",
        ),
        max_temporary_bytes=_gib(
            args.max_temporary_gib,
            "--max-temporary-gib",
        ),
        max_path_depth=int(args.max_path_depth),
        max_path_bytes=int(args.max_path_bytes),
        max_wall_time_s=float(args.max_wall_time_seconds),
        max_issues=int(args.max_issues),
    )


def _gib(value: object, option: str) -> int:
    if not isinstance(value, (int, float, str)):
        raise CLIUsageError(f"{option} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise CLIUsageError(f"{option} must be a number") from error
    if number <= 0:
        raise CLIUsageError(f"{option} must be positive")
    return int(number * _GIB)


def _validate_early_options(args: argparse.Namespace) -> None:
    if not bool(args.discover_only) and not args.database:
        raise CLIUsageError("--database is required unless --discover-only is selected")
    if int(args.log_max_mib) < 1:
        raise CLIUsageError("--log-max-mib must be positive")
    if int(args.log_backup_count) < 0:
        raise CLIUsageError("--log-backup-count must not be negative")
    if int(args.log_checkpoint_every) < 1:
        raise CLIUsageError("--log-checkpoint-every must be positive")
    if int(args.lock_timeout_seconds) < 0:
        raise CLIUsageError("--lock-timeout-seconds must not be negative")
    if float(args.backend_timeout_seconds) <= 0:
        raise CLIUsageError("--backend-timeout-seconds must be positive")
    _ = _budget(args)
