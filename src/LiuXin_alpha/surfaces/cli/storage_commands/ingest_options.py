"""Storage CLI ingest options ownership."""

from __future__ import annotations

import argparse
from uuid import UUID

from LiuXin_alpha.ingest.mixed_application import MixedIngestBudget
from LiuXin_alpha.surfaces.cli.storage_commands.constants import _GIB


def add_storage_ingest_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the complete mixed-ingest option contract to ``parser``."""
    _add_source_options(parser)
    _add_limits_options(parser)
    _add_backends_options(parser)
    _add_logging_group_options(parser)
    _add_locking_options(parser)


def _uuid_argument(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a UUID") from error


def _add_source_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-root",
        required=True,
        help="Existing local tree to inspect without following symlinks.",
    )
    parser.add_argument(
        "--database",
        help="SQLite LiuXin catalogue path (required for a real or preflight run).",
    )
    parser.add_argument(
        "--system-root",
        help=(
            "Use database, materialization, and log paths from "
            "SYSTEM_ROOT/liuxin-system.json. Explicit path options override it."
        ),
    )
    parser.add_argument(
        "--profile",
        help="Named profile, manifest path, or manifest directory.",
    )
    parser.add_argument(
        "--materialization-root",
        help=(
            "Managed cache outside source-root for nested container bytes; "
            "top-level-only runs do not require it."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--discover-only",
        action="store_true",
        help="Classify top-level files without database, Store, or cache writes.",
    )
    mode.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Perform discovery with no catalogue/cache writes plus path, "
            "capacity, and dependency readiness checks."
        ),
    )
    parser.add_argument(
        "--require-existing-database",
        action="store_true",
        help="Refuse to create a new catalogue when --database does not exist.",
    )
    parser.add_argument(
        "--no-recursive-filesystem",
        action="store_true",
        help="Inspect only files immediately below source-root.",
    )
    parser.add_argument(
        "--no-nested-containers",
        action="store_true",
        help="Inventory top-level containers without opening containers inside them.",
    )
    parser.add_argument(
        "--expand-ebook-containers",
        action="store_true",
        help="Treat EPUB/CBZ/CBR and other ZIP/RAR-like ebooks as containers.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-read adopted bytes and mark successful Replicas verified.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Stop at the first bad source, container, or member.",
    )


def _add_limits_options(parser: argparse.ArgumentParser) -> None:
    defaults = MixedIngestBudget()
    limits = parser.add_argument_group("run-wide safety limits")
    limits.add_argument(
        "--max-source-files", type=int, default=defaults.max_source_files
    )
    limits.add_argument("--max-containers", type=int, default=defaults.max_containers)
    limits.add_argument(
        "--max-container-depth", type=int, default=defaults.max_container_depth
    )
    limits.add_argument("--max-members", type=int, default=defaults.max_members)
    limits.add_argument(
        "--max-members-per-container",
        type=int,
        default=defaults.max_members_per_container,
    )
    limits.add_argument(
        "--max-member-gib",
        type=float,
        default=defaults.max_member_bytes / _GIB,
    )
    limits.add_argument(
        "--max-container-expanded-gib",
        type=float,
        default=defaults.max_container_expanded_bytes / _GIB,
    )
    limits.add_argument(
        "--max-total-expanded-gib",
        type=float,
        default=defaults.max_total_expanded_bytes / _GIB,
    )
    limits.add_argument(
        "--max-expansion-ratio",
        type=float,
        default=defaults.max_container_expansion_ratio,
    )
    limits.add_argument(
        "--max-materialized-gib",
        type=float,
        default=defaults.max_materialized_bytes / _GIB,
    )
    limits.add_argument(
        "--max-temporary-gib",
        type=float,
        default=defaults.max_temporary_bytes / _GIB,
    )
    limits.add_argument("--max-path-depth", type=int, default=defaults.max_path_depth)
    limits.add_argument("--max-path-bytes", type=int, default=defaults.max_path_bytes)
    limits.add_argument("--max-issues", type=int, default=defaults.max_issues)
    limits.add_argument(
        "--max-wall-time-seconds",
        type=float,
        default=defaults.max_wall_time_s,
    )


def _add_backends_options(parser: argparse.ArgumentParser) -> None:
    backends = parser.add_argument_group("container backends")
    backends.add_argument(
        "--unsquashfs-exe",
        default="unsquashfs",
        help="unsquashfs executable name or path (default: unsquashfs).",
    )
    backends.add_argument(
        "--rar-extractor-exe",
        default=None,
        help="Optional unrar/rar executable for compressed RAR members.",
    )
    backends.add_argument(
        "--backend-timeout-seconds",
        type=float,
        default=300.0,
        help="Per external backend operation timeout (default: 300).",
    )


def _add_logging_group_options(parser: argparse.ArgumentParser) -> None:
    logging_group = parser.add_argument_group("logging and reports")
    logging_group.add_argument(
        "--run-id",
        type=_uuid_argument,
        default=None,
        help="Operator correlation UUID; generated when omitted.",
    )
    logging_group.add_argument(
        "--log-directory",
        help=(
            "Directory for the rotating text and authoritative JSONL logs; "
            "defaults beside the catalogue and must be outside source-root."
        ),
    )
    logging_group.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="DEBUG",
        help="Durable minimum level (default: DEBUG, including every object).",
    )
    logging_group.add_argument(
        "--log-max-mib",
        type=int,
        default=100,
        help="Maximum human-log segment size in MiB (default: 100).",
    )
    logging_group.add_argument(
        "--log-backup-count",
        type=int,
        default=10,
        help="Rotated human-log backups to retain (default: 10).",
    )
    logging_group.add_argument(
        "--log-checkpoint-every",
        type=int,
        default=1_000,
        help="Aggregate source/member checkpoint interval (default: 1000).",
    )
    logging_group.add_argument(
        "--report-file",
        help="Full atomic JSON report path; defaults beside this run's logs.",
    )
    logging_group.add_argument(
        "--replace-report",
        action="store_true",
        help="Allow an explicit existing --report-file to be atomically replaced.",
    )
    logging_group.add_argument(
        "--compact-json",
        action="store_true",
        help="Emit compact JSON on stdout and in the report file.",
    )
    logging_group.add_argument(
        "--no-stdout-report",
        action="store_true",
        help="Write only the report file; leave stdout empty.",
    )
    logging_group.add_argument(
        "--no-console-progress",
        action="store_true",
        help="Suppress stderr progress; durable logs are unaffected.",
    )


def _add_locking_options(parser: argparse.ArgumentParser) -> None:
    locking = parser.add_argument_group("concurrency")
    locking.add_argument(
        "--lock-file",
        help="Stable advisory run-lock path; defaults in the log directory.",
    )
    locking.add_argument(
        "--lock-timeout-seconds",
        type=int,
        default=0,
        help="Seconds to wait for another real ingest to release its lock.",
    )
    locking.add_argument(
        "--no-run-lock",
        action="store_true",
        help="Disable the real-run advisory lock (unsafe for one catalogue).",
    )
