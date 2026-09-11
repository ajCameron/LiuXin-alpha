"""
Register SquashFS publication and provenance grammar without executing workflows.

Each leaf receives Core connection arguments and a bound command-owner handler.
Archive validation, source-ID collection, provenance filter requirements, and
publication guarantees belong to handlers/Core, not parser construction.
"""

from __future__ import annotations

import argparse

from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    cmd_provenance,
    cmd_publish_from_ids,
    cmd_publish_store,
)
from LiuXin_alpha.surfaces.core import add_core_client_arguments


def build_squashfs_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Add required SquashFS subcommands for store publication, file IDs, and provenance.

    Publish-store defaults to duplicating verified metadata rows and refreshing
    storage; strict/report-error options control handler failure policy. File-ID
    collection and provenance's requirement for at least one filter are deferred
    to execution. The parser does not check paths, positive IDs, tool availability,
    supported compression codecs, or mutually constrain provenance filters.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_squashfs_parser(root.add_subparsers())
        >>> args = root.parse_args(['squashfs', 'publish-store', '--database', 'library.sqlite', '--store-id', '7'])
        >>> (args.compression, args.duplicate_verified_files, args.strict)
        ('zstd', True, False)


    :param subparsers: Parent argparse collection receiving the squashfs command family.
    :return: None; register three leaf parsers with their argument and handler defaults.
    """
    parser = subparsers.add_parser(
        "squashfs",
        help="SquashFS archival workflows (designated files -> archive -> locked store).",
    )
    squashfs_subparsers = parser.add_subparsers(dest="squashfs_command", required=True)

    publish_store = squashfs_subparsers.add_parser(
        "publish-store",
        help="Publish an already-designated open SquashFS store by store id.",
    )
    add_core_client_arguments(publish_store)
    publish_store.add_argument(
        "--db-type", default="SQLite", help="Database backend type (default: SQLite)."
    )
    publish_store.add_argument(
        "--store-id", required=True, type=int, help="Open SquashFS store row id."
    )
    publish_store.add_argument(
        "--output-archive",
        default=None,
        help="Optional output archive path override (defaults to stores.store_root_uri).",
    )
    publish_store.add_argument(
        "--compression", default="zstd", help="mksquashfs compression codec."
    )
    publish_store.add_argument(
        "--deterministic",
        action="store_true",
        help="Enable deterministic squashfs flags.",
    )
    publish_store.add_argument(
        "--force",
        action="store_true",
        help="Overwrite archive output path if it exists.",
    )
    publish_store.add_argument(
        "--no-duplicate-verified-files",
        action="store_false",
        dest="duplicate_verified_files",
        help="Do not duplicate verified files into the archive store in `files`.",
    )
    publish_store.set_defaults(duplicate_verified_files=True)
    publish_store.add_argument(
        "--strict",
        action="store_true",
        help="Fail fast on any verification/publish error.",
    )
    publish_store.add_argument(
        "--json", action="store_true", help="Print report as JSON."
    )
    publish_store.add_argument(
        "--fail-on-report-errors",
        action="store_true",
        help="Return exit code 2 when report.errors is non-empty.",
    )
    publish_store.add_argument(
        "--no-refresh-storage-manager",
        action="store_true",
        help="Skip db.bootstrap_storage_manager(...) after publish.",
    )
    publish_store.set_defaults(handler=cmd_publish_store)

    publish_from_ids = squashfs_subparsers.add_parser(
        "publish-from-ids",
        help="Designate file ids and publish SquashFS archive in one command.",
    )
    add_core_client_arguments(publish_from_ids)
    publish_from_ids.add_argument(
        "--db-type", default="SQLite", help="Database backend type (default: SQLite)."
    )
    publish_from_ids.add_argument(
        "--archive", required=True, help="Output archive path (.squashfs/.sqfs)."
    )
    publish_from_ids.add_argument(
        "--store-name", default=None, help="Optional open store name override."
    )
    publish_from_ids.add_argument(
        "--file-id",
        action="append",
        type=int,
        default=[],
        help="Source file row id to include (repeatable).",
    )
    publish_from_ids.add_argument(
        "--file-ids-file",
        default=None,
        help="Path to newline-separated file ids (blank lines and '#' comments allowed).",
    )
    publish_from_ids.add_argument(
        "--compression", default="zstd", help="mksquashfs compression codec."
    )
    publish_from_ids.add_argument(
        "--deterministic",
        action="store_true",
        help="Enable deterministic squashfs flags.",
    )
    publish_from_ids.add_argument(
        "--force",
        action="store_true",
        help="Overwrite archive output path if it exists.",
    )
    publish_from_ids.add_argument(
        "--strict",
        action="store_true",
        help="Fail fast on any verification/publish error.",
    )
    publish_from_ids.add_argument(
        "--json", action="store_true", help="Print report as JSON."
    )
    publish_from_ids.add_argument(
        "--fail-on-report-errors",
        action="store_true",
        help="Return exit code 2 when report.errors is non-empty.",
    )
    publish_from_ids.add_argument(
        "--no-refresh-storage-manager",
        action="store_true",
        help="Skip db.bootstrap_storage_manager(...) after publish.",
    )
    publish_from_ids.set_defaults(handler=cmd_publish_from_ids)

    provenance = squashfs_subparsers.add_parser(
        "provenance",
        help="Inspect file provenance edges (file_derivations) for a store and/or file.",
    )
    add_core_client_arguments(provenance)
    provenance.add_argument(
        "--db-type", default="SQLite", help="Database backend type (default: SQLite)."
    )
    provenance.add_argument(
        "--store-id", type=int, default=None, help="Filter by store id."
    )
    provenance.add_argument(
        "--file-id", type=int, default=None, help="Filter by file id."
    )
    provenance.add_argument("--json", action="store_true", help="Print report as JSON.")
    provenance.set_defaults(handler=cmd_provenance)


__all__ = ["build_squashfs_parser"]
