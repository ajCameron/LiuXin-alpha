"""
Scan an unmanaged drive into a local SQLite catalogue through the Library facade.

This standalone command does not select PostgreSQL or a remote Core endpoint.
It can create/update the audit database and file/Store registrations; 'audit' does
not mean a read-only database operation. Library setup/scan stdout is redirected
to stderr so the final successful report can occupy stdout as JSON.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
import sys

from collections.abc import Sequence
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    """
    Declare required local database/drive selectors and scan-policy flags.

    Defaults permit database creation, hashing, and Store links while disabling
    symlink traversal and storage-manager refresh. Parsing does not inspect paths
    or require an existing database when --no-create-db is chosen.

    Example:
        >>> args = build_parser().parse_args(["--database", "audit.sqlite", "--disk-root", "drive"])
        >>> args.create_db, args.no_hash, args.refresh_storage_manager
        (True, False, False)


    :return: Fresh ArgumentParser for the standalone local storage audit command.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Scan ebook files on a storage drive into a local LiuXin SQLite database. "
            "This command never connects to PostgreSQL."
        )
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Local SQLite database path (created by default if it does not exist).",
    )
    parser.add_argument("--disk-root", required=True, help="Mounted storage-drive root to scan.")
    parser.add_argument("--store-name", default=None, help="Optional name recorded for the drive.")
    parser.add_argument(
        "--no-create-db",
        action="store_false",
        dest="create_db",
        help="Require the SQLite database to exist instead of creating it.",
    )
    parser.add_argument("--no-hash", action="store_true", help="Skip SHA-256 hashing while scanning.")
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinked directories while scanning.",
    )
    parser.add_argument(
        "--no-store-links",
        action="store_true",
        help="Do not create file_store_links rows.",
    )
    parser.add_argument(
        "--refresh-storage-manager",
        action="store_true",
        help="Refresh the in-process storage manager after registration.",
    )
    parser.set_defaults(create_db=True)
    return parser


def _open_library(**kwargs: object):
    """
    Lazily import Library and construct it with the caller's unmodified keywords.

    This helper performs no validation itself; main uses it only after path checks.
    Import/constructor errors propagate to that caller.

    Example:
        >>> library = _open_library(database_path=path, db_type="SQLite")  # doctest: +SKIP


    :param kwargs: Constructor keywords forwarded directly to the Library facade.
    :return: Newly constructed Library instance, not yet entered as a context manager.
    """
    library_module = importlib.import_module("LiuXin_alpha.library")
    return library_module.Library(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """
    Validate local paths, register a drive with SQLite, and print its JSON report.

    Resolve both paths before the operation catch. Reject a missing/non-directory
    drive and an existing non-file database with stderr errors/status two. Open
    Library with backup disabled and storage startup disabled; enable the storage
    manager only when refresh is requested. Redirect stdout through library
    construction, scanning, and context exit, preserving legacy chatter on stderr.

    Operation Exceptions become stderr errors/status two, without undoing writes.
    Argument parsing, path resolution, final to_dict/JSON/output, and report.errors
    access lie outside that catch and can raise. A successful operation still emits
    the complete report before returning two when its errors collection is truthy.

    Example:
        >>> main(["--database", "audit.sqlite", "--disk-root", "/mnt/books"])  # doctest: +SKIP


    :param argv: Explicit CLI token sequence, or None to parse process arguments.
    :return: Zero after a report with no errors; two for rejected paths, caught
        operation failures, or reported scan errors.
    """
    args = build_parser().parse_args(argv)
    database_path = Path(args.database).expanduser().resolve()
    disk_root = Path(args.disk_root).expanduser().resolve()

    if not disk_root.is_dir():
        print(f"ERROR: disk root is not a directory: {disk_root}", file=sys.stderr)
        return 2
    if database_path.exists() and not database_path.is_file():
        print(f"ERROR: database path is not a file: {database_path}", file=sys.stderr)
        return 2

    try:
        # Legacy database setup still emits informational text with print().
        # Keep stdout machine-readable JSON by routing that text to stderr.
        with contextlib.redirect_stdout(sys.stderr):
            with _open_library(
                database_path=database_path,
                db_type="SQLite",
                create=bool(args.create_db),
                backup=False,
                enable_storage_manager=bool(args.refresh_storage_manager),
                storage_startup_on_add=False,
            ) as library:
                report = library.register_unmanaged_disk(
                    disk_root,
                    store_name=args.store_name,
                    compute_hash=not args.no_hash,
                    follow_symlinks=args.follow_symlinks,
                    attach_store_links=not args.no_store_links,
                    refresh_storage_manager=args.refresh_storage_manager,
                )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        json.dumps(
            {
                "database_path": str(database_path),
                "disk_root": str(disk_root),
                "registration_report": report.to_dict(),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not report.errors else 2


__all__ = ["build_parser", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
