"""Portable, SQLite-only command for auditing an unmanaged storage drive."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys

from collections.abc import Sequence
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser without touching a database or disk."""
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
    """Import the large library surface only after argument validation."""
    from LiuXin_alpha.library import Library

    return Library(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """Scan a drive into an explicitly local SQLite audit database."""
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
