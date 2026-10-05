#!/usr/bin/env python3
"""
Register an unmanaged disk by passing a catalogue path to the reconciliation helper.

Delegate database ownership and scanning to register_existing_disk_with_database_path,
then print its diagnostic report. Unlike the Library example, this parser has no
catalogue-creation flag. Hashing, symlink following, Store links, and manager refresh
are controlled by the explicit command-line options.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.storage.reconcile import register_existing_disk_with_database_path


def parse_args() -> argparse.Namespace:
    """
    Parse required database/disk paths, database type defaulting to SQLite, and optional Store name.
    Enable hashing, Store links, and manager refresh by default; their --no-* flags disable them.
    --follow-symlinks opts into directory-link traversal. Path and registration validation are
    deferred to main and the helper.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="register_existing_disk_with_database_path example")
    parser.add_argument("--database", required=True, help="Path to LiuXin database file")
    parser.add_argument("--disk-root", required=True, help="Disk root to scan")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type")
    parser.add_argument("--store-name", default=None, help="Optional store name override")
    parser.add_argument("--no-hash", action="store_true", help="Skip SHA256 hashing")
    parser.add_argument("--follow-symlinks", action="store_true", help="Follow symlinked directories")
    parser.add_argument("--no-store-links", action="store_true", help="Do not create file_store_links rows")
    parser.add_argument("--no-refresh-storage", action="store_true", help="Skip storage manager refresh")
    return parser.parse_args()


def main() -> int:
    """
    Register the resolved disk against the expanded catalogue path and print the report. Forward all
    registration flags to register_existing_disk_with_database_path, which owns the opened database
    and scanning lifecycle. The database path is expanded but not resolved here; the disk path is
    both expanded and resolved. Use diagnostic JSON rendering after the helper returns, with no
    extra rollback or cleanup wrapper.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero when report.errors is empty, otherwise two; helper and rendering errors propagate.
    """
    args = parse_args()
    report = register_existing_disk_with_database_path(
        database_path=Path(args.database).expanduser(),
        disk_path=Path(args.disk_root).expanduser().resolve(),
        db_type=args.db_type,
        store_name=args.store_name,
        compute_hash=not args.no_hash,
        follow_symlinks=args.follow_symlinks,
        attach_store_links=not args.no_store_links,
        refresh_storage_manager=not args.no_refresh_storage,
    )
    print(dump_json(report))
    return 0 if not report.errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
