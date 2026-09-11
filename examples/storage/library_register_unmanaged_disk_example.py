#!/usr/bin/env python3
"""
Register an existing disk tree through the Library facade's unmanaged-disk helper.

Open the selected database and scan the source in place, with optional hashing,
directory-symlink following, Store links, and storage-manager refresh. Print the
registration report and Store names visible to this Library instance. Registration
does not itself mean the source bytes were copied into managed storage.
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

from LiuXin_alpha.library import Library


def parse_args() -> argparse.Namespace:
    """
    Parse required catalogue/disk paths and Library registration options. Default to SQLite with no
    Store-name override. --create-db opts into creation; hashing, Store links, and storage refresh
    are enabled unless their --no-* flags are given. Directory-symlink following is opt-in. These
    flags are passed to Library/the registrar.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Register an unmanaged disk into the DB files table")
    parser.add_argument("--database", required=True, help="Path to LiuXin database file")
    parser.add_argument("--disk-root", required=True, help="Disk root to scan")
    parser.add_argument("--store-name", default=None, help="Optional store name override")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type")
    parser.add_argument("--create-db", action="store_true", help="Create database if missing")
    parser.add_argument("--no-hash", action="store_true", help="Skip SHA256 hashing while ingesting")
    parser.add_argument("--follow-symlinks", action="store_true", help="Follow symlinked directories")
    parser.add_argument("--no-store-links", action="store_true", help="Do not create file_store_links rows")
    parser.add_argument(
        "--no-refresh-storage",
        action="store_true",
        help="Skip storage manager refresh after registration",
    )
    return parser.parse_args()


def main() -> int:
    """
    Open a Library, register the disk in place, and print report plus loaded Store names. Expand the
    database path without resolving it and expand/resolve the disk root. Disable database backup and
    automatic Store startup on addition; forward catalogue creation and registration flags
    explicitly. Read Store names after registration, including its optional manager refresh. Print
    through the shared diagnostic sanitizer inside the Library context, then close the Library.
    Completed registration writes remain; this function supplies no transaction or source-copy
    operation around the helper.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero when report.errors is empty, otherwise two; parser, Library, registration, and cleanup exceptions propagate.
    """
    args = parse_args()
    db_path = Path(args.database).expanduser()
    disk_root = Path(args.disk_root).expanduser().resolve()

    with Library(
        database_path=db_path,
        db_type=args.db_type,
        create=args.create_db,
        backup=False,
        storage_startup_on_add=False,
    ) as lib:
        report = lib.register_unmanaged_disk(
            disk_root,
            store_name=args.store_name,
            compute_hash=not args.no_hash,
            follow_symlinks=args.follow_symlinks,
            attach_store_links=not args.no_store_links,
            refresh_storage_manager=not args.no_refresh_storage,
        )

        loaded_stores = [
            store.configuration.store_name for store in lib.iter_stores()
        ]

        payload = {
            "database_path": str(db_path),
            "disk_root": str(disk_root),
            "registration_report": report,
            "loaded_stores_after": loaded_stores,
        }
        print(dump_json(payload))

    return 0 if not report.errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
