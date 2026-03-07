#!/usr/bin/env python3
"""
Example: register an existing disk tree as an unmanaged store via Library.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.library import Library


def parse_args() -> argparse.Namespace:
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

        try:
            loaded_stores = [store.name for store in lib.iter_stores()]
        except Exception:
            loaded_stores = []

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
