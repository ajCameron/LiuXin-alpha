#!/usr/bin/env python3
"""
Example: call storage.reconcile helper using a database path directly.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.storage.reconcile import register_existing_disk_with_database_path


def parse_args() -> argparse.Namespace:
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
