#!/usr/bin/env python3
"""
Example: inspect store bootstrap from the database `stores` table.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.databases.database import Database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print storage bootstrap report")
    parser.add_argument("--database", required=True, help="Path to LiuXin database file")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type")
    parser.add_argument("--include-offline", action="store_true", help="Load rows marked offline")
    parser.add_argument("--startup-on-add", action="store_true", help="Call store.startup() while loading")
    parser.add_argument("--strict", action="store_true", help="Raise on bootstrap errors")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.database).expanduser()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=args.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
    ) as db:
        report = db.bootstrap_storage_manager(
            startup_on_add=args.startup_on_add,
            include_offline=args.include_offline,
            clear_existing=True,
            strict=args.strict,
        )
        payload = {
            "database_path": str(db_path),
            "report": report,
            "loaded_store_names": [store.name for store in db.storage.iter_stores()] if db.storage is not None else [],
        }
        print(dump_json(payload))
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
