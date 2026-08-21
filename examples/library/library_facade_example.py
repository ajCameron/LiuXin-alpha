#!/usr/bin/env python3
"""
Example: use the unified Library facade for DB + storage.

This script:
1) opens/creates a database,
2) ensures a managed on-disk store row exists,
3) refreshes storage manager bindings,
4) writes and reads one file via `Library`.
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

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.library import Library


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Library facade round-trip example")
    parser.add_argument("--database", required=True, help="Path to LiuXin database file")
    parser.add_argument("--store-root", required=True, help="Root path for managed store")
    parser.add_argument("--store-name", default="demo_managed_store", help="Store name to ensure")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type")
    parser.add_argument("--payload", default="hello from Library facade", help="Payload text to store")
    parser.add_argument("--create-db", action="store_true", help="Create database if it does not exist")
    return parser.parse_args()


def ensure_managed_store_row(lib: Library, *, store_root: Path, store_name: str) -> int:
    db = lib.db
    existing = db.search("stores", "store_root_uri", str(store_root))
    if existing:
        row = existing[0]
        changed = False
        for key, value in (
            ("store_name", store_name),
            ("store_kind", "on_disk_existing_managed_drive"),
            ("store_access_protocol", "file"),
            ("store_is_read_only", 0),
            ("store_online_status", "online"),
        ):
            if key not in row.allowed_columns:
                continue
            if row[key] != value:
                row[key] = value
                changed = True
        if changed:
            row.sync()
        return int(row.row_id if row.row_id is not None else row["store_id"])

    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": store_name,
            "store_kind": "on_disk_existing_managed_drive",
            "store_access_protocol": "file",
            "store_root_uri": str(store_root),
            "store_is_read_only": 0,
            "store_online_status": "online",
        },
        table="stores",
    )
    return int(row.row_id if row.row_id is not None else row["store_id"])


def main() -> int:
    args = parse_args()
    db_path = Path(args.database).expanduser()
    store_root = Path(args.store_root).expanduser().resolve()
    store_root.mkdir(parents=True, exist_ok=True)

    with Library(
        database_path=db_path,
        db_type=args.db_type,
        create=args.create_db,
        backup=False,
        storage_startup_on_add=False,
    ) as lib:
        store_id = ensure_managed_store_row(lib, store_root=store_root, store_name=args.store_name)
        bootstrap = lib.refresh_storage(clear_existing=True)
        store = next(
            candidate
            for candidate in lib.iter_stores()
            if candidate.configuration.store_name == args.store_name
        )

        added = lib.add_file(
            args.payload.encode("utf-8"),
            metadata={
                "title": "Library Facade Demo",
                "authors": ["LiuXin"],
                "file_extension": "txt",
            },
            store=store,
            name="library-facade-demo.txt",
        )
        location = lib.locate_file(added, store=store)
        with lib.retrieve_file(added, store=store) as fetched:
            retrieved = fetched.read()

        payload = {
            "database_path": str(db_path),
            "store_id": store_id,
            "store_name": args.store_name,
            "store_root": str(store_root),
            "bootstrap_report": bootstrap,
            "digital_asset_id": int(added.digital_asset_id),
            "stored_location": location,
            "stored_file_url": store.location_uri(location),
            "retrieved_bytes": len(retrieved),
            "retrieved_text_preview": retrieved.decode("utf-8")[:160],
        }
        print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
