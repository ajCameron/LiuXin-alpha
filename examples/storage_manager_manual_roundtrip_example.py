#!/usr/bin/env python3
"""
Example: use StorageManager directly with a managed on-disk store.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)
from LiuXin_alpha.storage.store_manager import StorageManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StorageManager direct round-trip example")
    parser.add_argument("--store-root", required=True, help="Managed store root")
    parser.add_argument("--store-name", default="manual_demo_store", help="Store name")
    parser.add_argument("--payload", default="manual storage manager demo", help="Payload text")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store_root = Path(args.store_root).expanduser().resolve()
    store_root.mkdir(parents=True, exist_ok=True)

    store = OnDiskExistingManagedStorageBackend(url=str(store_root), name=args.store_name)
    manager = StorageManager(stores=[store], startup_on_add=True)

    added = manager.add_file(
        args.payload.encode("utf-8"),
        metadata={
            "title": "Manual Storage Demo",
            "authors": ["LiuXin"],
            "file_extension": "txt",
        },
        preferred_store=args.store_name,
    )
    fetched = manager.retrieve_file(file_url=added.file_url, preferred_store=args.store_name)

    payload = {
        "store_root": str(store_root),
        "store_name": args.store_name,
        "stored_file_url": added.file_url,
        "retrieved_size": len(fetched.as_bytes()),
        "retrieved_preview": fetched.as_string()[:160],
        "iter_urls": [f.file_url for f in manager.iter()],
    }
    print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
