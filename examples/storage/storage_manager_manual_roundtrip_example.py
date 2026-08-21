#!/usr/bin/env python3
"""Example: start ``StorageManager`` with one filesystem Store and use it."""

from __future__ import annotations

import argparse
import sys
from contextlib import redirect_stdout
from pathlib import Path

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import (  # pyright: ignore[reportImplicitRelativeImport]
    bootstrap_src_path,
    dump_json,
)

_ = bootstrap_src_path()

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start StorageManager and perform a local round-trip",
    )
    parser.add_argument("--store-root", required=True, help="Managed store root")
    parser.add_argument("--store-name", default="manual_demo_store", help="Store name")
    parser.add_argument("--payload", default="manual storage manager demo", help="Payload text")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store_root = Path(args.store_root).expanduser().resolve()
    store_root.mkdir(parents=True, exist_ok=True)

    store = FilesystemStore(store_root, name=args.store_name)
    source_bytes = args.payload.encode("utf-8")
    catalogue_path = store_root.with_name(f"{store_root.name}-catalog.sqlite")

    # StorageManager owns the Asset/Replica catalogue. FilesystemStore owns
    # the published bytes. Application metadata is database-backed by default;
    # the nested context managers close Stores before closing the catalogue.
    # Legacy schema construction still emits progress messages on stdout;
    # keep this JSON-producing example's stdout machine-readable.
    with redirect_stdout(sys.stderr):
        database = Database(
            metadata={"database_path": str(catalogue_path)},
            create=True,
            backup=False,
            enable_storage_manager=False,
        )
    with database, StorageManager(
        db=database,
        stores=[store],
        startup_on_add=True,
    ) as manager:
        asset = manager.store_bytes(
            source_bytes,
            name="Manual Storage Demo",
            media_type="text/plain",
            original_name="manual-demo.txt",
            metadata={
                "title": "Manual Storage Demo",
                "primary_agents": ["LiuXin"],
                "file_formats": ["TXT"],
            },
        )
        sha256 = next(
            digest for digest in asset.digests
            if digest.algorithm == "sha256"
        )
        replica = next(
            manager.iter_replica_records(
                digital_asset_id=asset.digital_asset_id,
            )
        )

        # IDs, records, and Digest values are all accepted by the convenient
        # read surface. Large callers should prefer open_asset/open_file.
        read_from_record = manager.read_asset(asset)
        read_from_id = manager.read_file(asset.digital_asset_id)
        read_from_digest = manager.read_file(sha256)

        payload = {
            "store_root": str(store_root),
            "catalogue": str(catalogue_path),
            "metadata_is_durable": manager.metadata_is_durable,
            "store_name": args.store_name,
            "default_store_uuid": str(manager.get_default_store_ref()),
            "digital_asset_id": int(asset.digital_asset_id),
            "replica_id": int(replica.replica_id),
            "storage_key": replica.location.key,
            "sha256": sha256.value,
            "retrieved_size": len(read_from_record),
            "retrieved_preview": read_from_record.decode("utf-8")[:160],
            "all_read_forms_match": (
                read_from_record
                == read_from_id
                == read_from_digest
                == source_bytes
            ),
        }
    print(dump_json(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
