#!/usr/bin/env python3
"""
Copy selected files from an unmanaged directory into a managed filesystem Store.

The source is exposed read-only, while the destination receives published bytes.
This invocation supplies no database to StorageManager, so its Asset/Replica
metadata is transient even though the copied files persist. Print an ingest report
with source/destination keys, IDs, digests, failures, and a read-length check.
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

from LiuXin_alpha.ingest.stores import ingest_store
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def parse_args() -> argparse.Namespace:
    """
    Parse process arguments for required source/destination roots, repeatable extension filters, and
    an integer worker count defaulting to one. Omitted extensions mean no extension filter. Path
    resolution and ingest-option validation occur later.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Expose an existing disk read-only, enumerate it through StoreAPI, "
            "and copy selected files into a managed filesystem Store"
        ),
    )
    parser.add_argument("--source-root", required=True, help="Existing disk tree")
    parser.add_argument(
        "--destination-root",
        required=True,
        help="Managed filesystem Store receiving copies",
    )
    parser.add_argument(
        "--extension",
        action="append",
        dest="extensions",
        help=(
            "Only ingest this filename extension; repeat as needed. "
            "Without this option every regular file is ingested."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent copy workers",
    )
    return parser.parse_args()


def main() -> int:
    """
    Copy an existing disk tree and print a diagnostic ingest summary. Expand and resolve both roots,
    start the read-only source, and attach the destination to a manager with transient metadata.
    Ingest with the requested filters/workers and continue_on_error=True. For each successful item,
    read its asset back and report whether its length matches the recorded size; this flag does not
    compare bytes or recompute a hash. Close the manager's Stores on context exit and close the
    source in finally after that block is entered. Source startup failure occurs before the
    source-cleanup block. Published copies remain on disk, including after later reporting or
    cleanup failures.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero when report.ok is true, otherwise one; startup, unexpected read/report, and cleanup errors propagate.
    """
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    destination_root = Path(args.destination_root).expanduser().resolve()
    source = OnDiskUnmanagedStorageBackend(
        source_root,
        name="existing-disk",
    )
    destination = FilesystemStore(
        destination_root,
        name="managed-destination",
    )

    source_status = source.startup()
    if not source_status.available:
        raise RuntimeError(source_status.message or "existing disk is unavailable")

    try:
        with StorageManager(
            stores=[destination],
            default_store_ref=destination.store_ref,
        ) as manager:
            report = ingest_store(
                manager,
                source,
                extensions=args.extensions,
                workers=args.workers,
                continue_on_error=True,
            )
            items = [
                {
                    "source_key": item.source_info.location.key,
                    "source_uri": item.source_uri,
                    "destination_key": item.result.location.key,
                    "digital_asset_id": int(
                        item.result.asset_record.digital_asset_id
                    ),
                    "original_name": (
                        item.result.asset_record.metadata.original_name
                    ),
                    "sha256": next(
                        digest.value
                        for digest in item.result.asset_record.digests
                        if digest.algorithm == "sha256"
                    ),
                    "retrievable": (
                        len(manager.read_file(item.result.asset_record))
                        == item.result.asset_record.size_bytes
                    ),
                }
                for item in report.items
            ]
            print(
                dump_json(
                    {
                        "mode": report.mode,
                        "source_root": str(source_root),
                        "source_read_only": not source_status.writable,
                        "destination_root": str(destination_root),
                        "enumeration": report.enumeration,
                        "scanned_files": report.scanned_files,
                        "skipped_files": report.skipped_files,
                        "ingested_files": report.ingested_files,
                        "deduplicated_files": report.deduplicated_files,
                        "ok": report.ok,
                        "items": items,
                        "failures": report.failures,
                    }
                )
            )
            return 0 if report.ok else 1
    finally:
        source.close()


if __name__ == "__main__":
    raise SystemExit(main())
