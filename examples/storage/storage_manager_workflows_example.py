#!/usr/bin/env python3
"""
Exercise local ingest, lookup, replication, verification, and Composite delivery.

Build two filesystem Stores with a durable catalogue beneath a caller-owned work
directory. Seed small placeholder book/cover bytes, attach Assets to an Item, export
a Composite directory, and inspect its streamed ZIP. These payloads demonstrate
storage contracts, not valid EPUB/JPEG parsing. Files and catalogue remain afterward.
"""

from __future__ import annotations

import argparse
import sys
import zipfile

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
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.durable_manager import StorageManager


def parse_args() -> argparse.Namespace:
    """
    Parse the required --work-dir directory for Stores, incoming sample, catalogue, and exports.
    main/run perform expansion and creation; argparse does not reserve the directory or check
    existing contents.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Start a two-Store manager and demonstrate ingest, lookup, "
            "replication, verification, and Composite delivery"
        ),
    )
    parser.add_argument(
        "--work-dir",
        required=True,
        help="Directory in which the example may create Stores and exports",
    )
    return parser.parse_args()


def run(work_dir: Path) -> dict[str, object]:
    """
    Run the local storage demonstration and return its summary after closing owned resources.
    Expand/resolve and create the work directory, overwrite the fixed incoming sample path, and open
    storage-catalog.sqlite with create=True and backup=False. Redirect schema output to stderr.
    Create an Item plus primary/archive Stores, choose the primary default, ingest with placement
    hints and verification, then compare streamed, ID, Digest, and bare-hash reads against the
    seeded bytes.

    Replicate to the archive Store with verification, assess both Replica IDs, create an Item-linked
    Composite from the book path and cover bytes, export its directory, and inspect ZIP member
    names. Resolve the package role requiring verified content. Report IDs, operation ID, read
    equality, reused placement hints, healthy Replica IDs, and export members. Flags report
    observations rather than asserting that every value is true.

    The summary is computed inside the manager/database contexts and returned after they exit,
    closing Stores before the catalogue. Creation before context entry has no outer cleanup wrapper.
    Prefer a fresh scratch directory: fixed-path files and completed catalogue/storage effects
    remain on success or later failure. This call does not reopen the catalogue to test persistence
    across a restart.

    Example:
        >>> summary = run(Path("/tmp/liuxin-workflow-demo"))  # doctest: +SKIP


    :param work_dir: Writable demo directory for the fixed incoming, catalogue, Store, and export paths; contents are retained.
    :return: Summary dictionary of paths, IDs, verification/read observations, and exported member names.
    """

    root = work_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    incoming = root / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    book_path = incoming / "The Left Hand of Darkness — sample.epub"
    book_bytes = b"example epub payload\n"
    cover_bytes = b"example cover payload\n"
    book_path.write_bytes(book_bytes)

    catalogue_path = root / "storage-catalog.sqlite"

    # A normal application manager is database-backed: Store configuration,
    # Assets, Replicas, provenance, Item links, policies, and ingest operation
    # IDs all survive a restart. TransientStorageManager remains available for
    # focused tests and deliberately disposable work.
    # Legacy schema construction still emits progress messages on stdout;
    # keep this JSON-producing example's stdout machine-readable.
    with redirect_stdout(sys.stderr):
        database = Database(
            metadata={"database_path": str(catalogue_path)},
            create=True,
            backup=False,
            enable_storage_manager=False,
        )
    with database, StorageManager(db=database) as manager:
        item_row = Row.from_idless_row_dict(
            database,
            row_dict={
                "item_type": "digital",
                "item_source": "storage-manager-workflows-example",
            },
            table="items",
        )
        item_id = api.ItemID(int(item_row["item_id"]))
        primary = manager.add_filesystem_store(
            "primary",
            root / "primary-store",
        )
        archive = manager.add_filesystem_store(
            "archive",
            root / "archive-store",
            operational_role="archive",
        )
        manager.set_default_store(primary.store_uuid)

        # The detailed ingest API returns the operation, Asset, Replica, and
        # verification result. Use store_file() when only the Asset is needed.
        ingest = manager.ingest_file(
            book_path,
            item_id=item_id,
            role="primary_payload",
            metadata=api.DigitalAssetMetadata(
                name="The Left Hand of Darkness",
                media_type="application/epub+zip",
            ),
            placement_hints={
                "title": "The Left Hand of Darkness",
                "primary_agents": ["Ursula K. Le Guin"],
                "file_formats": ["EPUB"],
            },
            verify=True,
        )
        asset = ingest.asset_record
        source_replica = ingest.replica_record
        sha256 = next(
            digest for digest in asset.digests
            if digest.algorithm == "sha256"
        )

        # Lookup can use the Asset record, its ID, a Digest value, or a bare
        # SHA-256 string. Stream-returning methods avoid loading large files.
        with manager.open_asset(asset, verified=True) as source:
            streamed_bytes = source.read()
        read_by_id = manager.read_file(asset.digital_asset_id, verified=True)
        read_by_digest = manager.read_file(sha256, verified=True)
        read_by_hash_text = manager.read_file(sha256.value, verified=True)

        # With metadata omitted, replication reuses the source Replica's
        # recorded placement hints for a rich destination Store.
        archive_replica = manager.replicate_asset(
            asset,
            to=archive,
            replica_mode="archive",
            verify=True,
        )
        verification = manager.verify_digital_asset(
            asset.digital_asset_id,
            replica_ids=(
                source_replica.replica_id,
                archive_replica.replica_id,
            ),
        )

        # Composite members remain ordinary atomic Assets. Logical paths are
        # preserved by directory and ZIP delivery helpers.
        package = manager.store_composite(
            {
                "book.epub": book_path,
                "images/cover.jpg": cover_bytes,
            },
            name="book package",
            item=item_id,
            role="package",
            verify=True,
        )
        exported = manager.export_composite_to_directory(
            package,
            root / "exported-package",
        )
        with manager.open_composite_zip(package) as zip_stream:
            with zipfile.ZipFile(zip_stream) as archive_file:
                zip_members = tuple(archive_file.namelist())

        item_resolution = manager.resolve_item_digital_asset(
            item_id,
            role="package",
            require_verified=True,
        )

        return {
            "work_dir": str(root),
            "catalogue": str(catalogue_path),
            "metadata_is_durable": manager.metadata_is_durable,
            "stores": sorted(
                configuration.store_name
                for configuration in manager.iter_store_configurations()
            ),
            "default_store": str(manager.get_default_store_ref()),
            "digital_asset_id": int(asset.digital_asset_id),
            "sha256": sha256.value,
            "ingest_operation_id": str(ingest.operation_id),
            "ingest_verified": ingest.verified,
            "all_read_forms_match": (
                streamed_bytes
                == read_by_id
                == read_by_digest
                == read_by_hash_text
                == book_bytes
            ),
            "source_replica_id": int(source_replica.replica_id),
            "archive_replica_id": int(archive_replica.replica_id),
            "placement_hints_reused": (
                archive_replica.placement_hints
                == source_replica.placement_hints
            ),
            "verified_replica_ids": [
                int(report.replica_id)
                for report in verification.replica_reports
                if report.healthy
            ],
            "composite_id": int(package.composite_digital_asset_id),
            "composite_item_role": item_resolution.role,
            "exported_members": [
                path.relative_to(root).as_posix() for path in exported
            ],
            "zip_members": list(zip_members),
        }


def main() -> int:
    """
    Run the workflows in the command-line work directory and print the sanitized summary. Parse
    process arguments, convert work_dir to Path, and delegate all storage effects and cleanup to
    run. Print only after run returns; false observation flags in its summary do not independently
    change this command's exit status.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero after rendering the summary; parser, workflow, cleanup, and rendering failures propagate.
    """
    args = parse_args()
    print(dump_json(run(Path(args.work_dir))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
