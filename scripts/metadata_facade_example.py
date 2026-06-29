#!/usr/bin/env python3
"""Example workflow for the top-level metadata facade."""

from __future__ import annotations

import argparse
from pathlib import Path

from LiuXin_alpha import metadata
from LiuXin_alpha.databases.database import Database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Hydrate metadata, round-trip it through OPF, and optionally write a changed tag back.",
    )
    parser.add_argument("database", type=Path, help="Path to a LiuXin .test_db/.sqlite database.")
    parser.add_argument("item_id", type=int, help="Item id to hydrate.")
    parser.add_argument(
        "--source",
        choices=("database", "cache"),
        default="database",
        help="Read directly from the database or through a storage cache.",
    )
    parser.add_argument(
        "--lazy",
        action="store_true",
        help="Hydrate a lazy metadata object.",
    )
    parser.add_argument(
        "--opf",
        type=Path,
        default=None,
        help="Optional OPF path to write, then read back.",
    )
    parser.add_argument(
        "--add-tag",
        default=None,
        help="Optional tag to add to the OPF-read metadata.",
    )
    parser.add_argument(
        "--write-back",
        action="store_true",
        help="Write --add-tag back to the database.",
    )
    args = parser.parse_args()

    with Database(
        metadata={"database_path": str(args.database)},
        create=False,
        backup=False,
        enable_storage_manager=False,
        enable_maintenance=bool(args.write_back),
        repair_bootstrap_rows=False,
    ) as db:
        hydrated = metadata.metadata_from_database(
            db,
            item_id=args.item_id,
            source=args.source,
            lazy=args.lazy,
        )
        print(hydrated)

        if args.opf is None:
            return 0

        metadata.metadata_to_opf_file(hydrated, args.opf)
        print("Wrote OPF:", args.opf)

        round_tripped = metadata.metadata_from_opf(
            args.opf,
            kind="wemi",
            database=db,
            item_id=args.item_id,
        )
        print(round_tripped)

        if args.add_tag is None:
            return 0

        round_tripped.tags = args.add_tag
        print("Added tag:", args.add_tag)

        if args.write_back:
            report = round_tripped.write_to_database(
                db,
                fields=("tags",),
                item_id=args.item_id,
            )
            print("Write-back changed:", report.changed)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
