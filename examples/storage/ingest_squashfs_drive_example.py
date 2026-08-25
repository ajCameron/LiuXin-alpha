#!/usr/bin/env python3
"""Catalogue existing SquashFS images and members without copying them."""

from __future__ import annotations

import argparse
import sys

from collections.abc import Mapping
from contextlib import redirect_stdout
from pathlib import Path


EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import (  # pyright: ignore[reportImplicitRelativeImport]
    bootstrap_src_path,
    dump_json,
)


bootstrap_src_path()

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage.ingest import SquashfsDriveIngestWorkflow
from LiuXin_alpha.storage.store_manager import StorageManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Discover SquashFS images beneath an existing drive, register the "
            "drive and images as read-only Stores, and adopt their bytes into "
            "the durable Digital Asset catalogue"
        )
    )
    parser.add_argument("--drive-root", required=True, help="Existing drive tree")
    parser.add_argument("--database", required=True, help="LiuXin catalogue path")
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Inspect only files directly beneath the drive root",
    )
    parser.add_argument("--max-archives", type=int)
    parser.add_argument("--max-members-per-archive", type=int)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-read adopted bytes to mark their Replicas verified",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Stop at the first bad archive or member instead of reporting it",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    drive_root = Path(args.drive_root).expanduser().resolve(strict=False)
    database_path = Path(args.database).expanduser().resolve(strict=False)
    create = not database_path.exists()

    # Legacy schema construction is chatty; preserve stdout for the JSON result.
    with redirect_stdout(sys.stderr):
        database = Database(
            metadata={"database_path": str(database_path)},
            create=create,
            backup=False,
            enable_storage_manager=False,
        )

    def progress(event: str, details: Mapping[str, object]) -> None:
        if event == "archive_started":
            print(
                "[{}/{}] {}".format(
                    details["archive_number"],
                    details["archive_count"],
                    details["archive_path"],
                ),
                file=sys.stderr,
                flush=True,
            )
        elif event == "archive_complete":
            print(
                "  members={} new_replicas={} issues={}".format(
                    details["members_discovered"],
                    details["member_replicas_created"],
                    details["issue_count"],
                ),
                file=sys.stderr,
                flush=True,
            )

    manager = StorageManager(
        db=database,
        startup_on_add=True,
    )
    with database, manager:
        if not create:
            bootstrap = manager.load_from_database(startup=True)
            if not bootstrap.ok:
                for issue in bootstrap.issues:
                    print(
                        f"Store bootstrap warning: {issue.store_name}: {issue.reason}",
                        file=sys.stderr,
                    )
        report = SquashfsDriveIngestWorkflow(
            manager,
            recursive=not args.no_recursive,
            continue_on_error=not args.strict,
            max_archives=args.max_archives,
            max_members_per_archive=args.max_members_per_archive,
            verify_archive_images=args.verify,
            verify_members=args.verify,
            progress_callback=progress,
        ).ingest(drive_root)
        payload = {
            "database": str(database_path),
            "metadata_is_durable": manager.metadata_is_durable,
            "source_root": report.source_root,
            "source_store_ref": str(report.source_store_ref),
            "source_store_created": report.source_store_created,
            "files_examined": report.files_examined,
            "non_squashfs_files": report.non_squashfs_files,
            "skipped_symlinks": report.skipped_symlinks,
            "archives_discovered": report.archives_discovered,
            "archives_succeeded": report.archives_succeeded,
            "archives_failed": report.archives_failed,
            "members_discovered": report.members_discovered,
            "member_assets_created": report.member_assets_created,
            "member_replicas_created": report.member_replicas_created,
            "truncated": report.truncated,
            "ok": report.ok,
            "archives": report.archives,
            "issues": report.issues,
        }
    print(dump_json(payload))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
