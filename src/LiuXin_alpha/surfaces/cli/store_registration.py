"""Register a local disk through Core's one-shot library lifecycle.

Run with ``python -m LiuXin_alpha.surfaces.cli.store_registration``. The command
prints a report only after the owned graph closes successfully.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from LiuXin_alpha.core.store_registration import (
    register_existing_disk_with_database_path,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    """
    Construct the standalone local-disk registration grammar without opening a database.

    Parsing does not open a library. Remote registration uses the Core helpers
    or the general workflow CLI.

    Example:
        >>> _build_arg_parser().parse_args(["--database", "catalogue.sqlite", "--disk", "/books"]).no_hash
        False


    :return: Fresh ArgumentParser with required database/disk paths and output/failure flags.
    """
    parser = argparse.ArgumentParser(
        description="Register ebook files from an unmanaged disk into LiuXin files table."
    )
    parser.add_argument(
        "--database", required=True, help="Path to the target LiuXin database file."
    )
    parser.add_argument(
        "--disk", required=True, help="Root path of the existing disk/folder to index."
    )
    parser.add_argument(
        "--db-type", default="SQLite", help="Database driver type (default: SQLite)."
    )
    parser.add_argument(
        "--store-name", default=None, help="Optional store name override."
    )
    parser.add_argument(
        "--no-hash",
        action="store_true",
        help="Skip content hashing while ingesting files (faster, less integrity data).",
    )
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinked directories while walking the disk.",
    )
    parser.add_argument(
        "--no-store-links",
        action="store_true",
        help="Do not create `file_store_links` rows (only set files.file_store_id).",
    )
    parser.add_argument("--json", action="store_true", help="Print report as JSON.")
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit non-zero if any file-level errors occurred.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """
    Parse local-disk registration options, run the database-path wrapper, and print its report.

    Emit either sorted JSON or fixed text counters. Report errors affect the return code only with
    --fail-on-errors. Parsing can raise SystemExit and setup/output exceptions propagate after any
    prior effects.

    Example:
        >>> status = main(["--database", str(catalogue), "--disk", str(root), "--json"])  # doctest: +SKIP


    :param argv: Argument sequence, or None to read the process command line.
    :return: 2 when fail-on-errors is selected and report.errors is nonempty; otherwise 0.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    report = register_existing_disk_with_database_path(
        database_path=args.database,
        disk_path=args.disk,
        db_type=args.db_type,
        store_name=args.store_name,
        compute_hash=not args.no_hash,
        follow_symlinks=args.follow_symlinks,
        attach_store_links=not args.no_store_links,
    )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(f"Store: {report.store_name} ({report.store_root_uri})")
        print(f"Store row id: {report.store_row_id}")
        print(f"Scanned files: {report.scanned_files}")
        print(f"Ebook candidates: {report.ebook_candidates}")
        print(f"Inserted files: {report.inserted_files}")
        print(f"Updated files: {report.updated_files}")
        print(f"Unchanged files: {report.unchanged_files}")
        print(f"Linked files: {report.linked_files}")
        print(f"Skipped non-ebook files: {report.skipped_non_ebook_files}")
        print(f"Errors: {len(report.errors)}")
        if report.duration_seconds is not None:
            print(f"Duration (seconds): {report.duration_seconds:.3f}")

    if args.fail_on_errors and report.errors:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
