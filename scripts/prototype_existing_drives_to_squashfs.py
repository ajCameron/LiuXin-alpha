#!/usr/bin/env python3
"""Prototype: index existing drives and build SquashFS backup packs."""

from __future__ import annotations

import argparse
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from LiuXin_alpha.storage.backup import ExistingDriveSquashfsPrototype


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index one or more existing directories and build SquashFS backup packs.")
    parser.add_argument("paths", nargs="+", help="Existing directories to index and back up.")
    parser.add_argument("--db-path", required=True, help="SQLite database path to create/update.")
    parser.add_argument("--output-dir", required=True, help="Directory where SquashFS packs will be written.")
    parser.add_argument("--target-pack-size-bytes", type=int, default=None, help="Target pack size in bytes. Overrides --target-pack-size-gib.")
    parser.add_argument("--target-pack-size-gib", type=float, default=4.0, help="Target pack size in GiB when bytes are not provided.")
    parser.add_argument("--max-files-per-pack", type=int, default=None, help="Optional hard cap on files per pack.")
    parser.add_argument("--staging-root", default=None, help="Optional root directory for per-pack staging dirs.")
    parser.add_argument("--extension", action="append", default=None, dest="extensions", help="Optional ebook extension filter (repeatable).")
    parser.add_argument("--no-verify", action="store_true", help="Skip post-build SquashFS verification.")
    parser.add_argument("--cleanup-staging", action="store_true", help="Delete staging directories after successful build.")
    parser.add_argument("--delete-originals", action="store_true", help="Reserved for a later guarded workflow. Currently rejected on purpose.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.delete_originals:
        raise SystemExit("--delete-originals is intentionally not implemented yet. Build, verify, and review the packs first.")
    target_pack_size_bytes = int(args.target_pack_size_bytes) if args.target_pack_size_bytes is not None else int(float(args.target_pack_size_gib) * (1024 ** 3))
    if target_pack_size_bytes <= 0:
        raise SystemExit("Target pack size must be > 0.")
    prototype = ExistingDriveSquashfsPrototype(database_path=pathlib.Path(args.db_path), output_dir=pathlib.Path(args.output_dir), target_pack_size_bytes=target_pack_size_bytes, max_files_per_pack=args.max_files_per_pack, ebook_extensions=args.extensions, verify_after_build=not args.no_verify, cleanup_staging_after_success=bool(args.cleanup_staging), staging_root=None if args.staging_root is None else pathlib.Path(args.staging_root))
    prototype.run([pathlib.Path(item) for item in args.paths])
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
