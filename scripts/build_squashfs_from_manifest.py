#!/usr/bin/env python3
"""
Build a SquashFS archive from a JSON file manifest.

Manifest format:
  - Either a JSON list of entry objects, or: {"files": [ ... ]}.
  - Each entry:
      {
        "source": "path/to/source/file.ext",     # required (aliases: src/path/file)
        "archive_path": "inside/archive/file.ext" # optional (aliases: internal_path/dest/target)
      }
  - If archive_path is omitted, the source basename is used.
"""

from __future__ import annotations

import argparse
import json
import sys

from pathlib import Path


def _bootstrap_src() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    return repo_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build SquashFS archive from JSON manifest")
    parser.add_argument("--manifest", required=True, help="Path to JSON manifest file")
    parser.add_argument("--output", required=True, help="Output .squashfs/.sqfs file path")
    parser.add_argument(
        "--manifest-base-dir",
        default=None,
        help="Base directory for relative source paths (default: manifest parent directory)",
    )
    parser.add_argument("--compression", default="zstd", help="mksquashfs compression codec (default: zstd)")
    parser.add_argument("--deterministic", action="store_true", help="Enable deterministic squashfs build flags")
    parser.add_argument("--force", action="store_true", help="Overwrite output archive if it exists")
    parser.add_argument("--no-quiet", action="store_true", help="Pass through mksquashfs output")
    return parser.parse_args()


def main() -> int:
    _bootstrap_src()
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import build_squashfs_from_manifest

    args = parse_args()
    report = build_squashfs_from_manifest(
        manifest_path=Path(args.manifest).expanduser(),
        output_archive=Path(args.output).expanduser(),
        manifest_base_dir=Path(args.manifest_base_dir).expanduser() if args.manifest_base_dir else None,
        compression=args.compression,
        deterministic=args.deterministic,
        force=args.force,
        quiet=not args.no_quiet,
    )
    print(json.dumps(report.__dict__, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
