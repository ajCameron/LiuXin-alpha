#!/usr/bin/env python3
"""
Clean cached test assets after significant DB/schema changes.

What this deletes (best-effort):
  - <repo>/.pytest_cache and <repo>/tests/.pytest_cache
  - "liuxin_test_resources" template caches under OS temp, created by pytest's tmp_path_factory

Safe by default: only removes LiuXin-specific caches; does NOT delete whole pytest temp trees.

Usage:
  python scripts/clean_cached_test_assets.py
  python scripts/clean_cached_test_assets.py --dry-run
  python scripts/clean_cached_test_assets.py --aggressive   # also remove all "pytest-of-*" trees (dangerous)
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path


def _rm_tree(p: Path, *, dry_run: bool) -> None:
    if not p.exists():
        return
    if dry_run:
        print(f"[dry-run] would remove: {p}")
        return
    shutil.rmtree(p, ignore_errors=True)
    print(f"removed: {p}")


def _repo_root() -> Path:
    # scripts/clean_cached_test_assets.py -> repo root
    return Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print what would be deleted without deleting it.")
    ap.add_argument(
        "--aggressive",
        action="store_true",
        help="Also delete all pytest temp trees (pytest-of-*) under the OS temp dir.",
    )
    args = ap.parse_args(argv)

    repo = _repo_root()

    # 1) Local pytest caches in the repo
    _rm_tree(repo / ".pytest_cache", dry_run=args.dry_run)
    _rm_tree(repo / "tests" / ".pytest_cache", dry_run=args.dry_run)

    # 2) Pytest temp trees (OS temp) that contain our template caches
    temp_root = Path(tempfile.gettempdir())

    # Our cache_dir is: <basetemp>/liuxin_test_resources
    # basetemp typically looks like: <temp>/pytest-of-<user>/pytest-<n>
    removed_any = False
    for pytest_of in temp_root.glob("pytest-of-*"):
        if not pytest_of.is_dir():
            continue

        # Remove only LiuXin template caches unless aggressive
        if args.aggressive:
            _rm_tree(pytest_of, dry_run=args.dry_run)
            removed_any = True
            continue

        for cache in pytest_of.rglob("liuxin_test_resources"):
            if cache.is_dir():
                _rm_tree(cache, dry_run=args.dry_run)
                removed_any = True

    if not removed_any:
        print("no liuxin_test_resources caches found under OS temp (nothing to do)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
