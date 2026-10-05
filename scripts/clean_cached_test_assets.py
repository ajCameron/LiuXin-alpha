#!/usr/bin/env python3
"""
Remove regenerable cached test assets.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise clean cached test assets through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path


def _rm_tree(p: Path, *, dry_run: bool) -> None:
    """
    Perform the rm tree operation under explicit file-format and conversion rules.

    Example:
        Exercise  rm tree through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param p: Path-like value normalized or validated by the operation.
    :param dry_run: Value supplied for dry run under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not p.exists():
        return
    if dry_run:
        print(f"[dry-run] would remove: {p}")
        return
    shutil.rmtree(p, ignore_errors=True)
    print(f"removed: {p}")


def _repo_root() -> Path:
    # scripts/clean_cached_test_assets.py -> repo root
    """
    Perform the repo root operation under explicit file-format and conversion rules.

    Example:
        Exercise  repo root through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
