#!/usr/bin/env python3
"""Build the reproducible SQLite fixture used by benchmark scripts."""

from __future__ import annotations

import argparse
import sqlite3
import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from _benchmark_common import DEFAULT_CACHE_DIR, DEFAULT_DATABASES_DIR  # noqa: E402
from tests.support.test_resources_manager import (  # noqa: E402
    TestResourcesManager,
    build_profiled_test_database,
)


BENCHMARK_DB_NAMES = (
    "benchmark_db_smoke",
    "benchmark_db_medium",
    "benchmark_db_large",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic benchmark test DB for LiuXin-alpha."
    )
    parser.add_argument(
        "--name",
        default="benchmark_db_medium",
        choices=BENCHMARK_DB_NAMES,
        help="Named benchmark profile to provision.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Destination .test_db path. Defaults to LiuXin_data/benchmarks/databases/<name>.test_db.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Cache directory used when provisioning a named benchmark DB.",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of the cached template before provisioning.",
    )
    parser.add_argument(
        "--books",
        type=int,
        help="Override named profiles and build a custom DB with this many books.",
    )
    parser.add_argument(
        "--folders",
        type=int,
        help="Override named profiles and build a custom DB with this many folders.",
    )
    parser.add_argument(
        "--files",
        type=int,
        help="Override named profiles and build a custom DB with this many files.",
    )
    parser.add_argument(
        "--db-name",
        default="benchmark_db_custom",
        help="Logical DB name used for deterministic synthetic row values when using custom counts.",
    )
    return parser.parse_args()


def _count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()
    assert row is not None
    return int(row[0])


def _print_summary(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        print(f"db_path={db_path}")
        print(f"works={_count(conn, 'works')}")
        print(f"books={_count(conn, 'books')}")
        print(f"folders={_count(conn, 'folders')}")
        print(f"files={_count(conn, 'files')}")
    finally:
        conn.close()


def main() -> int:
    args = parse_args()

    output_raw = str(args.output or "").strip()
    if output_raw:
        output = Path(output_raw).expanduser().resolve()
    elif any(value is not None for value in (args.books, args.folders, args.files)):
        output = (DEFAULT_DATABASES_DIR / f"{args.db_name}.test_db").resolve()
    else:
        output = (DEFAULT_DATABASES_DIR / f"{args.name}.test_db").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    has_custom = any(value is not None for value in (args.books, args.folders, args.files))
    if has_custom:
        if args.books is None or args.folders is None or args.files is None:
            raise SystemExit("Custom benchmark builds require --books, --folders, and --files together.")
        build_profiled_test_database(
            db_path=output,
            db_name=args.db_name,
            books=args.books,
            folders=args.folders,
            files=args.files,
        )
        _print_summary(output)
        return 0

    mgr = TestResourcesManager(
        cache_dir=Path(args.cache_dir).expanduser(),
        regenerate=args.regenerate,
    )
    provision_root = output.parent / f".{args.name}.provisioned"
    provisioned = mgr.provision_named_test_database(
        name=args.name,
        dst_dir=provision_root,
    )
    shutil.copy2(provisioned.db_path, output)
    shutil.rmtree(provisioned.root, ignore_errors=True)
    _print_summary(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
