#!/usr/bin/env python3
"""
Inspect Store restoration from an existing catalogue and print its bootstrap report.

Open the database without automatic manager construction, then bootstrap explicitly
with offline-row, startup, and strictness flags. Report loaded Store names alongside
issues. Startup is opt-in; restored configuration alone does not prove that a Store
is currently reachable or that its bytes were verified.
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

from LiuXin_alpha.databases.database import Database


def parse_args() -> argparse.Namespace:
    """
    Parse a required catalogue path and database type defaulting to SQLite. --include-offline admits
    offline rows, --startup-on-add probes added Stores, and --strict requests bootstrap exceptions.
    All three flags default to false; this command does not expose database creation.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Print storage bootstrap report")
    parser.add_argument("--database", required=True, help="Path to LiuXin database file")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type")
    parser.add_argument("--include-offline", action="store_true", help="Load rows marked offline")
    parser.add_argument("--startup-on-add", action="store_true", help="Call store.startup() while loading")
    parser.add_argument("--strict", action="store_true", help="Raise on bootstrap errors")
    return parser.parse_args()


def main() -> int:
    """
    Open the catalogue, explicitly bootstrap its manager, and print report plus Store names. Expand
    the database path without resolving it. Disable creation, backup, and automatic manager
    initialization, then call bootstrap_storage_manager with clear_existing=True and the requested
    flags. Read names from db.storage when present, otherwise report an empty list. Print the
    sanitized payload inside the database context, whose exit owns cleanup. A report can contain
    issues without raising when strict mode is disabled.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Zero for report.ok, otherwise two; parser, database, strict bootstrap, and cleanup errors propagate.
    """
    args = parse_args()
    db_path = Path(args.database).expanduser()

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=args.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
    ) as db:
        report = db.bootstrap_storage_manager(
            startup_on_add=args.startup_on_add,
            include_offline=args.include_offline,
            clear_existing=True,
            strict=args.strict,
        )
        payload = {
            "database_path": str(db_path),
            "report": report,
            "loaded_store_names": (
                [
                    store.configuration.store_name
                    for store in db.storage.iter_stores()
                ]
                if db.storage is not None
                else []
            ),
        }
        print(dump_json(payload))
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
