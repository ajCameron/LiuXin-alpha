"""Explicit offline rebuild command for supported SQLite catalogue search indexes.

Run with the interpreter that will subsequently open the catalogue. Stop every
other catalogue user and retain a backup first; this is not a format upgrader.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from LiuXin_alpha.databases.query_indexes import rebuild_sqlite_search_indexes


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the catalogue path and report success only after the rebuild commits."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-path", required=True)
    args = parser.parse_args(argv)
    rebuild_sqlite_search_indexes(args.database_path)
    print("Search indexes rebuilt successfully; the catalogue may now be reopened.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
