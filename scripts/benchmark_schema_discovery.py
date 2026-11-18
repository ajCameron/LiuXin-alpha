"""Measure full PostgreSQL schema discovery in an owned disposable schema.

Set LIUXIN_TEST_POSTGRES_URL to a test server. Only the uniquely named schema
created by this script is dropped. Setup/teardown are outside the measurements.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
from collections import Counter
from contextlib import closing
from time import perf_counter
from unittest.mock import patch
from uuid import uuid4

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)


def main() -> None:
    """Report timings and actual SQL/connection counts for forced schema rebuilds."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url or args.repetitions < 1:
        parser.error("Set LIUXIN_TEST_POSTGRES_URL and use positive repetitions")
    metadata = {"postgres_url": url, "schema": f"discovery_bench_{uuid4().hex}"}
    samples = []
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        try:
            with Database(
                metadata=metadata,
                db_type="PostgreSQL",
                create=True,
                backup=False,
                repair_bootstrap_rows=False,
                enable_storage_manager=False,
                enable_maintenance=False,
            ) as db:
                execute = PostgresConnectionAdapter.execute
                connect = db.driver.get_connection
                counts: Counter[str] = Counter()

                def counted_execute(connection, sql, *params, **kwargs):
                    counts["statements"] += 1
                    if "as schema_fingerprint" in sql.lower():
                        counts["fingerprints"] += 1
                    return execute(connection, sql, *params, **kwargs)

                def counted_connect():
                    counts["connections"] += 1
                    return connect()

                for _ in range(args.repetitions):
                    counts.clear()
                    with (
                        patch.object(
                            PostgresConnectionAdapter, "execute", counted_execute
                        ),
                        patch.object(db.driver, "get_connection", counted_connect),
                    ):
                        start = perf_counter()
                        schema = db.driver_wrapper.get_schema_spec(force_refresh=True)
                        elapsed = perf_counter() - start
                    assert {"works", "agents", "digital_assets"} <= schema.tables.keys()
                    samples.append(
                        {
                            "seconds": elapsed,
                            **counts,
                            "tables": len(schema.tables),
                            "interlinks": len(schema.interlinks),
                            "intralinks": len(schema.intralinks),
                        }
                    )
        finally:
            cleanup.rollback()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(
                        f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                    )
    print(
        json.dumps(
            {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "workload": "full generated native schema; forced wrapper rebuild; driver caches warm",
                "samples": samples,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
