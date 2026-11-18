"""Repeated full-schema cache loads and real captures around committed writes."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from LiuXin_alpha.caches.api import CacheQuery, CacheSort, CacheState
from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)
from tests.support.cache_long_runs import BACKENDS, long_marks
from tests.support.cache_long_runs import measurements as measurements
from tests.support.cache_long_runs import workload as workload

pytestmark = [*long_marks(), pytest.mark.integration, pytest.mark.db]


@pytest.fixture(params=("SQLite", "SQLite_apsw", "PostgreSQL"))
def database(request, tmp_path):
    """Own a full native catalogue, including all schema and empty link tables."""
    if request.param == "PostgreSQL":
        yield request.getfixturevalue("pg_database")[0]
        return
    with Database(
        metadata={"database_path": str(tmp_path / "long-cache.sqlite")},
        db_type=request.param,
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        yield db


@pytest.mark.parametrize("storage_type", BACKENDS)
def test_full_load_capture_and_transaction_cycles(
    database, storage_type, workload, measurements
):
    """No captured view is reused: each live request must observe durable changes."""
    db = database
    catalog = Catalog(db)
    # SQLite bulk insertion owns a fresh connection; do not hold an outer writer.
    db.driver.conn.commit()
    db.driver_wrapper.add_multiple_rows(
        [{"work_title": f"Longcache {i:06d}"} for i in range(workload.database_records)]
    )
    db.driver.conn.commit()
    # An independent source scan seeds the oracle; subsequent edits update it in Python.
    source = {
        row["work_id"]: row["work_title"]
        for row in db.macros.get_rows("works")
        if row["work_title"].startswith("Longcache ")
    }
    assert len(source) == workload.database_records
    db.driver.conn.commit()
    target = max(source)
    owned_connections = tuple(getattr(db.driver, "_open_connections", ()))
    query = CacheQuery(
        "works",
        where=ComparisonMatch("work_title", "prefix", "Longcache "),
        sort=(CacheSort("work_id", False),),
        limit=7,
        projection=("work_title",),
    )

    for cycle in range(workload.cycles):
        cache = ReadCache(db, storage_type=storage_type)
        captures = []
        capture_patch = None
        live = cache.capabilities.consistency.value == "live"
        try:
            measurements.measure("load", cache.load)
            if live:
                capture = cache.storage.query_view

                def counted_capture(capture=capture, captures=captures):
                    view = measurements.measure("live_capture", capture)
                    captures.append(1)
                    return view

                # Count real captures without replacing their work or results.
                capture_patch = patch.object(
                    cache.storage, "query_view", counted_capture
                )
                capture_patch.start()

            def check(phase, cache=cache, cycle=cycle):
                expected = tuple(
                    sorted(
                        (
                            i
                            for i, title in source.items()
                            if title.startswith("Longcache ")
                        ),
                        reverse=True,
                    )
                )
                result = measurements.measure(phase, cache.query, query)
                assert result.ids == expected[:7], (cycle, phase)
                assert result.total_count == len(expected)
                assert tuple(
                    record.values["work_title"] for record in result.records
                ) == tuple(source[i] for i in expected[:7])

            check("first_query")
            check("repeated_query")
            db.driver.conn.commit()
            title = (
                f"Changed outside snapshot {cycle}"
                if cycle % 2 == 0
                else f"Longcache restored {cycle}"
            )
            catalog.works.update(target, {"title": title})
            db.driver.conn.commit()
            source[target] = title
            if not live:
                cache.invalidate(ids={"works": (target,)})
            check("after_commit")
            # End any read-owned transaction before testing rollback ownership.
            db.driver.conn.commit()
            with pytest.raises(RuntimeError, match="rollback sentinel"):
                with db.macros.transaction():
                    catalog.works.update(target, {"title": "Longcache rolled back"})
                    raise RuntimeError("rollback sentinel")
            if not live:
                cache.invalidate(ids={"works": (target,)})
            check("after_rollback")
            if live:
                assert len(captures) == 4
        finally:
            if capture_patch is not None:
                capture_patch.stop()
            cache.close()
        assert cache.state == CacheState.CLOSED
        assert not cache._query_engine._results
        if owned_connections:
            assert tuple(db.driver._open_connections) == owned_connections
        db.driver.conn.commit()
        assert catalog.works.require(target)["work_title"] == source[target]
        db.driver.conn.commit()
