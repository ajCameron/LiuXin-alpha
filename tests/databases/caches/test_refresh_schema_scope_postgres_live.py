"""Bound real refresh probes while retaining caller-owned PostgreSQL transactions."""

import pytest

from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.databases.transactions import transaction_active
from tests.databases.test_schema_discovery_postgres_live import probes as probes
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.mark.parametrize(
    "mode", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_refresh_shares_physical_probes_without_completing_outer_work(
    pg_database, probes, mode
):
    db, metadata, independent = pg_database
    probes.clear()
    cache = create_cache(db, mode)
    try:
        assert probes["fingerprints"] == 2
        for refresh in (cache.reload, cache.reload_data):
            probes.clear()
            refresh()
            assert probes["fingerprints"] == 2

        # The driver scope owns short discovery connections, never this SQL unit.
        db.driver.conn.rollback()
        with pytest.raises(RuntimeError, match="caller rolls back"):
            with db.macros.transaction() as outer:
                outer.execute("INSERT INTO probe VALUES ('caller-owned')").close()
                probes.clear()
                cache.reload_data()
                assert probes["fingerprints"] == 2
                assert db.macros.current_transaction_connection() is outer
                assert transaction_active(outer)
                independent.rollback()
                with independent.cursor() as cursor:
                    cursor.execute(f'SELECT value FROM "{metadata["schema"]}".probe')
                    assert cursor.fetchall() == []
                raise RuntimeError("caller rolls back")
        independent.rollback()
        with independent.cursor() as cursor:
            cursor.execute(f'SELECT value FROM "{metadata["schema"]}".probe')
            assert cursor.fetchall() == []
    finally:
        cache.close()
