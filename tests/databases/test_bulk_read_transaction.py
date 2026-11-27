"""Full cache captures borrow one database snapshot without closing or committing it."""

import pytest

from tests.core.test_native_metadata_postgres_live import pg_database as pg_database
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_bulk_read_sees_owned_writes_and_preserves_rollback(metadata_db):
    check_owned_snapshot(metadata_db)


def test_postgres_bulk_read_preserves_snapshot(pg_database):
    check_owned_snapshot(pg_database[0])


def check_owned_snapshot(database):
    with pytest.raises(RuntimeError, match="rollback"):
        with database.macros.transaction() as connection:
            identity = database.macros.insert_row(
                "works", {"work_title": "Uncommitted capture"}
            )
            rows = database.driver_wrapper.get_all_rows("works")
            assert any(row["work_id"] == identity for row in rows)
            assert connection.execute("SELECT 1").fetchone()[0] == 1
            raise RuntimeError("rollback")
    assert not any(
        row["work_title"] == "Uncommitted capture"
        for row in database.driver_wrapper.get_all_rows("works")
    )
