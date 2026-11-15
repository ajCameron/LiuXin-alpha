"""Exercise schema admission and undeclared-reference refusal on PostgreSQL."""

from contextlib import closing

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases.test_database_rereview_contracts import (
    UNDECLARED,
    assert_dynamic_fields_keep_base_revision,
    assert_pruning_refuses_undeclared_reference,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.mark.parametrize("damage", ["absent", "empty", "future", "malformed"])
def test_postgres_rejects_unsupported_revision(pg_database, damage):
    db, metadata, cleanup = pg_database
    commands = {
        "absent": "DROP TABLE schema_revision",
        "empty": "DELETE FROM schema_revision",
        "future": "UPDATE schema_revision SET schema_revision_revision=999999",
        "malformed": "ALTER TABLE schema_revision RENAME COLUMN schema_revision_revision TO broken",
    }
    with closing(db.driver.get_connection()) as conn:
        conn.execute(commands[damage]).close()
        conn.commit()
    for open_catalogue in (
        db.driver.get_connection,
        lambda: Database(metadata=metadata, db_type="PostgreSQL"),
    ):
        with pytest.raises(DatabaseIntegrityError, match="[Rr]egenerate"):
            open_catalogue()
    assert not transaction_active(db.driver.conn)


def test_postgres_dynamic_fields_keep_base_revision(pg_database):
    assert_dynamic_fields_keep_base_revision(pg_database[0])


@pytest.mark.parametrize(
    "reference", UNDECLARED, ids=[entry[1] for entry in UNDECLARED]
)
@pytest.mark.parametrize("bulk", [False, True])
def test_postgres_pruning_refuses_undeclared_reference(pg_database, reference, bulk):
    assert_pruning_refuses_undeclared_reference(pg_database[0], reference, bulk)
