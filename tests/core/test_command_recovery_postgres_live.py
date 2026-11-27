"""Exercise command receipts and language repairs on disposable PostgreSQL schemas."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.catalog_upgrade import (
    apply_catalog_schema_upgrade,
    plan_catalog_schema_upgrade,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@pytest.mark.parametrize(
    "mode", (None, "schema_backed", "database_backed", "numpy_vectorized")
)
def test_postgres_command_replay_and_outer_rollback(pg_database, mode):
    db, _, _ = pg_database
    command = CoreCommand("metadata.create", {"data": {"title": "Once"}})
    with CoreRuntime(library=SimpleNamespace(database=db), cache_type=mode) as runtime:
        first = runtime.execute_command(command).result
        assert runtime.execute_command(command).result["result"] == first["result"]
        assert len(runtime.catalog.works.list()) == 1
        rolled_back = CoreCommand("metadata.create", {"data": {"title": "Rolled back"}})
        with pytest.raises(RuntimeError, match="rollback"):
            with db.macros.transaction():
                assert (
                    runtime.execute_command(rolled_back).result["persistence"]
                    == "pending"
                )
                raise RuntimeError("rollback")
        assert (
            runtime.query("commands.outcome", {"command_id": rolled_back.command_id})[
                "state"
            ]
            == "not_found"
        )


def test_postgres_missing_language_upgrade_preserves_references(pg_database):
    db, _, _ = pg_database
    with db.macros.read_transaction() as connection:
        english = connection.execute(
            "SELECT language_id FROM languages WHERE language_code='eng'"
        ).fetchone()[0]
        total = connection.execute("SELECT COUNT(*) FROM languages").fetchone()[0]
    assert total > 400
    with db.macros.transaction() as connection:
        connection.execute(
            "CREATE TABLE language_reference (language_id bigint REFERENCES languages(language_id))"
        ).close()
        connection.execute(
            "INSERT INTO language_reference VALUES (?)", (english,)
        ).close()
        connection.execute(
            "DELETE FROM languages WHERE language_id<>?", (english,)
        ).close()
    preview = plan_catalog_schema_upgrade(db)
    assert preview.statements and not preview.blockers
    apply_catalog_schema_upgrade(db, preview)
    with db.macros.read_transaction() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM languages").fetchone()[0] == total
        )
        assert (
            connection.execute("SELECT language_id FROM language_reference").fetchone()[
                0
            ]
            == english
        )
    assert not plan_catalog_schema_upgrade(db).statements


def test_postgres_language_upgrade_rejects_stale_and_conflicting_input(pg_database):
    db, _, _ = pg_database
    with db.macros.transaction() as connection:
        connection.execute("DELETE FROM languages WHERE language_code='eng'").close()
    preview = plan_catalog_schema_upgrade(db)
    with db.macros.transaction() as connection:
        connection.execute(
            "UPDATE languages SET language='Changed display' WHERE language_code='fre'"
        ).close()
    with pytest.raises(ValueError, match="stale"):
        apply_catalog_schema_upgrade(db, preview)
    with db.macros.transaction() as connection:
        connection.execute(
            "UPDATE languages SET language_code='conflicting' WHERE language_iso639_2_b='fre'"
        ).close()
    conflicted = plan_catalog_schema_upgrade(db)
    assert conflicted.blockers
    with pytest.raises(ValueError, match="blocked"):
        apply_catalog_schema_upgrade(db, conflicted)
