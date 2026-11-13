"""Require a complete, recoverable facade after replacing a live backend."""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template


def replacement(db, path=None):
    driver_type = load_database_driver(db.db_type)
    return driver_type({"database_path": str(path or db.driver.database_path)}, db=None)


def assert_live(db, label):
    for collaborator in (db.driver, db.driver_wrapper, db.macros, db.metadata_sql):
        assert collaborator.db is db
    assert db.driver.dirty_records_queue is db.dirty_records_queue
    assert db.driver_wrapper.dirty_records_queue is db.dirty_records_queue
    assert db.driver_wrapper.main_tables == db.main_tables
    assert db.driver_wrapper.dirtiable_tables == db.dirtiable_tables
    row_id = db.macros.insert_row("works", {"work_title": label})
    assert db.get_row_from_id("works", row_id)["work_title"] == label
    assert (
        db.driver_wrapper.fetch_scalar(
            "SELECT work_title FROM works WHERE work_id = ?", (row_id,)
        )
        == label
    )


@pytest.mark.parametrize("same_driver", [False, True])
@pytest.mark.parametrize("maintenance", [False, True])
def test_replacement_preserves_services_queues_and_functional_writes(
    db, same_driver, maintenance
):
    db._initialise_runtime_collaborators(enable_maintenance=maintenance)
    worker, queue, telemetry = (
        db.maintenance,
        db.dirty_records_queue,
        db.write_telemetry,
    )
    queue.put(("works", 1, "updated"))
    old_driver, old_wrapper, old_connection = db.driver, db.driver_wrapper, db.conn
    new_driver = old_driver if same_driver else replacement(db)
    extra = old_driver.get_connection()
    db._link_has_priority = {"obsolete": True}
    db._library_id_ = "obsolete"
    db._database_version_ = "obsolete"
    db.set_driver(new_driver)
    assert db.maintenance is db.maintainer is worker
    assert db.dirty_records_queue is queue
    assert db.write_telemetry is telemetry
    assert queue.get_nowait() == ("works", 1, "updated")
    assert db._link_has_priority == {}
    assert db._library_id_ is db._database_version_ is None
    assert db.metadata == new_driver.db_metadata
    assert old_wrapper.lock is None
    if same_driver:
        assert db.conn is old_connection
        assert extra.execute("SELECT 1").fetchone() == (1,)
    else:
        assert old_driver.db is old_driver.macros.db is None
        for connection in (old_connection, extra):
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
    if worker is not None:
        assert worker.maintainer.is_alive()
        assert db.driver.maintainer_callback is db._maintainer_callback_proxy
    assert_live(db, "after replacement")


@pytest.mark.parametrize("failure", ["schema", "wrapper"])
def test_failed_preparation_preserves_previous_driver_and_candidate_ownership(
    db, tmp_path, monkeypatch, failure
):
    from LiuXin_alpha.databases import database as module

    old_driver, old_wrapper, old_conn, old_macros = (
        db.driver,
        db.driver_wrapper,
        db.conn,
        db.macros,
    )
    if failure == "schema":
        path = tmp_path / "invalid.sqlite"
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("CREATE TABLE bogus (bogus_id INTEGER PRIMARY KEY)")
        new_driver = replacement(db, path)
        expected = DatabaseIntegrityError
    else:
        new_driver = replacement(db)

        def fail(*args, **kwargs):
            raise RuntimeError("injected wrapper failure")

        monkeypatch.setattr(module, "DriverWrapper", fail)
        expected = RuntimeError
    try:
        with pytest.raises(expected):
            db.set_driver(new_driver)
        assert (db.driver, db.driver_wrapper, db.conn, db.macros) == (
            old_driver,
            old_wrapper,
            old_conn,
            old_macros,
        )
        assert new_driver.db is new_driver.macros.db is None
        assert new_driver.conn.execute("SELECT 1").fetchone() == (1,)
        assert_live(db, "after failed replacement")
    finally:
        new_driver.close()


def test_replacement_discovers_new_catalogue_and_clears_identity_cache(db, tmp_path):
    path = tmp_path / "replacement.sqlite"
    shutil.copyfile(db.driver.database_path, path)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "CREATE TABLE replacement_items (replacement_item_id INTEGER PRIMARY KEY, replacement_item TEXT)"
        )
    db._library_id_ = "obsolete"
    new_driver = replacement(db, path)
    db.set_driver(new_driver)
    assert "replacement_items" in db.main_tables
    assert db.driver_wrapper.main_tables == db.main_tables
    assert db.metadata["database_path"] == str(path)
    assert db._library_id_ is None
    assert_live(db, "new catalogue")


def test_replacement_rejects_active_transaction_without_disturbing_it(db):
    new_driver = replacement(db)
    try:
        with db.macros.transaction():
            first = db.macros.insert_row("works", {"work_title": "before"})
            with pytest.raises(InputIntegrityError, match="macro transaction"):
                db.set_driver(new_driver)
            assert db.macros.get_row("works", first)["work_title"] == "before"
        assert_live(db, "after rejected switch")
    finally:
        new_driver.close()


def test_driver_cannot_be_stolen_from_another_facade(db):
    with Database(
        existing_driver=replacement(db), repair_bootstrap_rows=False
    ) as other:
        with pytest.raises(InputIntegrityError, match="another database"):
            db.set_driver(other.driver)
        assert_live(other, "other still owns its driver")
        assert_live(db, "original still owns its driver")


def test_catalogue_switch_with_queued_work_is_rejected(db, tmp_path):
    path = tmp_path / "other.sqlite"
    shutil.copyfile(db.driver.database_path, path)
    new_driver = replacement(db, path)
    db.dirty_records_queue.put(("works", 1, "updated"))
    old = db.driver
    try:
        with pytest.raises(InputIntegrityError, match="separate Database"):
            db.set_driver(new_driver)
        assert db.driver is old
        assert db.dirty_records_queue.get_nowait() == ("works", 1, "updated")
        assert_live(db, "original")
    finally:
        new_driver.close()


def test_replacement_keeps_storage_repository_on_the_current_transaction(db, tmp_path):
    from uuid import uuid4

    from LiuXin_alpha.storage.api import storage_manager_api as api
    from LiuXin_alpha.storage.durable_manager import StorageManager

    with StorageManager(db=db, startup_on_add=False) as manager:
        db.storage = manager
        report = db.storage_bootstrap_report
        repository = manager._metadata_repository
        assert repository is not None
        configuration = api.StoreConfiguration(
            store_uuid=uuid4(),
            store_name="replacement store",
            store_kind="local",
            store_root_uri=tmp_path.as_uri(),
        )
        new_driver = replacement(db)
        db.set_driver(new_driver)
        assert db.storage is manager
        assert db.storage_bootstrap_report is report
        assert repository.macros is db.macros
        with pytest.raises(RuntimeError, match="caller rollback"):
            with db.macros.transaction():
                store_id = repository.ensure_store(configuration)
                assert db.macros.get_row("stores", store_id) is not None
                raise RuntimeError("caller rollback")
        assert (
            db.macros.get_rows(
                "stores", where={"store_uuid": str(configuration.store_uuid)}
            )
            == ()
        )
        repository.ensure_store(configuration)
        assert (
            len(
                db.macros.get_rows(
                    "stores", where={"store_uuid": str(configuration.store_uuid)}
                )
            )
            == 1
        )
        path = tmp_path / "other.sqlite"
        shutil.copyfile(db.driver.database_path, path)
        other = replacement(db, path)
        try:
            with pytest.raises(InputIntegrityError, match="separate Database"):
                db.set_driver(other)
            assert db.storage is manager
        finally:
            other.close()


def test_catalogue_switch_cannot_discard_detached_owned_storage(db, tmp_path):
    """Removing an attachment does not release the facade's shutdown ownership."""
    db.bootstrap_storage_manager()
    manager = db.storage
    db.storage = None
    path = tmp_path / "other.sqlite"
    shutil.copyfile(db.driver.database_path, path)
    new_driver = replacement(db, path)
    original = db.driver
    try:
        with pytest.raises(InputIntegrityError, match="separate Database"):
            db.set_driver(new_driver)
        assert db.driver is original
        assert not manager._closed
        assert new_driver.db is None
        assert_live(db, "after refused owned-storage switch")
    finally:
        new_driver.close()
    db.close()
    assert manager._closed
