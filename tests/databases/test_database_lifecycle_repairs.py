"""Require deterministic resource ownership on failed construction and shutdown."""

from contextlib import closing
from threading import Event
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.api import DatabaseAPI
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.databases.maintenance.engine import MaintenanceEngine
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.storage.durable_manager import StorageManager
from tests.databases import test_row_write_safety as fixtures

# The PostgreSQL module runs these same contracts with its own disposable schema.
db = fixtures.db
template = fixtures.template


@pytest.mark.parametrize("existing", [False, True])
def test_failed_construction_releases_acquired_resources(db, monkeypatch, existing):
    """A real worker, primary/auxiliary connections and manager all get closed."""
    captured = {}
    failure = RuntimeError("injected storage load failure")

    def fail_load(manager, database, **kwargs):
        captured.update(
            database=database,
            worker=database.maintenance.maintainer,
            primary=database.conn,
            extra=database.driver.get_connection(),
            manager=manager,
        )
        assert captured["worker"].is_alive()
        raise failure

    monkeypatch.setattr(StorageManager, "load_from_database", fail_load)
    options = (
        {
            "existing_driver": load_database_driver(db.db_type)(
                dict(db.metadata), db=None
            )
        }
        if existing
        else {"metadata": dict(db.metadata), "db_type": db.db_type}
    )
    with pytest.raises(RuntimeError) as error:
        Database(
            **options,
            repair_bootstrap_rows=False,
            enable_maintenance=True,
            enable_storage_manager=True,
            strict_storage_manager_bootstrap=True,
        )
    failed = captured["database"]
    try:
        assert error.value is failure
        assert failed.lifecycle_state == "closed"
        assert not captured["worker"].is_alive()
        assert captured["manager"]._closed
        for name in ("primary", "extra"):
            with pytest.raises(Exception, match="closed"):
                captured[name].execute("SELECT 1")
    finally:
        failed.close()


@pytest.mark.parametrize("existing", [False, True])
def test_constructor_binding_failure_respects_candidate_ownership(
    db, monkeypatch, existing
):
    """Only candidates created by the failing constructor belong to its cleanup."""
    from LiuXin_alpha.databases import database as module

    driver_type = load_database_driver(db.db_type)
    candidate = driver_type(dict(db.metadata), db=None)
    connection = candidate.conn
    failure = RuntimeError("wrapper preparation failed")

    def fail_wrapper(*args, **kwargs):
        raise failure

    monkeypatch.setattr(module, "DriverWrapper", fail_wrapper)
    monkeypatch.setattr(
        module, "load_database_driver", lambda name: lambda *a, **kw: candidate
    )
    try:
        with pytest.raises(RuntimeError) as error:
            if existing:
                Database(existing_driver=candidate, repair_bootstrap_rows=False)
            else:
                Database(
                    metadata=dict(db.metadata),
                    db_type=db.db_type,
                    repair_bootstrap_rows=False,
                )
        assert error.value is failure
        if existing:
            assert connection.execute("SELECT 1").fetchone() == (1,)
            assert candidate.db is None
        else:
            with pytest.raises(Exception, match="closed"):
                connection.execute("SELECT 1")
    finally:
        candidate.close()


def test_constructor_cleanup_failure_preserves_original_error_and_retry(
    db, monkeypatch
):
    captured = {}
    failure = ValueError("original bootstrap failure")

    def fail_load(manager, database, **kwargs):
        captured.update(database=database, manager=manager, close=manager.close)
        manager.close = lambda: (_ for _ in ()).throw(RuntimeError("shutdown failure"))
        raise failure

    monkeypatch.setattr(StorageManager, "load_from_database", fail_load)
    with pytest.raises(ValueError) as error:
        Database(
            metadata=dict(db.metadata),
            db_type=db.db_type,
            repair_bootstrap_rows=False,
            enable_storage_manager=True,
            strict_storage_manager_bootstrap=True,
        )
    failed = captured["database"]
    try:
        assert error.value is failure
        assert "shutdown failure" in " ".join(error.value.__notes__)
        assert failed.lifecycle_state == "closing"
        assert failed.conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        captured["manager"].close = captured["close"]
        failed.close()
    assert failed.lifecycle_state == "closed"
    assert captured["manager"]._closed


def test_close_timeout_preserves_worker_dependencies_and_permits_retry(db, monkeypatch):
    entered, release = Event(), Event()
    errors, observed = [], []
    driver, wrapper, macros = db.driver, db.driver_wrapper, db.macros

    def dispatch(engine, **kwargs):
        entered.set()
        try:
            assert release.wait(15), "test worker was not released"
            observed.append(engine.db.macros.get_rows("works"))
        except BaseException as error:
            errors.append(error)
        return {}

    monkeypatch.setattr(MaintenanceEngine, "run_once", dispatch)
    db._initialise_runtime_collaborators(enable_maintenance=True)
    worker = db.maintenance.maintainer
    try:
        assert entered.wait(10)
        with pytest.raises(RuntimeError, match="still running"):
            db.close()
        assert db.lifecycle_state == "closing"
        assert worker.is_alive()
        assert (db.driver, db.driver_wrapper, db.macros) == (driver, wrapper, macros)
        assert db.execute.__self__ is wrapper
        for operation in (
            lambda: db.set_driver(driver),
            lambda: db._initialise_runtime_collaborators(enable_maintenance=True),
            db.bootstrap_storage_manager,
        ):
            with pytest.raises(DatabaseIntegrityError, match="closing"):
                operation()
    finally:
        release.set()
        worker.join(10)
        db.close()
    assert not worker.is_alive()
    assert errors == []
    assert observed
    assert db.lifecycle_state == "closed"
    db.close()


def configured_store(db, tmp_path):
    """Boot a real SQLite object Store from a persisted configuration."""
    db.macros.insert_row(
        "stores",
        {
            "store_uuid": str(uuid4()),
            "store_name": "lifecycle object store",
            "store_kind": "single_file_sqlite",
            "store_access_protocol": "file",
            "store_root_uri": (tmp_path / "objects.sqlite").as_uri(),
            "store_is_read_only": 0,
            "store_online_status": "online",
        },
    )
    report = db.bootstrap_storage_manager(startup_on_add=True, strict=True)
    assert report.loaded_stores == 1
    store = tuple(db.storage.iter_stores())[0]
    assert store.driver._started
    return db.storage, store


def test_close_shuts_down_owned_storage_before_database(db, tmp_path, monkeypatch):
    manager, store = configured_store(db, tmp_path)
    original = store.driver.close
    observations = []

    def close_store():
        observations.append(db.driver.conn.execute("SELECT 1").fetchone())
        original()

    monkeypatch.setattr(store.driver, "close", close_store)
    db.close()
    assert observations == [(1,)]
    assert manager._closed and not store.driver._started
    db.close()
    assert observations == [(1,)]


@pytest.mark.parametrize("replace_owned", [False, True])
def test_injected_storage_remains_caller_owned(db, tmp_path, replace_owned):
    owned = configured_store(db, tmp_path) if replace_owned else None
    with StorageManager(db=db, startup_on_add=False) as injected:
        db.storage = injected
        db.bootstrap_storage_manager(startup_on_add=False)
        db.close()
        assert not injected._closed
        if owned:
            assert owned[0]._closed and not owned[1].driver._started
    assert injected._closed


def test_owned_store_shutdown_failure_is_reported_and_retried(
    db, tmp_path, monkeypatch
):
    manager, store = configured_store(db, tmp_path)
    original = store.driver.close
    attempts = []

    def fail_once():
        attempts.append(True)
        if len(attempts) == 1:
            raise RuntimeError("Store shutdown failed")
        original()

    monkeypatch.setattr(store.driver, "close", fail_once)
    with pytest.raises(RuntimeError, match="Store shutdown failed"):
        db.close()
    assert db.lifecycle_state == "closing"
    assert db.driver.conn.execute("SELECT 1").fetchone() == (1,)
    assert not manager._closed and store.driver._started
    db.close()
    assert len(attempts) == 2
    assert manager._closed and not store.driver._started


def test_context_preserves_body_exception_when_cleanup_fails(db, tmp_path, monkeypatch):
    manager, _ = configured_store(db, tmp_path)
    original = manager.close
    failure = ValueError("body failed")

    def fail_close():
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(manager, "close", fail_close)
    try:
        with pytest.raises(ValueError) as error, db:
            raise failure
        assert error.value is failure
        assert "cleanup failed" in " ".join(error.value.__notes__)
        assert db.lifecycle_state == "closing"
    finally:
        monkeypatch.setattr(manager, "close", original)
        db.close()


@pytest.mark.parametrize("collaborator", ["macros", "driver_wrapper", "metadata_sql"])
def test_shallow_setters_are_absent_and_collaborators_are_read_only(db, collaborator):
    driver_type = load_database_driver(db.db_type)
    with Database(
        existing_driver=driver_type(dict(db.metadata), db=None),
        repair_bootstrap_rows=False,
    ) as other:
        before = getattr(db, collaborator)
        assert not hasattr(DatabaseAPI, "set_" + collaborator)
        assert not hasattr(db, "set_" + collaborator)
        with pytest.raises(AttributeError):
            setattr(db, collaborator, getattr(other, collaborator))
        assert getattr(db, collaborator) is before
        row_id = db.macros.insert_row("works", {"work_title": "coherent owner"})
        with closing(db.driver.get_connection()) as independent:
            assert independent.execute(
                "SELECT work_title FROM works WHERE work_id = ?", (row_id,)
            ).fetchone() == ("coherent owner",)
        assert db.driver_wrapper.macros is db.driver.macros is db.macros
        assert db.execute.__self__ is db.driver_wrapper
