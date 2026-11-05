"""Check live collaborator contracts and cleanup against real database facades."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.database import TelemetryMaintainerProxy
from LiuXin_alpha.databases.runtime import initialise_database_runtime
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template


@pytest.mark.parametrize("name", ["driver", "driver_wrapper", "macros"])
def test_live_collaborator_access_rejects_closed_database(db, name):
    assert getattr(db, name) is not None
    db.close()
    assert getattr(db, "_" + name) is None
    with pytest.raises(DatabaseIntegrityError, match="not bound|closed"):
        getattr(db, name)
    db.close()


def test_runtime_helper_rebinds_database_owners(db, monkeypatch):
    from LiuXin_alpha.databases.maintenance import service
    from LiuXin_alpha.databases import runtime

    maintainer = SimpleNamespace(
        clean=lambda table, item_ids: None, start=lambda: None, close=lambda: None
    )
    factory = lambda database, **kwargs: maintainer
    monkeypatch.setattr(service, "Maintainer", factory)
    monkeypatch.setattr(runtime, "Maintainer", factory)
    driver, wrapper, macros, metadata_sql = (
        db.driver,
        db.driver_wrapper,
        db.macros,
        db.metadata_sql,
    )
    for collaborator in (driver, wrapper, macros, metadata_sql):
        collaborator.db = None
    preferences = {"runtime-test": True}
    initialise_database_runtime(
        db, callback_proxy_cls=TelemetryMaintainerProxy, preferences_obj=preferences
    )
    assert db.preferences is preferences
    assert db.maintenance is db.maintainer is maintainer
    assert driver.maintainer_callback is db._maintainer_callback_proxy
    for collaborator in (driver, wrapper, macros, metadata_sql):
        assert collaborator.db is db


def test_closed_database_preference_write_preserves_cached_value(db):
    from LiuXin_alpha.databases.dbprefs import DBPrefs

    preferences = DBPrefs(db)
    preferences.disable_setting = True
    preferences["lifecycle_probe"] = 7
    preferences.disable_setting = False
    db.close()
    with pytest.raises(DatabaseIntegrityError, match="closed database"):
        preferences["lifecycle_probe"] = 8
    assert preferences["lifecycle_probe"] == 7


class PreparedMaintainer:
    """Expose leaked starts and injected failures without timing-dependent sleeps."""

    def __init__(self, database, *, autostart=True):
        self.db = database
        self.started = 0
        self.closed = 0
        self.fail_start = False
        self.fail_close = False
        if autostart:
            self.start()

    def clean(self, *args, **kwargs):
        pass

    def start(self):
        self.started += 1
        if self.fail_start:
            raise RuntimeError("injected start failure")

    def close(self):
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("maintenance worker is still running")


@pytest.fixture
def prepared_maintainers(monkeypatch):
    from LiuXin_alpha.databases.maintenance import service

    created = []

    def create(database, **kwargs):
        worker = PreparedMaintainer(database, **kwargs)
        created.append(worker)
        return worker

    monkeypatch.setattr(service, "Maintainer", create)
    return created


def test_reinitialization_reuses_worker_and_disabling_closes_it(
    db, prepared_maintainers
):
    db._initialise_runtime_collaborators(enable_maintenance=True)
    worker = db.maintenance
    db._initialise_runtime_collaborators(enable_maintenance=True)
    assert db.maintenance is db.maintainer is worker
    assert len(prepared_maintainers) == worker.started == 1
    db._initialise_runtime_collaborators(enable_maintenance=False)
    assert worker.closed == 1
    assert db.maintenance is db.maintainer is None
    assert db._maintainer_callback_proxy is None
    db._initialise_runtime_collaborators(enable_maintenance=True)
    assert db.maintenance is not worker
    assert len(prepared_maintainers) == 2


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("failure", ["callback", "start"])
def test_runtime_failure_preserves_previous_graph_and_preferences(
    db, prepared_maintainers, existing, failure
):
    if existing and failure == "start":
        # An existing worker must never be started a second time.
        db._initialise_runtime_collaborators(enable_maintenance=True)
        db.maintenance.fail_start = True
        db._initialise_runtime_collaborators(enable_maintenance=True)
        assert db.maintenance.started == 1
        return
    if existing:
        db._initialise_runtime_collaborators(enable_maintenance=True)
    before = (
        db.maintenance,
        db.maintainer,
        db.driver.maintainer_callback,
        db._maintainer_callback_proxy,
        db.clean,
        db.preferences,
    )

    def proxy(worker, telemetry):
        assert worker.started == (1 if existing else 0)
        if failure == "callback":
            raise RuntimeError("injected callback failure")
        worker.fail_start = True
        return TelemetryMaintainerProxy(worker, telemetry)

    with pytest.raises(RuntimeError, match="injected"):
        initialise_database_runtime(
            db, callback_proxy_cls=proxy, preferences_obj={"should-not-survive": True}
        )
    assert (
        db.maintenance,
        db.maintainer,
        db.driver.maintainer_callback,
        db._maintainer_callback_proxy,
        db.clean,
        db.preferences,
    ) == before
    worker = prepared_maintainers[-1]
    assert worker.closed == (0 if existing else 1)
    assert worker.started == (1 if existing or failure == "start" else 0)


def test_failed_worker_shutdown_does_not_discard_its_owner(db, prepared_maintainers):
    db._initialise_runtime_collaborators(enable_maintenance=True)
    worker, callback = db.maintenance, db.driver.maintainer_callback
    worker.fail_close = True
    with pytest.raises(RuntimeError, match="still running"):
        db._initialise_runtime_collaborators(enable_maintenance=False)
    assert db.maintenance is db.maintainer is worker
    assert db.driver.maintainer_callback is callback


def test_real_worker_lifecycle_has_no_orphan_threads(db):
    db._initialise_runtime_collaborators(enable_maintenance=True)
    worker = db.maintenance.maintainer
    assert worker.is_alive()
    db._initialise_runtime_collaborators(enable_maintenance=True)
    assert db.maintenance.maintainer is worker
    db._initialise_runtime_collaborators(enable_maintenance=False)
    assert not worker.is_alive()


def test_real_prepared_maintainer_can_close_without_starting(db):
    from LiuXin_alpha.databases.maintenance.service import Maintainer

    worker = Maintainer(db, plugins=[], autostart=False)
    assert worker.maintainer.ident is None
    worker.close()
    worker.close()
    assert not worker.maintainer.is_alive()


def test_wiring_failure_restores_all_backrefs_without_starting_worker(
    db, prepared_maintainers
):
    class FailingOwner:
        def __init__(self):
            self._db = None
            self.failed = False

        @property
        def db(self):
            return self._db

        @db.setter
        def db(self, value):
            if value is db and not self.failed:
                self.failed = True
                raise RuntimeError("injected owner wiring failure")
            self._db = value

    helper = db._metadata_sql
    db._metadata_sql = FailingOwner()
    owners = (db.driver, db.driver_wrapper, db.macros)
    for owner in owners:
        owner.db = None
    try:
        with pytest.raises(RuntimeError, match="owner wiring"):
            db._initialise_runtime_collaborators(enable_maintenance=True)
        assert all(owner.db is None for owner in owners)
        assert db._metadata_sql.db is None
        assert db.maintenance is None
        assert prepared_maintainers[0].started == 0
        assert prepared_maintainers[0].closed == 1
    finally:
        db._metadata_sql = helper
        for owner in owners:
            owner.db = db


def test_missing_collaborator_is_rejected_before_worker_construction(
    db, prepared_maintainers
):
    helper = db._metadata_sql
    db._metadata_sql = None
    try:
        with pytest.raises(RuntimeError, match="not initialized"):
            db._initialise_runtime_collaborators(enable_maintenance=True)
        assert prepared_maintainers == []
    finally:
        db._metadata_sql = helper


def test_startup_cleanup_failure_retains_worker_for_close(db, prepared_maintainers):
    def proxy(worker, telemetry):
        worker.fail_start = True
        worker.fail_close = True
        return TelemetryMaintainerProxy(worker, telemetry)

    with pytest.raises(RuntimeError, match="injected start") as error:
        db._initialise_runtime_collaborators(
            enable_maintenance=True, callback_proxy_cls=proxy
        )
    worker = prepared_maintainers[0]
    assert db.maintenance is None
    assert db._failed_runtime_maintainers == [worker]
    assert "retained for close" in " ".join(error.value.__notes__)
    worker.fail_close = False
    db.close()
    assert worker.closed == 2
    assert db._failed_runtime_maintainers == []
