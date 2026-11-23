"""Keep Core composition failures and shutdown inside their ownership boundary."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event, RLock
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock

import pytest

from LiuXin_alpha.caches.api import CacheAPI, CacheState
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.core.services import CoreServiceReconciliationError, CoreServices


def cache_for(database):
    return Mock(spec=CacheAPI, database=database, state=CacheState.READY, generation=1)


def bound_storage(previous=None):
    storage = SimpleNamespace(metadata_cache=previous)
    storage.bind_metadata_cache = Mock(
        side_effect=lambda value: setattr(storage, "metadata_cache", value)
    )
    return storage


@pytest.mark.parametrize("dependency", ("catalog", "read_source", "cache"))
def test_rejects_foreign_dependencies_before_preparing_cache(monkeypatch, dependency):
    database = object()
    cache = cache_for(database)
    cache.state = CacheState.DIRTY
    inputs = {"cache": cache, dependency: SimpleNamespace(database=object())}
    prepare = Mock()
    monkeypatch.setattr(CoreServices, "_prepare_supplied_cache", prepare)
    with pytest.raises(ValueError, match="library database"):
        CoreServices(library=SimpleNamespace(database=database), **inputs)
    prepare.assert_not_called()


def test_valid_database_binding_does_not_evaluate_legacy_alias():
    database = object()
    catalog = Mock(database=database)
    type(catalog).db = PropertyMock(side_effect=AssertionError("legacy alias accessed"))
    services = CoreServices(library=SimpleNamespace(database=database), catalog=catalog)
    assert services.catalog is catalog
    services.close()


@pytest.mark.parametrize("retain_after_success", (False, True))
def test_failed_storage_binding_closes_new_cache_and_restores_prior_binding(
    monkeypatch, retain_after_success
):
    previous = object()
    storage = bound_storage(previous)
    database = SimpleNamespace(storage=storage)
    cache = cache_for(database)

    def bind(value):
        storage.metadata_cache = value
        if value is cache:
            raise RuntimeError("binding failed after attachment")

    storage.bind_metadata_cache.side_effect = bind
    monkeypatch.setattr("LiuXin_alpha.caches.cache.create_cache", lambda *a, **k: cache)
    library = SimpleNamespace(database=database, close=Mock())
    with pytest.raises(RuntimeError, match="binding failed"):
        CoreServices(
            library=library,
            cache_type="schema_backed",
            close_cache_on_shutdown=not retain_after_success,
        )
    cache.close.assert_called_once()
    library.close.assert_not_called()
    assert storage.metadata_cache is previous


def test_startup_keeps_original_error_when_cache_cleanup_also_fails(monkeypatch):
    storage = bound_storage()
    database = SimpleNamespace(storage=storage)
    cache = cache_for(database)
    storage.bind_metadata_cache.side_effect = RuntimeError("bind failure")
    cache.close.side_effect = ValueError("close failure")
    monkeypatch.setattr("LiuXin_alpha.caches.cache.create_cache", lambda *a, **k: cache)
    with pytest.raises(BaseExceptionGroup) as caught:
        CoreServices(
            library=SimpleNamespace(database=database), cache_type="schema_backed"
        )
    assert isinstance(caught.value.exceptions[0], RuntimeError)
    assert "bind failure" in str(caught.value.exceptions[0])
    assert any("close failure" in str(error) for error in caught.value.exceptions[1:])


def test_unbind_failure_does_not_skip_cache_or_library_close():
    storage = bound_storage()
    database = SimpleNamespace(storage=storage)
    cache = cache_for(database)
    library = SimpleNamespace(database=database, close=Mock())
    services = CoreServices(
        library=library,
        cache=cache,
        close_cache_on_shutdown=True,
        close_library_on_shutdown=True,
    )
    storage.bind_metadata_cache.side_effect = RuntimeError("unbind failed")
    with pytest.raises(BaseExceptionGroup):
        services.close()
    services.close()
    cache.close.assert_called_once()
    library.close.assert_called_once()


def test_old_owner_does_not_detach_replacement_session_cache():
    storage = bound_storage()
    database = SimpleNamespace(storage=storage)
    cache = cache_for(database)
    services = CoreServices(
        library=SimpleNamespace(database=database),
        cache=cache,
        close_cache_on_shutdown=True,
    )
    replacement = cache_for(database)
    storage.bind_metadata_cache(replacement)
    services.close()
    assert storage.metadata_cache is replacement
    cache.close.assert_called_once()
    replacement.close.assert_not_called()


@pytest.mark.parametrize(
    "name",
    (
        "catalog",
        "read_source",
        "preferences",
        "library_preferences",
        "field_metadata",
        "maintenance",
        "metadata",
        "field_writer",
    ),
)
def test_closed_services_reject_lazy_and_retained_service_access(name):
    supplied = {
        key: object()
        for key in (
            "catalog",
            "read_source",
            "preferences",
            "library_preferences",
            "field_metadata",
            "maintenance",
        )
    }
    services = CoreServices(library=SimpleNamespace(database=object()), **supplied)
    services.close()
    with pytest.raises(RuntimeError, match="closed"):
        getattr(services, name)


def test_status_failure_after_reload_preserves_committed_receipt():
    database = object()
    cache = cache_for(database)
    services = CoreServices(library=SimpleNamespace(database=database), cache=cache)
    type(cache).generation = PropertyMock(side_effect=ValueError("status unavailable"))
    receipt = {"entity_id": 7, "committed": True}
    with pytest.raises(CoreServiceReconciliationError) as caught:
        services.reconcile(receipt)
    assert caught.value.receipt == receipt
    assert isinstance(caught.value.__cause__, ValueError)
    services.close()


def test_closed_owner_rejects_refresh_without_discarding_borrowed_fields():
    fields = object()
    services = CoreServices(
        library=SimpleNamespace(database=object()), field_metadata=fields
    )
    services.close()
    with pytest.raises(RuntimeError, match="closed"):
        services.refresh_field_metadata()
    assert services._field_metadata is fields


def test_failed_direct_runtime_install_closes_constructed_cache(monkeypatch):
    database = object()
    cache = cache_for(database)
    monkeypatch.setattr("LiuXin_alpha.caches.cache.create_cache", lambda *a, **k: cache)
    monkeypatch.setattr(
        "LiuXin_alpha.core.application_api.install_application_api",
        Mock(side_effect=RuntimeError("endpoint install failed")),
    )
    manager = Mock()
    with pytest.raises(RuntimeError, match="endpoint install failed"):
        CoreRuntime(
            library=SimpleNamespace(database=database),
            cache_type="schema_backed",
            job_manager=manager,
        )
    cache.close.assert_called_once()
    manager.shutdown.assert_not_called()


def test_shutdown_attempts_entire_graph_when_durable_stop_fails(monkeypatch):
    runtime = CoreRuntime(
        library=SimpleNamespace(database=object()), job_manager=Mock()
    )
    runtime._durable_jobs = Mock()
    runtime._durable_jobs.stop.side_effect = RuntimeError("durable stop failed")
    close = Mock()
    monkeypatch.setattr(runtime.services, "close", close)
    with pytest.raises(BaseExceptionGroup):
        runtime.shutdown()
    close.assert_called_once()
    assert runtime.is_shutdown


def test_shutdown_preserves_service_and_manager_failures(monkeypatch):
    manager = Mock()
    manager.shutdown.side_effect = ValueError("manager stop failed")
    runtime = CoreRuntime(
        library=SimpleNamespace(database=object()),
        job_manager=manager,
        close_job_manager_on_shutdown=True,
    )
    monkeypatch.setattr(
        runtime.services, "close", Mock(side_effect=RuntimeError("services failed"))
    )
    with pytest.raises(BaseExceptionGroup) as caught:
        runtime.shutdown()
    assert [str(error) for error in caught.value.exceptions] == [
        "services failed",
        "manager stop failed",
    ]


def test_factory_failure_stops_new_durable_service_before_closing_graph(
    monkeypatch, tmp_path
):
    from LiuXin_alpha.core.factory import create_core

    calls = []
    library = SimpleNamespace(db=SimpleNamespace(uuid="review-library"))
    owner = SimpleNamespace(
        library=library, _abort_startup=lambda: calls.append("graph")
    )
    durable = Mock()
    durable.stop.side_effect = lambda **kwargs: calls.append("durable")
    monkeypatch.setattr(
        "LiuXin_alpha.core.library_services.LibraryServices.open",
        lambda **kwargs: owner,
    )
    monkeypatch.setattr(
        "LiuXin_alpha.core.durable_jobs.DurableJobService", lambda **kwargs: durable
    )
    monkeypatch.setattr(
        "LiuXin_alpha.core.factory.CoreRuntime",
        Mock(side_effect=RuntimeError("install failed")),
    )
    with pytest.raises(RuntimeError, match="install failed"):
        create_core(database_path=tmp_path / "library.sqlite", preferences={})
    assert calls == ["durable", "graph"]


def test_failed_initial_load_closes_created_cache(monkeypatch):
    database = object()
    cache = cache_for(database)
    cache.load.side_effect = RuntimeError("initial load failed")
    monkeypatch.setattr("LiuXin_alpha.caches.cache.create_cache", lambda *a, **k: cache)
    with pytest.raises(RuntimeError, match="initial load failed"):
        CoreServices(
            library=SimpleNamespace(database=database), cache_type="schema_backed"
        )
    cache.close.assert_called_once()


def test_runtime_startup_failure_preserves_borrowed_services(monkeypatch):
    storage = bound_storage()
    database = SimpleNamespace(storage=storage)
    cache = cache_for(database)
    library = SimpleNamespace(database=database, close=Mock())
    monkeypatch.setattr(
        "LiuXin_alpha.core.application_api.install_application_api",
        Mock(side_effect=RuntimeError("endpoint install failed")),
    )
    with pytest.raises(RuntimeError, match="endpoint install failed"):
        CoreRuntime(library=library, cache=cache, job_manager=Mock())
    assert storage.metadata_cache is None
    cache.close.assert_not_called()
    library.close.assert_not_called()


@pytest.mark.parametrize("failure_at", ("catalog", "runtime"))
@pytest.mark.parametrize("created_cache", (False, True))
def test_graph_startup_failure_restores_borrowed_database_binding(
    monkeypatch, failure_at, created_cache
):
    from LiuXin_alpha.core.factory import create_core

    previous = object()
    storage = bound_storage(previous)
    database = SimpleNamespace(storage=storage, close=Mock())
    cache = cache_for(database)
    monkeypatch.setattr("LiuXin_alpha.caches.cache.create_cache", lambda *a, **k: cache)
    monkeypatch.setattr(
        "LiuXin_alpha.library.Library",
        lambda **kwargs: SimpleNamespace(database=database, db=database),
    )
    target = (
        "LiuXin_alpha.catalog.catalog.Catalog"
        if failure_at == "catalog"
        else "LiuXin_alpha.core.factory.CoreRuntime"
    )
    monkeypatch.setattr(target, Mock(side_effect=RuntimeError("startup failed")))
    with pytest.raises(RuntimeError, match="startup failed"):
        create_core(
            database=database,
            catalog=None if failure_at == "catalog" else object(),
            cache=None if created_cache else cache,
            cache_type="schema_backed" if created_cache else None,
            enable_maintenance=False,
            enable_storage_manager=False,
            preferences={},
        )
    assert storage.metadata_cache is previous
    database.close.assert_not_called()
    assert cache.close.call_count == int(created_cache)


def test_lazy_maintenance_construction_cannot_escape_concurrent_close(monkeypatch):
    started, release = Event(), Event()
    maintainer = Mock()

    def build(database):
        started.set()
        assert release.wait(5)
        return maintainer

    monkeypatch.setattr("LiuXin_alpha.databases.maintenance.service.Maintainer", build)
    services = CoreServices(library=SimpleNamespace(database=object()))
    with ThreadPoolExecutor(2) as pool:
        construction = pool.submit(lambda: services.maintenance)
        try:
            assert started.wait(5)
            closing = pool.submit(services.close)
            with pytest.raises(TimeoutError):
                closing.result(timeout=0.1)
        finally:
            release.set()
        assert construction.result(timeout=5) is maintainer
        closing.result(timeout=5)
    maintainer.stop.assert_called_once()
    maintainer.maintainer.join.assert_called_once_with(timeout=1)
    with pytest.raises(RuntimeError, match="closed"):
        _ = services.maintenance


def test_waiting_for_database_does_not_hold_service_lifecycle_lock(monkeypatch):
    waiting = Event()
    actual = RLock()

    class DatabaseLock:
        def __enter__(self):
            waiting.set()
            actual.acquire()

        def __exit__(self, *args):
            actual.release()

    database = SimpleNamespace(lock=DatabaseLock())
    services = CoreServices(library=SimpleNamespace(database=database))

    def build(db):
        with db.lock:
            return object()

    monkeypatch.setattr("LiuXin_alpha.catalog.catalog.Catalog", build)
    with ThreadPoolExecutor(1) as pool:
        actual.acquire()
        try:
            construction = pool.submit(lambda: services.catalog)
            assert waiting.wait(5)
            # A database owner must be able to enter Core services while this
            # competitor waits, otherwise the two owners deadlock.
            available = services._lifecycle_lock.acquire(timeout=0.5)
            if available:
                services._lifecycle_lock.release()
            assert available
        finally:
            actual.release()
        construction.result(timeout=5)
    services.close()
