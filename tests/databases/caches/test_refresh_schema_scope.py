"""Keep schema verification bounded without publishing failed cache refreshes."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.caches.api import CacheState
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


def backend_method(cache, operation):
    if operation == "load":
        return "read"
    if operation == "reload_data" and callable(
        getattr(cache.storage, "reload_data", None)
    ):
        return "reload_data"
    return "reload"


@pytest.mark.parametrize("operation", ("load", "reload", "reload_data"))
def test_full_refresh_publishes_only_after_schema_verification(
    modern_cache, monkeypatch, operation
):
    cache, db = modern_cache
    events = []
    active = False

    @contextmanager
    def scope():
        nonlocal active
        events.append("enter")
        active = True
        try:
            yield
            assert cache.state is CacheState.DIRTY
            events.append("verified")
        finally:
            active = False

    monkeypatch.setattr(
        db, "driver", SimpleNamespace(schema_read_scope=scope), raising=False
    )
    name = backend_method(cache, operation)
    original = getattr(cache.storage, name)

    def refresh(*args, **kwargs):
        assert active
        events.append("refresh")
        return original(*args, **kwargs)

    monkeypatch.setattr(cache.storage, name, refresh)
    generation = cache.generation
    getattr(cache, operation)()
    assert events == ["enter", "refresh", "verified"]
    assert cache.state is CacheState.READY
    assert cache.generation == generation + 1
    assert not active


@pytest.mark.parametrize("operation", ("load", "reload", "reload_data"))
@pytest.mark.parametrize("failure", ("enter", "refresh", "exit"))
def test_scope_failures_require_recovery_before_reading(
    modern_cache, monkeypatch, operation, failure
):
    cache, db = modern_cache
    failed_at = failure
    events = []

    @contextmanager
    def scope():
        events.append("enter")
        if failed_at == "enter":
            raise OSError("schema entry failed")
        try:
            yield
            if failed_at == "exit":
                raise OSError("schema changed before publication")
        finally:
            events.append("leave")

    monkeypatch.setattr(
        db, "driver", SimpleNamespace(schema_read_scope=scope), raising=False
    )
    name = backend_method(cache, operation)
    original = getattr(cache.storage, name)

    def refresh(*args, **kwargs):
        events.append("refresh")
        if failed_at == "refresh":
            raise OSError("table read failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(cache.storage, name, refresh)
    generation = cache.generation
    with pytest.raises(OSError):
        getattr(cache, operation)()
    assert cache.state is CacheState.DIRTY
    assert cache.generation == generation + 1
    assert cache._full_reload_required
    if failure == "enter":
        assert events == ["enter"]
    else:
        assert events == ["enter", "refresh", "leave"]
    failed_at = None
    assert cache.get("books", 1).value["rating"] == 4
    assert cache.state is CacheState.READY
    assert not cache._full_reload_required
    assert cache.generation == generation + 2


@pytest.mark.parametrize("operation", ("clear", "close"))
def test_cache_cleanup_does_not_probe_the_database(
    modern_cache, monkeypatch, operation
):
    cache, db = modern_cache
    scope = Mock(side_effect=AssertionError("database already closed"))
    monkeypatch.setattr(
        db, "driver", SimpleNamespace(schema_read_scope=scope), raising=False
    )
    getattr(cache, operation)()
    scope.assert_not_called()
    assert cache.state is (
        CacheState.EMPTY if operation == "clear" else CacheState.CLOSED
    )
