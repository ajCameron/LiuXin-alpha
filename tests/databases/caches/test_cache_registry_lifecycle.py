"""Atomic backend naming, immutable bindings, and failure/concurrency contracts."""

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Barrier, Event
from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches.cache_plugins import registry


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch):
    monkeypatch.setattr(
        registry, "_CACHE_PLUGIN_REGISTRY", dict(registry._CACHE_PLUGIN_REGISTRY)
    )


def test_identical_registration_is_idempotent_and_normalizes_names():
    registry.register_cache_plugin(
        " Custom ", cache_module="example.cache", aliases=(" FAST ", "Quick")
    )
    first = registry._get_registration("fast")
    registry.register_cache_plugin(
        "CUSTOM", cache_module="example.cache", aliases=("quick", "fast")
    )
    assert registry._get_registration(" quick ") is first
    assert registry._get_registration(" CUSTOM ") is first
    assert registry.get_registered_cache_plugin_names().count("custom") == 1
    registry.register_builtin_cache_plugins()


@pytest.mark.parametrize(
    "kwargs",
    (
        {"name": "schema", "cache_module": "example.replacement"},
        {
            "name": "new",
            "cache_module": "example.replacement",
            "aliases": ("fresh", "live"),
        },
        {"name": "schema_backed", "cache_module": "example.replacement"},
    ),
)
def test_collisions_reject_the_entire_registration(kwargs):
    before = dict(registry._CACHE_PLUGIN_REGISTRY)
    with pytest.raises(registry.CachePluginError):
        registry.register_cache_plugin(**kwargs)
    assert registry._CACHE_PLUGIN_REGISTRY == before


@pytest.mark.parametrize(
    "kwargs",
    (
        {"name": " ", "cache_module": "example.cache"},
        {"name": "new", "cache_module": ""},
        {"name": "new", "cache_module": "example.cache", "cache_attr": ""},
        {"name": "new", "cache_module": "example.cache", "aliases": ("fine", " ")},
        {"name": "new", "cache_module": "example.cache", "aliases": "ambiguous"},
    ),
)
def test_invalid_input_never_partially_publishes(kwargs):
    before = dict(registry._CACHE_PLUGIN_REGISTRY)
    with pytest.raises(registry.CachePluginError):
        registry.register_cache_plugin(**kwargs)
    assert registry._CACHE_PLUGIN_REGISTRY == before


def test_existing_aliases_cannot_be_removed_or_repointed():
    registry.register_cache_plugin(
        "custom", cache_module="example.cache", aliases=("fast",)
    )
    before = dict(registry._CACHE_PLUGIN_REGISTRY)
    with pytest.raises(registry.CachePluginError):
        registry.register_cache_plugin("custom", cache_module="example.cache")
    with pytest.raises(registry.CachePluginError):
        registry.register_cache_plugin(
            "custom", cache_module="example.other", aliases=("fast",)
        )
    assert registry._CACHE_PLUGIN_REGISTRY == before


def test_concurrent_collisions_publish_exactly_one_complete_registration():
    barrier = Barrier(2)

    def register(name):
        barrier.wait(timeout=5)
        try:
            registry.register_cache_plugin(
                name,
                cache_module=f"example.{name}",
                aliases=("shared", f"{name}_alias"),
            )
            return name
        except registry.CachePluginError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        winners = tuple(pool.map(register, ("alpha", "beta")))
    winner = next(name for name in winners if name)
    assert sum(name is not None for name in winners) == 1
    loser = "beta" if winner == "alpha" else "alpha"
    assert registry._get_registration("shared").canonical_name == winner
    assert registry._get_registration(winner + "_alias") is registry._get_registration(
        winner
    )
    for name in (loser, loser + "_alias"):
        with pytest.raises(registry.CachePluginError):
            registry._get_registration(name)


def test_import_runs_outside_registry_lock_and_keeps_the_binding(monkeypatch):
    registry.register_cache_plugin(
        "custom", cache_module="example.cache", aliases=("fast",)
    )
    entered, release = Event(), Event()
    backend = object()

    def load(name):
        assert name == "example.cache"
        entered.set()
        assert release.wait(timeout=5)
        return SimpleNamespace(StorageCache=backend)

    monkeypatch.setattr(registry.importlib, "import_module", load)
    with ThreadPoolExecutor(max_workers=2) as pool:
        loading = pool.submit(registry.load_cache_plugin, "fast")
        assert entered.wait(timeout=5)
        try:
            registering = pool.submit(
                registry.register_cache_plugin,
                "dependency",
                cache_module="example.dependency",
            )
            registering.result(timeout=2)
            with pytest.raises(registry.CachePluginError):
                registry.register_cache_plugin(
                    "custom", cache_module="example.other", aliases=("fast",)
                )
            assert "dependency" in registry.get_registered_cache_plugin_names()
        finally:
            release.set()
        assert loading.result(timeout=5) is backend


@pytest.mark.parametrize(
    "failure", ("import", "attribute", "constructor", "capabilities")
)
def test_load_failure_preserves_names_and_allows_retry(monkeypatch, failure):
    registry.register_cache_plugin(
        "custom", cache_module="example.cache", aliases=("fast",)
    )
    binding = registry._get_registration("fast")

    class Backend:
        plugin_capabilities = registry.StorageCacheCapabilities()

        def __init__(self, db):
            self.db = db

    def broken_import(name):
        raise ImportError("plugin unavailable")

    if failure == "import":
        monkeypatch.setattr(registry.importlib, "import_module", broken_import)
        operation = partial(registry.load_cache_plugin, "fast")
        error = ImportError
    elif failure == "attribute":
        monkeypatch.setattr(
            registry.importlib, "import_module", lambda name: SimpleNamespace()
        )
        operation = partial(registry.load_cache_plugin, "fast")
        error = AttributeError
    elif failure == "constructor":
        monkeypatch.setattr(
            registry.importlib,
            "import_module",
            lambda name: SimpleNamespace(
                StorageCache=lambda db: broken_import("constructor")
            ),
        )
        operation = partial(registry.create_storage_cache, object(), "fast")
        error = ImportError
    else:
        monkeypatch.setattr(
            registry.importlib,
            "import_module",
            lambda name: SimpleNamespace(
                StorageCache=SimpleNamespace(plugin_capabilities=None)
            ),
        )
        operation = partial(registry.get_cache_plugin_capabilities, "fast")
        error = registry.CachePluginError
    with pytest.raises(error):
        operation()
    assert registry._get_registration("fast") is binding
    with pytest.raises(registry.CachePluginError):
        registry.register_cache_plugin(
            "custom", cache_module="example.fallback", aliases=("fast",)
        )
    monkeypatch.setattr(
        registry.importlib,
        "import_module",
        lambda name: SimpleNamespace(StorageCache=Backend),
    )
    db = object()
    assert registry.create_storage_cache(db, "fast").db is db
    assert registry.get_cache_plugin_capabilities("fast") is Backend.plugin_capabilities


def test_package_location_rejects_namespace_package_without_file(monkeypatch):
    registry.register_cache_plugin("namespace", cache_module="example.cache")
    monkeypatch.setattr(
        registry.importlib, "import_module", lambda name: SimpleNamespace(__file__=None)
    )
    with pytest.raises(registry.CachePluginError, match="has no package file"):
        registry.get_cache_plugin_location("namespace")
