"""Plugin registry for storage-cache backends."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)


class CachePluginError(RuntimeError):
    """Raised when a cache plugin cannot be resolved or loaded."""


@dataclass(frozen=True)
class CachePluginRegistration:
    """Describe the import target and optional package root for one cache plugin."""

    canonical_name: str
    cache_module: str
    cache_attr: str = "StorageCache"
    package_dir: str | None = None


_CACHE_PLUGIN_REGISTRY: dict[str, CachePluginRegistration] = {}


def register_cache_plugin(
    name: str,
    *,
    cache_module: str,
    cache_attr: str = "StorageCache",
    package_dir: str | None = None,
    aliases: tuple[str, ...] = (),
) -> None:
    """Register one canonical cache backend and any case-insensitive aliases."""

    registration = CachePluginRegistration(
        canonical_name=name,
        cache_module=cache_module,
        cache_attr=cache_attr,
        package_dir=package_dir,
    )
    for key in (name, *aliases):
        _CACHE_PLUGIN_REGISTRY[key.lower()] = registration


def _get_registration(cache_type: str) -> CachePluginRegistration:
    try:
        return _CACHE_PLUGIN_REGISTRY[cache_type.lower()]
    except KeyError as exc:
        available = sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()})
        raise CachePluginError(
            f"Unknown cache plugin {cache_type!r}. Available: {available!r}"
        ) from exc


def load_cache_plugin(cache_type: str):
    """Import and return the cache class registered for ``cache_type``."""

    registration = _get_registration(cache_type)
    module = importlib.import_module(registration.cache_module)
    return getattr(module, registration.cache_attr)


def create_storage_cache(db: Any, cache_type: str = "schema_backed", **kwargs: Any):
    """Construct a registered cache backend for ``db``."""

    cache_cls = load_cache_plugin(cache_type)
    return cache_cls(db, **kwargs)


def get_cache_plugin_capabilities(cache_type: str) -> StorageCacheCapabilities:
    """Return validated capabilities advertised by a cache plugin class."""

    cache_cls = load_cache_plugin(cache_type)
    declared = getattr(cache_cls, "plugin_capabilities", StorageCacheCapabilities())
    if isinstance(declared, StorageCacheCapabilities):
        return declared
    raise CachePluginError(
        f"Cache plugin {cache_type!r} exposes invalid plugin_capabilities: {declared!r}"
    )


def get_registered_cache_plugin_names() -> tuple[str, ...]:
    """Return canonical cache-plugin names in stable order."""

    return tuple(sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()}))


def get_cache_plugin_location(cache_type: str) -> str:
    """Return the package directory associated with a cache plugin."""

    registration = _get_registration(cache_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.cache_module.rsplit(".", 1)[0])
    return os.path.dirname(os.path.realpath(module.__file__))


def register_builtin_cache_plugins() -> None:
    """Populate the registry with LiuXin's bundled cache implementations."""

    base_dir = os.path.dirname(os.path.realpath(__file__))
    register_cache_plugin(
        "schema_backed",
        cache_module="LiuXin_alpha.caches.cache_plugins.schema_backed",
        cache_attr="SchemaBackedStorageCache",
        package_dir=os.path.join(base_dir, "schema_backed"),
        aliases=("schema",),
    )
    register_cache_plugin(
        "database_backed",
        cache_module="LiuXin_alpha.caches.cache_plugins.database_backed",
        cache_attr="DatabaseBackedStorageCache",
        package_dir=os.path.join(base_dir, "database_backed"),
        aliases=("database", "live", "passthrough"),
    )
    register_cache_plugin(
        "numpy_vectorized",
        cache_module="LiuXin_alpha.caches.cache_plugins.numpy_vectorized",
        cache_attr="NumpyVectorizedStorageCache",
        package_dir=os.path.join(base_dir, "numpy_vectorized"),
        aliases=("numpy", "vectorized"),
    )


register_builtin_cache_plugins()

__all__ = [
    "CachePluginError",
    "CachePluginRegistration",
    "create_storage_cache",
    "get_cache_plugin_capabilities",
    "get_cache_plugin_location",
    "get_registered_cache_plugin_names",
    "load_cache_plugin",
    "register_cache_plugin",
]
