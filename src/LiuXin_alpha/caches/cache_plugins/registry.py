"""Plugin registry for storage-cache backends."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any


class CachePluginError(RuntimeError):
    """Raised when a cache plugin cannot be resolved or loaded."""


@dataclass(frozen=True)
class CachePluginRegistration:
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
    registration = _get_registration(cache_type)
    module = importlib.import_module(registration.cache_module)
    return getattr(module, registration.cache_attr)


def create_storage_cache(db: Any, cache_type: str = "schema_backed", **kwargs: Any):
    cache_cls = load_cache_plugin(cache_type)
    return cache_cls(db, **kwargs)


def get_registered_cache_plugin_names() -> tuple[str, ...]:
    return tuple(sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()}))


def get_cache_plugin_location(cache_type: str) -> str:
    registration = _get_registration(cache_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.cache_module.rsplit(".", 1)[0])
    return os.path.dirname(os.path.realpath(module.__file__))


def register_builtin_cache_plugins() -> None:
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
    "get_cache_plugin_location",
    "get_registered_cache_plugin_names",
    "load_cache_plugin",
    "register_cache_plugin",
]
