"""
Register storage backend import targets and resolve case-insensitive names.

Import registers the schema_backed, database_backed and numpy_vectorized
builtins with their aliases. Registration stores metadata without loading
backend classes; loading imports the registered module on demand. Registration
is process-local, overwrites matching lowercase keys, and is not synchronized.
Import and attribute failures propagate without a generic plugin wrapper.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)


class CachePluginError(RuntimeError):
    """
    Report unknown plugin names or invalid declared plugin capabilities.

    Module import failures and missing class attributes propagate as their
    original exceptions rather than being converted to this error.

    Example:
        >>> isinstance(CachePluginError("unknown backend"), RuntimeError)
        True
    """


@dataclass(frozen=True)
class CachePluginRegistration:
    """
    Retain a canonical name, import target and optional package directory.

    cache_module is imported and cache_attr names its exported backend class,
    defaulting to StorageCache. package_dir bypasses package discovery when
    provided. The frozen dataclass records values without validating or importing
    them; aliases are stored separately in the module registry.

    Example:
        >>> item = CachePluginRegistration("custom", "example.cache")
        >>> item.cache_attr, item.package_dir
        ('StorageCache', None)
    """

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
    """
    Bind a canonical backend name and aliases to one import registration.

    Existing lowercase keys are overwritten, including aliases owned by other
    registrations. Name whitespace is not stripped and inputs are not validated
    before insertion. This does not automatically remove other aliases from an
    overwritten registration or check whether the import target is loadable.

    Example:
        Register a custom backend with ``register_cache_plugin("custom",
        cache_module="example.cache", aliases=("fast",))`` before loading fast.


    :param name: Canonical name retained as supplied in registration metadata.
    :param cache_module: Dotted module path to import when the backend is loaded.
    :param cache_attr: Attribute to return from that module; defaults to StorageCache.
    :param package_dir: Optional package location returned verbatim by location lookup.
    :param aliases: Additional names bound to the same registration using lowercase keys.
    :return: None; updates the process-local registry without importing the plugin.
    """

    registration = CachePluginRegistration(
        canonical_name=name,
        cache_module=cache_module,
        cache_attr=cache_attr,
        package_dir=package_dir,
    )
    for key in (name, *aliases):
        _CACHE_PLUGIN_REGISTRY[key.lower()] = registration


def _get_registration(cache_type: str) -> CachePluginRegistration:
    """
    Resolve a canonical name or alias by its lowercase registry key.

    Example:
        >>> _get_registration("LiVe").canonical_name
        'database_backed'


    :param cache_type: Plugin name or alias; surrounding whitespace is retained.
    :return: Stored CachePluginRegistration object.
    :raises CachePluginError: The name is absent; the message lists currently available canonical names.
    """

    try:
        return _CACHE_PLUGIN_REGISTRY[cache_type.lower()]
    except KeyError as exc:
        available = sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()})
        raise CachePluginError(
            f"Unknown cache plugin {cache_type!r}. Available: {available!r}"
        ) from exc


def load_cache_plugin(cache_type: str):
    """
    Import a registered module and return its configured attribute.

    Python import caching applies. Import errors and missing-attribute errors
    propagate directly; only name resolution uses CachePluginError.

    Example:
        >>> load_cache_plugin("schema").__name__
        'SchemaBackedStorageCache'


    :param cache_type: Canonical backend name or alias, resolved case-insensitively.
    :return: Registered attribute, normally a backend class; it is not instantiated or type-checked.
    :raises CachePluginError: The requested name is not registered.
    :raises AttributeError: The imported module lacks the registered attribute.
    """

    registration = _get_registration(cache_type)
    module = importlib.import_module(registration.cache_module)
    return getattr(module, registration.cache_attr)


def create_storage_cache(db: Any, cache_type: str = "schema_backed", **kwargs: Any):
    """
    Construct a registered storage backend with the supplied database and options.

    The backend constructor determines whether a detached database or particular
    options are acceptable. Registry, import and constructor errors propagate.

    Example:
        >>> cache = create_storage_cache(None, "schema")
        >>> cache.cache_type, cache.is_initialized
        ('schema_backed', False)


    :param db: Database passed unchanged as the constructor's first positional argument.
    :param cache_type: Canonical plugin name or alias; defaults to schema_backed.
    :param kwargs: Keyword arguments forwarded unchanged to the loaded constructor.
    :return: Backend constructor result, without an additional read or load call.
    """

    cache_cls = load_cache_plugin(cache_type)
    return cache_cls(db, **kwargs)


def get_cache_plugin_capabilities(cache_type: str) -> StorageCacheCapabilities:
    """
    Read and validate capability metadata from the registered backend class.

    The class is loaded but not instantiated. This checks the capability object
    type, not whether every advertised behavior is actually implemented. An
    explicit invalid value, including None, is rejected rather than defaulted.

    Example:
        >>> get_cache_plugin_capabilities("live").live_reads
        True


    :param cache_type: Canonical backend name or alias.
    :return: Declared StorageCacheCapabilities instance, or a default instance when absent.
    :raises CachePluginError: The name is unknown or plugin_capabilities has the wrong type.
    """

    cache_cls = load_cache_plugin(cache_type)
    declared = getattr(cache_cls, "plugin_capabilities", StorageCacheCapabilities())
    if isinstance(declared, StorageCacheCapabilities):
        return declared
    raise CachePluginError(
        f"Cache plugin {cache_type!r} exposes invalid plugin_capabilities: {declared!r}"
    )


def get_registered_cache_plugin_names() -> tuple[str, ...]:
    """
    List distinct canonical names still reachable through registry keys.

    An overwritten canonical key can leave its old registration reachable by
    another alias; that old canonical name still appears in this listing.

    Example:
        >>> "database_backed" in get_registered_cache_plugin_names()
        True


    :return: Sorted tuple of canonical names, excluding alias keys.
    """

    return tuple(sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()}))


def get_cache_plugin_location(cache_type: str) -> str:
    """
    Return a registered package directory or derive it from the parent package.

    An explicit directory is not checked for existence or normalized. Otherwise
    import the parent of cache_module and inspect its __file__; import, attribute
    and path-conversion failures propagate, including packages without a file.

    Example:
        >>> os.path.basename(get_cache_plugin_location("schema"))
        'schema_backed'


    :param cache_type: Canonical backend name or alias.
    :return: Explicit package_dir unchanged, or the directory of the parent module's real file path.
    :raises CachePluginError: The plugin name is not registered.
    """

    registration = _get_registration(cache_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.cache_module.rsplit(".", 1)[0])
    return os.path.dirname(os.path.realpath(module.__file__))


def register_builtin_cache_plugins() -> None:
    """
    Install or restore registry entries for the three bundled backends.

    Register schema_backed/schema; database_backed/database/live/passthrough;
    and numpy_vectorized/numpy/vectorized. Package directories are relative to
    this registry module. Repeated calls replace these entries but do not import
    or construct backend classes.

    Example:
        Calling this after overriding the live alias restores that alias to
        database_backed while unrelated custom registrations remain available.


    :return: None; overwrites builtin canonical and alias keys without clearing other entries.
    """

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
