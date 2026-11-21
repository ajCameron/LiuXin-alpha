"""
Register storage backend import targets and resolve case-insensitive names.

Import registers the schema_backed, database_backed and numpy_vectorized
builtins with their aliases. Registration stores metadata without loading
backend classes; loading imports the registered module on demand. Names and
aliases have immutable process-local bindings. Identical registration is
idempotent; collisions fail before any key is published. A lock serializes
registry access, but imports and constructors run outside it. Import and attribute
failures propagate without changing the intended registration or selecting a fallback.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI

import importlib
import os
from collections.abc import Iterable
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)


class CachePluginError(RuntimeError):
    """
    Report invalid registration, name collisions or invalid plugin capabilities.

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
    them. Registration normalizes names/aliases and publishes them together.

    Example:
        >>> item = CachePluginRegistration("custom", "example.cache")
        >>> item.cache_attr, item.package_dir
        ('StorageCache', None)
    """

    canonical_name: str
    cache_module: str
    cache_attr: str = "StorageCache"
    package_dir: str | None = None
    aliases: tuple[str, ...] = ()


_CACHE_PLUGIN_REGISTRY: dict[str, CachePluginRegistration] = {}
_REGISTRY_LOCK = RLock()


def _required_text(value: str, label: str) -> str:
    """
    Validate and strip one registry name or import-target string.

    Example:
        >>> _required_text("  backend  ", "Name")
        'backend'


    :param value: String that must contain at least one non-whitespace character.
    :param label: Field label included in a CachePluginError.
    :return: Surrounding-whitespace-stripped text; invalid input raises
        CachePluginError.
    """
    if not isinstance(value, str) or not value.strip():
        raise CachePluginError(f"{label} must be a nonempty string")
    return value.strip()


def _name(value: str) -> str:
    """
    Normalize a nonempty plugin name for case-insensitive lookup.

    Example:
        >>> _name("  FAST  ")
        'fast'


    :param value: Canonical name or alias to normalize.
    :return: Stripped and casefolded name; invalid input raises CachePluginError.
    """
    return _required_text(value, "Cache plugin name").casefold()


def register_cache_plugin(
    name: str,
    *,
    cache_module: str,
    cache_attr: str = "StorageCache",
    package_dir: str | None = None,
    aliases: Iterable[str] = (),
) -> None:
    """
    Bind a canonical backend name and aliases to one import registration.

    Strip surrounding name whitespace and casefold names. Bind the canonical
    name and all aliases atomically, without importing the target. A name cannot
    be reassigned or have its aliases changed during this process. Repeating the
    same normalized registration is harmless, including reordered aliases.
    Register before lookup; new distinct backends may be registered at any time.
    A failed import leaves the intended binding available for a later retry.

    Example:
        Register a custom backend with ``register_cache_plugin("custom",
        cache_module="example.cache", aliases=("fast",))`` before loading fast.


    :param name: Canonical name, stripped and casefolded in registration metadata.
    :param cache_module: Dotted module path to import when the backend is loaded.
    :param cache_attr: Attribute to return from that module; defaults to StorageCache.
    :param package_dir: Optional package location with surrounding whitespace stripped.
    :param aliases: Additional names, stripped/casefolded and deduplicated.
    :return: None; updates the process-local registry without importing the plugin.
    :raises CachePluginError: Invalid input or any name/alias conflicts with an existing binding.
    """

    canonical = _name(name)
    if isinstance(aliases, (str, bytes)):
        raise CachePluginError("Cache plugin aliases must be an iterable of names")
    try:
        normalized_aliases = tuple(
            sorted({_name(alias) for alias in aliases} - {canonical})
        )
    except TypeError as exc:
        raise CachePluginError(
            "Cache plugin aliases must be an iterable of names"
        ) from exc
    if package_dir is not None:
        package_dir = _required_text(package_dir, "Cache plugin package directory")
    registration = CachePluginRegistration(
        canonical_name=canonical,
        cache_module=_required_text(cache_module, "Cache plugin module"),
        cache_attr=_required_text(cache_attr, "Cache plugin attribute"),
        package_dir=package_dir,
        aliases=normalized_aliases,
    )
    keys = (canonical, *normalized_aliases)
    with _REGISTRY_LOCK:
        conflicts = [
            key
            for key in keys
            if key in _CACHE_PLUGIN_REGISTRY
            and _CACHE_PLUGIN_REGISTRY[key] != registration
        ]
        if conflicts:
            raise CachePluginError(
                f"Cache plugin names already registered: {conflicts!r}"
            )
        if canonical in _CACHE_PLUGIN_REGISTRY:
            return
        _CACHE_PLUGIN_REGISTRY.update(dict.fromkeys(keys, registration))


def _get_registration(cache_type: str) -> CachePluginRegistration:
    """
    Resolve a canonical name or alias to an immutable registration under the lock.

    Example:
        >>> _get_registration("LiVe").canonical_name
        'database_backed'


    :param cache_type: Plugin name or alias; surrounding whitespace is stripped.
    :return: Stored CachePluginRegistration object.
    :raises CachePluginError: The name is absent; the message lists currently available canonical names.
    """

    key = _name(cache_type)
    with _REGISTRY_LOCK:
        try:
            return _CACHE_PLUGIN_REGISTRY[key]
        except KeyError as exc:
            available = sorted(
                {reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()}
            )
            raise CachePluginError(
                f"Unknown cache plugin {cache_type!r}. Available: {available!r}"
            ) from exc


if TYPE_CHECKING:
    from LiuXin_alpha.caches.api import StorageCacheAPI


def load_cache_plugin(cache_type: str) -> type[StorageCacheAPI]:
    """
    Import a registered module and return its configured attribute.

    Python import caching applies. Imports run outside the registry lock so a
    plugin can register dependencies. The captured registration cannot change.
    Import errors and missing-attribute errors propagate directly; the binding
    remains reserved for the same target, allowing retry after installation/repair.

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
    return cast("type[StorageCacheAPI]", getattr(module, registration.cache_attr))


def create_storage_cache(
    db: DatabaseAPI | None, cache_type: str = "schema_backed", **kwargs: Any
) -> StorageCacheAPI:
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

    Capture a consistent listing under the registry lock. Canonical and alias
    bindings cannot be overwritten, so every listed name still identifies its
    originally registered backend.

    Example:
        >>> "database_backed" in get_registered_cache_plugin_names()
        True


    :return: Sorted tuple of canonical names, excluding alias keys.
    """

    with _REGISTRY_LOCK:
        return tuple(
            sorted({reg.canonical_name for reg in _CACHE_PLUGIN_REGISTRY.values()})
        )


def get_cache_plugin_location(cache_type: str) -> str:
    """
    Return a registered package directory or derive it from the parent package.

    An explicit directory is not checked for existence or normalized. Otherwise
    import the parent of cache_module and inspect its __file__; import, attribute
    and path-conversion failures propagate. A package without a file is rejected
    with CachePluginError.

    Example:
        >>> os.path.basename(get_cache_plugin_location("schema"))
        'schema_backed'


    :param cache_type: Canonical backend name or alias.
    :return: Explicit package_dir unchanged, or the directory of the parent module's real file path.
    :raises CachePluginError: The plugin name is not registered or its package has no file.
    """

    registration = _get_registration(cache_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.cache_module.rsplit(".", 1)[0])
    if module.__file__ is None:
        raise CachePluginError(f"Cache plugin {cache_type!r} has no package file")
    return os.path.dirname(os.path.realpath(module.__file__))


def register_builtin_cache_plugins() -> None:
    """
    Install or restore registry entries for the three bundled backends.

    Register schema_backed/schema; database_backed/database/live/passthrough;
    and numpy_vectorized/numpy/vectorized. Package directories are relative to
    this registry module. Repeated calls preserve these identical entries without
    importing or constructing backend classes.

    Example:
        Calling this again leaves the live alias bound to database_backed and
        preserves unrelated custom registrations.


    :return: None; registers missing builtins and preserves identical existing bindings.
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
