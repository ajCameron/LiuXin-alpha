"""
Register backend import locations and load their concrete owners on demand.

Importing this module registers SQLite, SQLite_apsw and PostgreSQL names without
importing their drivers. Direct-access and builder modules are cached by canonical
name; registering a replacement does not clear those caches.
"""

from __future__ import annotations

import importlib
import os
import pathlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from LiuXin_alpha.errors import DatabaseDriverError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


@dataclass(frozen=True)
class DriverRegistration:
    """
    Immutable import locations for one backend and its optional build/access helpers.

    ``canonical_name`` is the display/cache name; ``driver_module`` and ``driver_attr``
    locate the driver class. Optional module names identify direct access and building;
    ``package_dir`` bypasses package import when resolving the backend directory.

    Example:
        ``DriverRegistration("Custom", "my_backend.driver")`` uses
        ``DatabaseDriver`` as the driver attribute.
    """
    canonical_name: str
    driver_module: str
    driver_attr: str = "DatabaseDriver"
    direct_access_module: str | None = None
    builder_module: str | None = None
    package_dir: str | None = None


_DRIVER_REGISTRY: dict[str, DriverRegistration] = {}
_DIRECT_ACCESS_CACHE: dict[str, object] = {}
_BUILDER_CACHE: dict[str, object] = {}


def register_database_driver(
    name: str,
    *,
    driver_module: str,
    driver_attr: str = "DatabaseDriver",
    direct_access_module: str | None = None,
    builder_module: str | None = None,
    package_dir: str | None = None,
    aliases: tuple[str, ...] = (),
) -> None:
    """
    Bind a name and aliases to one registration, replacing any matching keys.

    Keys are lowercased but not stripped. This does not import the modules or invalidate
    previously cached direct-access/builder modules.

    Example:
        ``register_database_driver("Custom", driver_module="my_backend.driver",
        aliases=("custom_alias",))`` installs two case-insensitive lookup keys.


    :param name: Canonical backend name used in listings and cache keys.
    :param driver_module: Import path of the module owning the driver class.
    :param driver_attr: Attribute retrieved from the imported driver module.
    :param direct_access_module: Optional module exposing backend direct access.
    :param builder_module: Optional module containing a create_new_database entry point.
    :param package_dir: Optional directory returned directly by get_driver_location.
    :param aliases: Additional names bound to the same registration.
    :return: None; updates the process-local registry.
    """
    registration = DriverRegistration(
        canonical_name=name,
        driver_module=driver_module,
        driver_attr=driver_attr,
        direct_access_module=direct_access_module,
        builder_module=builder_module,
        package_dir=package_dir,
    )
    for key in (name, *aliases):
        _DRIVER_REGISTRY[key.lower()] = registration


def _get_registration(db_type: str) -> "DriverRegistration":
    """
    Resolve a case-insensitive backend name or raise DatabaseDriverError.

    The error lists the currently registered canonical names.

    Example:
        ``_get_registration("pg").canonical_name`` is ``"PostgreSQL"``.


    :param db_type: Registered canonical name or case-insensitive alias.
    :return: The shared immutable registration.
    """
    try:
        return _DRIVER_REGISTRY[db_type.lower()]
    except KeyError as exc:
        available = sorted({reg.canonical_name for reg in _DRIVER_REGISTRY.values()})
        err_str = "Requested db_driver_location not found.\n"
        err_str += "db_type: " + six_unicode(db_type) + "\n"
        err_str += "available: " + repr(available) + "\n"
        raise DatabaseDriverError(err_str) from exc


def load_database_driver(db_type: str):
    """
    Import the registered implementation and return its driver attribute.

    Returns the driver class for built-ins, not the imported module. Import and missing
    attribute errors propagate; an unknown backend raises DatabaseDriverError.

    Example:
        ``load_database_driver("pg")`` returns the PostgreSQL DatabaseDriver class.


    :param db_type: Registered canonical name or case-insensitive alias.
    :return: The configured attribute from the implementation module.
    """
    registration = _get_registration(db_type)
    module = importlib.import_module(registration.driver_module)
    return getattr(module, registration.driver_attr)


def get_registered_database_driver_names() -> tuple[str, ...]:
    """
    List distinct canonical backend names in sorted order.

    Example:
        >>> "PostgreSQL" in get_registered_database_driver_names()
        True


    :return: Tuple of canonical names, with aliases omitted.
    """
    return tuple(sorted({reg.canonical_name for reg in _DRIVER_REGISTRY.values()}))


def get_driver_location(db_type: str) -> str:
    """
    Resolve a backend package directory from its registration.

    An explicit package_dir is returned unchanged; otherwise the parent of the driver
    module is imported and its real file location supplies the directory.

    Example:
        ``get_driver_location("pg")`` identifies the PostgreSQL plugin directory.


    :param db_type: Registered canonical name or case-insensitive alias.
    :return: Registered directory or resolved parent-package directory.
    """
    registration = _get_registration(db_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.driver_module.rsplit('.', 1)[0])
    return os.path.dirname(os.path.realpath(module.__file__))


def get_direct_access_module(db_type: str):
    """
    Import and cache the registered direct-access module.

    Aliases share the canonical-name cache. An unknown backend or absent direct-access
    module raises DatabaseDriverError.

    Example:
        ``get_direct_access_module("pg")`` loads the PostgreSQL databasedriver module.


    :param db_type: Registered canonical name or case-insensitive alias.
    :return: Cached or newly imported direct-access module.
    """
    registration = _get_registration(db_type)
    key = registration.canonical_name.lower()
    if key in _DIRECT_ACCESS_CACHE:
        return _DIRECT_ACCESS_CACHE[key]
    if not registration.direct_access_module:
        raise DatabaseDriverError(f"Driver {registration.canonical_name!r} does not expose a direct access module")
    module = importlib.import_module(registration.direct_access_module)
    _DIRECT_ACCESS_CACHE[key] = module
    return module


def get_database_builder_module(db_type: str):
    """
    Import and cache the registered schema-builder module.

    Aliases share the canonical-name cache. An unknown backend or missing builder
    registration raises DatabaseDriverError.

    Example:
        ``get_database_builder_module("pg")`` loads the PostgreSQL schema module.


    :param db_type: Registered canonical name or case-insensitive alias.
    :return: Cached or newly imported builder module.
    """
    registration = _get_registration(db_type)
    key = registration.canonical_name.lower()
    if key in _BUILDER_CACHE:
        return _BUILDER_CACHE[key]
    if not registration.builder_module:
        raise DatabaseDriverError(f"Driver {registration.canonical_name!r} does not expose a builder module")
    module = importlib.import_module(registration.builder_module)
    _BUILDER_CACHE[key] = module
    return module


def create_new_database(db_type: str, target_location: Union[str, pathlib.Path]):
    """
    Invoke a backend builder when it exposes a callable create_new_database.

    The target is passed through unchanged. If no callable entry point exists, return
    the builder module itself; connection and creation errors otherwise propagate.

    Example:
        ``create_new_database("SQLite", new_path)`` delegates file creation to the
        registered SQLite builder.


    :param db_type: Registered canonical name or case-insensitive alias.
    :param target_location: Target passed unchanged to the backend builder.
    :return: The builder result, or the builder module when no callable entry point exists.
    """
    module = get_database_builder_module(db_type)
    create_fn = getattr(module, "create_new_database", None)
    if callable(create_fn):
        return create_fn(target_location)
    return module


def register_builtin_database_drivers() -> None:
    """
    Install the three built-in registrations and their historical backend aliases.

    This runs at import time and can replace registrations under the same keys without
    clearing existing module caches.

    Example:
        ``register_builtin_database_drivers()`` restores the built-in ``pg`` alias.


    :return: None; updates registry entries.
    """
    base_dir = os.path.dirname(os.path.realpath(__file__))
    register_database_driver(
        "SQLite",
        driver_module="LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver",
        direct_access_module="LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver",
        builder_module="LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator",
        package_dir=os.path.join(base_dir, "SQLite"),
        aliases=("sqlite",),
    )
    register_database_driver(
        "SQLite_apsw",
        driver_module="LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver",
        direct_access_module="LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver",
        builder_module="LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator",
        package_dir=os.path.join(base_dir, "SQLite_apsw"),
        aliases=("sqlite_apsw", "apsw"),
    )
    register_database_driver(
        "PostgreSQL",
        driver_module="LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver",
        direct_access_module="LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver",
        builder_module="LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema",
        package_dir=os.path.join(base_dir, "PostgreSQL"),
        aliases=("postgres", "postgresql", "pg"),
    )


# Get the existing database drivers in the regustry.
register_builtin_database_drivers()

__all__ = [
    "DriverRegistration",
    "create_new_database",
    "get_database_builder_module",
    "get_direct_access_module",
    "get_driver_location",
    "get_registered_database_driver_names",
    "load_database_driver",
    "register_database_driver",
]
