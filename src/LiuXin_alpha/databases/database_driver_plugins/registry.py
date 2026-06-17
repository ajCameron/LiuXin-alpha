"""
Driver registry for database backends.

Historically the package root used hard-coded if/else routing.
This registry keeps the public ``loadDatabaseDriver`` function stable while allowing real registration and
lightweight extension.
"""

from __future__ import annotations

import pathlib

import importlib
import os
from dataclasses import dataclass

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from LiuXin_alpha.errors import DatabaseDriverError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


@dataclass(frozen=True)
class DriverRegistration:
    """
    Record of registering a driver class on the database.
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
    Preform registratiion of DatabaseDriver class.

    :param name:
    :param driver_module:
    :param driver_attr:
    :param direct_access_module:
    :param builder_module:
    :param package_dir:
    :param aliases:
    :return:
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
    Retrieve a recorded registration of a driver.

    :param db_type:
    :return:
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
    Load and return the database driver module.

    :param db_type:
    :return:
    """
    registration = _get_registration(db_type)
    module = importlib.import_module(registration.driver_module)
    return getattr(module, registration.driver_attr)


def get_registered_database_driver_names() -> tuple[str, ...]:
    """
    Get a tuple of all the types of driver known to the system.

    :return:
    """
    return tuple(sorted({reg.canonical_name for reg in _DRIVER_REGISTRY.values()}))


def get_driver_location(db_type: str) -> str:
    """
    Map the name of a driver to its actual file location.

    :param db_type:
    :return:
    """
    registration = _get_registration(db_type)
    if registration.package_dir is not None:
        return registration.package_dir
    module = importlib.import_module(registration.driver_module.rsplit('.', 1)[0])
    return os.path.dirname(os.path.realpath(module.__file__))


def get_direct_access_module(db_type: str):
    """
    Directly get the raw module containing the driver.

    :param db_type:
    :return:
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
    Get the database builder module from its name.

    :param db_type:
    :return:
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
    Create a new database of the given type, at the target location.

    :param db_type:
    :param target_location:
    :return:
    """
    module = get_database_builder_module(db_type)
    create_fn = getattr(module, "create_new_database", None)
    if callable(create_fn):
        return create_fn(target_location)
    return module


def register_builtin_database_drivers() -> None:
    """
    Register built-in database drivers.

    :return:
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
