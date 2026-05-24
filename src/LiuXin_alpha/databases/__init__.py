#!/usr/bin/env python3
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""Public root surface for the :mod:`LiuXin_alpha.databases` package.

Keep this file deliberately small and mostly lazy. Importing the package root should
be cheap, and callers should not need deep module paths for the common public entry
points.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES, VALID_DATA_TYPES


if TYPE_CHECKING:  # pragma: no cover
    from LiuXin_alpha.databases.api import DatabaseAPI, DatabaseDriverAPI, DatabaseDriverWrapperAPI, RowAPI
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import (
        get_registered_database_driver_names,
        loadDatabaseDriver,
        register_database_driver,
    )
    from LiuXin_alpha.databases.maintenance import Maintainer
    from LiuXin_alpha.databases.row import Row


__all__ = [
    "CUSTOM_DATA_TYPES",
    "Database",
    "DatabaseAPI",
    "DatabaseDriverAPI",
    "DatabaseDriverWrapperAPI",
    "Maintainer",
    "Row",
    "RowAPI",
    "VALID_DATA_TYPES",
    "_get_next_series_num_for_list",
    "_get_series_values",
    "cleanup_tags",
    "get_data_as_dict",
    "get_registered_database_driver_names",
    "loadDatabaseDriver",
    "register_database_driver",
]


def __getattr__(name: str):
    """


    :param name:
    :return:
    """
    if name == "Database":
        from LiuXin_alpha.databases.database import Database

        return Database
    if name == "Row":
        from LiuXin_alpha.databases.row import Row

        return Row
    if name == "Maintainer":
        from LiuXin_alpha.databases.maintenance import Maintainer

        return Maintainer
    if name in {"DatabaseAPI", "DatabaseDriverAPI", "DatabaseDriverWrapperAPI", "RowAPI"}:
        from LiuXin_alpha.databases import api as _api

        return getattr(_api, name)
    if name in {
        "loadDatabaseDriver",
        "register_database_driver",
        "get_registered_database_driver_names",
    }:
        from LiuXin_alpha.databases import database_driver_plugins as _drivers

        return getattr(_drivers, name)
    if name in {
        "_get_next_series_num_for_list",
        "_get_series_values",
        "cleanup_tags",
        "get_data_as_dict",
    }:
        from LiuXin_alpha.databases import utils as _utils

        return getattr(_utils, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
