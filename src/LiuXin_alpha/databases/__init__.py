#!/usr/bin/env python3
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Public root surface for the :mod:`LiuXin_alpha.databases` package.

Keep this file deliberately small and mostly lazy. Importing the package root should
be cheap, and callers should not need deep module paths for the common public entry
points.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES, VALID_DATA_TYPES


if TYPE_CHECKING:  # pragma: no cover
    from LiuXin_alpha.databases.api import DatabaseAPI, DatabaseDriverAPI, DatabaseDriverWrapperAPI, RowAPI
    from LiuXin_alpha.databases.column_metadata import (
        ColumnEmptyValuePolicy,
        ColumnMergePolicy,
        ColumnMetadata,
        ColumnNormalizationProfile,
        ColumnOptions,
        ColumnOptionValue,
        ColumnSemanticRole,
        ColumnValidationProfile,
    )
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import (
        get_registered_database_driver_names,
        loadDatabaseDriver,
        register_database_driver,
    )
    from LiuXin_alpha.databases.maintenance import Maintainer
    from LiuXin_alpha.databases.macro_types import (
        CanonicalIdentity,
        LinkRow,
        LinkValue,
        NormalizedIdentityCollision,
        NormalizedIdentityMigrationReport,
        UnreferencedRowsSpec,
    )
    from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
    from LiuXin_alpha.databases.row import Row
    from LiuXin_alpha.databases.schema_specs import LinkCapabilities, LinkKind


__all__ = [
    "CUSTOM_DATA_TYPES",
    "CanonicalIdentity",
    "ColumnEmptyValuePolicy",
    "ColumnMergePolicy",
    "ColumnMetadata",
    "ColumnNormalizationProfile",
    "ColumnOptions",
    "ColumnOptionValue",
    "ColumnSemanticRole",
    "ColumnValidationProfile",
    "Database",
    "DatabaseAPI",
    "DatabaseDriverAPI",
    "DatabaseDriverWrapperAPI",
    "Maintainer",
    "LinkCapabilities",
    "LinkKind",
    "LinkRow",
    "LinkValue",
    "NormalizedIdentityCollision",
    "NormalizedIdentityMigrationReport",
    "NormalizedIdentitySpec",
    "Row",
    "RowAPI",
    "UnreferencedRowsSpec",
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
    Decent front end.

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
    if name in {
        "CanonicalIdentity",
        "LinkRow",
        "LinkValue",
        "NormalizedIdentityCollision",
        "NormalizedIdentityMigrationReport",
        "UnreferencedRowsSpec",
    }:
        from LiuXin_alpha.databases import macro_types as _macro_types

        return getattr(_macro_types, name)
    if name == "NormalizedIdentitySpec":
        from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec

        return NormalizedIdentitySpec
    if name in {"LinkCapabilities", "LinkKind"}:
        from LiuXin_alpha.databases import schema_specs as _schema_specs

        return getattr(_schema_specs, name)
    if name in {
        "ColumnEmptyValuePolicy",
        "ColumnMergePolicy",
        "ColumnMetadata",
        "ColumnNormalizationProfile",
        "ColumnOptions",
        "ColumnOptionValue",
        "ColumnSemanticRole",
        "ColumnValidationProfile",
    }:
        from LiuXin_alpha.databases import column_metadata as _column_metadata

        return getattr(_column_metadata, name)
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
