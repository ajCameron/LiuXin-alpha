#!/usr/bin/env python3
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose database constants eagerly and resolve heavier public entry points lazily.

The package root imports datatype constants immediately. Runtime access to advertised database, Row, API, driver-registry, schema/metadata and utility exports loads their owning modules through __getattr__. TYPE_CHECKING imports support static users; these exports do not require callers to use deep implementation paths.
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
    Resolve a recognized public database export from its owning module on demand.

    Dispatch exact names to concrete database/Row/maintenance modules, API contracts, macro/schema/column metadata, the driver registry or selected utilities. Results are not explicitly assigned into this module globals by the hook. Import failures and missing owner attributes propagate; arbitrary names are rejected.

    Example:
        >>> __getattr__("LinkKind").TYPED_PRIORITY.value
        'typed_priority'


    :param name: Requested public export name.
    :return: The owning module object attribute or explicitly imported class/function.
    :raises AttributeError: The name is unrecognized, or its owning module lacks the requested attribute.
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
