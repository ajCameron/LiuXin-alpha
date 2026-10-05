"""
Check database owner imports and built-in driver registry names.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_root_surface_imports.py
"""
from __future__ import annotations


def test_database_owners_expose_expected_helpers() -> None:
    """
    Check callable utility helpers and nonempty custom-column constants on their owning modules.

    Example:
        >>> test_database_owners_expose_expected_helpers()


    :return: None; failed expectations raise AssertionError.
    """
    import LiuXin_alpha.databases.constants as database_constants
    import LiuXin_alpha.databases.utils as database_utils

    assert callable(database_utils._get_next_series_num_for_list)
    assert callable(database_utils._get_series_values)
    assert callable(database_utils.get_data_as_dict)
    assert database_constants.CUSTOM_DATA_TYPES
    assert database_constants.VALID_DATA_TYPES


def test_database_owners_expose_concrete_types() -> None:
    """
    Check imported database types are present and link policy enums retain expected string values.

    Example:
        >>> test_database_owners_expose_concrete_types()


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.column_metadata import (
        ColumnMergePolicy,
        ColumnMetadata,
        ColumnNormalizationProfile,
        ColumnOptions,
        ColumnSemanticRole,
    )
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.maintenance import Maintainer
    from LiuXin_alpha.databases.row import Row
    from LiuXin_alpha.databases.schema_specs import LinkCapabilities, LinkKind

    assert Database is not None
    assert Row is not None
    assert Maintainer is not None
    assert LinkCapabilities is not None
    assert LinkKind.TYPED_PRIORITY.value == "typed_priority"
    assert ColumnMetadata is not None
    assert ColumnSemanticRole.TITLE.value == "title"
    assert ColumnNormalizationProfile.TAG_SEARCH_TERM.value == "tag_search_term"
    assert ColumnOptions is not None
    assert ColumnMergePolicy.SET_UNION.value == "set_union"


def test_database_driver_registry_lists_builtins() -> None:
    """
    Check that SQLite, SQLite_apsw, and PostgreSQL are registered, allowing additional drivers.

    Example:
        >>> test_database_driver_registry_lists_builtins()


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        get_registered_database_driver_names,
    )

    names = set(get_registered_database_driver_names())
    assert "SQLite" in names
    assert "SQLite_apsw" in names
    assert "PostgreSQL" in names
