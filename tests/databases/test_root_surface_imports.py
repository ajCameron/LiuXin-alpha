from __future__ import annotations


def test_databases_root_exports_expected_helpers() -> None:
    import LiuXin_alpha.databases as dbmod

    assert callable(dbmod._get_next_series_num_for_list)
    assert callable(dbmod._get_series_values)
    assert callable(dbmod.get_data_as_dict)
    assert dbmod.CUSTOM_DATA_TYPES
    assert dbmod.VALID_DATA_TYPES


def test_databases_root_lazy_exports_concrete_types() -> None:
    from LiuXin_alpha.databases import (
        ColumnMergePolicy,
        ColumnMetadata,
        ColumnNormalizationProfile,
        ColumnSemanticRole,
        Database,
        Maintainer,
        Row,
    )

    assert Database is not None
    assert Row is not None
    assert Maintainer is not None
    assert ColumnMetadata is not None
    assert ColumnSemanticRole.TITLE.value == "title"
    assert ColumnNormalizationProfile.TAG_SEARCH_TERM.value == "tag_search_term"
    assert ColumnMergePolicy.SET_UNION.value == "set_union"


def test_database_driver_registry_lists_builtins() -> None:
    from LiuXin_alpha.databases.database_driver_plugins import get_registered_database_driver_names

    names = set(get_registered_database_driver_names())
    assert "SQLite" in names
    assert "SQLite_apsw" in names
    assert "PostgreSQL" in names
