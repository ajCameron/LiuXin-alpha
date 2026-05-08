"""Additional tests for LiuXin_alpha.databases package __init__.py.

Focuses on the lazy __getattr__ loader edge cases and completeness of
__all__ — aspects not covered by the existing test_root_surface_imports.py.
"""
from __future__ import annotations

import pytest


class TestDatabasesInitLazyLoading:
    """Verify that all names listed in __all__ are accessible via the package root."""

    def test_cleanup_tags_is_callable(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert callable(dbmod.cleanup_tags)

    def test_database_class_is_the_database_type(self) -> None:
        import LiuXin_alpha.databases as dbmod
        from LiuXin_alpha.databases.database import Database

        assert dbmod.Database is Database

    def test_row_class_is_the_row_type(self) -> None:
        import LiuXin_alpha.databases as dbmod
        from LiuXin_alpha.databases.row import Row

        assert dbmod.Row is Row

    def test_maintainer_class_is_maintainer_type(self) -> None:
        import LiuXin_alpha.databases as dbmod
        from LiuXin_alpha.databases.maintenance import Maintainer

        assert dbmod.Maintainer is Maintainer

    def test_database_api_resolves(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert dbmod.DatabaseAPI is not None

    def test_database_driver_api_resolves(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert dbmod.DatabaseDriverAPI is not None

    def test_database_driver_wrapper_api_resolves(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert dbmod.DatabaseDriverWrapperAPI is not None

    def test_row_api_resolves(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert dbmod.RowAPI is not None

    def test_load_database_driver_is_callable(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert callable(dbmod.loadDatabaseDriver)

    def test_register_database_driver_is_callable(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert callable(dbmod.register_database_driver)

    def test_get_registered_database_driver_names_is_callable(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert callable(dbmod.get_registered_database_driver_names)

    def test_get_series_values_is_callable(self) -> None:
        import LiuXin_alpha.databases as dbmod

        assert callable(dbmod._get_series_values)

    def test_unknown_attribute_raises_attribute_error(self) -> None:
        import LiuXin_alpha.databases as dbmod

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = dbmod.this_does_not_exist_at_all_xyz

    def test_all_exports_listed_in_dunder_all_are_accessible(self) -> None:
        """Every name in __all__ should be resolvable via getattr."""
        import LiuXin_alpha.databases as dbmod

        for name in dbmod.__all__:
            assert getattr(dbmod, name) is not None, f"__all__ member {name!r} resolved to None"


class TestDatabasesInitConstants:
    def test_custom_data_types_exported(self) -> None:
        from LiuXin_alpha.databases import CUSTOM_DATA_TYPES

        assert isinstance(CUSTOM_DATA_TYPES, frozenset)
        assert None not in CUSTOM_DATA_TYPES

    def test_valid_data_types_exported(self) -> None:
        from LiuXin_alpha.databases import VALID_DATA_TYPES

        assert isinstance(VALID_DATA_TYPES, frozenset)
        assert None in VALID_DATA_TYPES

    def test_custom_data_types_is_subset_of_valid(self) -> None:
        from LiuXin_alpha.databases import CUSTOM_DATA_TYPES, VALID_DATA_TYPES

        assert CUSTOM_DATA_TYPES < VALID_DATA_TYPES
