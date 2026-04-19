class TestCacheImportAPIs:
    """
    Tests that we can actually import cache objects at all.
    """

    def test_cache_imports_smple(self) -> None:
        """
        Tries to import cache objects.

        :return:
        """
        from LiuXin_alpha.library.caches.base_calibre.fields import BaseField

        assert BaseField is not None

    def test_storage_cache_plugin_imports(self) -> None:
        from LiuXin_alpha.caches import (
            NumpyVectorizedStorageCache,
            SchemaBackedStorageCache,
            StorageCache,
            create_storage_cache,
            get_registered_cache_plugin_names,
            load_cache_plugin,
        )

        assert StorageCache is SchemaBackedStorageCache
        assert load_cache_plugin("schema_backed") is SchemaBackedStorageCache
        assert load_cache_plugin("numpy_vectorized") is NumpyVectorizedStorageCache
        assert "schema_backed" in get_registered_cache_plugin_names()
        assert "numpy_vectorized" in get_registered_cache_plugin_names()

        cache = create_storage_cache(None, "schema_backed")
        assert isinstance(cache, SchemaBackedStorageCache)
        assert cache.cache_type == "schema_backed"

        numpy_cache = create_storage_cache(None, "numpy_vectorized", require_numpy=False)
        assert isinstance(numpy_cache, NumpyVectorizedStorageCache)
        assert numpy_cache.cache_type == "numpy_vectorized"

    def test_numpy_vectorized_plugin_can_be_loaded_even_if_numpy_is_optional(self) -> None:
        from LiuXin_alpha.caches import NumpyVectorizedStorageCache

        assert NumpyVectorizedStorageCache is not None

    def test_legacy_implementation_imports_resolve_to_canonical_schema_backed_types(self) -> None:
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
            SchemaBackedStorageCache as CanonicalSchemaBackedStorageCache,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
            SchemaBackedManyManyField as CanonicalSchemaBackedManyManyField,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
            SchemaBackedManyOneField as CanonicalSchemaBackedManyOneField,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
            SchemaBackedOneManyField as CanonicalSchemaBackedOneManyField,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
            SchemaBackedSameTableField as CanonicalSchemaBackedSameTableField,
            SchemaBackedTwoTableOneOneField as CanonicalSchemaBackedTwoTableOneOneField,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
            SchemaBackedLinkTable as CanonicalSchemaBackedLinkTable,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
            SchemaBackedMainTableCache as CanonicalSchemaBackedMainTableCache,
        )
        from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_view import (
            SchemaBackedCacheView as CanonicalSchemaBackedCacheView,
            SchemaBackedCacheViewRow as CanonicalSchemaBackedCacheViewRow,
        )
        from LiuXin_alpha.caches.implementation.storage_cache import (
            SchemaBackedStorageCache as LegacySchemaBackedStorageCache,
        )
        from LiuXin_alpha.caches.implementation.storage_fields.many_many_field import (
            SchemaBackedManyManyField as LegacySchemaBackedManyManyField,
        )
        from LiuXin_alpha.caches.implementation.storage_fields.many_one_field import (
            SchemaBackedManyOneField as LegacySchemaBackedManyOneField,
        )
        from LiuXin_alpha.caches.implementation.storage_fields.one_many_field import (
            SchemaBackedOneManyField as LegacySchemaBackedOneManyField,
        )
        from LiuXin_alpha.caches.implementation.storage_fields.one_one_field import (
            SchemaBackedSameTableField as LegacySchemaBackedSameTableField,
            SchemaBackedTwoTableOneOneField as LegacySchemaBackedTwoTableOneOneField,
        )
        from LiuXin_alpha.caches.implementation.storage_tables.link_tables.link_table import (
            SchemaBackedLinkTable as LegacySchemaBackedLinkTable,
        )
        from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
            SchemaBackedMainTableCache as LegacySchemaBackedMainTableCache,
        )
        from LiuXin_alpha.caches.implementation.storage_view import (
            SchemaBackedCacheView as LegacySchemaBackedCacheView,
            SchemaBackedCacheViewRow as LegacySchemaBackedCacheViewRow,
        )

        assert LegacySchemaBackedStorageCache is CanonicalSchemaBackedStorageCache
        assert LegacySchemaBackedCacheView is CanonicalSchemaBackedCacheView
        assert LegacySchemaBackedCacheViewRow is CanonicalSchemaBackedCacheViewRow
        assert LegacySchemaBackedMainTableCache is CanonicalSchemaBackedMainTableCache
        assert LegacySchemaBackedLinkTable is CanonicalSchemaBackedLinkTable
        assert LegacySchemaBackedSameTableField is CanonicalSchemaBackedSameTableField
        assert LegacySchemaBackedTwoTableOneOneField is CanonicalSchemaBackedTwoTableOneOneField
        assert LegacySchemaBackedOneManyField is CanonicalSchemaBackedOneManyField
        assert LegacySchemaBackedManyOneField is CanonicalSchemaBackedManyOneField
        assert LegacySchemaBackedManyManyField is CanonicalSchemaBackedManyManyField
