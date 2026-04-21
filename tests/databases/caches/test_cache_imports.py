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
            DatabaseBackedStorageCache,
            NumpyVectorizedStorageCache,
            SchemaBackedStorageCache,
            StorageCache,
            StorageCacheCapabilities,
            create_storage_cache,
            get_cache_plugin_capabilities,
            get_registered_cache_plugin_names,
            load_cache_plugin,
        )

        assert StorageCache is SchemaBackedStorageCache
        assert load_cache_plugin("schema_backed") is SchemaBackedStorageCache
        assert load_cache_plugin("database_backed") is DatabaseBackedStorageCache
        assert load_cache_plugin("numpy_vectorized") is NumpyVectorizedStorageCache
        assert "database_backed" in get_registered_cache_plugin_names()
        assert "schema_backed" in get_registered_cache_plugin_names()
        assert "numpy_vectorized" in get_registered_cache_plugin_names()
        assert get_cache_plugin_capabilities("schema_backed") == StorageCacheCapabilities(
            live_reads=False,
            live_child_objects=False,
            vectorized_helpers=False,
            requires_reload_for_external_changes=True,
        )
        assert get_cache_plugin_capabilities("live") == StorageCacheCapabilities(
            live_reads=True,
            live_child_objects=True,
            vectorized_helpers=False,
            requires_reload_for_external_changes=False,
        )
        assert get_cache_plugin_capabilities("numpy") == StorageCacheCapabilities(
            live_reads=False,
            live_child_objects=False,
            vectorized_helpers=True,
            requires_reload_for_external_changes=True,
        )

        cache = create_storage_cache(None, "schema_backed")
        assert isinstance(cache, SchemaBackedStorageCache)
        assert cache.cache_type == "schema_backed"
        assert cache.capabilities == get_cache_plugin_capabilities("schema_backed")

        database_cache = create_storage_cache(None, "database_backed")
        assert isinstance(database_cache, DatabaseBackedStorageCache)
        assert database_cache.cache_type == "database_backed"
        assert database_cache.capabilities == get_cache_plugin_capabilities("database_backed")

        numpy_cache = create_storage_cache(None, "numpy_vectorized", require_numpy=False)
        assert isinstance(numpy_cache, NumpyVectorizedStorageCache)
        assert numpy_cache.cache_type == "numpy_vectorized"
        assert numpy_cache.capabilities.live_reads is False
        assert numpy_cache.capabilities.live_child_objects is False
        assert numpy_cache.capabilities.requires_reload_for_external_changes is True
        assert (
            numpy_cache.capabilities.vectorized_helpers
            is NumpyVectorizedStorageCache.numpy_available()
        )

    def test_numpy_vectorized_plugin_can_be_loaded_even_if_numpy_is_optional(self) -> None:
        from LiuXin_alpha.caches import NumpyVectorizedStorageCache

        assert NumpyVectorizedStorageCache is not None

    def test_schema_backed_public_surface_resolves_to_canonical_schema_backed_types(
        self,
    ) -> None:
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
        from LiuXin_alpha.caches.schema_backed import (
            SchemaBackedCacheView as PublicSchemaBackedCacheView,
            SchemaBackedCacheViewRow as PublicSchemaBackedCacheViewRow,
            SchemaBackedLinkTable as PublicSchemaBackedLinkTable,
            SchemaBackedMainTableCache as PublicSchemaBackedMainTableCache,
            SchemaBackedManyManyField as PublicSchemaBackedManyManyField,
            SchemaBackedManyOneField as PublicSchemaBackedManyOneField,
            SchemaBackedOneManyField as PublicSchemaBackedOneManyField,
            SchemaBackedSameTableField as PublicSchemaBackedSameTableField,
            SchemaBackedStorageCache as PublicSchemaBackedStorageCache,
            SchemaBackedTwoTableOneOneField as PublicSchemaBackedTwoTableOneOneField,
            StorageCache as PublicStorageCache,
            StorageCacheField as PublicStorageCacheField,
            StorageCacheLinkTable as PublicStorageCacheLinkTable,
            StorageCacheMainTable as PublicStorageCacheMainTable,
            StorageCacheView as PublicStorageCacheView,
        )

        assert PublicSchemaBackedStorageCache is CanonicalSchemaBackedStorageCache
        assert PublicSchemaBackedCacheView is CanonicalSchemaBackedCacheView
        assert PublicSchemaBackedCacheViewRow is CanonicalSchemaBackedCacheViewRow
        assert PublicSchemaBackedMainTableCache is CanonicalSchemaBackedMainTableCache
        assert PublicSchemaBackedLinkTable is CanonicalSchemaBackedLinkTable
        assert PublicSchemaBackedSameTableField is CanonicalSchemaBackedSameTableField
        assert PublicSchemaBackedTwoTableOneOneField is CanonicalSchemaBackedTwoTableOneOneField
        assert PublicSchemaBackedOneManyField is CanonicalSchemaBackedOneManyField
        assert PublicSchemaBackedManyOneField is CanonicalSchemaBackedManyOneField
        assert PublicSchemaBackedManyManyField is CanonicalSchemaBackedManyManyField
        assert PublicStorageCache is CanonicalSchemaBackedStorageCache
        assert PublicStorageCacheField is CanonicalSchemaBackedSameTableField
        assert PublicStorageCacheLinkTable is CanonicalSchemaBackedLinkTable
        assert PublicStorageCacheMainTable is CanonicalSchemaBackedMainTableCache
        assert PublicStorageCacheView is CanonicalSchemaBackedCacheView
