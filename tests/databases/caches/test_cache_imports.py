from __future__ import annotations


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
            FieldBasicInterfaceAPI,
            NumpyVectorizedStorageCache,
            SchemaBackedStorageCache,
            StorageCache,
            StorageCacheAPI,
            StorageCacheBaseTableAPI,
            StorageCacheCapabilities,
            StorageCacheSingleTableAPI,
            TableTypes,
            create_storage_cache,
            get_cache_plugin_capabilities,
            get_registered_cache_plugin_names,
            load_cache_plugin,
        )

        assert StorageCache is SchemaBackedStorageCache
        assert StorageCacheAPI is not None
        assert callable(StorageCacheAPI.create_writer)
        assert callable(StorageCacheAPI.write)
        assert callable(StorageCacheAPI.write_one)
        assert StorageCacheBaseTableAPI is not None
        assert StorageCacheSingleTableAPI is not None
        assert FieldBasicInterfaceAPI is not None
        assert TableTypes is not None
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

    def test_cache_api_contract_root_exports_storage_contracts(self) -> None:
        import LiuXin_alpha.caches.api as cache_api
        import LiuXin_alpha.caches.api.storage_cache_api as storage_cache_api

        expected = {
            "CacheViewAPI",
            "CacheViewSpec",
            "FieldBasicInterfaceAPI",
            "ManyToManyFieldAPI",
            "RelationFieldBasicInterfaceAPI",
            "StorageCacheAPI",
            "StorageCacheBaseTableAPI",
            "StorageCacheCapabilities",
            "StorageCacheLinkTableBaseAPI",
            "StorageCacheSingleTableAPI",
            "TableTypes",
        }

        assert cache_api.__all__ == storage_cache_api.__all__
        assert expected <= set(cache_api.__all__)
        for name in expected:
            assert hasattr(cache_api, name), f"caches.api is missing {name}"

        for concrete_name in (
            "SchemaBackedStorageCache",
            "DatabaseBackedStorageCache",
            "NumpyVectorizedStorageCache",
        ):
            assert not hasattr(cache_api, concrete_name)

    def test_numpy_vectorized_plugin_can_be_loaded_even_if_numpy_is_optional(self) -> None:
        from LiuXin_alpha.caches import NumpyVectorizedStorageCache

        assert NumpyVectorizedStorageCache is not None

    def test_numpy_vectorized_plugin_uses_independent_cache_and_field_types(self) -> None:
        from LiuXin_alpha.caches import NumpyVectorizedStorageCache, SchemaBackedStorageCache
        from LiuXin_alpha.caches.cache_plugins.numpy_vectorized.link_table import (
            NumpyVectorizedLinkTable,
        )
        from LiuXin_alpha.caches.cache_plugins.numpy_vectorized.storage_cache import (
            NumpyVectorizedMainTableCache,
            NumpyVectorizedSameTableField,
            NumpyVectorizedTwoTableOneOneField,
        )
        from LiuXin_alpha.databases.schema_specs import (
            LinkCardinality,
            StorageLinkSpec,
            StorageSchemaSpec,
        )
        from tests.support.storage_cache_test_harness import make_fake_db, make_table

        books = make_table(
            "books",
            ("id", "title"),
            is_main_table=True,
            linked_tables=("covers",),
        )
        covers = make_table(
            "covers",
            ("id", "path"),
            is_main_table=True,
            linked_tables=("books",),
        )
        book_covers = make_table(
            "book_covers",
            ("id", "book_id", "cover_id"),
            is_link_table=True,
            linked_tables=("books", "covers"),
        )

        schema = StorageSchemaSpec(
            tables={
                "books": books,
                "covers": covers,
                "book_covers": book_covers,
            },
            interlinks=(
                StorageLinkSpec(
                    primary_table="books",
                    secondary_table="covers",
                    link_table="book_covers",
                    cardinality=LinkCardinality.ONE_TO_ONE,
                    primary_link_col="book_id",
                    secondary_link_col="cover_id",
                ),
            ),
            intralinks=(),
        )
        db = make_fake_db(
            schema=schema,
            rows_by_table={
                "books": [{"id": 1, "title": "Book One"}],
                "covers": [{"id": 10, "path": "/covers/one.jpg"}],
                "book_covers": [{"id": 100, "book_id": 1, "cover_id": 10}],
            },
        )

        cache = NumpyVectorizedStorageCache(db, require_numpy=False)
        cache.read()

        assert not issubclass(NumpyVectorizedStorageCache, SchemaBackedStorageCache)
        assert type(cache) is NumpyVectorizedStorageCache
        assert type(cache.get_main_table("books")) is NumpyVectorizedMainTableCache
        assert type(cache.get_field("title")) is NumpyVectorizedSameTableField
        assert type(cache.get_field("books.covers.path")) is NumpyVectorizedTwoTableOneOneField
        assert type(cache.get_one_one_link_table("books", "covers")) is NumpyVectorizedLinkTable

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
