"""Native Catalog predicates retain semantics when executed against loaded caches."""

import pytest

from LiuXin_alpha.caches.read_cache import ReadCache
from tests.catalog.test_wemi_database_search import collection as collection


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_native_catalog_queries_use_explicit_cache(collection, storage_type):
    catalog = collection.catalog
    cache = ReadCache(catalog.db, storage_type=storage_type)
    cache.load()
    try:
        for level in ("work", "item"):
            for query in ("title:Searchfixture", "work.author:Novel"):
                expected = catalog.search.find(query, level=level, limit=None)
                actual = catalog.search.find(
                    query, level=level, limit=None, read_cache=cache
                )
                assert actual == expected, (storage_type, level, query)
    finally:
        cache.close()


def test_physical_comparisons_agree_with_sql(collection):
    from LiuXin_alpha.caches.api import CacheQuery
    from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch

    cache = ReadCache(collection.catalog.db, storage_type="schema_backed")
    cache.load()
    try:
        for operator, operand in (
            ("eq", 1),
            ("ne", 1),
            ("lt", 2),
            ("gte", 2),
            ("in", (1, None)),
            ("is_null", False),
        ):
            predicate = ComparisonMatch("work_id", operator, operand)
            actual = cache.query(CacheQuery("works", where=predicate)).ids
            expected = collection.catalog.db.macros.find_rows_matching(
                "works", predicate, limit=None
            )
            assert actual == tuple(row["work_id"] for row in expected)
        for operator in ("contains", "prefix"):
            predicate = ComparisonMatch("work_title", operator, "Searchfixture")
            actual = cache.query(CacheQuery("works", where=predicate)).ids
            expected = collection.catalog.db.macros.find_rows_matching(
                "works", predicate, limit=None
            )
            assert actual == tuple(row["work_id"] for row in expected)
    finally:
        cache.close()
