"""Compare populated WEMI search and nullable physical predicates with SQL."""

import operator
import random

import pytest

from LiuXin_alpha.caches.api import CacheQuery
from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.databases.db_types.query_types import BooleanMatch, ComparisonMatch
from tests.catalog.test_wemi_database_search import collection as collection


def _accepts(row, node):
    """Reference scan also covers PostgreSQL, which has no normalized SQL compiler."""
    if isinstance(node, BooleanMatch):
        values = [_accepts(row, child) for child in node.operands]
        return (
            not values[0]
            if node.operator == "not"
            else all(values)
            if node.operator == "and"
            else any(values)
        )
    value, expected, op = row[node.column], node.value, node.operator
    if op == "is_null":
        return (value is not None) if expected is False else (value is None)
    if op == "in":
        return value in expected
    if op == "eq":
        return value == expected
    if op == "ne":
        return value != expected
    if value is None:
        return False
    if op == "contains":
        return expected in value
    if op == "prefix":
        return value.startswith(expected)
    return {
        "lt": operator.lt,
        "lte": operator.le,
        "gt": operator.gt,
        "gte": operator.ge,
    }[op](value, expected)


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_wemi_boolean_graph_queries_match_sql_at_every_level(collection, storage_type):
    catalog = collection.catalog
    queries = (
        "work.title:Novel",
        "expression:Translation",
        'item:"SEARCH-COPY"',
        'author:"Introduction Author"',
        "work.tag:IntroOnly",
        'translator:"Search Translator"',
        "work.title:Novel and not work.tag:IntroOnly",
        "work.title:Searchfixture and not item:SEARCH",
        "(work.title:Novel or work.title:Unpublished) and not work.tag:IntroOnly",
        "not (author:Novel or work.tag:IntroOnly)",
        'work.title:"=Searchfixture Novel"',
        'work.title:"\' OR 1=1 --"',
    )
    cache = ReadCache(catalog.db, storage_type=storage_type)
    try:
        cache.load()
        # Live captures are now demand-driven, so every parity check can exercise
        # its real per-call capture and close boundary instead of sharing a view.
        for level in ("work", "expression", "manifestation", "item"):
            for query in queries:
                expected = catalog.search.find(query, level=level, limit=None)
                actual = catalog.search.find(
                    query, level=level, limit=None, read_cache=cache
                )
                assert actual == expected, (storage_type, level, query)
                page = catalog.search.find(
                    query, level=level, offset=1, limit=1, read_cache=cache
                )
                assert page == expected[1:2], (storage_type, level, query)
        if cache.capabilities.consistency.value == "live":
            # No manual invalidation is involved in observing this external write.
            old_title = catalog.works.require(collection.novel)["work_title"]
            old_query = CacheQuery(
                "works", where=ComparisonMatch("work_title", "eq", old_title)
            )
            assert cache.query(old_query).ids == (collection.novel,)
            new_title = old_title + " live refresh"
            catalog.works.update(collection.novel, {"title": new_title})
            # The fixture owns the connection, including any PostgreSQL read
            # transaction opened by the preceding reference queries.
            catalog.db.driver.conn.commit()
            assert cache.query(old_query).ids == ()
            assert cache.query(
                CacheQuery(
                    "works", where=ComparisonMatch("work_title", "eq", new_title)
                )
            ).ids == (collection.novel,)
    finally:
        cache.close()


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_nullable_comparison_trees_match_database(collection, storage_type):
    catalog = collection.catalog
    with catalog.db.macros.transaction():
        for i, year in enumerate((None, -50, 0, 1900, 2000, 2026)):
            catalog.works.create(
                {"title": f"Stress %_ Café {i}", "original_year": year}
            )
    cache = ReadCache(catalog.db, storage_type=storage_type)
    rng = random.Random(512)
    leaves = [
        ComparisonMatch("work_original_year", op, value)
        for op in ("eq", "ne", "lt", "lte", "gt", "gte")
        for value in (0, 2000)
    ]
    leaves += [
        ComparisonMatch("work_original_year", "eq", None),
        ComparisonMatch("work_original_year", "ne", None),
        ComparisonMatch("work_original_year", "in", (None, 2000)),
        ComparisonMatch("work_original_year", "in", ()),
        ComparisonMatch("work_original_year", "is_null", False),
        ComparisonMatch("work_title", "contains", "%_"),
        ComparisonMatch("work_title", "prefix", "Stress %_"),
        ComparisonMatch("work_title", "contains", "Café"),
    ]
    trees = leaves + [BooleanMatch("not", (leaf,)) for leaf in leaves]
    trees += [
        BooleanMatch(rng.choice(("and", "or")), tuple(rng.sample(leaves, 3)))
        for _ in range(30)
    ]
    try:
        cache.load()
        source = catalog.db.macros.get_rows("works")
        for tree in trees:
            actual = cache.query(CacheQuery("works", where=tree)).ids
            expected = tuple(
                sorted(row["work_id"] for row in source if _accepts(row, tree))
            )
            assert actual == expected, (storage_type, tree)
            if catalog.db.macros.supports_normalized_search():
                sql = tuple(
                    row["work_id"]
                    for row in catalog.db.macros.find_rows_matching(
                        "works", tree, limit=None
                    )
                )
                assert sql == expected, tree
    finally:
        cache.close()
