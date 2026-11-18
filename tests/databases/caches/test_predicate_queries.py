"""Physical predicate and recovery contracts shared by every native backend."""

import pytest

from LiuXin_alpha.caches.api import (
    CacheDirtyError,
    CacheQuery,
    CacheState,
    UnknownCacheFieldError,
)
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ComparisonMatch,
    ForeignMatch,
    LinkedMatch,
    TextMatch,
    ValueMatch,
)
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


def test_nested_predicates_sort_and_page(modern_cache):
    cache, _ = modern_cache
    where = BooleanMatch(
        "and",
        (
            BooleanMatch(
                "or", (ValueMatch("rating", 5), TextMatch(("title",), "alpha"))
            ),
            BooleanMatch("not", (ValueMatch("title", "missing"),)),
        ),
    )
    result = cache.query(CacheQuery("books", where=where, offset=1, limit=1))
    assert result.ids == (3,)
    assert result.total_count == 2
    assert cache.query(
        CacheQuery("books", where=ComparisonMatch("rating", "gte", 4))
    ).ids == (1, 3)


def test_empty_branch_still_validates_fields(modern_cache):
    cache, _ = modern_cache
    with pytest.raises(UnknownCacheFieldError):
        cache.query(
            CacheQuery(
                "books",
                where=BooleanMatch(
                    "and",
                    (
                        BooleanMatch("or", ()),
                        ValueMatch("not_a_column", 1),
                    ),
                ),
            )
        )
    with pytest.raises(UnknownCacheFieldError):
        cache.query(
            CacheQuery("books", predicates=(), projection=("missing",), limit=0)
        )


def test_nested_relationship(modern_cache):
    cache, db = modern_cache
    spec = db.driver_wrapper.get_link_spec("books", "tags")
    where = LinkedMatch(spec, TextMatch(("name",), "fiction"))
    assert cache.query(CacheQuery("books", where=where)).ids == (1, 3)
    where = ForeignMatch("books", "id", "id", ComparisonMatch("rating", "gte", 5))
    assert cache.query(CacheQuery("books", where=where)).ids == (3,)


def test_result_reuse_and_invalidation(modern_cache):
    cache, db = modern_cache
    query = CacheQuery("books", where=ValueMatch("title", "Alpha"))
    assert cache.query(query).ids == (2,)
    cache.query(query)
    if cache.capabilities.consistency.value == "snapshot":
        assert cache._query_engine.result_hits == 1
    else:
        assert cache._query_engine.result_hits == 0
    db.driver_wrapper._rows_by_table["books"][1]["title"] = "Gamma"
    cache.invalidate(tables=("books",))
    assert cache.query(query).ids == ()


def test_invalid_request_cannot_partially_invalidate(modern_cache):
    cache, db = modern_cache
    query = CacheQuery("books", text="alpha", text_fields=("title",))
    cache.query(query)
    before = cache.generation
    with pytest.raises(KeyError):
        cache.invalidate(tables=("books",), fields=("books.missing",))
    assert cache.generation == before
    assert cache.state == CacheState.READY
    assert cache.query(query).ids == (2,)


def test_failed_full_refresh_requires_recovery(modern_cache, monkeypatch):
    cache, _ = modern_cache
    with monkeypatch.context() as patch:

        def fail():
            raise RuntimeError("injected refresh failure")

        patch.setattr(cache.storage, "reload", fail)
        with pytest.raises(RuntimeError):
            cache.reload()
        assert cache.state == CacheState.DIRTY
        with pytest.raises(CacheDirtyError):
            cache.query(CacheQuery("books"))
    assert cache.query(CacheQuery("books")).ids == (1, 2, 3)
    assert cache.state == CacheState.READY


def test_bounded_row_result_repair_and_deletion(modern_cache):
    cache, db = modern_cache
    if cache.capabilities.consistency.value != "snapshot":
        pytest.skip("Live requests retain no membership results")
    query = CacheQuery("books", where=ComparisonMatch("rating", "gte", 4))
    assert cache.query(query).ids == (1, 3)
    db.driver_wrapper._rows_by_table["books"][0]["rating"] = 1
    cache.invalidate(ids={"books": (1,)})
    assert cache.query(query).ids == (3,)
    assert cache._query_engine.repaired_results == 1
    assert cache._query_engine.result_hits == 1
    db.driver_wrapper._rows_by_table["books"] = [
        row for row in db.driver_wrapper._rows_by_table["books"] if row["id"] != 3
    ]
    cache.invalidate(ids={"books": (3,)})
    assert cache.query(query).ids == ()
    assert cache._query_engine.repaired_results == 2


def test_large_update_evicts_instead_of_repairing(modern_cache):
    cache, _ = modern_cache
    if cache.capabilities.consistency.value != "snapshot":
        pytest.skip("Live requests retain no membership results")
    cache.query(CacheQuery("books", where=ValueMatch("title", "Alpha")))
    cache.invalidate(ids={"books": range(1, 60)})
    cache.query(CacheQuery("books"))
    assert cache._query_engine.repaired_results == 0


def test_live_query_captures_once_and_sees_next_write(modern_cache, monkeypatch):
    cache, db = modern_cache
    if cache.capabilities.consistency.value != "live":
        pytest.skip("Only live backends capture an execution view")
    captures = []
    original = cache.storage.query_view

    def view():
        captures.append(None)
        return original()

    monkeypatch.setattr(cache.storage, "query_view", view)
    query = CacheQuery("books", where=ValueMatch("title", "Alpha"))
    assert cache.query(query).ids == (2,)
    db.driver_wrapper._rows_by_table["books"][1]["title"] = "Changed"
    assert cache.query(query).ids == ()
    assert len(captures) == 2


def test_provider_result_is_never_implicitly_cached(modern_cache):
    from dataclasses import dataclass

    from LiuXin_alpha.databases.db_types.query_types import ProvidedMatch

    cache, _ = modern_cache
    enabled = set()

    @dataclass(frozen=True)
    class Provider:
        def validate(self, table):
            assert table == "books"

        def match(self, table, candidates):
            return candidates & enabled

    query = CacheQuery("books", where=ProvidedMatch(Provider()))
    assert cache.query(query).ids == ()
    enabled.add(2)
    assert cache.query(query).ids == (2,)


def test_membership_cache_preserves_operand_container_type():
    from LiuXin_alpha.caches.projection import ProjectionQueryEngine

    engine = ProjectionQueryEngine(
        "books", {1: {"id": 1, "tags": ("a",)}}, ("id", "tags")
    )
    assert (
        engine.query(
            CacheQuery("books", where=ValueMatch("tags", ["a"])), generation=1
        ).ids
        == ()
    )
    assert engine.query(
        CacheQuery("books", where=ValueMatch("tags", ("a",))), generation=1
    ).ids == (1,)


def test_invalid_relationship_spec_is_rejected_before_pruning(modern_cache):
    from dataclasses import replace

    from LiuXin_alpha.errors import InputIntegrityError

    cache, db = modern_cache
    spec = replace(
        db.driver_wrapper.get_link_spec("books", "tags"), secondary_link_col="unknown"
    )
    where = BooleanMatch(
        "and",
        (BooleanMatch("or", ()), LinkedMatch(spec, ValueMatch("name", "Fiction"))),
    )
    with pytest.raises(InputIntegrityError, match="topology"):
        cache.query(CacheQuery("books", where=where))
