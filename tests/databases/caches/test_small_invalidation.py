"""Small row repairs must not scan the table or copy retained match populations."""

from dataclasses import replace

import pytest

from LiuXin_alpha.caches.api import CacheFilterOperator, CachePredicate, CacheQuery
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


@pytest.mark.parametrize("legacy_predicate", (False, True))
@pytest.mark.parametrize("change", ("unchanged", "leave", "enter", "delete", "insert"))
def test_repair_reads_only_changed_ids_and_keeps_public_results(
    monkeypatch, legacy_predicate, change
):
    rows = {i: {"id": i, "value": 1} for i in range(2000)}
    rows[0]["value"] = 0 if change == "enter" else 1
    engine = ProjectionQueryEngine("rows", rows, ("id", "value"))
    query = CacheQuery(
        "rows",
        predicates=(CachePredicate("value", CacheFilterOperator.EQ, 1),)
        if legacy_predicate
        else (),
        where=None if legacy_predicate else ComparisonMatch("value", "eq", 1),
    )
    original = engine.query(query, generation=1)
    original_ids = original.ids
    saved = engine.row_local_results()
    changed = 2000 if change == "insert" else 0
    if change == "delete":
        del engine._rows[changed]
    else:
        engine._rows[changed] = {"id": changed, "value": 0 if change == "leave" else 1}
    engine.row_ids = frozenset(engine._rows)
    engine.reset()

    def no_scan(*args, **kwargs):
        raise AssertionError("Bounded repair must not enumerate the table/value index")

    checks = []
    has_id = engine.has_id

    def contains(row_id):
        checks.append(row_id)
        return has_id(row_id)

    monkeypatch.setattr(engine, "_all_ids", no_scan)
    monkeypatch.setattr(engine, "_candidate_ids_for_predicate", no_scan)
    monkeypatch.setattr(engine, "has_id", contains)
    engine.repair_results(saved, {"rows": {changed}}, generation=2)
    result = engine.query(query, generation=2)
    expected = tuple(sorted(i for i, row in engine._rows.items() if row["value"] == 1))
    assert result.ids == expected
    assert result.total_count == len(expected)
    assert checks == [changed]
    assert original.ids == original_ids
    assert all(row["value"] == rows[row.row_id]["value"] for row in original.records)


class NoPopulationCopy(set):
    """Allow bounded membership edits, but reject full-population repair work."""

    def __iter__(self):
        raise AssertionError("Repair iterated the complete retained population")

    def copy(self):
        raise AssertionError("Repair copied the complete retained population")

    def __sub__(self, other):
        raise AssertionError("Repair copied the complete retained population")

    def __or__(self, other):
        raise AssertionError("Repair copied the complete retained population")


@pytest.mark.parametrize("change_table", ("rows", "unrelated"))
def test_repair_reuses_owned_membership_without_population_copy(change_table):
    engine = ProjectionQueryEngine(
        "rows", {i: {"id": i, "value": 1} for i in range(2000)}, ("id", "value")
    )
    query = CacheQuery("rows", where=ComparisonMatch("value", "eq", 1), limit=0)
    engine.query(query, generation=1)
    saved = engine.row_local_results()
    # The saved private membership is exclusively owned by the repair boundary.
    members = NoPopulationCopy(saved[0][1])
    saved = [(saved[0][0], members)]
    engine._rows[0]["value"] = 0
    engine.reset()
    engine.repair_results(saved, {change_table: {0}}, generation=2)
    assert next(iter(engine._results.values())) is members
    assert engine.query(query, generation=2).total_count == (
        1999 if change_table == "rows" else 2000
    )


def test_multiple_pending_invalidations_preserve_each_changed_row(modern_cache):
    cache, db = modern_cache
    query = CacheQuery("books", where=ComparisonMatch("rating", "gte", 4), limit=0)
    before = cache.query(replace(query, limit=None))
    db.driver_wrapper._rows_by_table["books"][0]["rating"] = 1
    cache.invalidate(ids={"books": (1,)})
    db.driver_wrapper._rows_by_table["books"][1]["rating"] = 5
    cache.invalidate(ids={"books": (2, 9999)})
    assert cache.query(query).total_count == 2
    assert cache.query(replace(query, limit=None)).ids == (2, 3)
    assert before.ids == (1, 3)
