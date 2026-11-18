"""Insert/delete/ID-reuse contracts checked against an independent source model."""

import random
from dataclasses import replace

import pytest

from LiuXin_alpha.caches.api import (
    CacheDirtyError,
    CacheFilterOperator,
    CacheLookupStatus,
    CachePredicate,
    CacheQuery,
    CacheRelation,
    CacheSort,
    CacheState,
)
from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch, ValueMatch
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
    StorageSchemaSpec,
)
from tests.support.cache_long_runs import BACKENDS
from tests.support.storage_cache_test_harness import make_fake_db, make_table


@pytest.fixture(params=BACKENDS)
def membership_cache(request):
    rows = {
        "books": [
            {"id": 2, "title": "Alpha", "rating": None},
            {"id": 10, "title": "Café", "rating": 0},
            {"id": 41, "title": "Straße", "rating": 5},
            {"id": 99, "title": "Zulu", "rating": 5},
        ],
        "tags": [{"id": 100, "name": "shared"}, {"id": 200, "name": "solo"}],
        "book_tags": [
            {"id": 1, "book_id": 2, "tag_id": 100},
            {"id": 2, "book_id": 41, "tag_id": 100},
            {"id": 3, "book_id": 99, "tag_id": 200},
        ],
    }
    schema = StorageSchemaSpec(
        tables={
            "books": make_table(
                "books",
                ("id", "title", "rating"),
                is_main_table=True,
                linked_tables=("tags",),
            ),
            "tags": make_table(
                "tags",
                ("id", "name"),
                is_main_table=True,
                linked_tables=("books",),
            ),
            "book_tags": make_table(
                "book_tags",
                ("id", "book_id", "tag_id"),
                is_link_table=True,
                linked_tables=("books", "tags"),
            ),
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="tags",
                link_table="book_tags",
                cardinality=LinkCardinality.MANY_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="tag_id",
            ),
        ),
        intralinks=(),
    )
    db = make_fake_db(schema, rows)
    cache = ReadCache(db, storage_type=request.param)
    try:
        cache.load()
        yield cache, rows
    finally:
        cache.close()


def _check_books(cache, rows, *, seen_ids=()):
    """Use Python source scans, never another cache or its predicate evaluator."""
    source = {row["id"]: row for row in rows["books"]}
    expected = tuple(sorted(source))
    all_rows = cache.query(CacheQuery("books"))
    assert all_rows.ids == expected
    assert all_rows.total_count == len(expected)
    assert all_rows.complete and all_rows.generation == cache.generation
    assert [dict(record.values) for record in all_rows.records] == [
        source[row_id] for row_id in expected
    ]
    for row_id in set(source) | set(seen_ids):
        lookup = cache.get("books", row_id)
        assert lookup.complete and lookup.generation == cache.generation
        assert lookup.status == (
            CacheLookupStatus.HIT if row_id in source else CacheLookupStatus.MISS
        )
        assert (
            None if lookup.value is None else dict(lookup.value.values)
        ) == source.get(row_id)

    queries = (
        (
            CacheQuery("books", where=ValueMatch("title", "Alpha")),
            lambda row: row["title"] == "Alpha",
        ),
        (
            CacheQuery("books", where=ComparisonMatch("rating", "gte", 4)),
            lambda row: row["rating"] is not None and row["rating"] >= 4,
        ),
        (
            CacheQuery("books", where=ComparisonMatch("rating", "is_null")),
            lambda row: row["rating"] is None,
        ),
        (
            CacheQuery(
                "books",
                predicates=(CachePredicate("rating", CacheFilterOperator.EQ, 5),),
            ),
            lambda row: row["rating"] == 5,
        ),
        (
            CacheQuery("books", text="strasse", text_fields=("title",)),
            lambda row: "strasse" in (row["title"] or "").casefold(),
        ),
        (
            CacheQuery("books", text="phoenix", text_fields=("title",)),
            lambda row: "phoenix" in (row["title"] or "").casefold(),
        ),
    )
    for query, oracle in queries:
        matches = tuple(i for i in expected if oracle(source[i]))
        for _ in range(2):
            result = cache.query(query)
            assert result.ids == matches
            assert result.total_count == len(matches)
        assert cache.query(replace(query, limit=0)).total_count == len(matches)

    for ascending in (True, False):
        # Nulls sort last; equal ratings retain ascending ID order in either direction.
        ordered = sorted(
            expected,
            key=lambda i: (
                source[i]["rating"] is None,
                (source[i]["rating"] or 0) * (1 if ascending else -1),
            ),
        )
        query = CacheQuery(
            "books", sort=(CacheSort("rating", ascending),), projection=("title",)
        )
        for offset, limit in ((0, 0), (0, 1), (1, 2), (len(source), 3)):
            page = cache.query(replace(query, offset=offset, limit=limit))
            assert page.ids == tuple(ordered[offset : offset + limit])
            assert page.total_count == len(source)
            assert [dict(row.values) for row in page.records] == [
                {"id": i, "title": source[i]["title"]} for i in page.ids
            ]

    # Fetch fresh backend handles after the facade refresh: old handles have their own lifetime.
    assert set(cache.storage.get_main_table("books").row_ids) == set(source)
    field = cache.storage.get_field("books.title")
    for value in {"Alpha", "Straße", "Phoenix", "Zulu", "Café"}:
        assert set(field.get_ids_from_value(value)) == {
            i for i, row in source.items() if row["title"] == value
        }
    return all_rows


def _invalidate_books(cache, mode, ids):
    if mode == "table":
        cache.invalidate(tables=("books",))
    else:
        cache.invalidate(ids={"books": ids})


@pytest.mark.parametrize("mode", ("ids", "table"))
@pytest.mark.parametrize("first_deleted", (2, 10, 41, 99))
def test_remove_every_row_then_refill(membership_cache, mode, first_deleted):
    cache, rows = membership_cache
    original = _check_books(cache, rows, seen_ids=(7,))
    saved = tuple(dict(row.values) for row in original.records)
    deleted = []
    for row_id in (first_deleted, *(i for i in (2, 10, 41, 99) if i != first_deleted)):
        rows["books"][:] = [row for row in rows["books"] if row["id"] != row_id]
        deleted.append(row_id)
        _invalidate_books(cache, mode, (row_id, row_id, 9999))
        _check_books(cache, rows, seen_ids=deleted)
    assert cache.query(CacheQuery("books")).total_count == 0
    # Repeated deletion of a missing row leaves a usable empty table.
    _invalidate_books(cache, mode, deleted)
    _check_books(cache, rows, seen_ids=deleted)
    rows["books"].append({"id": 7, "title": "Phoenix", "rating": 5})
    _invalidate_books(cache, mode, (7,))
    _check_books(cache, rows, seen_ids=deleted)
    assert original.ids == (2, 10, 41, 99)
    assert tuple(dict(row.values) for row in original.records) == saved


@pytest.mark.parametrize("mode", ("ids", "table"))
@pytest.mark.parametrize("row_id", (1, 7, 1000))
def test_insert_known_miss_delete_and_reuse_id(membership_cache, mode, row_id):
    cache, rows = membership_cache
    _check_books(cache, rows, seen_ids=(row_id,))
    for title, rating in (("Phoenix", 5), ("Alpha", None), ("Straße", 0)):
        rows["books"].append({"id": row_id, "title": title, "rating": rating})
        _invalidate_books(cache, mode, (row_id,))
        inserted = _check_books(cache, rows)
        rows["books"][:] = [row for row in rows["books"] if row["id"] != row_id]
        _invalidate_books(cache, mode, (row_id,))
        _check_books(cache, rows, seen_ids=(row_id,))
        assert (
            next(row for row in inserted.records if row.row_id == row_id)["title"]
            == title
        )


@pytest.mark.parametrize("bulk_size", (1, 64))
def test_pending_add_remove_and_reuse_coalesce(membership_cache, bulk_size):
    cache, rows = membership_cache
    _check_books(cache, rows)
    rows["books"][:] = [row for row in rows["books"] if row["id"] != 41]
    cache.invalidate(ids={"books": (41,)})
    # Reuse one deleted identity and add then remove another before any reader runs.
    rows["books"].extend(
        [
            {"id": 41, "title": "Phoenix", "rating": None},
            {"id": 700, "title": "Alpha", "rating": 5},
        ]
    )
    cache.invalidate(ids={"books": (41, 700)})
    rows["books"][:] = [row for row in rows["books"] if row["id"] != 700]
    cache.invalidate(ids={"books": (700,)})
    added = range(1000, 1000 + bulk_size)
    rows["books"].extend({"id": i, "title": "Straße", "rating": 5} for i in added)
    cache.invalidate(ids={"books": added})
    # One-row repairs and batches exceeding the repair budget share the same contract.
    _check_books(cache, rows, seen_ids=(700,))


def _check_links(cache, rows):
    for source, target, source_col, target_col in (
        ("books", "tags", "book_id", "tag_id"),
        ("tags", "books", "tag_id", "book_id"),
    ):
        target_ids = {row["id"] for row in rows[target]}
        for row_id in {2, 10, 41, 99, 100, 200, 300}:
            links = [link for link in rows["book_tags"] if link[source_col] == row_id]
            expected = {link[target_col] for link in links} & target_ids
            related = cache.related(source, (row_id,), target)
            assert set(related.ids) == expected
            assert related.total_count == len(expected)
            query = CacheQuery(target, relation=CacheRelation(source, (row_id,)))
            assert cache.query(query).ids == tuple(sorted(expected))
            assert cache.query(query).ids == tuple(sorted(expected))
            assert {
                record.row_id for record in cache.link_records(source, row_id, target)
            } == {link["id"] for link in links}


@pytest.mark.parametrize("mode", ("ids", "table"))
def test_links_and_endpoints_can_be_removed_and_recreated(membership_cache, mode):
    cache, rows = membership_cache
    _check_links(cache, rows)
    old = cache.related("tags", (100,), "books")
    # Remove only one edge: its shared destination and other owner must survive.
    rows["book_tags"].pop(0)
    cache.invalidate(links=(("books", "tags"), ("tags", "books")))
    _check_links(cache, rows)
    assert cache.get("tags", 100).value["name"] == "shared"
    # Model committed endpoint deletion plus its FK cascade explicitly.
    rows["books"][:] = [row for row in rows["books"] if row["id"] != 41]
    rows["book_tags"][:] = [link for link in rows["book_tags"] if link["book_id"] != 41]
    _invalidate_books(cache, mode, (41,))
    cache.invalidate(links=(("books", "tags"), ("tags", "books")))
    _check_books(cache, rows, seen_ids=(41,))
    _check_links(cache, rows)
    rows["books"].append({"id": 41, "title": "Phoenix", "rating": None})
    _invalidate_books(cache, mode, (41,))
    _check_links(cache, rows)  # Reusing an endpoint cannot resurrect old edges.
    rows["tags"].append({"id": 300, "name": "new"})
    rows["book_tags"].extend(
        [
            {"id": 1, "book_id": 41, "tag_id": 300},
            {"id": 2, "book_id": 2, "tag_id": 300},
        ]
    )
    cache.invalidate(ids={"tags": (300,)}, links=(("books", "tags"), ("tags", "books")))
    _check_links(cache, rows)
    # Remove every edge and destination, then refill using a previously cached ID.
    rows["book_tags"].clear()
    rows["tags"].clear()
    cache.invalidate(tables=("tags",), links=(("books", "tags"), ("tags", "books")))
    _check_links(cache, rows)
    rows["tags"].append({"id": 100, "name": "replacement"})
    rows["book_tags"].append({"id": 1, "book_id": 99, "tag_id": 100})
    cache.invalidate(ids={"tags": (100,)}, links=(("books", "tags"), ("tags", "books")))
    _check_links(cache, rows)
    assert cache.related("books", (99,), "tags").records[0]["name"] == "replacement"
    assert old.ids == (2, 41)


@pytest.mark.parametrize(
    "membership_cache", ("schema_backed", "numpy_vectorized"), indirect=True
)
@pytest.mark.parametrize("change", ("insert", "delete", "reuse"))
@pytest.mark.parametrize("failure", ("invalidate_ids", "reload_ids"))
def test_partial_membership_failure_requires_full_recovery(
    membership_cache, monkeypatch, change, failure
):
    cache, rows = membership_cache
    original = _check_books(cache, rows, seen_ids=(1000,))
    if change == "insert":
        changed = 1000
    else:
        changed = 41
        rows["books"][:] = [row for row in rows["books"] if row["id"] != changed]
    if change != "delete":
        rows["books"].append({"id": changed, "title": "Phoenix", "rating": None})
    operation = getattr(cache.storage, failure)

    def partial_failure(*args, **kwargs):
        operation(*args, **kwargs)
        raise RuntimeError("injected membership refresh failure")

    def unavailable():
        raise RuntimeError("source unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage, failure, partial_failure)
        if failure == "invalidate_ids":
            with pytest.raises(RuntimeError, match="membership"):
                cache.invalidate(ids={"books": (changed,)})
        else:
            cache.invalidate(ids={"books": (changed,)})
            with pytest.raises(CacheDirtyError):
                cache.query(CacheQuery("books"))
        assert cache.state == CacheState.DIRTY
        patch.setattr(cache.storage, "reload", unavailable)
        for read in (
            lambda: cache.get("books", changed),
            lambda: cache.query(CacheQuery("books")),
        ):
            with pytest.raises(CacheDirtyError):
                read()
        # Another committed change arrives while recovery is blocked. Recovery must
        # recapture it too, rather than trusting the partly refreshed backend.
        rows["books"].append({"id": 700, "title": "Alpha", "rating": 4})
        patch.setattr(cache.storage, failure, operation)
        cache.invalidate(ids={"books": (700,)})
    _check_books(cache, rows, seen_ids=(changed,))
    assert cache.state == CacheState.READY
    assert original.ids == (2, 10, 41, 99)


@pytest.mark.parametrize("seed", (109, 907))
def test_repeated_membership_churn_matches_source(membership_cache, seed):
    cache, rows = membership_cache
    rng = random.Random(seed)
    seen = {row["id"] for row in rows["books"]}
    _check_books(cache, rows)
    for cycle in range(32):
        changed = set()
        # Keep query results hot while batching additions, deletions and ID reuse.
        for _ in range(3):
            row_id = rng.randrange(1, 65)
            changed.add(row_id)
            seen.add(row_id)
            exists = any(row["id"] == row_id for row in rows["books"])
            rows["books"][:] = [row for row in rows["books"] if row["id"] != row_id]
            if not exists or rng.randrange(2):
                rows["books"].append(
                    {
                        "id": row_id,
                        "title": rng.choice((None, "Alpha", "Phoenix", "Straße")),
                        "rating": rng.choice((None, 0, 4, 5)),
                    }
                )
            cache.invalidate(ids={"books": (row_id,)})
        if cycle % 8 == 7:
            changed.update(row["id"] for row in rows["books"])
            rows["books"].clear()
            cache.invalidate(ids={"books": changed})
        try:
            _check_books(cache, rows, seen_ids=seen)
        except AssertionError as exc:
            raise AssertionError(
                f"seed={seed}, cycle={cycle}, changed={sorted(changed)}"
            ) from exc
