"""Bound live execution captures without weakening freshness or completeness."""

from contextlib import contextmanager

import pytest

from LiuXin_alpha.caches.api import CachePredicate, CacheQuery, CacheSort
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.db_types.query_types import LinkedMatch, TextMatch
from LiuXin_alpha.databases.transactions import transaction_active
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


@contextmanager
def seed_capture(database):
    """Own a small live cache around real native rows and typed relationships."""
    api = MetadataAPI(Catalog(database))

    def seed(current):
        works = tuple(
            current.works.create({"title": name}).result for name in ("A", "B")
        )
        tags = tuple(
            current.tags.create({"name": name}).result for name in ("first", "second")
        )
        current.works.set_tags(works[0], tags)
        return works, tags

    works, tags = api.batch(seed).result
    cache = create_cache(database, "database_backed")
    try:
        yield CatalogCache(Catalog(database), cache=cache), cache, works, tags
    finally:
        cache.close()


@pytest.fixture
def seeded(metadata_db):
    with seed_capture(metadata_db) as current:
        yield current


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_point_capture_never_reads_a_whole_table(seeded, monkeypatch):
    api, cache, _, tags = seeded
    macros = cache.database.macros
    points = []
    original = macros.get_row

    def point(table, row_id, **kwargs):
        points.append((table, row_id))
        return original(table, row_id, **kwargs)

    def no_population(*args, **kwargs):
        raise AssertionError("point lookup scanned a table")

    monkeypatch.setattr(macros, "get_row", point)
    monkeypatch.setattr(macros, "get_rows", no_population)
    records = api.tags.get_many((tags[0], tags[1], tags[0], 987654))
    assert [record.entity_id if record else None for record in records] == [
        tags[0],
        tags[1],
        tags[0],
        None,
    ]
    assert records[0] == records[2] and records[0] is not records[2]
    assert points == [("tags", tags[0]), ("tags", tags[1]), ("tags", 987654)]


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_relationship_capture_reads_only_its_route(seeded, monkeypatch):
    api, cache, works, tags = seeded
    macros = cache.database.macros
    populations = []
    original = macros.get_rows

    def population(table, **kwargs):
        populations.append(table)
        return original(table, **kwargs)

    monkeypatch.setattr(macros, "get_rows", population)
    members = api.works.list_tags(works[0])
    assert {member.member.entity_id for member in members} == set(tags)
    spec = cache.database.driver_wrapper.get_link_spec("works", "tags")
    assert populations == [spec.link_table]


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_query_capture_keeps_filter_sort_and_freshness(seeded, monkeypatch):
    api, cache, works, _ = seeded
    macros = cache.database.macros
    populations = []
    original = macros.get_rows

    def population(table, **kwargs):
        populations.append(table)
        return original(table, **kwargs)

    monkeypatch.setattr(macros, "get_rows", population)
    page = cache.query(
        CacheQuery("works", sort=(CacheSort("work_title"),), offset=1, limit=1)
    )
    assert page.ids == (works[1],) and page.total_count == 2
    assert populations == ["works"]
    # A separate coordinator writes without invalidating this live cache.
    direct = MetadataAPI(Catalog(cache.database))
    direct.works.update(works[0], {"title": "Z"})
    assert cache.query(
        CacheQuery("works", sort=(CacheSort("work_title"),), limit=1)
    ).ids == (works[1],)
    assert api.works.require(works[0]).work.work_title == "Z"


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_capture_uses_and_does_not_complete_callers_transaction(seeded):
    api, cache, works, _ = seeded
    database = cache.database
    with pytest.raises(RuntimeError, match="rollback"):
        with database.macros.transaction():
            database.macros.update_row("works", works[0], {"work_title": "uncommitted"})
            assert api.works.require(works[0]).work.work_title == "uncommitted"
            assert cache.query(
                CacheQuery(
                    "works",
                    predicates=(CachePredicate("work_title", "eq", "uncommitted"),),
                )
            ).ids == (works[0],)
            assert database.macros.current_transaction_connection() is not None
            raise RuntimeError("rollback")
    assert api.works.require(works[0]).work.work_title == "A"


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_capture_preserves_relationship_predicates_and_projection(seeded):
    _, cache, works, _ = seeded
    spec = cache.database.driver_wrapper.get_link_spec("works", "tags")
    column = cache.storage._snapshot_main_tables["tags"].default_value_column
    query = CacheQuery("works", where=LinkedMatch(spec, TextMatch((column,), "first")))
    assert cache.query(query).ids == (works[0],)
    projection = cache.query(CacheQuery("works", projection=(f"works.tags.{column}",)))
    assert set(projection.records[0].values[f"works.tags.{column}"]) == {
        "first",
        "second",
    }


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_failed_capture_finishes_its_owned_read_unit(seeded, monkeypatch):
    from LiuXin_alpha.caches.cache_plugins.database_backed.query_view import (
        DatabaseQueryView,
    )

    _, cache, _, _ = seeded
    views = []
    original = cache.storage.query_view

    def capture():
        view = original()
        views.append(view)
        return view

    def fail(*args, **kwargs):
        raise RuntimeError("injected capture failure")

    monkeypatch.setattr(cache.storage, "query_view", capture)
    monkeypatch.setattr(DatabaseQueryView, "get_field", fail)
    with pytest.raises(RuntimeError, match="injected capture failure"):
        cache.query(CacheQuery("works", projection=("work_title",)))
    assert len(views) == 1 and views[0].db is None
    assert cache.database.macros.current_transaction_connection() is None
    assert not transaction_active(cache.database.driver.conn)


def test_postgres_capture_reads_callers_rows_and_releases_owned_units(
    pg_database, monkeypatch
):
    database = pg_database[0]
    with seed_capture(database) as current:
        # Initialization may use the persistent driver connection for metadata.
        # Start the ownership checks with that fixture-only read unit finished.
        database.driver.conn.rollback()
        test_point_capture_never_reads_a_whole_table(current, monkeypatch)
        assert not transaction_active(database.driver.conn), "point lookup"
        monkeypatch.undo()
        test_relationship_capture_reads_only_its_route(current, monkeypatch)
        assert not transaction_active(database.driver.conn), "relationship lookup"
        monkeypatch.undo()
        test_capture_uses_and_does_not_complete_callers_transaction(current)
        assert not transaction_active(database.driver.conn), "caller rollback"
        test_capture_preserves_relationship_predicates_and_projection(current)
        assert not transaction_active(database.driver.conn), "relationship projection"
        test_failed_capture_finishes_its_owned_read_unit(current, monkeypatch)


@pytest.mark.parametrize("metadata_db", ["SQLite", "SQLite_apsw"], indirect=True)
def test_capture_retains_sqlite_declared_value_conversion(seeded):
    _, cache, works, _ = seeded
    database = cache.database
    with database.macros.transaction() as connection:
        connection.execute(
            "UPDATE works SET work_title=? WHERE work_id=?", (b"binary title", works[0])
        ).close()
    expected = database.driver_wrapper.get_all_rows("works")
    expected_by_id = {row["work_id"]: row for row in expected}
    with database.macros.read_transaction():
        view = ReadCache.from_storage(cache.storage.query_view())
        try:
            assert view.get("works", works[0]).value.values == expected_by_id[works[0]]
            assert {
                record.row_id: dict(record.values)
                for record in view.query(CacheQuery("works")).records
            } == expected_by_id
        finally:
            view.close()


def test_postgres_capture_converts_binary_buffers_before_copy(pg_database):
    database = pg_database[0]
    with database.macros.transaction() as connection:
        connection.execute(
            "ALTER TABLE works ADD COLUMN work_capture_bytes bytea"
        ).close()
    database.refresh_db_metadata()
    with seed_capture(database) as (_, cache, works, _):
        with database.macros.transaction() as connection:
            connection.execute(
                "UPDATE works SET work_capture_bytes=? WHERE work_id=?",
                (b"\x00binary\xff", works[0]),
            ).close()
        with database.macros.read_transaction():
            view = ReadCache.from_storage(cache.storage.query_view())
            try:
                value = view.get("works", works[0]).value.values["work_capture_bytes"]
                assert type(value) is bytes and value == b"\x00binary\xff"
                records = view.query(CacheQuery("works")).records
                assert records[0].values["work_capture_bytes"] == value
            finally:
                view.close()
