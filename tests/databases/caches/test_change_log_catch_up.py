"""
Bring snapshot caches up to date from the change log instead of reloading every table.

After a committed write, a snapshot re-reads only the rows the change log names.
The result must equal a full reload: these tests compare the two after a mixed
workload, and check writes from another connection, rolled-back batches, the
fallbacks to a full reload, and that the full reload is no longer used.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from shutil import copyfile

import pytest

from LiuXin_alpha.caches.api import CacheQuery
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases import change_log
from LiuXin_alpha.databases.database import Database
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

SNAPSHOT_MODES = ("schema_backed", "numpy_vectorized")
OPTIONS = dict(
    backup=False,
    repair_bootstrap_rows=False,
    enable_storage_manager=False,
    enable_maintenance=False,
)


@pytest.fixture
def catalogue(metadata_template, tmp_path):
    """
    Copy the empty catalogue, which has the change log, for one test.

    Example:
        >>> def test_example(catalogue): ...  # doctest: +SKIP


    :param metadata_template: Module-scoped empty catalogue.
    :param tmp_path: Per-test directory.
    :return: Path of this test's catalogue.
    """
    path = tmp_path / "catch-up.sqlite"
    copyfile(metadata_template, path)
    return path


class _ReloadCounter:
    """
    Count full reloads a storage backend performs.

    Example:
        >>> counter = _ReloadCounter(cache.storage, monkeypatch)  # doctest: +SKIP
        >>> counter.count
        0
    """

    def __init__(self, storage, monkeypatch):
        """
        Wrap the backend's full reload entry points.

        Example:
            >>> _ReloadCounter(cache.storage, monkeypatch)  # doctest: +SKIP


        :param storage: Storage backend to observe.
        :param monkeypatch: Pytest monkeypatch fixture.
        :return: ``None``.
        """
        self.count = 0
        for name in ("reload", "reload_data"):
            original = getattr(storage, name, None)
            if original is None:
                continue

            def counted(*args, _original=original, **kwargs):
                """
                Count one full reload, then run it.

                Example:
                    >>> storage.reload_data()  # doctest: +SKIP


                :param args: Positional arguments for the original method.
                :param _original: Wrapped bound method.
                :param kwargs: Keyword arguments for the original method.
                :return: The original method's result.
                """
                self.count += 1
                return _original(*args, **kwargs)

            monkeypatch.setattr(storage, name, counted)


def _snapshot(cache):
    """
    Read every main-table row and every link row a cache holds.

    Example:
        >>> _snapshot(cache)["works"]  # doctest: +SKIP
        {1: {...}}


    :param cache: Loaded cache.
    :return: Mapping of table names to rows, and of directed link views to rows.
    """
    tables = {}
    for table in sorted(cache.storage.main_tables):
        tables[table] = {
            record.row_id: dict(record.values)
            for record in cache.query(CacheQuery(table=table)).records
        }
    links = {}
    for source, target in sorted(cache.storage.link_tables):
        links[(source, target)] = sorted(
            (source_id, record.row_id, tuple(sorted(record.values.items())))
            for source_id in tables[source]
            for record in cache.link_records(source, source_id, target)
        )
    return tables, links


def _fresh_snapshot(db, mode):
    """
    Load a new cache of the same backend and read it.

    Example:
        >>> _fresh_snapshot(db, "schema_backed")  # doctest: +SKIP


    :param db: Open database.
    :param mode: Snapshot backend name.
    :return: The new cache's snapshot.
    """
    fresh = create_cache(db, mode)
    try:
        return _snapshot(fresh)
    finally:
        fresh.close()


def _stack(api, title):
    """
    Create a minimal WEMI stack.

    Example:
        >>> _stack(api, "Title")  # doctest: +SKIP


    :param api: Metadata service.
    :param title: Work title.
    :return: The creation result.
    """
    return api.mutations.create_wemi_stack(
        work={"title": title},
        expression={"label": "text"},
        manifestation={"edition_statement": "first"},
        items=({"inventory_code": title},),
    ).result


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_catch_up_matches_a_full_reload_without_reloading(catalogue, monkeypatch, mode):
    """
    A mixed workload leaves the caught-up snapshot equal to a freshly loaded one.

    Inserts, renames, link additions and removals, credits and deletions with
    detach all go through the log; none of them reloads the snapshot in full.

    Example:
        >>> test_catch_up_matches_a_full_reload_without_reloading(path, mp, "schema_backed")  # doctest: +SKIP


    :param catalogue: Catalogue file with the change log.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param mode: Snapshot backend.
    :return: ``None`` after comparing snapshots at each step.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        cache = create_cache(db, mode)
        assert cache._change_cursor is not None
        counter = _ReloadCounter(cache.storage, monkeypatch)
        api = CatalogCache(Catalog(db), cache=cache)

        first = _stack(api, "First")
        second = _stack(api, "Second")
        tag = api.works.add_tag(first.work_id, text="Fantasy").result
        api.works.add_tag(second.work_id, tag_id=tag)
        assert _snapshot(cache) == _fresh_snapshot(db, mode)

        api.works.update(first.work_id, {"title": "First, renamed"})
        api.tags.update(tag, {"name": "Fantasy fiction"})
        api.works.remove_tag(second.work_id, tag)
        agent = api.agents.create_person({"name": "An Author"}).result
        api.works.add_credit(first.work_id, agent_id=agent, role="aut")
        assert _snapshot(cache) == _fresh_snapshot(db, mode)

        api.agents.delete(agent, detach=True)
        api.tags.delete(tag, detach=True)
        assert _snapshot(cache) == _fresh_snapshot(db, mode)
        assert api.works.list_tags(first.work_id) == ()
        assert api.works.require(first.work_id).work.work_title == "First, renamed"
        assert counter.count == 0
        cache.close()


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_catch_up_shows_writes_from_another_connection(catalogue, monkeypatch, mode):
    """
    ``catch_up`` makes another connection's committed rows visible row by row.

    Example:
        >>> test_catch_up_shows_writes_from_another_connection(path, mp, "schema_backed")  # doctest: +SKIP


    :param catalogue: Catalogue file with the change log.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param mode: Snapshot backend.
    :return: ``None`` after checking visibility.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as first:
        with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as second:
            cache = create_cache(first, mode)
            counter = _ReloadCounter(cache.storage, monkeypatch)
            other = MetadataAPI(Catalog(second))
            work = _stack(other, "Elsewhere").work_id
            tag = other.works.add_tag(work, text="Remote").result

            assert cache.get("works", work).value is None
            generation = cache.generation
            cache.catch_up()
            assert cache.get("works", work).value is not None
            assert cache.get("tags", tag).value is not None
            assert [r.row_id for r in cache.link_records("works", work, "tags")]
            assert cache.generation > generation
            assert _snapshot(cache) == _fresh_snapshot(first, mode)

            generation = cache.generation
            cache.catch_up()
            assert cache.generation == generation, "nothing new: no new generation"
            assert counter.count == 0
            cache.close()


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_rolled_back_batch_and_committed_batch(catalogue, monkeypatch, mode):
    """
    A failed batch leaves nothing to apply; a committed one is caught up once.

    Example:
        >>> test_rolled_back_batch_and_committed_batch(path, mp, "schema_backed")  # doctest: +SKIP


    :param catalogue: Catalogue file with the change log.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param mode: Snapshot backend.
    :return: ``None`` after checking both batches.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        cache = create_cache(db, mode)
        counter = _ReloadCounter(cache.storage, monkeypatch)
        api = CatalogCache(Catalog(db), cache=cache)
        before = _snapshot(cache)

        def failing(service):
            """
            Write a Work, then fail so the batch rolls back.

            Example:
                >>> api.batch(failing)  # doctest: +SKIP


            :param service: Metadata interface inside the batch.
            :return: Never returns.
            :raises RuntimeError: Always, after the write.
            """
            service.works.create({"title": "Discarded"})
            raise RuntimeError("roll back")

        with pytest.raises(RuntimeError):
            api.batch(failing)
        assert _snapshot(cache) == before

        receipt = api.batch(
            lambda service: [
                service.works.create({"title": f"Batched {n}"}).result for n in range(3)
            ]
        )
        assert receipt.cache == "current"
        for work in receipt.result:
            assert cache.get("works", work).value is not None
        assert _snapshot(cache) == _fresh_snapshot(db, mode)
        assert counter.count == 0
        cache.close()


@pytest.mark.parametrize("mode", SNAPSHOT_MODES)
def test_falls_back_to_a_full_reload_when_the_log_cannot_help(
    catalogue, monkeypatch, mode
):
    """
    A backlog over the limit, or a catalogue without the log, reloads in full.

    Example:
        >>> test_falls_back_to_a_full_reload_when_the_log_cannot_help(path, mp, "schema_backed")  # doctest: +SKIP


    :param catalogue: Catalogue file with the change log.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param mode: Snapshot backend.
    :return: ``None`` after checking both fallbacks.
    """
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        cache = create_cache(db, mode)
        counter = _ReloadCounter(cache.storage, monkeypatch)
        api = CatalogCache(Catalog(db), cache=cache)
        monkeypatch.setattr(type(cache), "CATCH_UP_LIMIT", 2)
        work = _stack(api, "Too many rows at once").work_id
        assert counter.count == 1
        assert cache.get("works", work).value is not None
        assert cache._change_cursor is not None
        cache.close()

    with closing(sqlite3.connect(catalogue)) as connection, connection:
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'liuxin_c_%'"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute("DROP TABLE catalog_changes")
        connection.execute("DROP TABLE catalog_changes_metadata")
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        cache = create_cache(db, mode)
        assert cache._change_cursor is None
        counter = _ReloadCounter(cache.storage, monkeypatch)
        api = CatalogCache(Catalog(db), cache=cache)
        work = api.works.create({"title": "Without the log"}).result
        assert counter.count == 1
        assert cache.get("works", work).value is not None
        cache.close()


def test_uncovered_table_disables_catch_up(catalogue):
    """
    A snapshot reading a table the log does not cover never trusts the log.

    Example:
        >>> test_uncovered_table_disables_catch_up(path)  # doctest: +SKIP


    :param catalogue: Catalogue file with the change log.
    :return: ``None`` after checking the cursor.
    """
    with closing(sqlite3.connect(catalogue)) as connection, connection:
        name = change_log._sqlite_trigger_names("tags")[0]
        connection.execute(f'DROP TRIGGER "{name}"')
    with Database(metadata={"database_path": str(catalogue)}, **OPTIONS) as db:
        cache = create_cache(db, "schema_backed")
        assert cache._change_cursor is None
        cache.close()
