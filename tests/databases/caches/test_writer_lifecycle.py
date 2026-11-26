"""Writer execution shares the facade read/close lock and attachment lifetime."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event

import pytest

from LiuXin_alpha.caches.api import CacheClosedError, CacheState
from LiuXin_alpha.caches.metadata import CatalogCache
from LiuXin_alpha.catalog.catalog import Catalog
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


@pytest.mark.parametrize("operation", ("read", "close"))
def test_write_serializes_persistence_and_reconciliation_with_read_or_close(
    modern_cache, monkeypatch, operation
):
    cache, db = modern_cache
    writer = CatalogCache(Catalog(cache.database), cache=cache).create_writer(
        "books", "title"
    )
    prior = cache.get("books", 2).value
    original_title = prior["title"]
    entered, release, other_started = Event(), Event(), Event()
    update = db.macros.update_rows

    def paused_write(*args, **kwargs):
        entered.set()
        assert release.wait(10), "write was not released"
        return update(*args, **kwargs)

    def other_operation():
        other_started.set()
        return cache.get("books", 2) if operation == "read" else cache.close()

    monkeypatch.setattr(db.macros, "update_rows", paused_write)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            written = pool.submit(writer.write_one, 2, "updated")
            try:
                assert entered.wait(10)
                other = pool.submit(other_operation)
                assert other_started.wait(10)
                with pytest.raises(TimeoutError):
                    other.result(timeout=0.05)
            finally:
                release.set()
            assert written.result(timeout=10) == {2: "updated"}
            result = other.result(timeout=10)
        if operation == "read":
            assert result.value["title"] == "updated"
        else:
            assert cache.state == CacheState.CLOSED
            with pytest.raises(CacheClosedError):
                writer.write_one(2, "must not persist")
        assert prior["title"] == original_title
        assert db.get_row_from_id("books", 2).row_dict["title"] == "updated"
    finally:
        cache.close()


@pytest.mark.parametrize("owner", ("facade", "backend", "catalog"))
def test_old_writer_rejects_changed_database_identity_before_persistence(
    modern_cache, monkeypatch, owner
):
    cache, db = modern_cache
    catalog = Catalog(db)
    writer = CatalogCache(catalog, cache=cache).create_writer("books", "title")
    before = db.get_row_from_id("books", 2).row_dict["title"]
    with monkeypatch.context() as patch:
        if owner == "facade":
            patch.setattr(cache, "database", object())
        elif owner == "catalog":
            patch.setattr(catalog, "db", object())
        else:
            patch.setattr(cache.storage, "db", object())
        with pytest.raises(RuntimeError, match="attached to a different database"):
            writer.write_one(2, "must not persist")
    assert db.get_row_from_id("books", 2).row_dict["title"] == before
    cache.close()
