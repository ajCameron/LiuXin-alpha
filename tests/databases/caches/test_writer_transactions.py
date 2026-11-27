"""Enclosing SQL units guard every cache publication and field writer entry point."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event, get_ident

import pytest

from LiuXin_alpha.caches.api import CacheClosedError, CacheDirtyError, CacheState
from tests.databases.caches.test_metadata_api import MODES, service
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


def assert_other_thread_rejected(api, cache, work, column_writer, link_writer):
    """Check promptly; never join a regressed worker until its SQL owner unwinds."""
    update = link_writer.build_one_update(work, "Must not exist")

    def check():
        for operation in (
            lambda: cache.get("works", work),
            cache.reload,
            cache.reload_data,
            cache.catch_up,
            lambda: api.works.require(work),
            lambda: column_writer.write_one(work, "Must not persist"),
            lambda: link_writer.write_one(work, "Must not exist"),
            lambda: link_writer.apply_update(update),
            lambda: api.write_one("works", "tag", work, "Must not exist"),
        ):
            with pytest.raises(CacheDirtyError):
                operation()

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(check)
    try:
        future.result(timeout=15)
    finally:
        executor.shutdown(wait=False)


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES[1:])
@pytest.mark.parametrize("outcome", ("commit", "rollback", "savepoint_rollback"))
def test_field_writers_guard_outer_transaction(metadata_db, mode, outcome):
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Before"}).result
    column = api.create_writer("works", "work_title")
    link = api.create_writer("works", "tag")
    try:
        try:
            with metadata_db.macros.transaction():
                try:
                    with metadata_db.macros.transaction():
                        assert column.write_one(work, "Pending") == {work: "Pending"}
                        link.write_one(work, "Pending tag")
                        assert cache.state == CacheState.DIRTY
                        if outcome == "savepoint_rollback":
                            raise ValueError("rollback savepoint")
                except ValueError:
                    assert outcome == "savepoint_rollback"
                assert_other_thread_rejected(api, cache, work, column, link)
                # Repeated writes from the owner remain usable without a cache read.
                column.write_one(work, "Final")
                if outcome == "rollback":
                    raise ValueError("rollback outer")
        except ValueError:
            assert outcome == "rollback"
        assert cache.get("works", work).value["work_title"] == (
            "Before" if outcome == "rollback" else "Final"
        )
        assert [tag["tag"] for tag in metadata_db.get_all_rows("tags")] == (
            ["Pending tag"] if outcome == "commit" else []
        )
        assert cache.state == CacheState.READY
    finally:
        cache.close()


@pytest.mark.parametrize("mode", MODES[1:])
@pytest.mark.parametrize("close", (False, True))
def test_field_writer_completion_preserves_clear_or_close(metadata_db, mode, close):
    api, cache = service(metadata_db, mode)
    work = api.works.create({"title": "Before"}).result
    writer = api.create_writer("works", "tag")
    with metadata_db.macros.transaction():
        writer.write_one(work, "Pending")
        cache.close() if close else cache.clear()
        if not close:
            with pytest.raises(CacheDirtyError):
                cache.load()
    assert cache.state == (CacheState.CLOSED if close else CacheState.EMPTY)
    if close:
        with pytest.raises(CacheClosedError):
            writer.write_one(work, "Must not exist")
    else:
        cache.load()
        assert cache.get("works", work).value["work_title"] == "Before"
    assert [tag["tag"] for tag in metadata_db.get_all_rows("tags")] == ["Pending"]
    cache.close()


def test_field_writer_rejects_borrowed_transaction_before_write(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    work = api.works.create({"title": "Before"}).result
    writer = api.create_writer("works", "tag")
    connection = metadata_db.driver.conn
    try:
        with monkeypatch.context() as borrowed:
            borrowed.setattr(metadata_db.driver, "get_connection", None)
            connection.execute("BEGIN")
            try:
                with metadata_db.macros.transaction():
                    with pytest.raises(RuntimeError, match="borrowed raw transaction"):
                        writer.write_one(work, "Must not exist")
                assert (
                    connection.execute("SELECT COUNT(*) FROM tags").fetchone()[0] == 0
                )
            finally:
                connection.rollback()
    finally:
        cache.close()


@pytest.mark.parametrize("entry", ("storage", "facade"))
def test_live_data_reload_rebuilds_once_and_preserves_proxy(
    metadata_db, entry, monkeypatch
):
    """A native multi-table graph must not refresh once per live proxy attribute."""
    api, cache = service(metadata_db, "database_backed")
    try:
        work = api.works.create({"title": "Before"}).result
        storage = cache.storage
        proxy = storage.get_main_table("works")
        assert len(storage._snapshot_link_tables) > 1
        api._catalog.works.update(work, {"title": "After"})
        original = storage.read
        calls = []

        def read(*args, **kwargs):
            calls.append(None)
            assert len(calls) == 1, (
                "data reload rebuilt the entire catalogue repeatedly"
            )
            return original(*args, **kwargs)

        with monkeypatch.context() as patched:
            patched.setattr(storage, "read", read)
            (storage if entry == "storage" else cache).reload_data()
        assert len(calls) == 1
        assert storage._main_table_proxies["works"] is proxy
        assert (
            storage._snapshot_main_tables["works"].get_row_snapshot(work)["work_title"]
            == "After"
        )
    finally:
        cache.close()


@pytest.mark.parametrize("mode", MODES[1:])
@pytest.mark.parametrize(
    "kind", ("field", "metadata", "read", "edit_read", "reload", "dirty_read")
)
def test_waiting_writer_does_not_hold_cache_lock_before_owners_first_write(
    metadata_db, mode, kind, monkeypatch
):
    """Exercise database/cache ordering with bounded failure instead of hanging pytest."""
    api, cache = service(metadata_db, mode)
    first = api.works.create({"title": "First"}).result
    second = api.works.create({"title": "Second"}).result
    entered, waiting = Event(), Event()
    competitor = []
    original_lock = metadata_db.lock
    if kind == "dirty_read":
        cache.invalidate(tables=("works",))

    class BoundedLock:
        def acquire(self, blocking=True, timeout=-1):
            if competitor and get_ident() == competitor[0]:
                waiting.set()
            return (
                original_lock.acquire(False)
                if not blocking
                else original_lock.acquire(timeout=30 if timeout < 0 else timeout)
            )

        def release(self):
            original_lock.release()

        def __enter__(self):
            if not self.acquire():
                raise RuntimeError("writer lock order deadlocked")
            return self

        def __exit__(self, *exc):
            self.release()

    def write(work, title):
        if kind == "field":
            return api.write_one("works", "work_title", work, title)
        return api.works.update(work, {"title": title})

    def owner():
        with metadata_db.macros.transaction():
            entered.set()
            assert waiting.wait(15)
            write(first, "Owner finished")

    def other():
        assert entered.wait(15)
        competitor.append(get_ident())
        if kind == "read":
            assert api.works.require(first).work.work_title == "Owner finished"
        elif kind == "edit_read":
            assert (
                api.works.read_for_edit(first).value.work.work_title == "Owner finished"
            )
        elif kind == "reload":
            cache.reload_data()
        elif kind == "dirty_read":
            assert cache.get("works", first).value["work_title"] == "Owner finished"
        else:
            write(second, "Competitor finished")

    try:
        with monkeypatch.context() as patched:
            patched.setattr(metadata_db, "lock", BoundedLock())
            with ThreadPoolExecutor(max_workers=2) as pool:
                owning = pool.submit(owner)
                competing = pool.submit(other)
                owning.result(timeout=60)
                competing.result(timeout=60)
        assert cache.get("works", first).value["work_title"] == "Owner finished"
        assert cache.get("works", second).value["work_title"] == (
            "Competitor finished" if kind in ("field", "metadata") else "Second"
        )
    finally:
        cache.close()
