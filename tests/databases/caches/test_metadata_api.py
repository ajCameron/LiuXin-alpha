"""Exercise the native metadata service with real persistence and cache backends."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from shutil import copyfile
from threading import Event

import pytest

from LiuXin_alpha.caches.api import (
    CacheClosedError,
    CacheDirtyError,
    CacheLookupStatus,
    CacheNotReadyError,
    CacheState,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.caches.cache import Cache, create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.catalog.api.common import CatalogMutationError, CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_access import (
    MetadataAPIInterface,
    MetadataReconciliationError,
)
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.transaction_participants import (
    after_commit,
    after_transaction,
)

MODES = (None, "schema_backed", "database_backed", "numpy_vectorized")


@pytest.fixture(scope="module")
def metadata_template(tmp_path_factory):
    path = tmp_path_factory.mktemp("metadata-template") / "empty.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ):
        pass
    return path


@pytest.fixture
def metadata_db(metadata_template, tmp_path, request):
    db_type = getattr(request, "param", "SQLite")
    if db_type == "SQLite_apsw":
        pytest.importorskip("apsw")
    path = tmp_path / "metadata.sqlite"
    copyfile(metadata_template, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type=db_type,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        yield db


def service(db, mode):
    cache = None if mode is None else create_cache(db, mode)
    api = (
        MetadataAPI(Catalog(db))
        if cache is None
        else CatalogCache(Catalog(db), cache=cache)
    )
    return api, cache


def stack(api):
    return api.mutations.create_wemi_stack(
        work={"title": "The Metadata Garden"},
        expression={"label": "Original text"},
        manifestation={"edition_statement": "First edition"},
        items=({"inventory_code": "COPY-1"},),
    )


def assert_pending_in_other_thread(api, cache, entity_id):
    """Exercise every publication route outside the owner's thread-local state."""

    def check():
        for operation in (
            lambda: api.works.require(entity_id),
            lambda: cache.get("works", entity_id),
            cache.reload,
            cache.reload_data,
            cache.catch_up,
            lambda: api.works.update(entity_id, {"title": "Must not write"}),
        ):
            with pytest.raises(CacheDirtyError):
                operation()

    # Unwind the owning SQL transaction before joining a failed worker, so a
    # regression cannot deadlock fixture teardown while waiting on the DB lock.
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(check)
    try:
        future.result(timeout=15)
    finally:
        executor.shutdown(wait=False)


@pytest.mark.parametrize("mode", MODES)
def test_metadata_interface_crud_search_and_detachment(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    consumer: MetadataAPIInterface = api
    receipt = stack(consumer)
    assert receipt.changed and receipt.persistence == "committed"
    assert receipt.cache == ("not_configured" if cache is None else "current")
    ids = receipt.result
    for level, entity_id, update in (
        ("work", ids.work_id, {"title": "New Garden"}),
        ("expression", ids.expression_id, {"label": "Revised text"}),
        (
            "manifestation",
            ids.manifestation_id,
            {"edition_statement": "Second edition"},
        ),
        ("item", ids.item_ids[0], {"inventory_code": "COPY-2"}),
    ):
        entities = getattr(consumer, level + "s")
        before = entities.require(entity_id).to_mapping()
        assert before[level][level + "_id"] == entity_id
        assert len(entities.list(limit=1)) == 1
        assert entities.list(limit=0) == entities.list(offset=100) == ()
        assert entities.get(999999) is None
        with pytest.raises(CatalogNotFoundError):
            entities.require(999999)
        assert entities.update(entity_id, update).result == entity_id
        assert entities.require(entity_id).to_mapping() != before
        assert not entities.update(entity_id, {}).changed
    assert [ref.entity_id for ref in consumer.search.find("work.title:Garden")] == [
        ids.work_id
    ]
    assert consumer.search.find("work.title:missing") == ()
    assert consumer.search.find("", limit=0) == ()
    value = consumer.works.require(ids.work_id)
    value.work.work_title = "Only in this value"
    value.expressions[0]["expression_label"] = "Detached relation"
    again = consumer.works.require(ids.work_id)
    assert again.work.work_title == "New Garden"
    assert again.expressions[0]["expression_label"] == "Revised text"
    extra = consumer.works.create({"title": "Disposable"})
    assert consumer.works.delete(extra.result).result == extra.result
    assert consumer.works.get(extra.result) is None
    # Deleting a linked Item must also remove it from upstream metadata/search.
    consumer.items.delete(ids.item_ids[0])
    assert consumer.manifestations.require(ids.manifestation_id).items == []
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_metadata_compound_replacement_and_relationships(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    ids = stack(api).result
    api.mutations.attach_metadata(
        level="work",
        entity_id=ids.work_id,
        data={
            "notes": ["Retained note"],
            "identifiers": [{"scheme": "isbn", "value": "9780306406157"}],
            "agents": [{"role": "aut", "data": {"name": "Garden Writer"}}],
        },
    )
    before = api.works.require(ids.work_id)
    assert before.notes and before.identifiers and before.agents
    api.mutations.replace_metadata(
        level="work",
        entity_id=ids.work_id,
        data={"identifiers": {}, "agents": [], "title": "Replacement"},
    )
    after = api.works.require(ids.work_id)
    assert after.notes
    assert not after.identifiers and not after.agents
    assert after.work.work_title == "Replacement"
    assert api.search.find("author:Writer") == ()
    second = api.expressions.create({"label": "Second text"}).result
    edge = dict(
        parent_level="work",
        parent_id=ids.work_id,
        child_level="expression",
        child_id=second,
    )
    linked = api.mutations.link_wemi(**edge, primary=True, priority=2, origin="test")
    assert linked.result["child_id"] == second
    assert api.works.require(ids.work_id).primary_expression_id == second
    assert api.mutations.unlink_wemi(**edge).changed
    assert not api.mutations.unlink_wemi(**edge).changed
    assert api.expressions.require(second)
    item_edge = dict(
        parent_level="manifestation",
        parent_id=ids.manifestation_id,
        child_level="item",
        child_id=ids.item_ids[0],
    )
    api.mutations.unlink_wemi(**item_edge)
    assert not api.manifestations.require(ids.manifestation_id).items
    api.mutations.link_wemi(**item_edge)
    assert api.manifestations.require(ids.manifestation_id).items
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("rollback", (False, True))
@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_metadata_outer_transaction_receipts_and_recovery(metadata_db, mode, rollback):
    api, cache = service(metadata_db, mode)
    original = api.works.create({"title": "Committed"}).result
    try:
        with metadata_db.macros.transaction():
            receipt = api.works.update(original, {"title": "Pending"})
            assert receipt.persistence == "pending"
            assert receipt.cache == ("not_configured" if cache is None else "deferred")
            if cache is not None:
                assert cache.state == CacheState.DIRTY
                with pytest.raises(CacheDirtyError):
                    api.works.require(original)
                with pytest.raises(CacheDirtyError):
                    cache.get("works", original)
                assert_pending_in_other_thread(api, cache, original)
            # A second mutation must still participate without reading a dirty snapshot.
            created = api.works.create({"title": "New pending"}).result
            if rollback:
                raise RuntimeError("rollback")
    except RuntimeError as error:
        assert rollback and str(error) == "rollback"
    assert receipt.persistence == "pending"  # immutable return-time evidence
    assert api.works.require(original).work.work_title == (
        "Committed" if rollback else "Pending"
    )
    assert (api.works.get(created) is None) == rollback
    if cache is not None:
        assert cache.state == CacheState.READY
        cache.close()


@pytest.mark.parametrize("mode", MODES[1:])
def test_metadata_nested_rollback_keeps_guard_until_outer_completion(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    entity_id = api.works.create({"title": "Before"}).result
    with metadata_db.macros.transaction():
        with pytest.raises(RuntimeError, match="inner rollback"):
            with metadata_db.macros.transaction():
                api.works.update(entity_id, {"title": "Rolled back"})
                raise RuntimeError("inner rollback")
        assert_pending_in_other_thread(api, cache, entity_id)
    assert api.works.require(entity_id).work.work_title == "Before"
    cache.close()


@pytest.mark.parametrize("close", (False, True))
def test_metadata_completion_does_not_reopen_cleared_or_closed_cache(
    metadata_db, close
):
    api, cache = service(metadata_db, "schema_backed")
    with metadata_db.macros.transaction():
        entity_id = api.works.create({"title": "Committed"}).result
        cache.close() if close else cache.clear()
        if not close:
            with pytest.raises(CacheDirtyError):
                cache.load()
    assert cache.state == (CacheState.CLOSED if close else CacheState.EMPTY)
    if not close:
        cache.load()
        assert api.works.require(entity_id).work.work_title == "Committed"
        cache.close()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("rollback", (False, True))
def test_metadata_completion_releases_all_and_preserves_sql_outcome(
    metadata_db, rollback
):
    events = []

    def failed_release():
        events.append("failed release")
        raise ValueError("release failure")

    with pytest.raises(RuntimeError if rollback else ExceptionGroup) as caught:
        with metadata_db.macros.transaction():
            entity_id = Catalog(metadata_db).works.create({"title": "SQL outcome"})
            after_transaction(metadata_db.macros, failed_release)
            with metadata_db.macros.transaction():
                after_transaction(metadata_db.macros, lambda: events.append("released"))
                after_commit(metadata_db.macros, lambda: events.append("published"))
            if rollback:
                raise RuntimeError("original rollback")
    if rollback:
        assert str(caught.value) == "original rollback"
        assert "release failure" in " ".join(caught.value.__notes__)
        assert events == ["failed release", "released"]
        assert Catalog(metadata_db).works.get(entity_id) is None
    else:
        assert events == ["failed release", "released", "published"]
        assert (
            Catalog(metadata_db).works.require(entity_id)["work_title"] == "SQL outcome"
        )


@pytest.mark.parametrize("mode", MODES)
def test_metadata_failed_compound_write_is_atomic(metadata_db, mode):
    api, cache = service(metadata_db, mode)
    generation = None if cache is None else cache.generation
    with pytest.raises(CatalogMutationError):
        api.mutations.create_wemi_stack(
            work={"title": "Must roll back"},
            expression={"label": "Original"},
            manifestation={"edition_statement": "First"},
            items=({"inventory_code": "one"}, {"unknown_field": "bad"}),
        )
    assert Catalog(metadata_db).works.list() == ()
    assert api.works.list() == ()
    if cache is not None:
        assert cache.generation == generation
        cache.close()


@pytest.mark.parametrize("mode", ("schema_backed", "numpy_vectorized"))
def test_metadata_committed_refresh_failure_preserves_receipt(
    metadata_db, mode, monkeypatch
):
    api, cache = service(metadata_db, mode)
    method = "reload_data" if hasattr(cache.storage, "reload_data") else "reload"
    original = getattr(cache.storage, method)

    def fail():
        raise OSError("refresh unavailable")

    monkeypatch.setattr(cache.storage, method, fail)
    # Catch-up re-reads the logged rows first and reloads in full only if that fails.
    original_ids = cache.storage.reload_ids
    monkeypatch.setattr(cache.storage, "reload_ids", lambda *_, **__: fail())
    with pytest.raises(MetadataReconciliationError) as caught:
        api.works.create({"title": "Saved once"})
    receipt = caught.value.receipt
    assert receipt.persistence == "committed" and receipt.cache == "dirty"
    assert cache.state == CacheState.DIRTY
    assert len(Catalog(metadata_db).works.list()) == 1
    with monkeypatch.context() as failed_recovery:
        failed_recovery.setattr(cache.storage, "reload", fail)
        with pytest.raises(CacheDirtyError):
            api.works.require(receipt.result)
        assert cache.state == CacheState.DIRTY
    monkeypatch.setattr(cache.storage, method, original)
    monkeypatch.setattr(cache.storage, "reload_ids", original_ids)
    assert api.works.require(receipt.result).work.work_title == "Saved once"
    assert cache.state == CacheState.READY
    cache.close()


def test_metadata_rejects_borrowed_or_unsupported_transactions(
    metadata_db, monkeypatch
):
    api = MetadataAPI(Catalog(metadata_db))
    connection = metadata_db.driver.conn
    with monkeypatch.context() as borrowed:
        borrowed.setattr(metadata_db.driver, "get_connection", None)
        connection.execute("BEGIN")
        try:
            with pytest.raises(RuntimeError, match="ownership|outer transaction"):
                api.works.create({"title": "No owned transaction"})
            with metadata_db.macros.transaction():
                with pytest.raises(RuntimeError, match="borrowed raw transaction"):
                    api.works.create({"title": "No observed completion"})
            assert connection.in_transaction
        finally:
            connection.rollback()
    with monkeypatch.context() as unsupported:
        unsupported.setattr(metadata_db.macros, "transaction", None)
        with pytest.raises(TypeError):
            api.works.create({"title": "No transaction capability"})
    assert api.works.list() == ()


@pytest.mark.parametrize("closing", (False, True))
def test_metadata_write_serializes_reads_and_close(metadata_db, monkeypatch, closing):
    catalog = Catalog(metadata_db)
    cache = create_cache(metadata_db)
    api = MetadataAPI(catalog, cache=cache)
    entered, release, observer_started, observed = (Event() for _ in range(4))
    create = catalog.works.create

    def hold(data):
        entered.set()
        assert release.wait(10)
        return create(data)

    def observe():
        observer_started.set()
        result = cache.close() if closing else api.works.list()
        observed.set()
        return result

    monkeypatch.setattr(catalog.works, "create", hold)
    with ThreadPoolExecutor(max_workers=2) as executor:
        writer = executor.submit(api.works.create, {"title": "One complete operation"})
        try:
            assert entered.wait(10)
            reader = executor.submit(observe)
            assert observer_started.wait(10)
            assert not observed.wait(0.05)
        finally:
            release.set()
        receipt = writer.result(timeout=30)
        result = reader.result(timeout=30)
    assert receipt.persistence == "committed"
    if closing:
        assert cache.state == CacheState.CLOSED
    else:
        assert result[0].work.work_title == "One complete operation"
        cache.close()


def test_metadata_partial_pages_and_search_never_succeed(metadata_db, monkeypatch):
    api, cache = service(metadata_db, "schema_backed")
    entity_id = api.works.create({"title": "A complete result"}).result
    query = cache.query

    def partial(plan):
        return replace(query(plan), complete=False)

    monkeypatch.setattr(cache, "query", partial)
    for read in (
        lambda: api.works.list(),
        lambda: api.search.find("work.title:complete"),
    ):
        with pytest.raises(UnsupportedCacheQueryError):
            read()
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )
    assert fallback.works.list()[0].work.work_id == entity_id
    assert fallback.search.find("work.title:complete")[0].entity_id == entity_id
    cache.close()


def test_metadata_dangling_relationship_is_a_read_failure(metadata_db, monkeypatch):
    api, cache = service(metadata_db, "schema_backed")
    ids = stack(api).result
    lookup = cache.get

    def dangling(table, entity_id):
        if table == "expressions" and entity_id == ids.expression_id:
            return replace(
                lookup(table, entity_id), status=CacheLookupStatus.MISS, value=None
            )
        return lookup(table, entity_id)

    monkeypatch.setattr(cache, "get", dangling)
    with pytest.raises(CatalogNotFoundError, match="Missing referenced expressions"):
        api.works.require(ids.work_id)
    monkeypatch.setattr(cache, "get", lookup)
    links = cache.link_records

    def malformed(*args, **kwargs):
        return tuple(replace(record, values={}) for record in links(*args, **kwargs))

    monkeypatch.setattr(cache, "link_records", malformed)
    for entities, entity_id in (
        (api.works, ids.work_id),
        (api.expressions, ids.expression_id),
        (api.manifestations, ids.manifestation_id),
        (api.items, ids.item_ids[0]),
    ):
        with pytest.raises(ValueError, match="no resolvable target identity"):
            entities.require(entity_id)
    cache.close()


def test_metadata_cache_binding_and_retained_services(metadata_db):
    cache = Cache(metadata_db)
    with pytest.raises(CacheNotReadyError):
        CatalogCache(Catalog(metadata_db), cache=cache)
    cache.load()
    api = CatalogCache(Catalog(metadata_db), cache=cache)
    assert api.cache is cache
    assert not isinstance(api, Cache)
    for metadata_method in ("metadata", "works", "create_writer", "write", "write_one"):
        assert not hasattr(cache, metadata_method)
    entities = api.works
    with pytest.raises(RuntimeError, match="another database"):
        MetadataAPI(Catalog(object()), cache=cache)
    original = cache.storage.db
    cache.storage.db = object()
    with pytest.raises(RuntimeError, match="another database"):
        entities.create({"title": "Must not persist"})
    cache.storage.db = original
    cache.close()
    with pytest.raises(CacheClosedError):
        entities.list()
    assert Catalog(metadata_db).works.list() == ()


def test_catalog_cache_requires_a_loaded_core(metadata_db):
    """Cached composition cannot silently become an uncached service."""
    with pytest.raises(TypeError, match="requires a loaded cache"):
        CatalogCache(Catalog(metadata_db), cache=None)


def test_metadata_unavailable_partial_and_failed_reads_are_distinct(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    entity_id = api.works.create({"title": "Known"}).result
    original = cache.get

    def unavailable(*args):
        raise UnknownCacheTableError("unavailable")

    monkeypatch.setattr(cache, "get", unavailable)
    with pytest.raises(UnknownCacheTableError):
        api.works.get(entity_id)
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )
    assert fallback.works.require(entity_id).work.work_title == "Known"

    def incomplete(*args):
        return replace(original(*args), complete=False)

    monkeypatch.setattr(cache, "get", incomplete)
    with pytest.raises(UnsupportedCacheQueryError):
        api.works.get(entity_id)
    assert fallback.works.require(entity_id)

    def broken(*args):
        raise OSError("damaged read")

    monkeypatch.setattr(cache, "get", broken)
    with pytest.raises(OSError, match="damaged"):
        fallback.works.get(entity_id)
    monkeypatch.setattr(cache, "get", original)
    # A successful root read must not hide a later relationship read failure.
    monkeypatch.setattr(cache, "link_records", broken)
    with pytest.raises(OSError, match="damaged"):
        fallback.works.require(entity_id)
    cache.close()


def test_metadata_invalid_inputs_and_known_miss_do_not_fallback(
    metadata_db, monkeypatch
):
    api, cache = service(metadata_db, "schema_backed")
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )

    def unexpected(*args, **kwargs):
        raise AssertionError("unexpected database row read")

    monkeypatch.setattr(metadata_db, "get_row_from_id", unexpected)
    assert fallback.works.get(999999) is None
    for invalid in (True, "1", -1):
        with pytest.raises((TypeError, ValueError)):
            api.works.get(invalid)
    for arguments in ({"limit": -1}, {"offset": -1}, {"limit": True}):
        with pytest.raises((TypeError, ValueError)):
            api.works.list(**arguments)
        with pytest.raises((TypeError, ValueError)):
            api.search.find("", **arguments)
    for method in (api.mutations.attach_metadata, api.mutations.replace_metadata):
        with pytest.raises(CatalogNotFoundError):
            method(level="work", entity_id=999999, data={})
    cache.close()
