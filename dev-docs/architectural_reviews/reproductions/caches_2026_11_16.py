"""Manual review probes; five cases demonstrate open defects at 2d2a9cc3.

Run explicitly as documented in the adjacent cache review. These probes are
outside normal test discovery; convert them to regressions when repairing CR01–03.
"""
import pytest

from LiuXin_alpha.caches.api import CacheDirtyError, CacheNotReadyError, CacheQuery
from tests.databases.caches.test_modern_cache_facade import modern_cache
from tests.databases.caches.test_native_custom_fields import projection, native, db, template


def test_reentrant_receipt_cannot_overwrite_newer_projection(projection):
    service = projection.service
    nested = []
    entered = False

    def listener(receipt):
        nonlocal entered
        if entered:
            return
        entered = True
        with projection.write():
            nested.append(service.create_field("works", "from_listener"))

    unsubscribe = service.subscribe(listener)
    try:
        with projection.write():
            first = service.create_field("works", "outer")
    finally:
        unsubscribe()
    durable = {field.key for field in service.list_fields()}
    visible = {key for key in durable if key in projection.field_metadata}
    print(f"durable={sorted(durable)} visible={sorted(visible)}")
    assert first.key in visible
    assert nested[0].key in visible


def test_failed_reload_data_does_not_leave_mixed_ready_generation(modern_cache, monkeypatch):
    cache, database = modern_cache
    if type(cache.storage).__name__ != "SchemaBackedStorageCache":
        pytest.skip("schema-backed fast data refresh")
    alpha = CacheQuery(table="books", text="alpha", text_fields=("title",))
    assert cache.query(alpha).ids == (2,)
    before = cache.generation
    database.driver_wrapper._rows_by_table["books"][1]["title"] = "Gamma"
    database.driver_wrapper._rows_by_table["tags"][0]["name"] = "Changed tag"

    def fail(*args, **kwargs):
        raise RuntimeError("late table read failed")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage.main_tables["tags"], "read", fail)
        with pytest.raises(RuntimeError, match="late table"):
            cache.reload_data()
    try:
        title = cache.get("books", 2).value.values["title"]
        tag = cache.get("tags", 10).value.values["name"]
        old_query = cache.query(alpha).ids
    except (CacheDirtyError, CacheNotReadyError):
        return  # Explicitly unavailable is safer than serving mixed state.
    print(f"state={cache.state} generation={cache.generation}/{before} title={title!r} tag={tag!r} alpha={old_query}")
    observed = (title, tag, old_query)
    assert observed == ("Alpha", "Fiction", (2,)) or (
        observed == ("Gamma", "Changed tag", ()) and cache.generation > before
    )


def test_partial_invalidation_does_not_leave_query_index_stale(modern_cache):
    cache, database = modern_cache
    if type(cache.storage).__name__ == "DatabaseBackedStorageCache":
        pytest.skip("snapshot invalidation")
    alpha = CacheQuery(table="books", text="alpha", text_fields=("title",))
    gamma = CacheQuery(table="books", text="gamma", text_fields=("title",))
    assert cache.query(alpha).ids == (2,)
    before = cache.generation
    database.driver_wrapper._rows_by_table["books"][1]["title"] = "Gamma"
    with pytest.raises(KeyError):
        cache.invalidate(tables=("books",), fields=("books.not_a_field",))
    try:
        title = cache.get("books", 2).value.values["title"]
        new_query = cache.query(gamma).ids
    except (CacheDirtyError, CacheNotReadyError):
        return
    print(f"state={cache.state} generation={cache.generation}/{before} title={title!r} gamma={new_query}")
    assert (title, new_query) == ("Alpha", ()) or (
        (title, new_query) == ("Gamma", (2,)) and cache.generation > before
    )
