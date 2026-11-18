"""Real persistence receipts and recovery for every cache writer route."""

import json
import sqlite3
from dataclasses import replace

import pytest

from LiuXin_alpha.caches.api import (
    CacheDirtyError,
    CacheQuery,
    CacheReconciliationError,
    CacheState,
)
from LiuXin_alpha.caches.cache import Cache
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.db_types.query_types import ValueMatch
from LiuXin_alpha.storage import api as storage_api
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)


@pytest.fixture(params=("SQLite", "SQLite_apsw"))
def route_database(request, tmp_path, monkeypatch):
    if request.param == "SQLite_apsw":
        pytest.importorskip("apsw")
    path = tmp_path / "routes.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        db_type=request.param,
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        db.driver_wrapper.executescript("""
            CREATE TABLE route_sources (
                route_source_id INTEGER PRIMARY KEY,
                route_source_name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE route_values (
                route_value_id INTEGER PRIMARY KEY,
                route_value_name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE route_volumes (
                route_volume_id INTEGER PRIMARY KEY,
                route_volume_path TEXT NOT NULL
            );
            CREATE TABLE route_source_route_value_links (
                route_source_route_value_link_route_source_id INTEGER NOT NULL,
                route_source_route_value_link_route_value_id INTEGER NOT NULL,
                UNIQUE(route_source_route_value_link_route_source_id,
                       route_source_route_value_link_route_value_id),
                FOREIGN KEY(route_source_route_value_link_route_source_id)
                    REFERENCES route_sources(route_source_id),
                FOREIGN KEY(route_source_route_value_link_route_value_id)
                    REFERENCES route_values(route_value_id)
            );
            CREATE TABLE route_source_route_volume_links (
                route_source_route_volume_link_route_source_id INTEGER NOT NULL UNIQUE,
                route_source_route_volume_link_route_volume_id INTEGER NOT NULL UNIQUE,
                UNIQUE(route_source_route_volume_link_route_source_id,
                       route_source_route_volume_link_route_volume_id),
                FOREIGN KEY(route_source_route_volume_link_route_source_id)
                    REFERENCES route_sources(route_source_id),
                FOREIGN KEY(route_source_route_volume_link_route_volume_id)
                    REFERENCES route_volumes(route_volume_id)
            );
            INSERT INTO route_sources VALUES (1, 'initial');
            INSERT INTO route_sources VALUES (2, 'second');
        """)
        db.driver.conn.commit()
        # Keep real persistence and backend execution, but capture only the
        # selected route families. Full catalogue captures have their own long lane.
        schema = db.driver_wrapper.get_schema_spec(force_refresh=True)
        if (
            request.node.originalname
            == "test_writer_commit_failure_rollback_and_external_write"
        ):
            selected = {"route_sources", "route_values", "route_volumes"}
        elif (
            request.node.originalname == "test_membership_commit_rollback_and_id_reuse"
        ):
            selected = {"route_sources"}
        elif (
            request.node.originalname
            == "test_storage_migration_bookkeeping_failure_still_invalidates"
        ):
            selected = {"digital_assets", "asset_replicas"}
        else:
            selected = {
                "digital_assets",
                "asset_replicas",
                "composite_digital_assets",
                "items",
            }
        interlinks = tuple(
            link
            for link in schema.interlinks
            if link.primary_table in selected and link.secondary_table in selected
        )
        intralinks = tuple(
            link
            for link in schema.intralinks
            if link.primary_table in selected and link.secondary_table in selected
        )
        selected.update(link.link_table for link in (*interlinks, *intralinks))
        scoped_schema = replace(
            schema,
            tables={
                name: table for name, table in schema.tables.items() if name in selected
            },
            interlinks=interlinks,
            intralinks=intralinks,
        )
        monkeypatch.setattr(
            db.driver_wrapper, "get_schema_spec", lambda **kwargs: scoped_schema
        )
        yield db, path


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
@pytest.mark.parametrize("route", ("column", "shared", "owned"))
def test_writer_commit_failure_rollback_and_external_write(
    route_database, storage_type, route, monkeypatch
):
    db, path = route_database
    column, destination = {
        "column": ("route_source_name", "route_sources"),
        "shared": ("route_value_name", "route_values"),
        "owned": ("route_volume_path", "route_volumes"),
    }[route]
    options = {"destination_owned": True} if route == "owned" else {}
    catalog = Catalog(db)
    cache = Cache(db, storage_type=storage_type)
    cache.load()
    writer = cache.create_writer("route_sources", column, **options)
    snapshot = cache.capabilities.consistency.value == "snapshot"

    def cached_value():
        if route == "column":
            return cache.get("route_sources", 1).value[column]
        records = cache.related("route_sources", (1,), destination).records
        assert len(records) == 1
        return records[0][column]

    def durable_value():
        with sqlite3.connect(path) as reader:
            if route == "column":
                return reader.execute(
                    "SELECT route_source_name FROM route_sources WHERE route_source_id=1"
                ).fetchone()[0]
            singular = "route_value" if route == "shared" else "route_volume"
            link = f"route_source_{singular}_links"
            prefix = f"route_source_{singular}_link"
            return reader.execute(
                f"SELECT {column} FROM {destination} JOIN {link} "
                f"ON {singular}_id={prefix}_{singular}_id "
                f"WHERE {prefix}_route_source_id=1"
            ).fetchone()[0]

    try:
        first = writer.write_one(1, "committed")
        assert first and durable_value() == cached_value() == "committed"
        if route == "column":
            generation = cache.generation
            with pytest.raises(Exception, match="(?i)unique"):
                writer.write({1: "duplicate", 2: "duplicate"})
            assert durable_value() == cached_value() == "committed"
            assert cache.get("route_sources", 2).value[column] == "second"
            assert cache.generation == generation
            assert cache.state == CacheState.READY
        db.driver.conn.commit()
        # An error after persistence must carry its receipt and keep stale data unavailable.
        if snapshot:
            reload_table = cache.storage.reload_main_table

            def fail_reload(*args, **kwargs):
                raise RuntimeError("refresh unavailable")

            with monkeypatch.context() as patch:
                patch.setattr(cache.storage, "reload_main_table", fail_reload)
                patch.setattr(cache.storage, "reload", fail_reload)
                if route == "shared":
                    # Value resolution owns an outer transaction: its receipt
                    # returns before the deferred cache refresh on the next read.
                    receipt = writer.write_one(1, "persisted despite refresh failure")
                    assert receipt
                    with pytest.raises(CacheDirtyError):
                        cached_value()
                else:
                    with pytest.raises(CacheReconciliationError) as caught:
                        writer.write_one(1, "persisted despite refresh failure")
                    assert caught.value.receipt
                assert durable_value() == "persisted despite refresh failure"
                assert cache.state == CacheState.DIRTY
                with pytest.raises(CacheDirtyError):
                    cached_value()
            assert cache.storage.reload_main_table == reload_table
            assert cached_value() == "persisted despite refresh failure"
        else:
            writer.write_one(1, "persisted despite refresh failure")
            assert cached_value() == "persisted despite refresh failure"
        db.driver.conn.commit()
        before = durable_value()
        with pytest.raises(RuntimeError, match="rollback sentinel"):
            with db.macros.transaction():
                writer.write_one(1, "rolled back")
                if snapshot:
                    assert cache.state == CacheState.DIRTY
                    with pytest.raises(CacheDirtyError):
                        cached_value()
                raise RuntimeError("rollback sentinel")
        assert durable_value() == before
        assert cached_value() == before
        db.driver.conn.commit()
        # Direct Catalog mutation has an explicit external invalidation owner.
        catalog.write_one("route_sources", column, 1, "external", **options)
        assert durable_value() == "external"
        if snapshot:
            assert cached_value() == before
        links = (
            ()
            if route == "column"
            else (("route_sources", destination), (destination, "route_sources"))
        )
        cache.invalidate(tables=(destination,), links=links)
        assert cached_value() == "external"
        assert cache.state == CacheState.READY
    finally:
        cache.close()


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_membership_commit_rollback_and_id_reuse(route_database, storage_type):
    """Real inserts/deletes join transactions; rolled-back rows never leak afterward."""
    db, path = route_database
    cache = Cache(db, storage_type=storage_type)
    snapshot = cache.capabilities.consistency.value == "snapshot"
    expected = {1: "initial", 2: "second"}

    def durable_rows():
        with sqlite3.connect(path) as reader:
            return dict(
                reader.execute(
                    "SELECT route_source_id, route_source_name FROM route_sources"
                )
            )

    def check():
        assert durable_rows() == expected
        result = cache.query(CacheQuery("route_sources"))
        assert result.ids == tuple(sorted(expected))
        assert result.total_count == len(expected)
        assert {
            row.row_id: row["route_source_name"] for row in result.records
        } == expected
        for name in ("initial", "second", "added", "reused", "phantom"):
            query = CacheQuery(
                "route_sources", where=ValueMatch("route_source_name", name)
            )
            ids = tuple(sorted(i for i, value in expected.items() if value == name))
            assert cache.query(query).ids == ids
            assert cache.query(query).ids == ids
        for row_id in (1, 2, 7, 900):
            value = cache.get("route_sources", row_id).value
            assert (value["route_source_name"] if value else None) == expected.get(
                row_id
            )
        return result

    def insert(row_id, name):
        assert (
            db.macros.insert_row(
                "route_sources",
                {
                    "route_source_id": row_id,
                    "route_source_name": name,
                },
            )
            == row_id
        )

    try:
        cache.load()
        original = check()
        for action in ("insert", "reuse", "delete", "reuse"):
            row_id = 7 if action == "insert" else 2
            for rollback in (True, False):
                db.driver.conn.commit()

                def transaction(action=action, row_id=row_id, rollback=rollback):
                    with db.macros.transaction():
                        if action in {"delete", "reuse"}:
                            db.macros.delete_row("route_sources", row_id)
                        if action != "delete":
                            insert(row_id, "added" if action == "insert" else "reused")
                        cache.invalidate(ids={"route_sources": (row_id,)})
                        assert durable_rows() == expected
                        if snapshot:
                            with pytest.raises(CacheDirtyError):
                                cache.query(CacheQuery("route_sources"))
                            with pytest.raises(CacheDirtyError):
                                cache.get("route_sources", row_id)
                        if rollback:
                            raise RuntimeError("membership rollback sentinel")

                if rollback:
                    with pytest.raises(
                        RuntimeError, match="membership rollback sentinel"
                    ):
                        transaction()
                else:
                    transaction()
                    if action == "delete":
                        del expected[row_id]
                    else:
                        expected[row_id] = "added" if action == "insert" else "reused"
                check()

        # A successful first insert followed by a constraint violation must roll
        # back the whole batch, even with invalidation already queued for that ID.
        db.driver.conn.commit()
        with pytest.raises(Exception, match="(?i)unique"):
            with db.macros.transaction():
                insert(900, "phantom")
                cache.invalidate(ids={"route_sources": (900,)})
                insert(901, "initial")
        check()
        db.driver.conn.commit()
        db.macros.delete_rows_by_id("route_sources", tuple(expected))
        cache.invalidate(ids={"route_sources": tuple(expected)})
        expected.clear()
        check()
        db.driver.conn.commit()
        insert(2, "reused")
        cache.invalidate(ids={"route_sources": (2,)})
        expected[2] = "reused"
        check()
        assert original.ids == (1, 2)
        assert tuple(row["route_source_name"] for row in original.records) == (
            "initial",
            "second",
        )
    finally:
        cache.close()


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_storage_repository_commit_rollback_delete_and_failure(
    route_database, storage_type, monkeypatch
):
    """Storage publishes affected IDs; serialized units read their own writes."""
    db, path = route_database
    cache = Cache(db, storage_type=storage_type)
    repository = DatabaseStorageMetadataRepository(db, cache=cache)
    direct = DatabaseStorageMetadataRepository(db)
    record = storage_api.DigitalAssetRecord(
        storage_api.DigitalAssetID(100),
        4,
        (storage_api.Digest("sha256", "a" * 64),),
        storage_api.DigitalAssetMetadata(name="initial"),
    )
    snapshot = cache.capabilities.consistency.value == "snapshot"

    def durable_name():
        with sqlite3.connect(path) as reader:
            row = reader.execute(
                "SELECT digital_asset_name FROM digital_assets WHERE digital_asset_id=100"
            ).fetchone()
            return row[0] if row else None

    def cached_name():
        return repository.get_asset(record.digital_asset_id).metadata.name

    def renamed(name):
        return replace(record, metadata=storage_api.DigitalAssetMetadata(name=name))

    def fail(*args, **kwargs):
        raise RuntimeError("storage invalidation unavailable")

    try:
        repository.upsert_asset(record)
        assert durable_name() == cached_name() == "initial"
        if snapshot:
            with monkeypatch.context() as patch:
                patch.setattr(cache.storage, "invalidate_ids", fail)
                with pytest.raises(RuntimeError, match="invalidation unavailable"):
                    repository.upsert_asset(renamed("persisted"))
                assert durable_name() == "persisted"
                assert cache.state == CacheState.DIRTY
                patch.setattr(cache.storage, "reload", fail)
                with pytest.raises(CacheDirtyError):
                    cached_name()
            assert cached_name() == "persisted"
        else:
            repository.upsert_asset(renamed("persisted"))
            assert cached_name() == "persisted"

        with pytest.raises(RuntimeError, match="rollback sentinel"):
            with repository.transaction():
                repository.upsert_asset(renamed("rolled back"))
                assert cached_name() == "rolled back"  # Authoritative transaction read.
                if snapshot:
                    with pytest.raises(CacheDirtyError):
                        cache.get("digital_assets", 100)
                raise RuntimeError("rollback sentinel")
        assert durable_name() == cached_name() == "persisted"

        direct.upsert_asset(renamed("external"))
        assert durable_name() == "external"
        if snapshot:
            assert cached_name() == "persisted"
        cache.invalidate(ids={"digital_assets": (100,)})
        assert cached_name() == "external"

        second_id = storage_api.DigitalAssetID(101)
        repository.upsert_asset(
            replace(
                record,
                digital_asset_id=second_id,
                digests=(storage_api.Digest("sha256", "b" * 64),),
            )
        )
        composite_id = storage_api.CompositeDigitalAssetID(100)
        original = storage_api.CompositeDigitalAssetRecord(
            composite_id,
            (storage_api.CompositeDigitalAssetMembership(record.digital_asset_id, 0),),
            name="collection",
        )
        changed = replace(
            original,
            members=(storage_api.CompositeDigitalAssetMembership(second_id, 0),),
        )
        links = (
            ("composite_digital_assets", "digital_assets"),
            ("digital_assets", "composite_digital_assets"),
        )

        def members():
            return cache.related(
                "composite_digital_assets", (100,), "digital_assets"
            ).ids

        def parents(asset_id):
            return cache.related(
                "digital_assets", (asset_id,), "composite_digital_assets"
            ).ids

        repository.upsert_composite(original)
        assert members() == (100,) and parents(100) == (100,)
        if snapshot:
            with monkeypatch.context() as patch:
                patch.setattr(cache.storage, "invalidate_link_table", fail)
                with pytest.raises(RuntimeError, match="invalidation unavailable"):
                    repository.upsert_composite(changed)
                with sqlite3.connect(path) as reader:
                    assert reader.execute(
                        "SELECT composite_digital_asset_digital_asset_link_digital_asset_id "
                        "FROM composite_digital_asset_digital_asset_links WHERE "
                        "composite_digital_asset_digital_asset_link_composite_digital_asset_id=100"
                    ).fetchall() == [(101,)]
                assert cache.state == CacheState.DIRTY
                patch.setattr(cache.storage, "reload", fail)
                with pytest.raises(CacheDirtyError):
                    members()
        else:
            repository.upsert_composite(changed)
        assert members() == (101,) and parents(100) == () and parents(101) == (100,)
        assert repository.get_composite(composite_id) == changed
        with pytest.raises(RuntimeError, match="rollback membership"):
            with repository.transaction():
                repository.upsert_composite(original)
                assert repository.get_composite(composite_id) == original
                raise RuntimeError("rollback membership")
        assert members() == (101,) and parents(100) == () and parents(101) == (100,)
        direct.upsert_composite(original)
        if snapshot:
            assert members() == (101,)
        cache.invalidate(ids={"composite_digital_assets": (100,)}, links=links)
        assert members() == (100,) and parents(100) == (100,) and parents(101) == ()

        item_id = storage_api.ItemID(
            Catalog(db).items.create({"inventory_code": "cache-route"})
        )
        cache.invalidate(ids={"items": (item_id,)})
        target_key = (item_id, "primary_payload")

        def item_targets(table):
            return cache.related("items", (item_id,), table).ids

        repository.upsert_item_target(
            (target_key, ("digital_asset", record.digital_asset_id))
        )
        assert item_targets("digital_assets") == (100,)
        assert item_targets("composite_digital_assets") == ()
        assert cache.related("digital_assets", (100,), "items").ids == (item_id,)
        repository.upsert_item_target((target_key, ("composite", composite_id)))
        assert item_targets("digital_assets") == ()
        assert cache.related("digital_assets", (100,), "items").ids == ()
        assert item_targets("composite_digital_assets") == (100,)
        assert cache.related("composite_digital_assets", (100,), "items").ids == (
            item_id,
        )
        with pytest.raises(RuntimeError, match="rollback target"):
            with repository.transaction():
                repository.upsert_item_target(
                    (target_key, ("digital_asset", record.digital_asset_id))
                )
                raise RuntimeError("rollback target")
        assert item_targets("digital_assets") == ()
        assert item_targets("composite_digital_assets") == (100,)
        delete_target = repository._delete_item_target

        def fail_after_deletion(*args):
            delete_target(*args)
            raise RuntimeError("target deletion interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(repository, "_delete_item_target", fail_after_deletion)
            with pytest.raises(RuntimeError, match="target deletion interrupted"):
                repository.remove_item_target(target_key)
        assert item_targets("composite_digital_assets") == (100,)
        with sqlite3.connect(path) as reader:
            assert (
                reader.execute(
                    "SELECT COUNT(*) FROM composite_digital_asset_item_links"
                ).fetchone()[0]
                == 1
            )
        repository.remove_item_target(target_key)
        assert (
            item_targets("digital_assets")
            == item_targets("composite_digital_assets")
            == ()
        )
        repository.remove_composite(composite_id)
        assert members() == parents(100) == ()

        with pytest.raises(RuntimeError, match="rollback deletion"):
            with repository.transaction():
                repository.remove_asset(record.digital_asset_id)
                with pytest.raises(KeyError):
                    repository.get_asset(record.digital_asset_id)
                raise RuntimeError("rollback deletion")
        assert durable_name() == cached_name() == "external"
        repository.remove_asset(record.digital_asset_id)
        assert durable_name() is None
        with pytest.raises(KeyError):
            repository.get_asset(record.digital_asset_id)
        assert cache.state == CacheState.READY
    finally:
        repository.set_cache(None)
        cache.close()


@pytest.mark.parametrize(
    "storage_type", ("schema_backed", "database_backed", "numpy_vectorized")
)
def test_storage_migration_bookkeeping_failure_still_invalidates(
    route_database, storage_type, monkeypatch
):
    """A failed migration receipt must not hide committed envelope rewrites."""
    from LiuXin_alpha.storage.utils import migrations

    db, path = route_database
    repository = DatabaseStorageMetadataRepository(db)
    record = storage_api.DigitalAssetRecord(
        storage_api.DigitalAssetID(100), 4, (storage_api.Digest("sha256", "a" * 64),)
    )
    repository.upsert_asset(record)
    row = db.macros.get_row("digital_assets", 100)
    envelope = json.loads(row["digital_asset_scratch"])
    legacy = {"format": envelope["format"], "version": 0, "record": envelope["payload"]}
    db.macros.update_row(
        "digital_assets", 100, {"digital_asset_scratch": json.dumps(legacy)}
    )
    cache = Cache(db, storage_type=storage_type)
    repository.set_cache(cache)

    def fail_recording(*args, **kwargs):
        raise RuntimeError("migration receipt unavailable")

    try:
        assert (
            json.loads(cache.get("digital_assets", 100).value["digital_asset_scratch"])[
                "version"
            ]
            == 0
        )
        monkeypatch.setattr(migrations, "record_envelope_migration", fail_recording)
        with pytest.raises(RuntimeError, match="migration receipt unavailable"):
            repository.migrate_envelopes()
        with sqlite3.connect(path) as reader:
            raw = reader.execute(
                "SELECT digital_asset_scratch FROM digital_assets WHERE digital_asset_id=100"
            ).fetchone()[0]
        assert json.loads(raw)["version"] == 1
        if cache.capabilities.consistency.value == "snapshot":
            assert cache.state == CacheState.DIRTY
        assert (
            json.loads(cache.get("digital_assets", 100).value["digital_asset_scratch"])[
                "version"
            ]
            == 1
        )
        assert repository.get_asset(record.digital_asset_id) == record
    finally:
        repository.set_cache(None)
        cache.close()
