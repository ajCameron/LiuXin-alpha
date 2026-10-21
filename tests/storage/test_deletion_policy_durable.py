"""Exercise committed deletion reservations across separate SQLite/PostgreSQL owners."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from threading import Event
from uuid import uuid4

import pytest

from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


@pytest.fixture(params=["SQLite", "PostgreSQL"])
def durable_owners(tmp_path, request):
    """Bind independent facade/macro/manager objects to one disposable catalogue."""
    backend = request.param
    metadata = {"database_path": str(tmp_path / "catalog.sqlite")}
    if backend == "PostgreSQL":
        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
        metadata = {"postgres_url": url, "schema": f"liuxin_deletion_{uuid4().hex}"}
    try:
        with ExitStack() as stack:
            first_db = stack.enter_context(
                Database(
                    metadata=metadata,
                    db_type=backend,
                    create=True,
                    backup=False,
                    enable_storage_manager=False,
                    enable_maintenance=False,
                )
            )
            source = FilesystemStore(tmp_path / "source")
            destination = FilesystemStore(tmp_path / "destination")
            first = stack.enter_context(
                StorageManager(db=first_db, stores=(source, destination))
            )
            ingested = first.ingest_bytes(
                b"preserve the only survivor", preferred_store_ref=source.store_ref
            )
            replica = ingested.replica_record
            other = first.replicate_digital_asset(
                replica.digital_asset_id, destination_store_ref=destination.store_ref
            )
            second_db = stack.enter_context(
                Database(
                    metadata=metadata,
                    db_type=backend,
                    create=False,
                    backup=False,
                    enable_storage_manager=False,
                    enable_maintenance=False,
                )
            )
            second, report = StorageManager.from_database(second_db)
            stack.enter_context(second)
            yield first, second, replica, other
    finally:
        if backend == "PostgreSQL":
            from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
                connect_postgres,
            )

            with closing(connect_postgres(metadata, prompt_for_password=False)) as conn:
                with conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                        )


def test_independent_cached_owner_cannot_rely_on_reserved_copy(
    durable_owners, monkeypatch
):
    """A stale per-facade cache cannot defeat the committed shared reservation."""
    first, second, replica, other = durable_owners
    from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec

    # Exercise the real snapshot cache for the two policy decision tables.
    # Full PostgreSQL schema discovery has a separate long-identifier defect;
    # this test deliberately does not certify every catalogue relationship.
    wrapper = second.db.driver_wrapper
    schema = StorageSchemaSpec(
        tables={
            name: wrapper.get_table_spec(name)
            for name in ("digital_assets", "asset_replicas")
        },
        interlinks=(),
        intralinks=(),
    )
    monkeypatch.setattr(wrapper, "get_schema_spec", lambda **_kwargs: schema)
    cache = create_cache(second.db, "schema_backed")
    second.bind_metadata_cache(cache)
    assert (
        second.get_replica_record(replica.replica_id).state is api.ReplicaState.VERIFIED
    )
    entered, release = Event(), Event()
    source = first.get_store(replica.location.store_ref)
    original = source.delete

    def pause(location, **kwargs):
        """Pause outside SQL after the first owner's reservation has committed."""
        entered.set()
        assert release.wait(20)
        original(location, **kwargs)

    monkeypatch.setattr(source, "delete", pause)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(first.remove_replica, replica.replica_id)
            try:
                assert entered.wait(20)
                with pytest.raises(api.StoragePolicyUnsatisfied):
                    second.remove_replica(other.replica_id)
                with pytest.raises(
                    api.StoragePreconditionFailed, match="pending deletion"
                ):
                    second.verify_replica(replica.replica_id)
            finally:
                release.set()
            assert future.result(timeout=20).bytes_deleted
        assert second.read_bytes(other.location) == b"preserve the only survivor"
    finally:
        second.bind_metadata_cache(None)
        cache.close()


def test_unknown_byte_outcome_survives_manager_reconstruction(
    durable_owners, monkeypatch
):
    """A new manager reads the durable pending state and refuses loss of the survivor."""
    first, second, replica, other = durable_owners
    source = first.get_store(replica.location.store_ref)
    original = source.delete

    def lost_acknowledgement(location, **kwargs):
        """Erase the object and fail before its caller receives acknowledgement."""
        original(location, **kwargs)
        raise OSError("acknowledgement lost")

    monkeypatch.setattr(source, "delete", lost_acknowledgement)
    with pytest.raises(api.StorageDeletionError) as caught:
        first.remove_replica(replica.replica_id)
    reopened, _report = StorageManager.from_database(second.db)
    with reopened:
        pending = reopened.get_replica_record(replica.replica_id)
        assert pending.state is api.ReplicaState.DELETING
        assert any(
            issue.replica_id == pending.replica_id
            for issue in reopened.get_operational_status().issues_for(
                "replica_deletion_pending"
            )
        )
        assert caught.value.recovery.reservations == (
            (pending.replica_id, pending.revision),
        )
        with pytest.raises(api.StoreNotFound):
            reopened.stat(replica.location)
        with pytest.raises(api.StoragePolicyUnsatisfied):
            reopened.delete(other.location)


@pytest.mark.parametrize("serialized", [False, True])
def test_deletion_refuses_caller_owned_transaction(durable_owners, serialized):
    """An unreleased outer transaction cannot hide reservation intent from peers."""
    first, second, replica, _other = durable_owners
    with first.db.macros.transaction(serialized=serialized):
        with pytest.raises(RuntimeError, match="outermost"):
            first.remove_replica(replica.replica_id)
    assert (
        second.get_replica_record(replica.replica_id).state is api.ReplicaState.VERIFIED
    )
    assert second.read_bytes(replica.location) == b"preserve the only survivor"


def test_failed_reservation_rolls_back_before_external_effects(
    durable_owners, monkeypatch
):
    """A reservation commit failure cannot launch the byte-deletion phase."""
    first, second, replica, _other = durable_owners
    original = first.metadata_unit_of_work_factory.repository.transaction

    @contextmanager
    def abort_commit(**kwargs):
        """Fail after metadata writes but before the actual database commit."""
        with original(**kwargs) as conn:
            yield conn
            raise OSError("commit interrupted")

    monkeypatch.setattr(
        first.metadata_unit_of_work_factory.repository, "transaction", abort_commit
    )
    with pytest.raises(api.StorageDeletionError) as caught:
        first.remove_replica(replica.replica_id)
    assert caught.value.recovery.phase == "reservation"
    assert caught.value.recovery.bytes_deleted is False
    assert (
        second.get_replica_record(replica.replica_id).state is api.ReplicaState.VERIFIED
    )
    assert second.read_bytes(replica.location) == b"preserve the only survivor"


@pytest.mark.parametrize("route", ["core", "cli"])
@pytest.mark.parametrize("failure", ["protected", "lost_acknowledgement"])
def test_core_and_cli_preserve_manager_refusals_and_recovery(
    durable_owners, monkeypatch, capsys, route, failure
):
    """Use the real Core endpoint and CLI handler over the same manager policy owner."""
    import argparse
    import json
    from contextlib import nullcontext

    from LiuXin_alpha.core import CoreRuntime
    from LiuXin_alpha.library import Library
    from LiuXin_alpha.surfaces.cli.storage_commands import administration

    first, _second, replica, other = durable_owners
    source = first.get_store(replica.location.store_ref)
    destination = first.get_store(other.location.store_ref)
    if failure == "protected":
        replica = first.ingest_bytes(
            b"protected archive",
            preferred_store_ref=source.store_ref,
            replica_mode=api.ReplicaMode.ARCHIVE,
        ).replica_record
        first.replicate_digital_asset(
            replica.digital_asset_id,
            destination_store_ref=destination.store_ref,
            mode=api.ReplicaMode.ARCHIVE,
            source_replica_id=replica.replica_id,
        )
    else:
        original = source.delete

        def lost_acknowledgement(location, **kwargs):
            """Fail after physical deletion so a truthful recovery receipt is required."""
            original(location, **kwargs)
            raise OSError("acknowledgement lost")

        monkeypatch.setattr(source, "delete", lost_acknowledgement)
    with CoreRuntime(library=Library(database=first.db, storage=first)) as runtime:
        if route == "core":
            result = runtime.command(
                "storage.store.evacuate.apply",
                {
                    "store": source.configuration.store_name,
                    "destination_store": destination.configuration.store_name,
                },
            )
        else:
            monkeypatch.setattr(
                administration,
                "open_cli_core",
                lambda _args, **_kwargs: nullcontext(runtime),
            )
            capsys.readouterr()
            code = administration.cmd_storage_store_evacuate(
                argparse.Namespace(
                    store=source.configuration.store_name,
                    destination_store=destination.configuration.store_name,
                    max_assets=100,
                    max_actions=1000,
                    max_transfer_gib=1,
                    keep_source_bytes=False,
                    yes=True,
                )
            )
            result = json.loads(capsys.readouterr().out)
            assert code == 1
        assert result["ok"] is False
        action = next(
            value
            for value in result["actions"]
            if value.get("replica_id") == replica.replica_id
            and value["action"] == "remove_source_replica"
        )
        assert action["ok"] is False
        if failure == "protected":
            assert "protects archive" in action["error"]
            assert first.read_bytes(replica.location) == b"protected archive"
            assert first.get_replica_record(replica.replica_id) == replica
        else:
            assert action["recovery"]["phase"] == "bytes"
            assert action["recovery"]["bytes_deleted"] is None
            assert action["recovery"]["reservations"][0][0] == replica.replica_id
