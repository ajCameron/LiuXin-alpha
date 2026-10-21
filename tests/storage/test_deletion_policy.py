"""Enforce Replica loss policy at both manager byte-deletion entry points."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import TransientStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


@pytest.fixture
def managed_stores(tmp_path: Path):
    """Use real disposable bytes with one metadata owner and two independent Stores."""
    source = FilesystemStore(tmp_path / "source", name="source")
    # This fixture explicitly supports borrowed copies so the refusal reaches deletion.
    source = FilesystemStore(
        tmp_path / "source",
        configuration=dataclasses.replace(
            source.configuration, supported_replica_modes=frozenset(api.ReplicaMode)
        ),
    )
    destination = FilesystemStore(tmp_path / "destination", name="destination")
    with TransientStorageManager(
        store_registrations=(
            (source.configuration, source),
            (destination.configuration, destination),
        )
    ) as manager:
        yield manager, source, destination


def _replica(manager, source, *, mode=api.ReplicaMode.ACTIVE):
    """Ingest and verify a concrete copy before testing its destruction boundary."""
    asset = manager.store_bytes(b"preserve this book", store=source, replica_mode=mode)
    replica = next(
        manager.iter_replica_records(digital_asset_id=asset.digital_asset_id)
    )
    assert replica.observation.state is api.ReplicaState.VERIFIED
    return asset, replica


def _delete(manager, replica, route):
    """Exercise the two public manager entry points that currently reach byte removal."""
    if route == "replica":
        return manager.remove_replica(replica.replica_id)
    return manager.delete(replica.location)


def _bytes(manager, replica):
    """Read physical bytes independently of the retained Replica observation."""
    with manager.get(replica.location) as source:
        return source.read()


@pytest.mark.parametrize("route", ["replica", "router"])
def test_last_verified_copy_is_refused_without_effects(managed_stores, route):
    """Neither public entry point may destroy an Asset's last verified copy."""
    manager, source, _ = managed_stores
    _, replica = _replica(manager, source)
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoragePolicyUnsatisfied):
        _delete(manager, replica, route)
    assert tuple(manager.iter_replica_records()) == before
    assert _bytes(manager, replica) == b"preserve this book"


@pytest.mark.parametrize("route", ["replica", "router"])
@pytest.mark.parametrize(
    "mode", [api.ReplicaMode.UNMANAGED, api.ReplicaMode.ARCHIVE, api.ReplicaMode.BACKUP]
)
def test_protected_mode_is_refused_even_with_another_copy(managed_stores, route, mode):
    """Another verified copy does not authorize deleting protected or borrowed bytes."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source, mode=mode)
    other = manager.replicate_digital_asset(
        asset.digital_asset_id,
        source_replica_id=replica.replica_id,
        destination_store_ref=destination.store_ref,
        verify=True,
    )
    assert other.observation.state is api.ReplicaState.VERIFIED
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoragePolicyUnsatisfied):
        _delete(manager, replica, route)
    assert tuple(manager.iter_replica_records()) == before
    assert _bytes(manager, replica) == _bytes(manager, other) == b"preserve this book"


@pytest.mark.parametrize("route", ["replica", "router"])
def test_redundant_active_copy_can_be_removed_honestly(managed_stores, route):
    """An allowed deletion leaves one readable copy and no verified claim for erased bytes."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    other = manager.replicate_digital_asset(
        asset.digital_asset_id,
        destination_store_ref=destination.store_ref,
        verify=True,
    )
    _delete(manager, replica, route)
    with pytest.raises(api.StoreNotFound):
        manager.stat(replica.location)
    assert _bytes(manager, other) == b"preserve this book"
    assert (
        manager.get_replica_record(replica.replica_id).observation.state
        is api.ReplicaState.DELETED
    )


def test_metadata_only_removal_preserves_last_copy_bytes(managed_stores):
    """Explicitly forgetting a claim does not imply permission to delete its bytes."""
    manager, source, _ = managed_stores
    _, replica = _replica(manager, source)
    report = manager.remove_replica(
        replica.replica_id, delete_bytes=False, retain_tombstone=False
    )
    assert report.replica_forgotten
    assert not report.bytes_deleted
    assert _bytes(manager, replica) == b"preserve this book"


@pytest.mark.parametrize("route", ["replica", "router"])
def test_stale_verified_survivor_cannot_authorize_loss(managed_stores, route):
    """A recorded VERIFIED claim is insufficient when its current bytes are corrupt."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    other = manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    destination.write_bytes(other.location, b"corrupt", mode=api.WriteMode.REPLACE)
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoragePolicyUnsatisfied):
        _delete(manager, replica, route)
    assert tuple(manager.iter_replica_records()) == before
    assert _bytes(manager, replica) == b"preserve this book"


def test_configured_minimum_is_enforced_even_with_a_survivor(managed_stores):
    """Two-copy retention is stricter than the unconditional last-copy guard."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    policy = manager.create_replication_policy(api.ReplicationPolicy(min_copies=2))
    manager.set_digital_asset_replication_policy(
        asset.digital_asset_id, policy.replication_policy_id
    )
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoragePolicyUnsatisfied, match="minima"):
        manager.remove_replica(replica.replica_id)
    assert tuple(manager.iter_replica_records()) == before


def test_unregistered_bytes_require_privileged_store_access(managed_stores):
    """Routing an arbitrary Location is not authority to bypass the metadata policy."""
    manager, source, _ = managed_stores
    location = source.location("unregistered")
    source.write_bytes(location, b"unregistered")
    with pytest.raises(api.StoragePolicyUnsatisfied, match="live Replica"):
        manager.delete(location)
    assert source.read_bytes(location) == b"unregistered"


@pytest.mark.parametrize("erased", [False, True])
def test_backend_failure_preserves_pending_claim_and_unknown_outcome(
    managed_stores, monkeypatch, erased
):
    """A lost acknowledgement cannot be reported as rollback or certain byte survival."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    other = manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    original = source.delete

    def fail(location, **kwargs):
        """Optionally erase bytes before simulating a failed acknowledgement."""
        if erased:
            original(location, **kwargs)
        raise OSError("lost acknowledgement")

    monkeypatch.setattr(source, "delete", fail)
    with pytest.raises(api.StorageDeletionError) as caught:
        manager.remove_replica(replica.replica_id)
    recovery = caught.value.recovery
    pending = manager.get_replica_record(replica.replica_id)
    assert pending.state is api.ReplicaState.DELETING
    assert recovery.phase == "bytes" and recovery.bytes_deleted is None
    assert recovery.reservations == ((pending.replica_id, pending.revision),)
    assert isinstance(caught.value.__cause__, OSError)
    with pytest.raises(api.StoragePolicyUnsatisfied):
        manager.remove_replica(other.replica_id)
    with pytest.raises(api.StoragePreconditionFailed, match="pending deletion"):
        manager.verify_replica(replica.replica_id)
    with pytest.raises(api.StoragePreconditionFailed, match="pending deletion"):
        manager.forget_replica(replica.replica_id, require_bytes_absent=False)
    assert _bytes(manager, other) == b"preserve this book"
    if erased:
        with pytest.raises(api.StoreNotFound):
            source.stat(replica.location)
        plan = manager.plan_reconciliation(source.store_ref)
        with pytest.raises(api.StoreReconciliationPlanStale, match="pending deletion"):
            manager.apply_reconciliation(plan)
        assert manager.get_replica_record(replica.replica_id) == pending
    else:
        assert _bytes(manager, replica) == b"preserve this book"


def test_final_metadata_failure_reports_known_byte_effect(managed_stores, monkeypatch):
    """Failure after successful deletion retains recoverable evidence instead of VERIFIED."""
    from contextlib import contextmanager

    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    original = manager._deletion_transaction
    phases = 0

    @contextmanager
    def fail_final_phase():
        """Interrupt final metadata entry, after reservation and physical deletion."""
        nonlocal phases
        phases += 1
        if phases == 2:
            raise OSError("metadata unavailable")
        with original():
            yield

    monkeypatch.setattr(manager, "_deletion_transaction", fail_final_phase)
    with pytest.raises(api.StorageDeletionError) as caught:
        manager.delete(replica.location)
    assert caught.value.recovery.phase == "metadata"
    assert caught.value.recovery.bytes_deleted is True
    with pytest.raises(api.StoreNotFound):
        source.stat(replica.location)
    assert (
        manager.get_replica_record(replica.replica_id).state
        is api.ReplicaState.DELETING
    )


def test_competing_threads_cannot_remove_both_copies(managed_stores, monkeypatch):
    """The reservation blocks the competing decision while the first byte call is paused."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    other = manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    entered, release = Event(), Event()
    original = source.delete

    def pause(location, **kwargs):
        """Hold physical removal after the first operation's reservation."""
        entered.set()
        assert release.wait(10)
        original(location, **kwargs)

    monkeypatch.setattr(source, "delete", pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(manager.remove_replica, replica.replica_id)
        try:
            assert entered.wait(10)
            with pytest.raises(api.StoragePolicyUnsatisfied):
                manager.remove_replica(other.replica_id)
        finally:
            release.set()
        assert first.result(timeout=10).bytes_deleted
    assert _bytes(manager, other) == b"preserve this book"


@pytest.mark.parametrize("route", ["replica", "router"])
def test_manager_read_only_policy_precedes_metadata_reservation(managed_stores, route):
    """A writable physical facade cannot bypass the manager's read-only configuration."""
    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    manager.attach_store(
        dataclasses.replace(source.configuration, read_only=True),
        source,
        startup=False,
        replace_existing=True,
    )
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoreReadOnly):
        _delete(manager, replica, route)
    assert tuple(manager.iter_replica_records()) == before
    assert _bytes(manager, replica) == b"preserve this book"


def test_two_survivors_on_one_host_do_not_satisfy_two_host_policy(
    managed_stores, tmp_path
):
    """Deletion uses joint separation capacity, rather than just counting other claims."""
    from uuid import UUID

    manager, source, destination = managed_stores
    for host, store in ((UUID(int=1), source), (UUID(int=2), destination)):
        manager.attach_store(
            dataclasses.replace(store.configuration, store_host_uuid=host),
            store,
            startup=False,
            replace_existing=True,
        )
    third = FilesystemStore(tmp_path / "third")
    manager.attach_store(
        dataclasses.replace(third.configuration, store_host_uuid=UUID(int=2)), third
    )
    asset, replica = _replica(manager, source)
    for store in (destination, third):
        manager.replicate_digital_asset(
            asset.digital_asset_id, destination_store_ref=store.store_ref
        )
    policy = manager.create_replication_policy(
        api.ReplicationPolicy(
            min_copies=2, distinct_by=(api.ReplicaSeparationDimension.HOST,)
        )
    )
    manager.set_digital_asset_replication_policy(
        asset.digital_asset_id, policy.replication_policy_id
    )
    before = tuple(manager.iter_replica_records())
    with pytest.raises(api.StoragePolicyUnsatisfied, match="separation"):
        manager.remove_replica(replica.replica_id)
    assert tuple(manager.iter_replica_records()) == before
    assert _bytes(manager, replica) == b"preserve this book"


def test_late_verification_cannot_revive_a_completed_deletion(
    managed_stores, monkeypatch
):
    """Evidence captured before removal cannot replace the final tombstone."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    manager, source, destination = managed_stores
    asset, replica = _replica(manager, source)
    manager.replicate_digital_asset(
        asset.digital_asset_id, destination_store_ref=destination.store_ref
    )
    inspected, release = Event(), Event()
    original = manager._inspect_replica

    def pause_after_inspection(record, asset_record, **kwargs):
        """Return an old successful report after the main thread removes its bytes."""
        report = original(record, asset_record, **kwargs)
        if record.replica_id == replica.replica_id:
            assert report.healthy
            inspected.set()
            assert release.wait(10)
        return report

    monkeypatch.setattr(manager, "_inspect_replica", pause_after_inspection)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(manager.verify_replica, replica.replica_id)
        try:
            assert inspected.wait(10)
            manager.remove_replica(replica.replica_id)
        finally:
            release.set()
        with pytest.raises(api.StoragePreconditionFailed, match="deleted"):
            future.result(timeout=10)
    assert (
        manager.get_replica_record(replica.replica_id).state is api.ReplicaState.DELETED
    )
    with pytest.raises(api.StoreNotFound):
        manager.stat(replica.location)
