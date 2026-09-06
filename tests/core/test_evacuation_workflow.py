"""Phase and safety contracts for the extracted evacuation workflow."""

from dataclasses import replace
from unittest.mock import create_autospec
from uuid import UUID

import pytest

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.evacuation_execution import execute_evacuation
from LiuXin_alpha.core.program_services.evacuation_models import EvacuationLimits
from LiuXin_alpha.core.program_services.evacuation_planning import build_evacuation_plan
from LiuXin_alpha.storage import api


@pytest.fixture
def evacuation():
    source = api.StoreConfiguration(
        UUID(int=1), "source", "filesystem", "file:///source"
    )
    destination = api.StoreConfiguration(
        UUID(int=2), "destination", "filesystem", "file:///destination"
    )
    asset = api.DigitalAssetRecord(api.DigitalAssetID(1), 4, (api.Digest("sha256", "aa"),))
    record = api.ReplicaRecord(
        api.ReplicaID(1),
        asset.digital_asset_id,
        api.Location(source.store_uuid, "book.epub"),
        api.ReplicaMode.ACTIVE,
        api.ReplicaObservation(api.ReplicaState.VERIFIED),
    )
    records = {record.replica_id: record}
    configurations = {value.store_uuid: value for value in (source, destination)}
    manager = create_autospec(api.StorageManagerAPI, instance=True)
    manager.get_default_store_ref.return_value = source.store_uuid
    manager.get_store_configuration.side_effect = configurations.__getitem__
    manager.iter_store_configurations.side_effect = lambda: iter(
        configurations.values()
    )
    manager.get_store.return_value.status.return_value = api.StoreStatus(True, True)
    manager.get_digital_asset_record.return_value = asset
    manager.get_replica_record.side_effect = records.__getitem__
    manager.resolve_effective_policies.return_value = api.ResolvedStoragePolicies(
        api.ReplicationPolicy(), api.BackupPolicy(), "default", "default"
    )

    def replicas(*, digital_asset_id=None, store_ref=None, mode=None, **_filters):
        return iter(
            tuple(
                value
                for value in records.values()
                if (
                    digital_asset_id is None
                    or value.digital_asset_id == digital_asset_id
                )
                and (store_ref is None or value.location.store_ref == store_ref)
                and (mode is None or value.mode == mode)
            )
        )

    manager.iter_replica_records.side_effect = replicas

    def replicate(asset_id, *, destination_store_ref, source_replica_id, mode, verify):
        assert verify is True
        assert source_replica_id == record.replica_id
        copied = replace(
            record,
            replica_id=api.ReplicaID(2),
            location=api.Location(destination_store_ref, "copy.epub"),
            mode=mode,
        )
        records[copied.replica_id] = copied
        return copied

    manager.replicate_digital_asset.side_effect = replicate
    manager.remove_replica.return_value = None
    plan = build_evacuation_plan(
        manager,
        source_ref=source.store_uuid,
        destination_ref=destination.store_uuid,
        max_assets=10,
    )
    return manager, plan, records, configurations


@pytest.mark.parametrize("limits", [EvacuationLimits(1, 100), EvacuationLimits(10, 3)])
def test_limits_refuse_an_entry_before_any_transfer_or_removal(evacuation, limits):
    manager, plan, _records, _configurations = evacuation
    result = execute_evacuation(manager, plan, limits, keep_source_bytes=False)
    assert result.truncated
    assert result.actions == []
    assert result.transferred_bytes == 0
    manager.replicate_digital_asset.assert_not_called()
    manager.remove_replica.assert_not_called()


def test_exact_limits_allow_copy_then_source_removal(evacuation):
    manager, plan, _records, _configurations = evacuation
    result = execute_evacuation(
        manager, plan, EvacuationLimits(2, 4), keep_source_bytes=False
    )
    assert not result.truncated
    assert result.failures == 0
    assert result.transferred_bytes == 4
    assert [action["action"] for action in result.actions] == [
        "replicate_digital_asset",
        "remove_source_replica",
    ]
    manager.remove_replica.assert_called_once_with(
        api.ReplicaID(1), delete_bytes=True, retain_tombstone=True
    )


def test_blocked_plan_does_not_attempt_placement(evacuation):
    manager, plan, _records, _configurations = evacuation
    plan = replace(plan, entries=(replace(plan.entries[0], shortfall=1),))
    result = execute_evacuation(
        manager, plan, EvacuationLimits(10, 100), keep_source_bytes=False
    )
    assert result.failures == 1
    assert result.actions[0]["entry"] == plan.entries[0].to_wire()
    manager.replicate_digital_asset.assert_not_called()
    manager.remove_replica.assert_not_called()


@pytest.mark.parametrize("failure", ["copy_error", "unverified", "topology_changed"])
def test_source_is_retained_when_replacements_are_not_safe(evacuation, failure):
    manager, plan, records, configurations = evacuation
    original = manager.replicate_digital_asset.side_effect

    def changed(*args, **kwargs):
        if failure == "copy_error":
            raise OSError("replacement unavailable")
        copied = original(*args, **kwargs)
        if failure == "unverified":
            records[copied.replica_id] = replace(
                copied, observation=api.ReplicaObservation(api.ReplicaState.UNVERIFIED)
            )
        else:
            configurations.pop(copied.location.store_ref)
        return copied

    manager.replicate_digital_asset.side_effect = changed
    result = execute_evacuation(
        manager, plan, EvacuationLimits(10, 100), keep_source_bytes=False
    )
    assert result.failures >= 1
    assert result.actions[-1]["action"] == "retain_source_replicas"
    manager.remove_replica.assert_not_called()


@pytest.mark.parametrize("retention", ["operator", "read_only", "unmanaged"])
def test_source_byte_retention_is_independent_of_claim_removal(evacuation, retention):
    manager, plan, records, configurations = evacuation
    if retention == "read_only":
        configurations[plan.source.store_uuid] = replace(plan.source, read_only=True)
    if retention == "unmanaged":
        records[api.ReplicaID(1)] = replace(
            records[api.ReplicaID(1)], mode=api.ReplicaMode.UNMANAGED
        )
    result = execute_evacuation(
        manager,
        plan,
        EvacuationLimits(10, 100),
        keep_source_bytes=retention == "operator",
    )
    assert result.failures == 0
    manager.remove_replica.assert_called_once_with(
        api.ReplicaID(1), delete_bytes=False, retain_tombstone=True
    )


def test_plan_wire_shape_and_self_destination_validation(evacuation):
    manager, plan, _records, _configurations = evacuation
    wire = plan.to_wire()
    assert wire["source_store_ref"] == str(UUID(int=1))
    assert wire["destination_store_ref"] == str(UUID(int=2))
    assert wire["assets_planned"] == wire["replicas_planned"] == 1
    assert wire["estimated_transfer_bytes"] == 4
    assert wire["blocked_entries"] == []
    assert plan.entries[0].source_replica_ids == (api.ReplicaID(1),)
    with pytest.raises(CoreDispatchError, match="must differ"):
        build_evacuation_plan(
            manager, source_ref=UUID(int=1), destination_ref=UUID(int=1), max_assets=10
        )
