"""
Check evacuation phase ordering, budget refusal, replacement safety, and source-byte retention with an autospecced manager.

The fixture maintains in-memory Replica/configuration dictionaries; no filesystem
Store is opened and no bytes are transferred. Removal assertions inspect mock
calls rather than physical deletion or a post-removal claim repository.
"""

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
    """
    Build a one-Asset, two-Store evacuation plan around an autospecced storage manager.

    The source holds a verified four-byte active Replica. Copying creates/replaces
    record ID 2 in the dictionary; removal only records a mock call and leaves the
    dictionary unchanged. Paths and digests are descriptive fixture values.

    Example:
        >>> manager, plan, records, configurations = evacuation.__wrapped__()
        >>> plan.assets_planned, plan.entries[0].estimated_transfer_bytes
        (1, 4)


    :return: Manager, initial typed plan, mutable Replica dictionary, and mutable Store configuration dictionary.
    """
    source = api.StoreConfiguration(
        UUID(int=1), "source", "filesystem", "file:///source"
    )
    destination = api.StoreConfiguration(
        UUID(int=2), "destination", "filesystem", "file:///destination"
    )
    asset = api.DigitalAssetRecord(
        api.DigitalAssetID(1), 4, (api.Digest("sha256", "aa"),)
    )
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
        """
        Snapshot fixture claims matching the three supported optional filters.

        The tuple is built at call time, so later dictionary edits do not change
        the membership of this returned iterator. Additional filters are ignored.

        Example:
            >>> list(replicas(store_ref=UUID(int=1)))  # doctest: +SKIP


        :param digital_asset_id: Required Asset identity for matches, or None to leave identity unrestricted.
        :param store_ref: Required location Store UUID, or None to accept every Store.
        :param mode: Required Replica mode, or None to accept every mode.
        :param _filters: Extra manager-interface filter keywords accepted but ignored by this fake.
        :return: Iterator over a newly materialized tuple in the fixture dictionary's insertion order.
        """
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
        """
        Require verified copying from the fixture source and publish a replacement record under fixed ID 2.

        The source observation/digest is reused; no bytes are copied or verified.

        Example:
            >>> copied = replicate(api.DigitalAssetID(1), destination_store_ref=UUID(int=2), source_replica_id=api.ReplicaID(1), mode=api.ReplicaMode.ACTIVE, verify=True)  # doctest: +SKIP


        :param asset_id: Forwarded Asset identity, accepted but not inspected by this fake.
        :param destination_store_ref: Destination UUID used in the new copy.epub location.
        :param source_replica_id: Expected ID of the fixture's original source claim.
        :param mode: Replica mode assigned to the copied record.
        :param verify: Must be the boolean True; asserted rather than implemented.
        :return: Copied record after inserting/replacing its fixed ID in the shared claims dictionary.
        """
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
    """
    Reject the whole entry when either action or transfer allowance is too small, before any write call.

    Example:
        >>> test_limits_refuse_an_entry_before_any_transfer_or_removal(evacuation.__wrapped__(), EvacuationLimits(1, 100))


    :param evacuation: Fixture tuple containing a one-copy/one-removal plan and its mock manager.
    :param limits: Parametrized budget allowing too few actions or fewer than four transfer bytes.
    :return: None if execution reports truncation with no receipts, transfer accounting, or write calls.
    """
    manager, plan, _records, _configurations = evacuation
    result = execute_evacuation(manager, plan, limits, keep_source_bytes=False)
    assert result.truncated
    assert result.actions == []
    assert result.transferred_bytes == 0
    manager.replicate_digital_asset.assert_not_called()
    manager.remove_replica.assert_not_called()


def test_exact_limits_allow_copy_then_source_removal(evacuation):
    """
    Accept exact inclusive budgets and require a verified-copy receipt before a byte-deleting source-removal call.

    The manager is mocked; the assertions prove call order, arguments, and accounting,
    not physical transfer/deletion or an empty source repository afterward.

    Example:
        >>> test_exact_limits_allow_copy_then_source_removal(evacuation.__wrapped__())


    :param evacuation: Fixture tuple with a four-byte replacement and one source claim to remove.
    :return: None if two action receipts, four counted bytes, and the removal arguments match.
    """
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
    """
    Mark an entry short of destinations and require one failure receipt without copy or removal attempts.

    Example:
        >>> test_blocked_plan_does_not_attempt_placement(evacuation.__wrapped__())


    :param evacuation: Fixture tuple whose initial entry is replaced with a positive-shortfall version.
    :return: None if the failure receipt embeds the blocked entry and neither manager write operation was called.
    """
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
    """
    Retain source claims after copy failure, an unverified replacement, or removal of its Store configuration.

    The mutated fixture state becomes visible to the executor's post-copy capacity
    read. Source retention is asserted through the absence of removal calls.

    Example:
        >>> test_source_is_retained_when_replacements_are_not_safe(evacuation.__wrapped__(), "unverified")


    :param evacuation: Fixture tuple supplying the manager and dictionaries altered by the simulated failure.
    :param failure: Parametrized copy_error, unverified, or topology_changed scenario.
    :return: None if the final receipt explains source retention and no source removal was attempted.
    """
    manager, plan, records, configurations = evacuation
    original = manager.replicate_digital_asset.side_effect

    def changed(*args, **kwargs):
        """
        Inject the selected copy failure or mutate replacement observations/topology after the fake copy.

        Example:
            >>> copied = changed(*copy_args, **copy_kwargs)  # doctest: +SKIP


        :param args: Positional arguments forwarded to the original fake copier unless copy_error is selected.
        :param kwargs: Copier keyword arguments forwarded without modification on non-error scenarios.
        :return: Original copied record after shared-state mutation; not the later unverified dictionary replacement.
        :raises OSError: For the copy_error scenario before the original fake copier runs.
        """
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
    """
    Require claim removal without byte deletion for operator retention, a now-read-only source, or unmanaged claims.

    The read-only and unmanaged variants change manager state after planning.
    The test checks removal arguments, not the aggregate source_bytes_retained flag.

    Example:
        >>> test_source_byte_retention_is_independent_of_claim_removal(evacuation.__wrapped__(), "read_only")


    :param evacuation: Fixture tuple whose current configuration or source claim may be replaced.
    :param retention: Parametrized operator, read_only, or unmanaged retention reason.
    :return: None if the attempt has no failed receipts and removal preserves bytes while retaining a tombstone.
    """
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
    """
    Check selected plan receipt fields and reject a destination UUID equal to the source UUID.

    Example:
        >>> test_plan_wire_shape_and_self_destination_validation(evacuation.__wrapped__())


    :param evacuation: Fixture tuple containing the known one-Asset source/destination plan.
    :return: None if identity/count/estimate fields match and self-destination planning raises the expected error.
    """
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
