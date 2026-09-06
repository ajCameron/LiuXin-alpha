"""Core-owned storage placement operations and wire translation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from uuid import UUID

from LiuXin_alpha.storage import api

type PlacementPolicy = api.ReplicationPolicy | api.BackupPolicy


def configuration_bucket(
    configuration: api.StoreConfiguration,
    dimension: api.ReplicaSeparationDimension,
) -> object:
    value = str(getattr(dimension, "value", dimension))
    if value == "store":
        return configuration.store_uuid
    if value == "host":
        return configuration.store_host_uuid or ("unknown_host",)
    if value == "device":
        return configuration.store_device_uuid or ("unknown_device",)
    if value == "failure_domain":
        return configuration.store_failure_domain or ("unknown_failure_domain",)
    return configuration.store_region or ("unknown_region",)


def policy_capacity_for_configurations(
    configurations: Iterable[api.StoreConfiguration],
    policy: PlacementPolicy,
) -> int:
    selected = tuple(configurations)
    if not selected:
        return 0
    capacities = [len(selected)]
    for dimension in policy.distinct_by:
        counts: dict[object, int] = {}
        for configuration in selected:
            bucket = configuration_bucket(configuration, dimension)
            counts[bucket] = counts.get(bucket, 0) + 1
        capacities.append(
            sum(
                min(count, int(policy.max_copies_per_bucket))
                for count in counts.values()
            )
        )
    return min(capacities)


def policy_for_mode(
    policies: api.ResolvedStoragePolicies, mode: api.ReplicaMode
) -> PlacementPolicy | None:
    """Select the policy whose copy target governs this Replica mode."""
    if mode == policies.replication.mode:
        return policies.replication
    if mode == policies.backup.mode:
        return policies.backup
    return None


def accepts_configuration(
    configuration: api.StoreConfiguration,
    mode: api.ReplicaMode,
    policy: PlacementPolicy | None,
) -> bool:
    """Check mode and tags without imposing destination writability."""
    if policy is None:
        return True
    tags = set(configuration.store_tags)
    return (
        mode in configuration.supported_replica_modes
        and policy.required_store_tags <= tags
        and not policy.forbidden_store_tags & tags
    )


def verified_outside_source(
    manager: api.StorageManagerAPI,
    *,
    asset_id: api.DigitalAssetID,
    mode: api.ReplicaMode,
    source_ref: UUID,
    configurations: Mapping[UUID, api.StoreConfiguration],
    policy: PlacementPolicy | None,
) -> tuple[api.ReplicaRecord, ...]:
    """Find verified replacements eligible under the current Store topology."""
    return tuple(
        record
        for record in manager.iter_replica_records(digital_asset_id=asset_id, mode=mode)
        if record.location.store_ref != source_ref
        and record.state is api.ReplicaState.VERIFIED
        and record.location.store_ref in configurations
        and accepts_configuration(
            configurations[record.location.store_ref], mode, policy
        )
    )


def placement_capacity(
    configurations: Iterable[api.StoreConfiguration],
    policy: PlacementPolicy | None,
) -> int:
    """Count independent copies using the same rules in planning and apply."""
    selected = tuple(configurations)
    if policy is None:
        return len({configuration.store_uuid for configuration in selected})
    return policy_capacity_for_configurations(selected, policy)


def respects_separation(
    configuration: api.StoreConfiguration,
    selected: Iterable[api.StoreConfiguration],
    policy: PlacementPolicy | None,
) -> bool:
    """Test whether adding a destination exceeds a failure-domain bucket."""
    if policy is None:
        return True
    previous = tuple(selected)
    return not any(
        sum(
            configuration_bucket(value, dimension)
            == configuration_bucket(configuration, dimension)
            for value in previous
        )
        >= int(policy.max_copies_per_bucket)
        for dimension in policy.distinct_by
    )
