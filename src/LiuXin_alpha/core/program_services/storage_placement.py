"""
Share Store tag/separation and observed-copy capacity rules between evacuation planning and execution.

These helpers inspect supplied configuration and Replica observations; they do
not reserve space, copy or verify bytes, or prove current backend availability.
Capacity is computed from per-dimension bucket counts, not by searching for a
subset that jointly satisfies every separation constraint.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from uuid import UUID

from LiuXin_alpha.storage import api

type PlacementPolicy = api.ReplicationPolicy | api.BackupPolicy


def configuration_bucket(
    configuration: api.StoreConfiguration,
    dimension: api.ReplicaSeparationDimension,
) -> object:
    """
    Return a configuration's separation key, grouping missing values in a shared dimension-specific bucket.

    Dimension.value is used when available. Any token other than store, host,
    device, or failure_domain falls through to region, without validation.

    Example:
        >>> configuration = api.StoreConfiguration(UUID(int=1), "primary", "filesystem", "file:///primary")
        >>> configuration_bucket(configuration, api.ReplicaSeparationDimension.HOST)
        ('unknown_host',)


    :param configuration: Store configuration supplying identity or topology values.
    :param dimension: Separation dimension selecting Store UUID, host, device, failure domain, or region.
    :return: Selected value, or a shared unknown-value tuple for a missing non-Store dimension.
    """
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
    """
    Count configurations subject to each dimension's per-bucket cap and take the smallest resulting total.

    Input is materialized once and not deduplicated. The result does not construct
    a jointly feasible subset across dimensions, check tags, or test availability.

    Example:
        >>> configuration = api.StoreConfiguration(UUID(int=1), "primary", "filesystem", "file:///primary")
        >>> policy_capacity_for_configurations([configuration, configuration], api.ReplicationPolicy())
        1


    :param configurations: Configurations representing candidate copies, consumed once in input order.
    :param policy: Policy supplying distinct_by dimensions and max_copies_per_bucket.
    :return: Zero for empty input; otherwise the minimum of input count and every capped dimension total.
    """
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
    """
    Select replication before backup when a policy's mode matches the requested Replica mode.

    Example:
        >>> policies = api.ResolvedStoragePolicies(api.ReplicationPolicy(), api.BackupPolicy(), "default", "default")
        >>> policy_for_mode(policies, api.ReplicaMode.ACTIVE) is policies.replication
        True


    :param policies: Resolved replication and backup policy pair for an Asset.
    :param mode: Mode to match; if both policies share it, replication wins.
    :return: Matching policy object itself, or None when neither policy governs this mode.
    """
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
    """
    Check supported mode and required/forbidden tags when a policy is supplied.

    None policy accepts every configuration, even an unsupported mode. Preferred
    tags, read_only, availability, capacity, and separation are not checked here.

    Example:
        >>> configuration = api.StoreConfiguration(UUID(int=1), "primary", "filesystem", "file:///primary")
        >>> accepts_configuration(configuration, api.ReplicaMode.UNMANAGED, None)
        True


    :param configuration: Store configuration whose supported modes and tags are inspected.
    :param mode: Requested mode, checked only when policy is not None.
    :param policy: Optional policy supplying required and forbidden tag sets.
    :return: True for no policy, or when mode support and both mandatory tag conditions pass.
    """
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
    """
    Select outside-source claims already marked VERIFIED on configured, policy-eligible Stores.

    Asset/mode filtering is delegated to the manager iterator. No fresh checksum,
    status, writability, or separation check is performed, and duplicates remain.

    Example:
        >>> records = verified_outside_source(manager, asset_id=api.DigitalAssetID(7), mode=api.ReplicaMode.ACTIVE, source_ref=UUID(int=1), configurations=configurations, policy=None)  # doctest: +SKIP


    :param manager: Storage manager supplying the Asset/mode-filtered Replica iterator.
    :param asset_id: Digital Asset identity forwarded to the iterator.
    :param mode: Replica mode forwarded to the iterator and optional policy eligibility check.
    :param source_ref: Store UUID whose claims must not count as outside replacements.
    :param configurations: Current Store configurations keyed by UUID; unknown Store claims are excluded.
    :param policy: Optional mode/tag policy applied to each known outside Store.
    :return: Tuple of eligible observed claims in manager iteration order, without independent verification.
    """
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
    """
    Count distinct Stores without a policy, or use the policy's marginal bucket-capacity calculation.

    Example:
        >>> configuration = api.StoreConfiguration(UUID(int=1), "primary", "filesystem", "file:///primary")
        >>> placement_capacity([configuration, configuration], None)
        1


    :param configurations: Copy-bearing configurations, consumed once and materialized before counting.
    :param policy: Optional policy selecting the per-dimension bucket calculation instead of distinct Store UUIDs.
    :return: Capacity count under the selected rule, not proof of live availability or joint placement feasibility.
    """
    selected = tuple(configurations)
    if policy is None:
        return len({configuration.store_uuid for configuration in selected})
    return policy_capacity_for_configurations(selected, policy)


def respects_separation(
    configuration: api.StoreConfiguration,
    selected: Iterable[api.StoreConfiguration],
    policy: PlacementPolicy | None,
) -> bool:
    """
    Require room for one more configuration in each of its selected-set separation buckets.

    Existing entries are counted as supplied, including duplicates. Only buckets
    shared with the candidate are checked; unrelated violations are not repaired.

    Example:
        >>> configuration = api.StoreConfiguration(UUID(int=1), "primary", "filesystem", "file:///primary")
        >>> respects_separation(configuration, [configuration], api.ReplicationPolicy())
        False


    :param configuration: Candidate Store whose dimension buckets are compared with the selected set.
    :param selected: Existing configurations consumed once when policy is present; not consumed when policy is None.
    :param policy: Optional separation policy; None accepts without checking any buckets.
    :return: True if no candidate bucket is already at or above the policy's per-bucket cap.
    """
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
