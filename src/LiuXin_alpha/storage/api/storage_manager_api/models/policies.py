"""Replication and backup policy values, assessments, and plans."""

from __future__ import annotations

import dataclasses

from enum import StrEnum

from LiuXin_alpha.storage.api.models import StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
    ReplicaID,
)


class DistinctBy(StrEnum):
    """Failure boundaries across which policy copies should be spread.

    Example:
        >>> DistinctBy.FAILURE_DOMAIN.value
        'failure_domain'
    """

    STORE = "store"
    HOST = "host"
    DEVICE = "device"
    FAILURE_DOMAIN = "failure_domain"
    REGION = "region"


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicy:
    """Desired state for live readable replicas.

    Example:
        >>> policy = ReplicationPolicy(
        ...     name="durable", min_copies=2, target_copies=3,
        ...     synchronous_write_copies=2,
        ... )
        >>> policy.effective_target_copies
        3
    """

    name: str = "default"
    min_copies: int = 1
    target_copies: int | None = None
    distinct_by: tuple[DistinctBy, ...] = (DistinctBy.STORE,)
    max_copies_per_bucket: int = 1
    required_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    synchronous_write_copies: int = 1
    auto_heal: bool = True
    mode: ReplicaMode = ReplicaMode.ACTIVE

    def __post_init__(self) -> None:
        """Validate copy counts, synchronous durability, and spread rules.

        Example:
            >>> ReplicationPolicy(min_copies=0)
            Traceback (most recent call last):
            ...
            ValueError: copy target must be at least the positive minimum.
        """

        target = self.effective_target_copies
        if self.min_copies < 1 or target < self.min_copies:
            raise ValueError("copy target must be at least the positive minimum.")
        if self.max_copies_per_bucket < 1:
            raise ValueError("max_copies_per_bucket must be positive.")
        if not 1 <= self.synchronous_write_copies <= target:
            raise ValueError("synchronous_write_copies must be within the copy target.")
        if not self.distinct_by:
            raise ValueError("distinct_by must not be empty.")

    @property
    def effective_target_copies(self) -> int:
        """Return the explicit target, falling back to the required minimum.

        Example:
            >>> ReplicationPolicy(min_copies=2).effective_target_copies
            2
        """

        return self.min_copies if self.target_copies is None else self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicy:
    """Desired state for recoverable backup or archival replicas.

    Example:
        >>> policy = BackupPolicy(
        ...     name="offsite", min_copies=1, target_copies=2,
        ...     mode=ReplicaMode.ARCHIVE,
        ... )
        >>> policy.effective_target_copies
        2
    """

    name: str = "default_backup"
    min_copies: int = 1
    target_copies: int | None = None
    distinct_by: tuple[DistinctBy, ...] = (DistinctBy.STORE,)
    max_copies_per_bucket: int = 1
    required_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    auto_heal: bool = True
    verify_after_write: bool = True
    periodic_verification: bool = True
    retention_locked: bool = False
    mode: ReplicaMode = ReplicaMode.BACKUP

    def __post_init__(self) -> None:
        """Validate backup copy counts, spread rules, and replica mode.

        Example:
            >>> BackupPolicy(mode=ReplicaMode.ACTIVE)
            Traceback (most recent call last):
            ...
            ValueError: backup policy mode must be backup or archive.
        """

        target = self.effective_target_copies
        if self.min_copies < 1 or target < self.min_copies:
            raise ValueError("copy target must be at least the positive minimum.")
        if self.max_copies_per_bucket < 1 or not self.distinct_by:
            raise ValueError("backup spread constraints must not be empty or zero.")
        if self.mode not in {ReplicaMode.BACKUP, ReplicaMode.ARCHIVE}:
            raise ValueError("backup policy mode must be backup or archive.")

    @property
    def effective_target_copies(self) -> int:
        """Return the explicit backup target or its required minimum.

        Example:
            >>> BackupPolicy(min_copies=2).effective_target_copies
            2
        """

        return self.min_copies if self.target_copies is None else self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class StoredReplicationPolicy:
    """Immutable domain snapshot of a registered replication policy.

    This is a policy definition with stable identity, not an exposed database
    row. ``revision`` may be used for optimistic concurrency control.

    Example:
        >>> stored = StoredReplicationPolicy(
        ...     ReplicationPolicyID(4), ReplicationPolicy(),
        ... )
        >>> stored.policy_id
        4
    """

    policy_id: ReplicationPolicyID
    policy: ReplicationPolicy
    revision: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class StoredBackupPolicy:
    """Immutable domain snapshot of a registered backup policy.

    Example:
        >>> stored = StoredBackupPolicy(BackupPolicyID(5), BackupPolicy())
        >>> stored.policy.mode is ReplicaMode.BACKUP
        True
    """

    policy_id: BackupPolicyID
    policy: BackupPolicy
    revision: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class EffectiveStoragePolicies:
    """Replication and backup policies resolved for one asset.

    The source strings explain where each effective policy was inherited or
    selected from.

    Example:
        >>> policies = EffectiveStoragePolicies(
        ...     ReplicationPolicy(), BackupPolicy(), "global", "store",
        ... )
        >>> policies.backup_source
        'store'
    """

    replication: ReplicationPolicy
    backup: BackupPolicy
    replication_source: str
    backup_source: str


@dataclasses.dataclass(slots=True, frozen=True)
class PolicyStatus:
    """Assessment of an asset against one replication or backup policy.

    ``ReplicationStatus`` and ``BackupStatus`` are semantic aliases of this
    value object.

    Example:
        >>> status = PolicyStatus(
        ...     digital_asset_id=7, policy_name="default",
        ...     mode=ReplicaMode.ACTIVE, present_replica_ids=(12,),
        ...     healthy_replica_ids=(12,), meets_minimum=True,
        ... )
        >>> status.meets_minimum
        True
    """

    digital_asset_id: DigitalAssetID
    policy_name: str
    mode: ReplicaMode
    present_replica_ids: tuple[ReplicaID, ...] = ()
    healthy_replica_ids: tuple[ReplicaID, ...] = ()
    meets_minimum: bool = False
    meets_target: bool = False
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Require positive identity and internally consistent target state.

        Example:
            >>> PolicyStatus(
            ...     DigitalAssetID(7), "default", ReplicaMode.ACTIVE,
            ...     meets_minimum=False, meets_target=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: meeting a target implies meeting its minimum.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if not self.policy_name.strip():
            raise ValueError("policy_name must not be empty.")
        if self.meets_target and not self.meets_minimum:
            raise ValueError("meeting a target implies meeting its minimum.")


ReplicationStatus = PolicyStatus
BackupStatus = PolicyStatus


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetStorageHealth:
    """Readable availability and policy state for one Digital Asset.

    Example:
        >>> replication = PolicyStatus(
        ...     7, "live", ReplicaMode.ACTIVE, meets_minimum=True,
        ... )
        >>> backup = PolicyStatus(
        ...     7, "backup", ReplicaMode.BACKUP, meets_minimum=True,
        ... )
        >>> health = DigitalAssetStorageHealth(
        ...     DigitalAssetID(7), replication, backup,
        ...     readable_replica_ids=(ReplicaID(12),),
        ... )
        >>> (health.readable, health.at_risk)
        (True, False)
    """

    digital_asset_id: DigitalAssetID
    replication: PolicyStatus
    backup: PolicyStatus
    readable_replica_ids: tuple[ReplicaID, ...] = ()

    def __post_init__(self) -> None:
        """Require both assessments to describe this Digital Asset.

        Example:
            >>> health.digital_asset_id == health.replication.digital_asset_id  # doctest: +SKIP
            True
        """

        if self.replication.digital_asset_id != self.digital_asset_id:
            raise ValueError("replication status belongs to another Asset.")
        if self.backup.digital_asset_id != self.digital_asset_id:
            raise ValueError("backup status belongs to another Asset.")

    @property
    def readable(self) -> bool:
        """Return whether at least one current Replica can serve the Asset.

        Example:
            >>> bool(health.readable_replica_ids) == health.readable  # doctest: +SKIP
            True
        """

        return bool(self.readable_replica_ids)

    @property
    def replication_satisfied(self) -> bool:
        """Return whether the live-copy minimum is met.

        Example:
            >>> health.replication_satisfied  # doctest: +SKIP
            True
        """

        return self.replication.meets_minimum

    @property
    def backup_satisfied(self) -> bool:
        """Return whether the backup-copy minimum is met.

        Example:
            >>> health.backup_satisfied  # doctest: +SKIP
            True
        """

        return self.backup.meets_minimum

    @property
    def at_risk(self) -> bool:
        """Return whether bytes are readable but minimum policy is unmet.

        Example:
            >>> health.at_risk  # doctest: +SKIP
            False
        """

        return self.readable and not (
            self.replication_satisfied and self.backup_satisfied
        )

    @property
    def unavailable(self) -> bool:
        """Return whether no current Replica can serve the Asset.

        Example:
            >>> health.unavailable is (not health.readable)  # doctest: +SKIP
            True
        """

        return not self.readable


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPlan:
    """Non-mutating plan for bringing live replicas toward desired state.

    Example:
        >>> plan = ReplicationPlan(
        ...     digital_asset_id=7, destination_stores=("primary", "mirror"),
        ... )
        >>> plan.destination_stores
        ('primary', 'mirror')
    """

    digital_asset_id: DigitalAssetID
    destination_stores: tuple[StoreRef, ...] = ()
    replicas_to_verify: tuple[ReplicaID, ...] = ()
    replicas_to_remove: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPlan:
    """Non-mutating plan for bringing backup replicas toward desired state.

    Example:
        >>> plan = BackupPlan(
        ...     digital_asset_id=7, destination_stores=("offsite",),
        ...     source_replica_ids=(12,),
        ... )
        >>> plan.source_replica_ids
        (12,)
    """

    digital_asset_id: DigitalAssetID
    destination_stores: tuple[StoreRef, ...] = ()
    source_replica_ids: tuple[ReplicaID, ...] = ()
    replicas_to_verify: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()


__all__ = [
    "BackupPlan", "BackupPolicy", "BackupStatus",
    "DigitalAssetStorageHealth", "DistinctBy", "EffectiveStoragePolicies",
    "PolicyStatus", "ReplicationPlan", "ReplicationPolicy",
    "ReplicationStatus", "StoredBackupPolicy", "StoredReplicationPolicy",
]
