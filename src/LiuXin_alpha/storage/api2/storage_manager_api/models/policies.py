"""Replication and backup policy values, assessments, and plans."""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from typing import Optional

from LiuXin_alpha.storage.api2.models import StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api2.storage_manager_api.models.identifiers import (
    AssetReplicaID,
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
)


class DistinctBy(StrEnum):
    """Failure boundaries across which policy copies should be spread.

    Example:
        >>> DistinctBy.FAILURE_DOMAIN.value
        'failure_domain'
    """

    STORE = "store"
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
    target_copies: Optional[int] = None
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
    target_copies: Optional[int] = None
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
class ReplicationPolicyRecord:
    """Persisted replication policy paired with its database identifier.

    ``policy_id`` is ``None`` for a record that has not yet been persisted.

    Example:
        >>> record = ReplicationPolicyRecord(4, ReplicationPolicy())
        >>> record.policy_id
        4
    """

    policy_id: Optional[ReplicationPolicyID]
    policy: ReplicationPolicy


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicyRecord:
    """Persisted backup policy paired with its database identifier.

    Example:
        >>> record = BackupPolicyRecord(5, BackupPolicy())
        >>> record.policy.mode is ReplicaMode.BACKUP
        True
    """

    policy_id: Optional[BackupPolicyID]
    policy: BackupPolicy


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
    present_replica_ids: tuple[AssetReplicaID, ...] = ()
    healthy_replica_ids: tuple[AssetReplicaID, ...] = ()
    meets_minimum: bool = False
    meets_target: bool = False
    errors: tuple[str, ...] = ()


ReplicationStatus = PolicyStatus
BackupStatus = PolicyStatus


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetStorageHealth:
    """Combined live-replication and backup health for one asset.

    Example:
        >>> replication = PolicyStatus(
        ...     7, "live", ReplicaMode.ACTIVE, meets_minimum=True,
        ... )
        >>> backup = PolicyStatus(
        ...     7, "backup", ReplicaMode.BACKUP, meets_minimum=True,
        ... )
        >>> DigitalAssetStorageHealth(7, replication, backup).healthy
        True
    """

    digital_asset_id: DigitalAssetID
    replication: PolicyStatus
    backup: PolicyStatus

    @property
    def healthy(self) -> bool:
        """Return whether minimum live and backup policy are both satisfied.

        Example:
            >>> live = PolicyStatus(7, "live", ReplicaMode.ACTIVE, meets_minimum=True)
            >>> backup = PolicyStatus(7, "backup", ReplicaMode.BACKUP, meets_minimum=True)
            >>> DigitalAssetStorageHealth(7, live, backup).healthy
            True
        """

        return self.replication.meets_minimum and self.backup.meets_minimum


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
    replicas_to_verify: tuple[AssetReplicaID, ...] = ()
    replicas_to_remove: tuple[AssetReplicaID, ...] = ()
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
    source_replica_ids: tuple[AssetReplicaID, ...] = ()
    replicas_to_verify: tuple[AssetReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()


__all__ = [
    "BackupPlan", "BackupPolicy", "BackupPolicyRecord", "BackupStatus",
    "DigitalAssetStorageHealth", "DistinctBy", "EffectiveStoragePolicies",
    "PolicyStatus", "ReplicationPlan", "ReplicationPolicy",
    "ReplicationPolicyRecord", "ReplicationStatus",
]
