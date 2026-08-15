"""
Replication and backup policy values, assessments, and plans.
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetDerivationID,
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
    ReplicaID,
)


class ReplicaSeparationDimension(StrEnum):
    """
    Failure boundaries across which policy copies should be spread.

    Example:
        >>> ReplicaSeparationDimension.FAILURE_DOMAIN.value
        'failure_domain'
    """

    STORE = "store"
    HOST = "host"
    DEVICE = "device"
    FAILURE_DOMAIN = "failure_domain"
    REGION = "region"


class DigitalAssetLossAction(StrEnum):
    """
    Intended response when no readable live Replica remains.

    ``RECREATE`` is valid only when the manager can find a complete exact
    derivation recipe whose own source Assets remain recoverable.

    Example:
        >>> DigitalAssetLossAction.RECREATE.value
        'recreate'
    """

    REQUIRE_COPY = "require_copy"
    RECREATE = "recreate"
    ACCEPT_LOSS = "accept_loss"


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicy:
    """
    Desired state for live readable replicas.

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
    distinct_by: tuple[ReplicaSeparationDimension, ...] = (
        ReplicaSeparationDimension.STORE,
    )
    max_copies_per_bucket: int = 1
    required_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    synchronous_write_copies: int = 1
    auto_heal: bool = True
    mode: ReplicaMode = ReplicaMode.ACTIVE
    loss_action: DigitalAssetLossAction = DigitalAssetLossAction.REQUIRE_COPY
    retention_priority: int = 100

    def __post_init__(self) -> None:
        """
        Validate copy counts, synchronous durability, and spread rules.

        Example:
            >>> ReplicationPolicy(min_copies=0)
            Traceback (most recent call last):
            ...
            ValueError: zero-copy policy must explicitly permit recreation or loss.


        :return:
        """

        target = self.effective_target_copies
        if self.min_copies < 0 or target < self.min_copies:
            raise ValueError("copy target must be at least the non-negative minimum.")
        if target == 0 and self.loss_action is DigitalAssetLossAction.REQUIRE_COPY:
            raise ValueError(
                "zero-copy policy must explicitly permit recreation or loss."
            )
        if self.max_copies_per_bucket < 1:
            raise ValueError("max_copies_per_bucket must be positive.")
        if not 0 <= self.synchronous_write_copies <= target:
            raise ValueError("synchronous_write_copies must be within the copy target.")
        if target > 0 and self.synchronous_write_copies == 0:
            raise ValueError(
                "a non-zero copy target requires one synchronous publication."
            )
        if not self.distinct_by:
            raise ValueError("distinct_by must not be empty.")
        if self.retention_priority < 0:
            raise ValueError("retention_priority must not be negative.")

    @property
    def effective_target_copies(self) -> int:
        """
        Return the explicit target, falling back to the required minimum.

        Example:
            >>> ReplicationPolicy(min_copies=2).effective_target_copies
            2


        :return:
        """

        return self.min_copies if self.target_copies is None else self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicy:
    """
    Desired state for recoverable backup or archival replicas.

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
    distinct_by: tuple[ReplicaSeparationDimension, ...] = (
        ReplicaSeparationDimension.STORE,
    )
    max_copies_per_bucket: int = 1
    required_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: frozenset[str] = dataclasses.field(default_factory=frozenset)
    auto_heal: bool = True
    verify_after_write: bool = True
    periodic_verification: bool = True
    retention_locked: bool = False
    mode: ReplicaMode = ReplicaMode.BACKUP
    retention_priority: int = 100

    def __post_init__(self) -> None:
        """
        Validate backup copy counts, spread rules, and replica mode.

        Example:
            >>> BackupPolicy(mode=ReplicaMode.ACTIVE)
            Traceback (most recent call last):
            ...
            ValueError: backup policy mode must be backup or archive.


        :return:
        """

        target = self.effective_target_copies
        if self.min_copies < 0 or target < self.min_copies:
            raise ValueError("copy target must be at least the non-negative minimum.")
        if self.max_copies_per_bucket < 1 or not self.distinct_by:
            raise ValueError("backup spread constraints must not be empty or zero.")
        if self.mode not in {ReplicaMode.BACKUP, ReplicaMode.ARCHIVE}:
            raise ValueError("backup policy mode must be backup or archive.")
        if self.retention_priority < 0:
            raise ValueError("retention_priority must not be negative.")
        if target == 0 and self.retention_locked:
            raise ValueError("a zero-copy backup policy cannot be retention locked.")

    @property
    def effective_target_copies(self) -> int:
        """
        Return the explicit backup target or its required minimum.

        Example:
            >>> BackupPolicy(min_copies=2).effective_target_copies
            2


        :return:
        """

        return self.min_copies if self.target_copies is None else self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicyRecord:
    """
    Manager-maintained registration of one replication policy.

    This is a policy definition with stable identity, not an exposed database
    row. ``revision`` may be used for optimistic concurrency control.

    Example:
        >>> record = ReplicationPolicyRecord(
        ...     ReplicationPolicyID(4), ReplicationPolicy(),
        ... )
        >>> record.replication_policy_id
        4
    """

    replication_policy_id: ReplicationPolicyID
    policy: ReplicationPolicy
    revision: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicyRecord:
    """
    Manager-maintained registration of one backup policy.

    Example:
        >>> record = BackupPolicyRecord(BackupPolicyID(5), BackupPolicy())
        >>> record.policy.mode is ReplicaMode.BACKUP
        True
    """

    backup_policy_id: BackupPolicyID
    policy: BackupPolicy
    revision: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class ResolvedStoragePolicies:
    """
    Replication and backup policies resolved for one asset.

    The source strings explain whether each effective policy was captured on
    the Asset or supplied by the manager default.

    Example:
        >>> policies = ResolvedStoragePolicies(
        ...     ReplicationPolicy(), BackupPolicy(),
        ...     "digital_asset", "manager_default",
        ... )
        >>> policies.backup_source
        'manager_default'
    """

    replication: ReplicationPolicy
    backup: BackupPolicy
    replication_source: str
    backup_source: str


@dataclasses.dataclass(slots=True, frozen=True)
class StoragePolicyAssessment:
    """
    Assessment of an asset against one replication or backup policy.

    ``mode`` states whether the assessment concerns live, backup, archival, or
    another Replica class without creating duplicate data structures.

    Example:
        >>> assessment = StoragePolicyAssessment(
        ...     digital_asset_id=7, policy_name="default",
        ...     mode=ReplicaMode.ACTIVE, present_replica_ids=(12,),
        ...     healthy_replica_ids=(12,), meets_minimum=True,
        ... )
        >>> assessment.meets_minimum
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
        """
        Require positive identity and internally consistent target state.

        Example:
            >>> StoragePolicyAssessment(
            ...     DigitalAssetID(7), "default", ReplicaMode.ACTIVE,
            ...     meets_minimum=False, meets_target=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: meeting a target implies meeting its minimum.


        :return:
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if not self.policy_name.strip():
            raise ValueError("policy_name must not be empty.")
        if self.meets_target and not self.meets_minimum:
            raise ValueError("meeting a target implies meeting its minimum.")


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetStorageAssessment:
    """
    Readable availability and policy state for one Digital Asset.

    Example:
        >>> replication_assessment = StoragePolicyAssessment(
        ...     7, "live", ReplicaMode.ACTIVE, meets_minimum=True,
        ... )
        >>> backup_assessment = StoragePolicyAssessment(
        ...     7, "backup", ReplicaMode.BACKUP, meets_minimum=True,
        ... )
        >>> assessment = DigitalAssetStorageAssessment(
        ...     DigitalAssetID(7), replication_assessment, backup_assessment,
        ...     readable_replica_ids=(ReplicaID(12),),
        ... )
        >>> (assessment.readable, assessment.at_risk)
        (True, False)
    """

    digital_asset_id: DigitalAssetID
    replication_assessment: StoragePolicyAssessment
    backup_assessment: StoragePolicyAssessment
    readable_replica_ids: tuple[ReplicaID, ...] = ()
    exact_recreation_derivation_ids: tuple[DigitalAssetDerivationID, ...] = ()

    def __post_init__(self) -> None:
        """
        Require both assessments to describe this Digital Asset.

        Example:
            >>> assessment.digital_asset_id == assessment.replication_assessment.digital_asset_id  # doctest: +SKIP
            True


        :return:
        """

        if self.replication_assessment.digital_asset_id != self.digital_asset_id:
            raise ValueError("replication assessment belongs to another Asset.")
        if self.backup_assessment.digital_asset_id != self.digital_asset_id:
            raise ValueError("backup assessment belongs to another Asset.")

    @property
    def readable(self) -> bool:
        """
        Return whether at least one current Replica can serve the Asset.

        Example:
            >>> bool(assessment.readable_replica_ids) == assessment.readable  # doctest: +SKIP
            True


        :return:
        """

        return bool(self.readable_replica_ids)

    @property
    def replication_satisfied(self) -> bool:
        """
        Return whether the live-copy minimum is met.

        Example:
            >>> assessment.replication_satisfied  # doctest: +SKIP
            True


        :return:
        """

        return self.replication_assessment.meets_minimum

    @property
    def backup_satisfied(self) -> bool:
        """
        Return whether the backup-copy minimum is met.

        Example:
            >>> assessment.backup_satisfied  # doctest: +SKIP
            True


        :return:
        """

        return self.backup_assessment.meets_minimum

    @property
    def at_risk(self) -> bool:
        """
        Return whether bytes are readable but minimum policy is unmet.

        Example:
            >>> assessment.at_risk  # doctest: +SKIP
            False


        :return:
        """

        return self.readable and not (
            self.replication_satisfied and self.backup_satisfied
        )

    @property
    def unavailable(self) -> bool:
        """
        Return whether no current Replica can serve the Asset.

        Example:
            >>> assessment.unavailable is (not assessment.readable)  # doctest: +SKIP
            True


        :return:
        """

        return not self.readable

    @property
    def recreatable(self) -> bool:
        """
        Return whether a complete exact recipe can regenerate the bytes.

        Source reachability is part of the manager's assessment before it
        places a derivation identifier in this collection.

        Example:
            >>> assessment.recreatable  # doctest: +SKIP
            True


        :return:
        """

        return bool(self.exact_recreation_derivation_ids)

    @property
    def recoverable(self) -> bool:
        """
        Return whether bytes are readable, backed up, or exactly recreatable.

        Example:
            >>> assessment.recoverable  # doctest: +SKIP
            True


        :return:
        """

        return (
            self.readable
            or bool(self.backup_assessment.healthy_replica_ids)
            or self.recreatable
        )

    @property
    def irrecoverable(self) -> bool:
        """
        Return whether no present copy or reachable exact recipe can recover bytes.

        Example:
            >>> assessment.irrecoverable is (not assessment.recoverable)  # doctest: +SKIP
            True


        :return:
        """

        return not self.recoverable


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetReplicationPlan:
    """
    Non-mutating plan for bringing live replicas toward desired state.

    Example:
        >>> plan = DigitalAssetReplicationPlan(
        ...     digital_asset_id=7, destination_store_refs=(StoreUUID(int=1),),
        ... )
        >>> plan.destination_store_refs
        (UUID('00000000-0000-0000-0000-000000000001'),)
    """

    digital_asset_id: DigitalAssetID
    destination_store_refs: tuple[StoreUUID, ...] = ()
    replica_ids_to_verify: tuple[ReplicaID, ...] = ()
    replica_ids_to_remove: tuple[ReplicaID, ...] = ()
    exact_recreation_derivation_id: DigitalAssetDerivationID | None = None
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetBackupPlan:
    """
    Non-mutating plan for bringing backup replicas toward desired state.

    Example:
        >>> plan = DigitalAssetBackupPlan(
        ...     digital_asset_id=7, destination_store_refs=(StoreUUID(int=1),),
        ...     source_replica_ids=(12,),
        ... )
        >>> plan.source_replica_ids
        (12,)
    """

    digital_asset_id: DigitalAssetID
    destination_store_refs: tuple[StoreUUID, ...] = ()
    source_replica_ids: tuple[ReplicaID, ...] = ()
    replica_ids_to_verify: tuple[ReplicaID, ...] = ()
    replica_ids_to_remove: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()


__all__ = [
    "DigitalAssetLossAction", "DigitalAssetBackupPlan", "BackupPolicy",
    "BackupPolicyRecord", "DigitalAssetStorageAssessment",
    "ReplicaSeparationDimension", "DigitalAssetReplicationPlan",
    "ReplicationPolicy", "ReplicationPolicyRecord", "ResolvedStoragePolicies",
    "StoragePolicyAssessment",
]
