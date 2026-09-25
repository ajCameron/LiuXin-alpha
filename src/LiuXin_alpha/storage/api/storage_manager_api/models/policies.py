"""
Represent replication/backup intent, registered definitions, assessments, and plans.

Constructors validate selected relationships without executing policy or proving
current storage state. Assessment predicates consume supplied evidence, while plan
values describe proposed work without reservations or publication side effects.
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models.replicas import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetDerivationID,
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
    ReplicaID,
)


class ReplicaSeparationDimension(StrEnum):
    """
    Name a configured boundary used to limit how many copies share a bucket.

    Store, host, device, failure-domain, and region values describe declared placement metadata.
    They do not discover physical independence or supply missing topology information.

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
    Name the intended response when an Asset has no usable retained copy.

    REQUIRE_COPY requests retained bytes, RECREATE permits an exact derivation route, and
    ACCEPT_LOSS permits loss. Choosing the enum performs no recovery or validation. Manager policy
    assignment separately checks recreation recipes and whether their prerequisites have retaining
    or recreating policies.

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
    Describe desired replication counts, placement constraints, and loss-handling intent.

    Construction validates selected numeric relationships and nonempty separation rules. It does not
    enforce integer types, validate label/tag contents, coerce enum fields, resolve recipes, or
    enforce the declared policy. Frozen fields can retain caller-owned mutable values.

    Example:
        >>> policy = ReplicationPolicy(
        ...     name="durable", min_copies=2, target_copies=3,
        ...     synchronous_write_copies=2,
        ... )
        >>> policy.effective_target_copies
        3

    :ivar name: Policy label retained without constructor-level nonblank validation.
    :ivar min_copies: Required minimum copy count;
                      numeric comparisons are checked without integer coercion.
    :ivar target_copies: Desired copy count, or None to use min_copies.
    :ivar distinct_by: Nonempty sequence of separation dimensions; entries are not normalized or validated here.
    :ivar max_copies_per_bucket: Maximum counted copies sharing each declared dimension bucket; must compare at least one.
    :ivar required_store_tags: Placement labels that a Store must contain, retained without copying.
    :ivar preferred_store_tags: Labels used to rank eligible destination Stores, without making them mandatory.
    :ivar forbidden_store_tags: Labels that disqualify a Store from the policy.
    :ivar synchronous_write_copies: Requested publication count within the target; a positive target requires at least one.
    :ivar auto_heal: Declared automatic-repair preference; this value does not schedule work.
    :ivar mode: Replica class considered by the policy, defaulting to ACTIVE without coercion.
    :ivar loss_action: Declared loss response; zero targets reject the REQUIRE_COPY enum singleton.
    :ivar retention_priority: Declared nonnegative retention priority, not an action or independent deletion guard.
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
        Check minimum/target order, bucket limit, synchronous count, separation presence, and
        retention priority.

        A zero target rejects the REQUIRE_COPY enum singleton; positive targets reject zero
        synchronous publications. Comparisons do not enforce integer types or finiteness, and enum
        fields are not coerced. Policy name, tag collections, individual dimensions, and auto_heal
        are not examined.

        Example:
            >>> ReplicationPolicy(min_copies=0)
            Traceback (most recent call last):
            ...
            ValueError: zero-copy policy must explicitly permit recreation or loss.


        :return: None when the selected count/spread constraints pass; invalid values or comparisons raise.
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
        Return min_copies only when target_copies is None, otherwise return the explicit target
        unchanged.

        Example:
            >>> ReplicationPolicy(min_copies=2).effective_target_copies
            2


        :return: Retained explicit target or minimum fallback, without conversion or new validation.
        """

        return self.min_copies if self.target_copies is None else self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicy:
    """
    Describe desired backup/archive counts, placement constraints, and verification/retention
    intent.

    Construction checks selected numeric relationships, nonempty separation rules, allowed mode
    equality, and the zero-target/retention-lock combination. It does not execute backups, normalize
    enum/tag values, or enforce all declared flags in every planning workflow.

    Example:
        >>> policy = BackupPolicy(
        ...     name="offsite", min_copies=1, target_copies=2,
        ...     mode=ReplicaMode.ARCHIVE,
        ... )
        >>> policy.effective_target_copies
        2


    :ivar name: Policy label retained without constructor-level nonblank validation.
    :ivar min_copies: Required minimum copy count; numeric comparisons are checked without integer coercion.
    :ivar target_copies: Desired copy count, or None to use min_copies.
    :ivar distinct_by: Nonempty sequence of separation dimensions; entries are not normalized or validated here.
    :ivar max_copies_per_bucket: Maximum counted copies sharing each declared dimension bucket; must compare at least one.
    :ivar required_store_tags: Placement labels that a Store must contain, retained without copying.
    :ivar preferred_store_tags: Labels used to rank eligible destination Stores, without making them mandatory.
    :ivar forbidden_store_tags: Labels that disqualify a Store from the policy.
    :ivar auto_heal: Declared automatic-repair preference, without scheduling or execution here.
    :ivar verify_after_write: Declared post-publication verification preference.
    :ivar periodic_verification: Declared preference for later verification of retained copies.
    :ivar retention_locked: Declared retention protection; a truthy value is rejected for a zero target.
    :ivar mode: BACKUP or ARCHIVE by equality; equal StrEnum strings can pass without coercion.
    :ivar retention_priority: Declared nonnegative retention priority, retained without integer coercion.
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
        Check target ordering, bucket/separation constraints, backup/archive mode equality, and
        retention limits.

        Mode membership uses equality, so matching strings can pass without becoming enum members.
        Name, tag contents, verification flags, and auto_heal are not checked. Numeric comparisons
        do not enforce integer or finite values.

        Example:
            >>> BackupPolicy(mode=ReplicaMode.ACTIVE)
            Traceback (most recent call last):
            ...
            ValueError: backup policy mode must be backup or archive.


        :return: None when the selected policy constraints pass; invalid spread/count/mode/retention combinations raise.
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
        Return the explicit target unchanged, using min_copies only for None.

        Example:
            >>> BackupPolicy(min_copies=2).effective_target_copies
            2


        :return: Retained backup target or minimum fallback.
        """

        return self.min_copies if self.target_copies is None else self.target_copies


# Todo: This ... should be a subclasses row? Or give the option to sync back to the database
# Todo: This may be true for all record holder classes
@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicyRecord:
    """
    Retain a registered replication-policy identity, definition, and optional revision.

    This frozen value adds no validation of ID positivity, definition type, or revision text.
    Manager methods own registration and optimistic-update behavior; constructing the record does
    not persist it.

    Example:
        >>> record = ReplicationPolicyRecord(
        ...     ReplicationPolicyID(4), ReplicationPolicy(),
        ... )
        >>> record.replication_policy_id
        4


    :ivar replication_policy_id: Attributed registered policy ID, not validated or allocated here.
    :ivar policy: Retained replication-policy definition without a defensive copy.
    :ivar revision: Optional optimistic-lock token, without constructor validation.
    """

    replication_policy_id: ReplicationPolicyID
    policy: ReplicationPolicy
    revision: str | None = None


# Todo: See above comments on records
@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicyRecord:
    """
    Retain a registered backup-policy identity, definition, and optional revision without additional
    validation.

    The value does not allocate a row, check ID positivity, or enforce retention. Persistence and
    update preconditions belong to manager operations.

    Example:
        >>> record = BackupPolicyRecord(BackupPolicyID(5), BackupPolicy())
        >>> record.policy.mode is ReplicaMode.BACKUP
        True


    :ivar backup_policy_id: Attributed backup-policy identity, not resolved here.
    :ivar policy: Retained backup/archive definition.
    :ivar revision: Optional optimistic-lock token retained as supplied.
    """

    backup_policy_id: BackupPolicyID
    policy: BackupPolicy
    revision: str | None = None


# Todo: Still not clear why they want this?
@dataclasses.dataclass(slots=True, frozen=True)
class ResolvedStoragePolicies:
    """
    Pair effective replication/backup definitions with their reported origins.

    The manager normally reports digital_asset for a captured explicit reference and manager_default
    otherwise. Construction does not validate those labels, copy the definitions, or resolve any
    policy itself.

    Example:
        >>> policies = ResolvedStoragePolicies(
        ...     ReplicationPolicy(), BackupPolicy(),
        ...     "digital_asset", "manager_default",
        ... )
        >>> policies.backup_source
        'manager_default'


    :ivar replication: Selected replication-policy definition.
    :ivar backup: Selected backup/archive-policy definition.
    :ivar replication_source: Reported origin of the replication definition.
    :ivar backup_source: Reported origin of the backup definition.
    """

    replication: ReplicationPolicy
    backup: BackupPolicy
    replication_source: str
    backup_source: str


@dataclasses.dataclass(slots=True, frozen=True)
class StoragePolicyAssessment:
    """
    Carry the observations and count-threshold flags produced for one Asset policy.

    The constructor checks the Asset-ID comparison, nonblank policy name, and the implication from
    meets_target to meets_minimum. It does not recompute counts, check Replica ownership, validate
    modes, or reconcile errors with the supplied flags.

    Example:
        >>> assessment = StoragePolicyAssessment(
        ...     digital_asset_id=7, policy_name="default",
        ...     mode=ReplicaMode.ACTIVE, present_replica_ids=(12,),
        ...     healthy_replica_ids=(12,), meets_minimum=True,
        ... )
        >>> assessment.meets_minimum
        True


    :ivar digital_asset_id: Attributed Asset ID, rejected when it compares at or below zero.
    :ivar policy_name: Required nonblank label retained without stripping.
    :ivar mode: Replica mode being assessed, retained without enum coercion.
    :ivar present_replica_ids: Reported currently readable claims in the policy mode.
    :ivar healthy_replica_ids: Reported VERIFIED claims that also satisfy Store configuration rules.
    :ivar meets_minimum: Reported minimum-capacity result, not calculated from these ID tuples.
    :ivar meets_target: Reported target-capacity result; truthy requires a truthy minimum result.
    :ivar errors: Reported diagnostics, which do not independently override the threshold flags.
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
        Reject nonpositive IDs, blank policy names, and a target result without a minimum result.

        Original values are retained; integer/bool types, mode, Replica IDs, diagnostics, and count
        consistency are not validated.

        Example:
            >>> StoragePolicyAssessment(
            ...     DigitalAssetID(7), "default", ReplicaMode.ACTIVE,
            ...     meets_minimum=False, meets_target=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: meeting a target implies meeting its minimum.


        :return: None when the selected identity/name/flag checks pass; validation and malformed-input errors propagate.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if not self.policy_name.strip():
            raise ValueError("policy_name must not be empty.")
        if self.meets_target and not self.meets_minimum:
            raise ValueError("meeting a target implies meeting its minimum.")


# Todo: Be good to include "can_be_replaced" details.
@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetStorageAssessment:
    """
    Combine reported policy satisfaction, readable claims, and exact-recreation routes for one
    Asset.

    Construction checks only that both policy assessments carry the same Asset ID. Derived flags
    trust the supplied lists and assessment values without querying storage or proving that a
    recorded recipe can execute.

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


    :ivar digital_asset_id: Asset attributed to the aggregate.
    :ivar replication_assessment: Replication assessment required to carry the same Asset ID.
    :ivar backup_assessment: Backup assessment required to carry the same Asset ID.
    :ivar readable_replica_ids: Reported readable claims across the modes considered by the producer.
    :ivar exact_recreation_derivation_ids: Reported exact recipes whose prerequisites the producer considered recoverable.
    """

    digital_asset_id: DigitalAssetID
    replication_assessment: StoragePolicyAssessment
    backup_assessment: StoragePolicyAssessment
    readable_replica_ids: tuple[ReplicaID, ...] = ()
    exact_recreation_derivation_ids: tuple[DigitalAssetDerivationID, ...] = ()

    def __post_init__(self) -> None:
        """
        Require each supplied policy assessment to carry the aggregate Asset ID, without checking
        other fields or physical state.

        Example:
            >>> assessment.digital_asset_id == assessment.replication_assessment.digital_asset_id  # doctest: +SKIP
            True


        :return: None for matching Asset IDs; disagreement raises ValueError and malformed attributes can raise.
        """

        if self.replication_assessment.digital_asset_id != self.digital_asset_id:
            raise ValueError("replication assessment belongs to another Asset.")
        if self.backup_assessment.digital_asset_id != self.digital_asset_id:
            raise ValueError("backup assessment belongs to another Asset.")

    @property
    def readable(self) -> bool:
        """
        Return the truthiness of readable_replica_ids without resolving any listed claim.

        Example:
            >>> bool(assessment.readable_replica_ids) == assessment.readable  # doctest: +SKIP
            True


        :return: True when the reported readable-Replica collection is nonempty.
        """

        return bool(self.readable_replica_ids)

    @property
    def replication_satisfied(self) -> bool:
        """
        Project the replication assessment's supplied minimum flag without interpreting target or
        error fields.

        Example:
            >>> assessment.replication_satisfied  # doctest: +SKIP
            True


        :return: The retained replication_assessment.meets_minimum value.
        """

        return self.replication_assessment.meets_minimum

    @property
    def backup_satisfied(self) -> bool:
        """
        Project the backup assessment's supplied minimum flag without counting its claims or
        inspecting diagnostics.

        Example:
            >>> assessment.backup_satisfied  # doctest: +SKIP
            True


        :return: The retained backup_assessment.meets_minimum value.
        """

        return self.backup_assessment.meets_minimum

    @property
    def at_risk(self) -> bool:
        """
        Return whether reported bytes are readable while either supplied policy minimum is
        unsatisfied. An unreadable Asset is not at_risk under this predicate.

        Example:
            >>> assessment.at_risk  # doctest: +SKIP
            False


        :return: True for a nonempty readable list and at least one false minimum flag.
        """

        return self.readable and not (
            self.replication_satisfied and self.backup_satisfied
        )

    @property
    def unavailable(self) -> bool:
        """
        Negate reported readability without querying current Store availability.

        Example:
            >>> assessment.unavailable is (not assessment.readable)  # doctest: +SKIP
            True


        :return: True when readable_replica_ids is empty.
        """

        return not self.readable

    @property
    def recreatable(self) -> bool:
        """
        Return the truthiness of reported exact-derivation IDs without inspecting recipes or their
        prerequisites.

        Example:
            >>> assessment.recreatable  # doctest: +SKIP
            True


        :return: True when at least one exact-recreation route is listed.
        """

        return bool(self.exact_recreation_derivation_ids)

    @property
    def recoverable(self) -> bool:
        """
        Return whether readable claims, healthy backup claims, or exact-recreation IDs are reported.

        Backup minimum/target flags and diagnostics are not consulted. This combines supplied
        evidence rather than independently verifying recoverability.

        Example:
            >>> assessment.recoverable  # doctest: +SKIP
            True


        :return: True when any of the three reported recovery routes is nonempty.
        """

        return (
            self.readable
            or bool(self.backup_assessment.healthy_replica_ids)
            or self.recreatable
        )

    @property
    def irrecoverable(self) -> bool:
        """
        Negate the aggregate recoverable predicate without a new storage or recipe check.

        Example:
            >>> assessment.irrecoverable is (not assessment.recoverable)  # doctest: +SKIP
            True


        :return: True when no readable, healthy-backup, or exact-recreation route is reported.
        """

        return not self.recoverable


# Todo: How do we tell if this plan can be implemented.
@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetReplicationPlan:
    """
    Carry proposed destination, verification, removal, and exact-recreation work for an Asset.

    Construction adds no validation of IDs, Store UUIDs, conflicts, or current feasibility.
    The value reserves no destination and executes no action; callers must apply current execution
    preconditions before acting.

    Example:
        >>> plan = DigitalAssetReplicationPlan(
        ...     digital_asset_id=7, destination_store_refs=(StoreUUID(int=1),),
        ... )
        >>> plan.destination_store_refs
        (UUID('00000000-0000-0000-0000-000000000001'),)


    :ivar digital_asset_id: Asset identity to which the proposed work is attributed.
    :ivar destination_store_refs: Proposed Store destinations, retained in planner order.
    :ivar replica_ids_to_verify: Recorded claims proposed for observation refresh.
    :ivar replica_ids_to_remove: Proposed removals, requiring execution-time checks.
    :ivar exact_recreation_derivation_id: Optional selected exact recipe, without execution or validation here.
    :ivar warnings: Planner diagnostics about incomplete or unavailable options.
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
    # Todo: This seems to be too many things for one object
    Carry proposed backup destinations, source claims, verification, and removal work.

    The frozen value does not validate identities, enforce retention locks, reserve Stores, or
    perform copying/deletion. Its tuples are the producing planner's proposals rather than durable
    execution commitments.

    Example:
        >>> plan = DigitalAssetBackupPlan(
        ...     digital_asset_id=7, destination_store_refs=(StoreUUID(int=1),),
        ...     source_replica_ids=(12,),
        ... )
        >>> plan.source_replica_ids
        (12,)


    :ivar digital_asset_id: Asset identity to which the backup proposal is attributed.
    :ivar destination_store_refs: Proposed destination Stores in planner order.
    :ivar source_replica_ids: Proposed source claims from other Replica modes.
    :ivar replica_ids_to_verify: Claims proposed for further verification.
    :ivar replica_ids_to_remove: Proposed removals subject to execution-time retention and race checks.
    :ivar warnings: Planner diagnostics retained without validation.
    """

    digital_asset_id: DigitalAssetID
    destination_store_refs: tuple[StoreUUID, ...] = ()
    source_replica_ids: tuple[ReplicaID, ...] = ()
    replica_ids_to_verify: tuple[ReplicaID, ...] = ()
    replica_ids_to_remove: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()


__all__ = [
    "DigitalAssetLossAction",
    "DigitalAssetBackupPlan",
    "BackupPolicy",
    "BackupPolicyRecord",
    "DigitalAssetStorageAssessment",
    "ReplicaSeparationDimension",
    "DigitalAssetReplicationPlan",
    "ReplicationPolicy",
    "ReplicationPolicyRecord",
    "ResolvedStoragePolicies",
    "StoragePolicyAssessment",
]
