"""
Policy value objects for the storage manager.

These objects describe desired state and planner output for storage concerns.
They deliberately do *not* contain live job state, mutable execution details,
or backend-specific transfer mechanics.

Examples:
    Ask for three active copies on separate stores::

        policy = ReplicationPolicy(
            name="durable-active", min_copies=2, target_copies=3,
            distinct_by=(DistinctBy.STORE,),
        )
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from typing import FrozenSet, Optional


class ReplicationMode(StrEnum):
    """
    High-level replication intent.

    This is intentionally broad. More specialised policy families can grow out
    of this later without changing the basic shape of the replication policy
    object itself.

    Examples:
        Select active, backup, or archival intent::

            mode = ReplicationMode.ARCHIVE
    """

    ACTIVE = "active"
    BACKUP = "backup"
    ARCHIVE = "archive"


class DistinctBy(StrEnum):
    """
    Dimensions across which copies should be kept distinct.

    Examples:
        Require copies in separate failure domains::

            dimensions = (DistinctBy.FAILURE_DOMAIN,)
    """

    STORE = "store"
    FAILURE_DOMAIN = "failure_domain"
    REGION = "region"


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicy:
    """
    Declarative desired-state policy for physical replication.

    This object describes what storage should try to maintain. It should not
    contain live status, chosen store ids, timestamps, or transfer progress.

    Examples:
        Require two active copies on different stores::

            policy = ReplicationPolicy(
                name="two-copy", min_copies=2, distinct_by=(DistinctBy.STORE,)
            )
    """

    name: str = "default"
    min_copies: int = 1
    target_copies: Optional[int] = None
    distinct_by: tuple[DistinctBy, ...] = (DistinctBy.STORE,)
    max_copies_per_bucket: int = 1

    required_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)

    required_capabilities: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    forbidden_capabilities: FrozenSet[str] = dataclasses.field(default_factory=frozenset)

    synchronous_write_copies: int = 1
    auto_heal: bool = True
    mode: ReplicationMode = ReplicationMode.ACTIVE

    def __post_init__(self) -> None:
        target = self.effective_target_copies

        if self.min_copies < 1:
            raise ValueError("min_copies must be >= 1.")

        if target < self.min_copies:
            raise ValueError("target_copies must be >= min_copies.")

        if self.max_copies_per_bucket < 1:
            raise ValueError("max_copies_per_bucket must be >= 1.")

        if self.synchronous_write_copies < 1:
            raise ValueError("synchronous_write_copies must be >= 1.")

        if self.synchronous_write_copies > target:
            raise ValueError("synchronous_write_copies cannot exceed target_copies.")

        if not self.distinct_by:
            raise ValueError("distinct_by must contain at least one dimension.")

    @property
    def effective_target_copies(self) -> int:
        """
        Target copy count with the defaulting rule applied.

        Examples:
            An omitted target defaults to the minimum::

                assert ReplicationPolicy(min_copies=2).effective_target_copies == 2
        """
        if self.target_copies is None:
            return self.min_copies
        return self.target_copies


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicy:
    """
    Declarative desired-state policy for backup copies.

    This is intentionally similar to `ReplicationPolicy`, but focused on
    recoverable backup copies rather than immediately-available active replicas.

    Examples:
        Require two verified backup copies::

            policy = BackupPolicy(
                name="two-backups", min_backup_copies=2, verify_after_write=True
            )
    """

    name: str = "default_backup"
    min_backup_copies: int = 1
    target_backup_copies: Optional[int] = None
    distinct_by: tuple[DistinctBy, ...] = (DistinctBy.STORE,)
    max_copies_per_bucket: int = 1

    required_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    preferred_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    forbidden_store_tags: FrozenSet[str] = dataclasses.field(default_factory=frozenset)

    required_capabilities: FrozenSet[str] = dataclasses.field(default_factory=frozenset)
    forbidden_capabilities: FrozenSet[str] = dataclasses.field(default_factory=frozenset)

    auto_heal: bool = True
    verify_after_write: bool = True
    periodic_verification: bool = True
    retention_locked: bool = False
    mode: ReplicationMode = ReplicationMode.BACKUP

    def __post_init__(self) -> None:
        target = self.effective_target_backup_copies

        if self.min_backup_copies < 1:
            raise ValueError("min_backup_copies must be >= 1.")
        if target < self.min_backup_copies:
            raise ValueError("target_backup_copies must be >= min_backup_copies.")
        if self.max_copies_per_bucket < 1:
            raise ValueError("max_copies_per_bucket must be >= 1.")
        if not self.distinct_by:
            raise ValueError("distinct_by must contain at least one dimension.")

    @property
    def effective_target_backup_copies(self) -> int:
        """
        Target backup copy count with the defaulting rule applied.

        Examples:
            An omitted target defaults to the minimum::

                policy = BackupPolicy(min_backup_copies=2)
                assert policy.effective_target_backup_copies == 2
        """
        if self.target_backup_copies is None:
            return self.min_backup_copies
        return self.target_backup_copies


# Todo: We want a backup status?
@dataclasses.dataclass(slots=True, frozen=True, init=False)
class ReplicationStatus:
    """
    Snapshot of how one managed digital asset currently relates to a replication policy.

    Examples:
        Represent a healthy two-copy asset::

            status = ReplicationStatus(
                "asset-42", "two-copy", copy_count=2,
                healthy_copy_count=2, meets_minimum=True, meets_target=True,
            )
    """

    digital_asset_identifier: Optional[str]

    policy_name: str

    present_store_identifiers: tuple[str, ...] = ()

    healthy_store_identifiers: tuple[str, ...] = ()

    copy_count: int = 0

    healthy_copy_count: int = 0

    meets_minimum: bool = False

    meets_target: bool = False

    errors: tuple[str, ...] = ()

    def __init__(
        self,
        digital_asset_identifier: Optional[str] = None,
        policy_name: Optional[str] = None,
        present_store_identifiers: tuple[str, ...] = (),
        healthy_store_identifiers: tuple[str, ...] = (),
        copy_count: int = 0,
        healthy_copy_count: int = 0,
        meets_minimum: bool = False,
        meets_target: bool = False,
        errors: tuple[str, ...] = (),
        *,
        file_identifier: Optional[str] = None,
    ) -> None:
        """Create a replication assessment snapshot.

        ``file_identifier`` is a compatibility alias for
        ``digital_asset_identifier``.

        Examples:
            Older callers may still supply the compatibility name::

                status = ReplicationStatus(
                    file_identifier="asset-42", policy_name="two-copy"
                )
        """
        if policy_name is None:
            raise TypeError("ReplicationStatus requires policy_name.")
        if (
            digital_asset_identifier is not None
            and file_identifier is not None
            and digital_asset_identifier != file_identifier
        ):
            raise ValueError("digital_asset_identifier and file_identifier must match when both are provided.")

        object.__setattr__(self, "digital_asset_identifier", digital_asset_identifier if file_identifier is None else file_identifier)
        object.__setattr__(self, "policy_name", policy_name)
        object.__setattr__(self, "present_store_identifiers", present_store_identifiers)
        object.__setattr__(self, "healthy_store_identifiers", healthy_store_identifiers)
        object.__setattr__(self, "copy_count", copy_count)
        object.__setattr__(self, "healthy_copy_count", healthy_copy_count)
        object.__setattr__(self, "meets_minimum", meets_minimum)
        object.__setattr__(self, "meets_target", meets_target)
        object.__setattr__(self, "errors", errors)

    @property
    def file_identifier(self) -> Optional[str]:
        """Return the compatibility alias for the digital asset identifier.

        Examples:
            Read an assessment created by an older file-oriented caller::

                assert status.file_identifier == status.digital_asset_identifier
        """
        return self.digital_asset_identifier


# Todo: Backup plan?
@dataclasses.dataclass(slots=True, frozen=True, init=False)
class ReplicationPlan:
    """
    Planner output describing the next storage actions needed for one managed digital asset.

    Examples:
        Plan one new copy on the archive store::

            plan = ReplicationPlan(
                "asset-42", "two-copy", stores_to_add=("archive",)
            )
    """

    digital_asset_identifier: Optional[str]
    policy_name: str
    stores_to_add: tuple[str, ...] = ()
    stores_to_remove: tuple[str, ...] = ()
    stores_to_verify: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __init__(
        self,
        digital_asset_identifier: Optional[str] = None,
        policy_name: Optional[str] = None,
        stores_to_add: tuple[str, ...] = (),
        stores_to_remove: tuple[str, ...] = (),
        stores_to_verify: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
        *,
        file_identifier: Optional[str] = None,
    ) -> None:
        """Create an immutable planner result.

        ``file_identifier`` is a compatibility alias for
        ``digital_asset_identifier``.

        Examples:
            Describe both additions and verification work::

                plan = ReplicationPlan(
                    "asset-42", "two-copy",
                    stores_to_add=("archive",), stores_to_verify=("main",),
                )
        """
        if policy_name is None:
            raise TypeError("ReplicationPlan requires policy_name.")
        if (
            digital_asset_identifier is not None
            and file_identifier is not None
            and digital_asset_identifier != file_identifier
        ):
            raise ValueError("digital_asset_identifier and file_identifier must match when both are provided.")

        object.__setattr__(self, "digital_asset_identifier", digital_asset_identifier if file_identifier is None else file_identifier)
        object.__setattr__(self, "policy_name", policy_name)
        object.__setattr__(self, "stores_to_add", stores_to_add)
        object.__setattr__(self, "stores_to_remove", stores_to_remove)
        object.__setattr__(self, "stores_to_verify", stores_to_verify)
        object.__setattr__(self, "warnings", warnings)

    @property
    def file_identifier(self) -> Optional[str]:
        """Return the compatibility alias for the digital asset identifier.

        Examples:
            Support file-oriented code during migration::

                assert plan.file_identifier == plan.digital_asset_identifier
        """
        return self.digital_asset_identifier


__all__ = [
    "BackupPolicy",
    "BackupPolicyRecord",
    "DistinctBy",
    "ReplicationMode",
    "ReplicationPlan",
    "ReplicationPolicy",
    "ReplicationPolicyRecord",
    "ReplicationStatus",
]


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPolicyRecord:
    """Persisted replication policy row.

    Examples:
        Pair a database id with its immutable policy value::

            record = ReplicationPolicyRecord(4, ReplicationPolicy(name="main"))
    """

    replication_policy_id: Optional[int]
    policy: ReplicationPolicy


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicyRecord:
    """Persisted backup policy row.

    Examples:
        Pair a database id with its immutable policy value::

            record = BackupPolicyRecord(6, BackupPolicy(name="nightly"))
    """

    backup_policy_id: Optional[int]
    policy: BackupPolicy
