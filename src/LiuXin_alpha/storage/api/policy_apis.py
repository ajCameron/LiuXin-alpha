"""
Policy value objects for the storage manager.

These objects describe desired state and planner output for storage concerns.
They deliberately do *not* contain live job state, mutable execution details,
or backend-specific transfer mechanics.
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
    """

    ACTIVE = "active"
    BACKUP = "backup"
    ARCHIVE = "archive"


class DistinctBy(StrEnum):
    """
    Dimensions across which copies should be kept distinct.
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
        """
        if self.target_backup_copies is None:
            return self.min_backup_copies
        return self.target_backup_copies


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationStatus:
    """
    Snapshot of how one managed digital asset currently relates to a replication policy.
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


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicationPlan:
    """
    Planner output describing the next storage actions needed for one managed digital asset.
    """

    digital_asset_identifier: Optional[str]
    policy_name: str
    stores_to_add: tuple[str, ...] = ()
    stores_to_remove: tuple[str, ...] = ()
    stores_to_verify: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


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
    """Persisted replication policy row."""

    replication_policy_id: Optional[int]
    policy: ReplicationPolicy


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPolicyRecord:
    """Persisted backup policy row."""

    backup_policy_id: Optional[int]
    policy: BackupPolicy
