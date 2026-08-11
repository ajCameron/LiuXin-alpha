"""Configured-store, bootstrap, and reconciliation value objects."""

from __future__ import annotations

import dataclasses

from typing import Optional

from LiuXin_alpha.storage.api2.models import Location, StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api2.storage_manager_api.models.identifiers import (
    AssetReplicaID,
    BackupPolicyID,
    ReplicationPolicyID,
    StoreID,
)


@dataclasses.dataclass(slots=True, frozen=True)
class StoreSpec:
    """Portable durable configuration for one store endpoint.

    Example:
        >>> spec = StoreSpec(
        ...     store_id=None, store_name="primary", store_kind="filesystem",
        ...     store_root_uri="file:///srv/liuxin",
        ... )
        >>> spec.supports_folders
        True
    """

    store_id: Optional[StoreID]
    store_name: str
    store_kind: str
    store_root_uri: str
    store_uuid: Optional[str] = None
    store_url: Optional[str] = None
    store_access_protocol: Optional[str] = None
    store_failure_domain: Optional[str] = None
    store_region: Optional[str] = None
    store_tags: tuple[str, ...] = ()
    store_default_replication_policy_id: Optional[ReplicationPolicyID] = None
    store_default_backup_policy_id: Optional[BackupPolicyID] = None
    supported_replica_modes: frozenset[ReplicaMode] = dataclasses.field(
        default_factory=lambda: frozenset(
            {ReplicaMode.ACTIVE, ReplicaMode.BACKUP, ReplicaMode.ARCHIVE}
        )
    )
    operational_role: Optional[str] = None
    read_only: bool = False
    supports_folders: bool = True

    def __post_init__(self) -> None:
        """Require names, backend kinds, and root URIs to contain text.

        Example:
            >>> StoreSpec(None, "", "filesystem", "file:///srv")
            Traceback (most recent call last):
            ...
            ValueError: store_name must not be empty.
        """

        for name, value in (
            ("store_name", self.store_name),
            ("store_kind", self.store_kind),
            ("store_root_uri", self.store_root_uri),
        ):
            if not value.strip():
                raise ValueError(f"{name} must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapIssue:
    """One configured store that could not be loaded during bootstrap.

    Example:
        >>> issue = StorageBootstrapIssue(
        ...     store_id=3, store_name="archive", reason="offline",
        ... )
        >>> issue.reason
        'offline'
    """

    store_id: Optional[StoreID]
    store_name: Optional[str]
    reason: str


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapReport:
    """Summary of rebuilding the runtime store registry from configuration.

    Example:
        >>> report = StorageBootstrapReport(discovered_rows=2, loaded_stores=2)
        >>> report.ok
        True
    """

    discovered_rows: int = 0
    loaded_stores: int = 0
    skipped_rows: int = 0
    failed_rows: int = 0
    issues: tuple[StorageBootstrapIssue, ...] = ()

    @property
    def ok(self) -> bool:
        """Return whether every configured row loaded without failure.

        Example:
            >>> StorageBootstrapReport(loaded_stores=2).ok
            True
        """

        return self.failed_rows == 0


@dataclasses.dataclass(slots=True, frozen=True)
class ReconciliationReport:
    """Differences found between catalogued replicas and backend inventory.

    Example:
        >>> report = ReconciliationReport(
        ...     store_ref="primary", dry_run=True,
        ...     enumeration_complete=True, expected_replicas=2,
        ...     observed_locations=2, matched_replicas=2,
        ... )
        >>> report.clean
        True
    """

    store_ref: StoreRef
    dry_run: bool
    enumeration_complete: bool
    expected_replicas: int = 0
    observed_locations: int = 0
    matched_replicas: int = 0
    missing_replica_ids: tuple[AssetReplicaID, ...] = ()
    unexpected_locations: tuple[Location, ...] = ()
    corrupt_replica_ids: tuple[AssetReplicaID, ...] = ()
    unavailable_replica_ids: tuple[AssetReplicaID, ...] = ()
    updated_replica_ids: tuple[AssetReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def clean(self) -> bool:
        """Return whether reconciliation found no missing or corrupt objects.

        Example:
            >>> ReconciliationReport("primary", True, True).clean
            True
        """

        return not (
            self.missing_replica_ids
            or self.unexpected_locations
            or self.corrupt_replica_ids
            or self.errors
        )


__all__ = [
    "ReconciliationReport", "StorageBootstrapIssue", "StorageBootstrapReport",
    "StoreSpec",
]
