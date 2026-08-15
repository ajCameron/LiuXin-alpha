"""
Configured-store, bootstrap, and reconciliation value objects.
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from uuid import UUID

from LiuXin_alpha.storage.api.models import (
    EnumerationCompleteness,
    Location,
    StoreStatus,
    StoreUUID,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    ReplicationPolicyID,
    ReplicaID,
)


class TopologyRelation(StrEnum):
    """
    Whether two configured Stores share a declared topology identity.

    ``UNKNOWN`` is distinct from ``DIFFERENT``: absence of host or device
    metadata must not be treated as evidence of physical separation.

    Example:
        >>> TopologyRelation.SAME.value
        'same'
    """

    SAME = "same"
    DIFFERENT = "different"
    UNKNOWN = "unknown"


@dataclasses.dataclass(slots=True, frozen=True)
class StoreConfiguration:
    """
    Portable durable configuration for one store endpoint.

    Store default policy identifiers are placement-time defaults. A manager
    captures them on a newly declared Digital Asset whose first Replica is
    placed in this Store; they are not dynamically inherited by every Asset
    that later acquires a Replica here.

    Example:
        >>> configuration = StoreConfiguration(
        ...     store_uuid=UUID(int=1),
        ...     store_name="primary", store_kind="filesystem",
        ...     store_root_uri="file:///srv/liuxin",
        ... )
        >>> configuration.supports_folders
        True
    """

    store_uuid: StoreUUID
    store_name: str
    store_kind: str
    store_root_uri: str
    store_url: str | None = None
    store_access_protocol: str | None = None
    store_failure_domain: str | None = None
    store_region: str | None = None
    store_host_uuid: UUID | None = None
    store_device_uuid: UUID | None = None
    store_tags: tuple[str, ...] = ()
    store_default_replication_policy_id: ReplicationPolicyID | None = None
    store_default_backup_policy_id: BackupPolicyID | None = None
    supported_replica_modes: frozenset[ReplicaMode] = dataclasses.field(
        default_factory=lambda: frozenset(
            {ReplicaMode.ACTIVE, ReplicaMode.BACKUP, ReplicaMode.ARCHIVE}
        )
    )
    operational_role: str | None = None
    read_only: bool = False
    supports_folders: bool = True

    def __post_init__(self) -> None:
        """
        Require a UUID plus textual names, kinds, and root URIs.

        Example:
            >>> StoreConfiguration(
            ...     UUID(int=1), "", "filesystem", "file:///srv",
            ... )
            Traceback (most recent call last):
            ...
            ValueError: store_name must not be empty.


        :return:
        """

        if not isinstance(self.store_uuid, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_uuid must be a UUID.")
        for name, value in (
            ("store_host_uuid", self.store_host_uuid),
            ("store_device_uuid", self.store_device_uuid),
        ):
            if value is not None and not isinstance(value, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"{name} must be a UUID or None.")
        for name, value in (
            ("store_name", self.store_name),
            ("store_kind", self.store_kind),
            ("store_root_uri", self.store_root_uri),
        ):
            if not value.strip():
                raise ValueError(f"{name} must not be empty.")


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapIssue:
    """
    One configured store that could not be loaded during bootstrap.

    Example:
        >>> issue = StorageBootstrapIssue(
        ...     store_ref=UUID(int=1), store_name="archive", reason="offline",
        ... )
        >>> issue.reason
        'offline'
    """

    store_ref: StoreUUID | None
    store_name: str | None
    reason: str


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapReport:
    """
    Summary of rebuilding the runtime store registry from configuration.

    Example:
        >>> report = StorageBootstrapReport(
        ...     discovered_configurations=2, loaded_stores=2,
        ... )
        >>> report.ok
        True
    """

    discovered_configurations: int = 0
    loaded_stores: int = 0
    skipped_configurations: int = 0
    failed_configurations: int = 0
    issues: tuple[StorageBootstrapIssue, ...] = ()

    def __post_init__(self) -> None:
        """
        Reject negative or impossible configuration counts.

        Example:
            >>> StorageBootstrapReport(discovered_configurations=-1)
            Traceback (most recent call last):
            ...
            ValueError: bootstrap counts must not be negative.


        :return:
        """

        counts = (
            self.discovered_configurations,
            self.loaded_stores,
            self.skipped_configurations,
            self.failed_configurations,
        )
        if any(count < 0 for count in counts):
            raise ValueError("bootstrap counts must not be negative.")
        handled = (
            self.loaded_stores
            + self.skipped_configurations
            + self.failed_configurations
        )
        if handled > self.discovered_configurations:
            raise ValueError(
                "handled Store configurations exceed those discovered."
            )

    @property
    def ok(self) -> bool:
        """
        Return whether every Store configuration loaded without failure.

        Example:
            >>> StorageBootstrapReport(
            ...     discovered_configurations=2, loaded_stores=2,
            ... ).ok
            True


        :return:
        """

        return self.failed_configurations == 0


@dataclasses.dataclass(slots=True, frozen=True)
class StoreStatusObservation:
    """
    One configured Store UUID paired with its dynamic status snapshot.

    A bare ``StoreStatus`` is sufficient after a caller addresses one Store.
    Enumeration needs this wrapper so the result remains attributable.

    Example:
        >>> observation = StoreStatusObservation(
        ...     UUID(int=1), StoreStatus(available=True, writable=False),
        ... )
        >>> observation.store_ref
        UUID('00000000-0000-0000-0000-000000000001')
    """

    store_ref: StoreUUID
    status: StoreStatus

    def __post_init__(self) -> None:
        """
        Require a UUID rather than a name or database identifier.

        Example:
            >>> StoreStatusObservation(
            ...     "archive", StoreStatus(available=True, writable=False),
            ... )
            Traceback (most recent call last):
            ...
            TypeError: store_ref must be a UUID.


        :return:
        """

        if not isinstance(self.store_ref, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_ref must be a UUID.")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreReconciliationPlan:
    """
    Non-mutating comparison between Replica claims and Store inventory.

    ``plan_id`` and ``repository_revision`` let an implementation reject a
    stale plan before applying repository state changes.

    Example:
        >>> plan = StoreReconciliationPlan(
        ...     UUID(int=2), UUID(int=1), True,
        ...     EnumerationCompleteness.COMPLETE,
        ... )
        >>> plan.conclusive
        True
    """

    plan_id: UUID
    store_ref: StoreUUID
    verify_digests: bool
    enumeration: EnumerationCompleteness
    expected_replicas: int = 0
    observed_locations: int = 0
    matched_replicas: int = 0
    missing_replica_ids: tuple[ReplicaID, ...] = ()
    unexpected_locations: tuple[Location, ...] = ()
    corrupt_replica_ids: tuple[ReplicaID, ...] = ()
    unavailable_replica_ids: tuple[ReplicaID, ...] = ()
    repository_revision: str | None = None
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        Validate plan identity, counts, and matched-inventory bounds.

        Example:
            >>> StoreReconciliationPlan(
            ...     UUID(int=2), UUID(int=1), False,
            ...     EnumerationCompleteness.COMPLETE,
            ...     expected_replicas=1, matched_replicas=2,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: matched_replicas exceeds a reconciliation total.


        :return:
        """

        if not isinstance(self.plan_id, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("plan_id must be a UUID.")
        if not isinstance(self.store_ref, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_ref must be a UUID.")
        counts = (
            self.expected_replicas,
            self.observed_locations,
            self.matched_replicas,
        )
        if any(count < 0 for count in counts):
            raise ValueError("reconciliation counts must not be negative.")
        if self.matched_replicas > min(
            self.expected_replicas,
            self.observed_locations,
        ):
            raise ValueError(
                "matched_replicas exceeds a reconciliation total."
            )

    @property
    def conclusive(self) -> bool:
        """
        Return whether inventory was complete and checks had no errors.

        Example:
            >>> plan.conclusive  # doctest: +SKIP
            True


        :return:
        """

        return (
            self.enumeration is EnumerationCompleteness.COMPLETE
            and not self.unavailable_replica_ids
            and not self.errors
        )


@dataclasses.dataclass(slots=True, frozen=True)
class StoreReconciliationReport:
    """
    Outcome of applying or previewing one reconciliation plan.

    Example:
        >>> report = StoreReconciliationReport(
        ...     plan=StoreReconciliationPlan(
        ...         UUID(int=2), UUID(int=1), True,
        ...         EnumerationCompleteness.COMPLETE,
        ...         expected_replicas=2, observed_locations=2,
        ...         matched_replicas=2,
        ...     ),
        ...     applied=False,
        ... )
        >>> report.clean
        True
    """

    plan: StoreReconciliationPlan
    applied: bool
    updated_replica_ids: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        Prevent a preview report from claiming applied mutations.

        Example:
            >>> StoreReconciliationReport(
            ...     plan, applied=False,
            ...     updated_replica_ids=(ReplicaID(1),),
            ... )  # doctest: +SKIP


        :return:
        """

        if not self.applied and self.updated_replica_ids:
            raise ValueError(
                "an unapplied reconciliation cannot update Replicas."
            )

    @property
    def clean(self) -> bool:
        """
        Return whether reconciliation found no missing or corrupt objects.

        Example:
            >>> report.clean  # doctest: +SKIP
            True


        :return:
        """

        return self.plan.conclusive and not (
            self.plan.missing_replica_ids
            or self.plan.unexpected_locations
            or self.plan.corrupt_replica_ids
            or self.errors
        )


__all__ = [
    "StoreReconciliationPlan", "StoreReconciliationReport",
    "StorageBootstrapIssue", "StorageBootstrapReport",
    "StoreConfiguration", "StoreStatusObservation", "TopologyRelation",
]
