"""
Configured-store, bootstrap, and reconciliation value objects.
"""

from __future__ import annotations

import dataclasses
import os

from collections.abc import Iterable, Mapping
from enum import StrEnum
from pathlib import Path
from typing import Self, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api.models import (
    EnumerationCompleteness,
    Location,
    StoreStatus,
    StoreUUID,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    DigitalAssetID,
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
class StoreBackingReference:
    """
    Durably identify the Digital Asset whose bytes back a Store view.

    The Asset is authoritative. ``preferred_replica_id`` is only a routing
    hint and may be replaced by another readable Replica of the same Asset.
    A materialization Store is required when the selected Replica cannot be
    exposed to a local-file container driver directly.

    Example:
        >>> backing = StoreBackingReference(
        ...     DigitalAssetID(7), preferred_replica_id=ReplicaID(12),
        ... )
        >>> int(backing.digital_asset_id)
        7
    """

    digital_asset_id: DigitalAssetID
    preferred_replica_id: ReplicaID | None = None
    materialization_store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Reject invalid catalogue identifiers and Store references.

        Example:
            >>> StoreBackingReference(DigitalAssetID(0))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be a positive integer.


        :return:
        """

        raw_digital_asset_id: object = self.digital_asset_id
        if isinstance(raw_digital_asset_id, bool) or int(
            self.digital_asset_id
        ) <= 0:
            raise ValueError("digital_asset_id must be a positive integer.")
        raw_preferred_replica_id: object = self.preferred_replica_id
        if self.preferred_replica_id is not None and (
            isinstance(raw_preferred_replica_id, bool)
            or int(self.preferred_replica_id) <= 0
        ):
            raise ValueError(
                "preferred_replica_id must be a positive integer or None."
            )
        if (
            self.materialization_store_ref is not None
            and not isinstance(self.materialization_store_ref, UUID)
        ):
            raise TypeError("materialization_store_ref must be a UUID or None.")


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
    backend_options: tuple[tuple[str, object], ...] = ()
    backing: StoreBackingReference | None = None

    @classmethod
    def for_backend(
        cls,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication_policy: ReplicationPolicyID | None = None,
        backup_policy: BackupPolicyID | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        backing: StoreBackingReference | None = None,
    ) -> Self:
        """
        Build portable configuration without spelling out model fields.

        Path-like roots are rendered as absolute ``file:`` URIs. String roots
        are preserved for remote and backend-native endpoint syntax.

        Example:
            >>> configuration = StoreConfiguration.for_backend(
            ...     "archive", "s3", "s3://books/archive",
            ...     tags={"offsite"},
            ... )
            >>> configuration.store_kind
            's3'


        :param name:
        :param kind:
        :param root:
        :param store_uuid:
        :param url:
        :param protocol:
        :param failure_domain:
        :param region:
        :param host:
        :param device:
        :param tags:
        :param replication_policy:
        :param backup_policy:
        :param modes:
        :param operational_role:
        :param read_only:
        :param folders:
        :param options:
        :param backing:
        :return:
        """

        option_pairs = _option_pairs(options)
        return cls(
            store_uuid=uuid4() if store_uuid is None else store_uuid,
            store_name=name,
            store_kind=kind,
            store_root_uri=_endpoint_text(root),
            store_url=url,
            store_access_protocol=protocol,
            store_failure_domain=failure_domain,
            store_region=region,
            store_host_uuid=host,
            store_device_uuid=device,
            store_tags=tuple(tags),
            store_default_replication_policy_id=replication_policy,
            store_default_backup_policy_id=backup_policy,
            supported_replica_modes=frozenset(
                mode if isinstance(mode, ReplicaMode) else ReplicaMode(mode)
                for mode in modes
            ),
            operational_role=operational_role,
            read_only=read_only,
            supports_folders=folders,
            backend_options=option_pairs,
            backing=backing,
        )

    @classmethod
    def for_backed_backend(
        cls,
        name: str,
        kind: str,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_replica_id: ReplicaID | None = None,
        materialization_store_ref: StoreUUID | None = None,
        store_uuid: StoreUUID | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        tags: Iterable[str] = (),
        modes: Iterable[ReplicaMode | str] = (ReplicaMode.ARCHIVE,),
        operational_role: str | None = "archive",
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
    ) -> Self:
        """
        Build a read-only Store view over one container Asset.

        Manager convenience APIs supply a content-derived stable UUID when
        none is requested. This lower-level value constructor otherwise uses
        the same generated-UUID convention as ``for_backend``. Physical
        Replica selection remains replaceable.

        Example:
            >>> configuration = StoreConfiguration.for_backed_backend(
            ...     "book pack", "zip_readonly", DigitalAssetID(7),
            ...     preferred_replica_id=ReplicaID(12),
            ... )
            >>> configuration.store_root_uri
            'asset://digital-asset/7'


        :param name:
        :param kind:
        :param digital_asset_id:
        :param preferred_replica_id:
        :param materialization_store_ref:
        :param store_uuid:
        :param protocol:
        :param failure_domain:
        :param region:
        :param tags:
        :param modes:
        :param operational_role:
        :param folders:
        :param options:
        :return:
        """

        backing = StoreBackingReference(
            digital_asset_id,
            preferred_replica_id=preferred_replica_id,
            materialization_store_ref=materialization_store_ref,
        )
        return cls.for_backend(
            name,
            kind,
            f"asset://digital-asset/{int(digital_asset_id)}",
            store_uuid=store_uuid,
            protocol=protocol,
            failure_domain=failure_domain,
            region=region,
            tags=tags,
            modes=modes,
            operational_role=operational_role,
            read_only=True,
            folders=folders,
            options=options,
            backing=backing,
        )

    @classmethod
    def filesystem(
        cls,
        name: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication_policy: ReplicationPolicyID | None = None,
        backup_policy: BackupPolicyID | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
    ) -> Self:
        """
        Build configuration for a local transactional filesystem Store.

        Plain paths and ``Path`` objects become absolute ``file:`` URIs;
        existing local file URIs remain valid. Non-file URI schemes are
        rejected early with a configuration-focused error.

        Example:
            >>> configuration = StoreConfiguration.filesystem(
            ...     "primary", Path("/srv/liuxin"),
            ... )
            >>> configuration.store_kind
            'filesystem'


        :param name:
        :param root:
        :param store_uuid:
        :param failure_domain:
        :param region:
        :param host:
        :param device:
        :param tags:
        :param replication_policy:
        :param backup_policy:
        :param modes:
        :param operational_role:
        :param read_only:
        :param options:
        :return:
        """

        return cls.for_backend(
            name,
            "filesystem",
            _filesystem_root_uri(root),
            store_uuid=store_uuid,
            protocol="file",
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=replication_policy,
            backup_policy=backup_policy,
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            folders=True,
            options=options,
        )

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
        if self.backing is not None:
            if not isinstance(self.backing, StoreBackingReference):
                raise TypeError("backing must be a StoreBackingReference or None.")
            if not self.read_only:
                raise ValueError(
                    "a Store backed by a catalogued Asset must be read-only."
                )
            if self.backing.materialization_store_ref == self.store_uuid:
                raise ValueError("a backed Store cannot materialize into itself.")
        for name, uuid_value in (
            ("store_host_uuid", self.store_host_uuid),
            ("store_device_uuid", self.store_device_uuid),
        ):
            if uuid_value is not None and not isinstance(uuid_value, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"{name} must be a UUID or None.")
        for name, text_value in (
            ("store_name", self.store_name),
            ("store_kind", self.store_kind),
            ("store_root_uri", self.store_root_uri),
        ):
            if not text_value.strip():
                raise ValueError(f"{name} must not be empty.")
        option_names: set[str] = set()
        for key, option_value in self.backend_options:
            if not isinstance(key, str) or not key.strip():
                raise ValueError("backend option names must be non-empty strings.")
            if key in option_names:
                raise ValueError(f"duplicate backend option: {key!r}.")
            option_names.add(key)
            if not (
                option_value is None
                or isinstance(option_value, (str, int, float, bool))
                or (
                    isinstance(option_value, tuple)
                    and all(isinstance(item, str) for item in option_value)
                )
            ):
                raise TypeError(
                    "backend option values must be JSON scalars or string tuples."
                )


def _endpoint_text(root: str | os.PathLike[str]) -> str:
    """
    Render a path-like root while preserving endpoint strings.

    Example:
        >>> _endpoint_text("s3://books/archive")
        's3://books/archive'


    :param root:
    :return:
    """

    if isinstance(root, os.PathLike):
        return Path(root).expanduser().resolve(strict=False).as_uri()
    text = str(root).strip()
    if not text:
        raise ValueError("store root must not be empty.")
    return text


def _filesystem_root_uri(root: str | os.PathLike[str]) -> str:
    """
    Render one local filesystem root as a portable file URI.

    Example:
        >>> _filesystem_root_uri("/srv/liuxin").startswith("file:")
        True


    :param root:
    :return:
    """

    if isinstance(root, os.PathLike):
        return Path(root).expanduser().resolve(strict=False).as_uri()
    text = str(root).strip()
    if not text:
        raise ValueError("filesystem Store root must not be empty.")
    parsed = urlparse(text)
    if parsed.scheme:
        if parsed.scheme != "file":
            raise ValueError(
                "filesystem Store root must be a local path or file URI."
            )
        return text
    return Path(text).expanduser().resolve(strict=False).as_uri()


def _option_pairs(
    options: Mapping[str, object] | Iterable[tuple[str, object]],
) -> tuple[tuple[str, object], ...]:
    """
    Freeze mapping or pair input for immutable configuration storage.

    Example:
        >>> _option_pairs({"region": "local"})
        (('region', 'local'),)


    :param options:
    :return:
    """

    if isinstance(options, Mapping):
        mapping = cast(Mapping[str, object], options)
        return tuple(mapping.items())
    return tuple(options)


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
