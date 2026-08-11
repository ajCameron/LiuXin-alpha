"""Asset, replica, ingest, verification, and composite value objects."""

from __future__ import annotations

import dataclasses

from enum import StrEnum
from typing import Optional, Protocol, runtime_checkable

from LiuXin_alpha.storage.api2.models import Location
from LiuXin_alpha.storage.api2.storage_manager_api.models.identifiers import (
    AssetReplicaID,
    CompositeDigitalAssetID,
    DigitalAssetID,
    ItemID,
)


class ReplicaMode(StrEnum):
    """Operational purpose of one concrete asset replica.

    Example:
        >>> ReplicaMode.BACKUP.value
        'backup'
    """

    ACTIVE = "active"
    BACKUP = "backup"
    ARCHIVE = "archive"
    CACHE = "cache"
    TRANSIENT = "transient"
    UNMANAGED = "unmanaged"


class ReplicaState(StrEnum):
    """Observed or expected availability state of one replica.

    Example:
        >>> ReplicaState("verified") is ReplicaState.VERIFIED
        True
    """

    STAGED = "staged"
    PRESENT = "present"
    UNVERIFIED = "unverified"
    VERIFIED = "verified"
    MISSING = "missing"
    CORRUPT = "corrupt"
    UNAVAILABLE = "unavailable"
    DELETED = "deleted"


@runtime_checkable
class DigitalAssetRecordAPI(Protocol):
    """Structural identity of a Digital Asset database record.

    Any database model exposing the property satisfies this protocol.

    Example:
        >>> def asset_id(record: DigitalAssetRecordAPI) -> Optional[DigitalAssetID]:
        ...     return record.digital_asset_id
    """

    @property
    def digital_asset_id(self) -> Optional[DigitalAssetID]:
        """Return the persisted asset identifier, or ``None`` before creation.

        Example:
            >>> asset_id = record.digital_asset_id  # doctest: +SKIP
        """
        ...


@runtime_checkable
class AssetReplicaRecordAPI(Protocol):
    """Structural identity of an Asset Replica database record.

    Example:
        >>> def replica_id(record: AssetReplicaRecordAPI) -> Optional[AssetReplicaID]:
        ...     return record.asset_replica_id
    """

    @property
    def asset_replica_id(self) -> Optional[AssetReplicaID]:
        """Return the persisted replica identifier, or ``None`` before creation.

        Example:
            >>> replica_id = record.asset_replica_id  # doctest: +SKIP
        """
        ...


@runtime_checkable
class CompositeDigitalAssetRecordAPI(Protocol):
    """Structural identity of a Composite Digital Asset database record.

    Example:
        >>> def composite_id(
        ...     record: CompositeDigitalAssetRecordAPI,
        ... ) -> Optional[CompositeDigitalAssetID]:
        ...     return record.composite_digital_asset_id
    """

    @property
    def composite_digital_asset_id(self) -> Optional[CompositeDigitalAssetID]:
        """Return the persisted composite identifier, if one is assigned.

        Example:
            >>> composite_id = record.composite_digital_asset_id  # doctest: +SKIP
        """
        ...


@dataclasses.dataclass(slots=True, frozen=True)
class IngestResult:
    """Atomic result of ingesting or adopting one byte-bearing asset.

    Example:
        >>> from types import SimpleNamespace
        >>> result = IngestResult(
        ...     digital_asset=SimpleNamespace(digital_asset_id=7),
        ...     replica=SimpleNamespace(asset_replica_id=12),
        ...     location=Location("primary", "objects/7"),
        ...     digital_asset_created=True,
        ...     replica_created=True,
        ... )
        >>> result.location.key
        'objects/7'
    """

    digital_asset: DigitalAssetRecordAPI
    replica: AssetReplicaRecordAPI
    location: Location
    digital_asset_created: bool
    replica_created: bool
    deduplicated: bool = False
    verified: bool = False
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaVerificationResult:
    """Detailed comparison between one replica and its Digital Asset.

    Example:
        >>> result = ReplicaVerificationResult(
        ...     asset_replica_id=12, digital_asset_id=7,
        ...     state=ReplicaState.VERIFIED, exists=True,
        ...     size_matches=True, digest_matches=True,
        ... )
        >>> result.healthy
        True
    """

    asset_replica_id: AssetReplicaID
    digital_asset_id: DigitalAssetID
    state: ReplicaState
    exists: Optional[bool]
    size_matches: Optional[bool] = None
    digest_matches: Optional[bool] = None
    observed_size_bytes: Optional[int] = None
    observed_sha256: Optional[str] = None
    checked_timestamp_ep_ms: Optional[int] = None
    errors: tuple[str, ...] = ()

    @property
    def healthy(self) -> bool:
        """Return whether this replica was verified without reported errors.

        Example:
            >>> ReplicaVerificationResult(
            ...     12, 7, ReplicaState.VERIFIED, True,
            ... ).healthy
            True
        """

        return self.state is ReplicaState.VERIFIED and not self.errors


@dataclasses.dataclass(slots=True, frozen=True)
class AssetVerificationResult:
    """Aggregate verification result for one Digital Asset.

    The asset is healthy when at least one replica is verified.

    Example:
        >>> replica = ReplicaVerificationResult(
        ...     12, 7, ReplicaState.VERIFIED, True,
        ... )
        >>> AssetVerificationResult(7, (replica,)).healthy
        True
    """

    digital_asset_id: DigitalAssetID
    replicas: tuple[ReplicaVerificationResult, ...]

    @property
    def healthy(self) -> bool:
        """Return whether at least one replica provides a verified copy.

        Example:
            >>> verified = ReplicaVerificationResult(
            ...     12, 7, ReplicaState.VERIFIED, True,
            ... )
            >>> AssetVerificationResult(7, (verified,)).healthy
            True
        """

        return any(replica.healthy for replica in self.replicas)


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRemovalResult:
    """Outcome of coordinated physical and catalogue replica removal.

    Example:
        >>> result = ReplicaRemovalResult(
        ...     asset_replica_id=12, bytes_deleted=True,
        ...     metadata_deleted=False, tombstone_retained=True,
        ... )
        >>> result.tombstone_retained
        True
    """

    asset_replica_id: AssetReplicaID
    bytes_deleted: bool
    metadata_deleted: bool
    tombstone_retained: bool
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeMemberSpec:
    """One ordered atomic member of a Composite Digital Asset.

    Example:
        >>> member = CompositeMemberSpec(
        ...     digital_asset_id=7, sequence_number=0, role="cover",
        ... )
        >>> member.required
        True
    """

    digital_asset_id: DigitalAssetID
    sequence_number: int
    role: Optional[str] = None
    required: bool = True

    def __post_init__(self) -> None:
        """Reject negative member positions.

        Example:
            >>> CompositeMemberSpec(7, -1)
            Traceback (most recent call last):
            ...
            ValueError: sequence_number must not be negative.
        """

        if self.sequence_number < 0:
            raise ValueError("sequence_number must not be negative.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeAssetHealth:
    """Completeness and member health for one composite asset.

    Example:
        >>> health = CompositeAssetHealth(
        ...     composite_digital_asset_id=3, expected_members=2,
        ...     resolved_members=2, healthy_members=2,
        ... )
        >>> health.healthy
        True
    """

    composite_digital_asset_id: CompositeDigitalAssetID
    expected_members: int
    resolved_members: int
    healthy_members: int
    missing_member_ids: tuple[DigitalAssetID, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def healthy(self) -> bool:
        """Return whether every expected member resolves to a healthy asset.

        Example:
            >>> CompositeAssetHealth(3, 2, 2, 2).healthy
            True
        """

        return (
            self.expected_members == self.resolved_members == self.healthy_members
            and not self.missing_member_ids
            and not self.errors
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ItemAssetSelection:
    """Resolved atomic or composite asset selected for one Item role.

    Exactly one of ``digital_asset_id`` and ``composite_digital_asset_id`` is
    required.

    Example:
        >>> selection = ItemAssetSelection(
        ...     item_id=9, role="primary_payload",
        ...     locations=(Location("primary", "objects/7"),),
        ...     digital_asset_id=7,
        ... )
        >>> selection.locations[0].store_ref
        'primary'
    """

    item_id: ItemID
    role: str
    locations: tuple[Location, ...]
    digital_asset_id: Optional[DigitalAssetID] = None
    composite_digital_asset_id: Optional[CompositeDigitalAssetID] = None

    def __post_init__(self) -> None:
        """Require exactly one atomic or composite selected asset identifier.

        Example:
            >>> ItemAssetSelection(9, "cover", (), digital_asset_id=7)
            ItemAssetSelection(item_id=9, role='cover', locations=(), digital_asset_id=7, composite_digital_asset_id=None)
        """

        if (self.digital_asset_id is None) == (self.composite_digital_asset_id is None):
            raise ValueError("exactly one atomic or composite asset id is required.")


__all__ = [
    "AssetReplicaRecordAPI",
    "AssetVerificationResult",
    "CompositeAssetHealth",
    "CompositeDigitalAssetRecordAPI",
    "CompositeMemberSpec",
    "DigitalAssetRecordAPI",
    "IngestResult",
    "ItemAssetSelection",
    "ReplicaMode",
    "ReplicaRemovalResult",
    "ReplicaState",
    "ReplicaVerificationResult",
]
