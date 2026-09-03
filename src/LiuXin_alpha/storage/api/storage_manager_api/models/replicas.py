"""Replica lifecycle, ingest-result, and verification values."""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from LiuXin_alpha.storage.api.models import Digest, Location
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.storage_manager_api.models.asset_identity import (
    DigitalAssetRecord,
    validate_unique_digests,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetID,
    ReplicaID,
)


class ReplicaMode(StrEnum):
    """Operational purpose of one concrete Replica.

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
    """Observed or expected availability state of one Replica claim.

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


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaObservation:
    """Latest observed physical state for a Replica claim.

    Example:
        >>> observation = ReplicaObservation(
        ...     ReplicaState.VERIFIED, observed_size_bytes=4,
        ... )
        >>> observation.state is ReplicaState.VERIFIED
        True
    """

    state: ReplicaState
    observed_size_bytes: int | None = None
    observed_digests: tuple[Digest, ...] = ()
    checked_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate observation size, digests, and timestamp.

        Example:
            >>> ReplicaObservation(
            ...     ReplicaState.PRESENT, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")
        if self.failure_reason is not None and not self.failure_reason.strip():
            raise ValueError("failure_reason must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaDeclaration:
    """Input for registering one concrete copy of a Digital Asset.

    Example:
        >>> declaration = ReplicaDeclaration(
        ...     DigitalAssetID(7), Location(UUID(int=1), "objects/7"),
        ... )
        >>> declaration.mode is ReplicaMode.ACTIVE
        True
    """

    digital_asset_id: DigitalAssetID
    location: Location
    mode: ReplicaMode = ReplicaMode.ACTIVE
    observation: ReplicaObservation = dataclasses.field(
        default_factory=lambda: ReplicaObservation(ReplicaState.UNVERIFIED)
    )
    placement_hints: StoragePlacementHints | None = dataclasses.field(
        default=None,
        hash=False,
    )

    def __post_init__(self) -> None:
        """Require a positive Digital Asset identifier.

        Example:
            >>> ReplicaDeclaration(
            ...     DigitalAssetID(0), Location(UUID(int=1), "bad"),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRecord:
    """Manager-maintained claim about one concrete Asset copy.

    ``placement_hints`` is the advisory metadata snapshot requested when this
    Replica was allocated and published. It belongs to the placement, not to
    the byte identity, and can seed later replication even when the original
    Store did not interpret rich hints.

    Example:
        >>> record = ReplicaRecord(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> record.state is ReplicaState.VERIFIED
        True
    """

    replica_id: ReplicaID
    digital_asset_id: DigitalAssetID
    location: Location
    mode: ReplicaMode
    observation: ReplicaObservation
    revision: str | None = None
    placement_hints: StoragePlacementHints | None = dataclasses.field(
        default=None,
        hash=False,
    )

    def __post_init__(self) -> None:
        """Validate identifiers and optional optimistic-lock revision.

        Example:
            >>> ReplicaRecord(
            ...     ReplicaID(0), DigitalAssetID(7),
            ...     Location(UUID(int=1), "bad"), ReplicaMode.ACTIVE,
            ...     ReplicaObservation(ReplicaState.UNVERIFIED),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: replica_id must be positive.
        """

        if self.replica_id <= 0:
            raise ValueError("replica_id must be positive.")
        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")

    @property
    def state(self) -> ReplicaState:
        """Return the latest observed state of this Replica.

        Example:
            >>> replica.state is replica.observation.state  # doctest: +SKIP
            True
        """

        return self.observation.state


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetIngestResult:
    """Outcome of publishing bytes and registering manager records.

    Example:
        >>> result.location == result.replica_record.location  # doctest: +SKIP
        True
    """

    operation_id: UUID
    asset_record: DigitalAssetRecord
    replica_record: ReplicaRecord
    asset_created: bool
    replica_created: bool
    deduplicated: bool = False
    verified: bool = False
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Require a UUID and matching Asset and Replica records.

        Example:
            >>> result = DigitalAssetIngestResult(  # doctest: +SKIP
            ...     UUID(int=2), asset, replica, True, True,
            ... )
        """

        if not isinstance(self.operation_id, UUID):
            raise TypeError("operation_id must be a UUID.")
        if (
            self.replica_record.digital_asset_id
            != self.asset_record.digital_asset_id
        ):
            raise ValueError("ingested Replica does not belong to the Asset.")

    @property
    def location(self) -> Location:
        """Return the concrete Location carried by the resulting Replica.

        Example:
            >>> result.location.key  # doctest: +SKIP
            'objects/7'
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaVerificationReport:
    """Observed comparison of one Replica with its Asset identity.

    Example:
        >>> report = ReplicaVerificationReport(
        ...     ReplicaID(12), DigitalAssetID(7), ReplicaState.VERIFIED,
        ...     True, size_matches=True, digest_matches=True,
        ... )
        >>> report.healthy
        True
    """

    replica_id: ReplicaID
    digital_asset_id: DigitalAssetID
    state: ReplicaState
    exists: bool | None
    size_matches: bool | None = None
    digest_matches: bool | None = None
    observed_size_bytes: int | None = None
    observed_digests: tuple[Digest, ...] = ()
    checked_at: datetime | None = None
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate observed size, digests, and verification timestamp.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(1), DigitalAssetID(2), ReplicaState.PRESENT,
            ...     True, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")

    @property
    def healthy(self) -> bool:
        """Return whether verification confirmed the expected bytes.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(12), DigitalAssetID(7),
            ...     ReplicaState.VERIFIED, True,
            ... ).healthy
            True
        """

        return self.state is ReplicaState.VERIFIED and not self.errors


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetVerificationReport:
    """Aggregate verification results for one Digital Asset.

    Example:
        >>> report = DigitalAssetVerificationReport(
        ...     DigitalAssetID(7), (),
        ... )
        >>> report.readable
        False
    """

    digital_asset_id: DigitalAssetID
    replica_reports: tuple[ReplicaVerificationReport, ...]

    @property
    def readable(self) -> bool:
        """Return whether at least one checked Replica was healthy.

        Example:
            >>> DigitalAssetVerificationReport(
            ...     DigitalAssetID(7), (),
            ... ).readable
            False
        """

        return any(report.healthy for report in self.replica_reports)


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRemovalReport:
    """Outcome of coordinated byte deletion and record mutation.

    Example:
        >>> report = ReplicaRemovalReport(
        ...     ReplicaID(12), True, False, True,
        ... )
        >>> report.tombstone_retained
        True
    """

    replica_id: ReplicaID
    bytes_deleted: bool
    replica_forgotten: bool
    tombstone_retained: bool
    warnings: tuple[str, ...] = ()


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """Reject a timestamp without an unambiguous timezone.

    Example:
        >>> _require_aware_datetime(None, "checked_at")
    """

    if value is not None and (
        value.tzinfo is None or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware.")


__all__ = [
    "DigitalAssetIngestResult",
    "DigitalAssetVerificationReport",
    "ReplicaDeclaration",
    "ReplicaMode",
    "ReplicaObservation",
    "ReplicaRecord",
    "ReplicaRemovalReport",
    "ReplicaState",
    "ReplicaVerificationReport",
]
