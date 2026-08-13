"""Asset, Replica, ingest, verification, and composite domain values."""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from LiuXin_alpha.storage.api.models import Digest, Location
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    CompositeDigitalAssetID,
    DigitalAssetID,
    ItemID,
    ReplicationPolicyID,
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
class DigitalAssetMetadata:
    """Descriptive and technical metadata belonging to a Digital Asset.

    These values describe the byte-bearing object, not a database row, Item,
    bibliographic record, or physical Replica.

    Example:
        >>> metadata = DigitalAssetMetadata(
        ...     media_type="application/epub+zip", original_name="book.epub",
        ... )
        >>> metadata.original_name
        'book.epub'
    """

    name: str | None = None
    media_type: str | None = None
    original_name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Reject empty labels and duplicate extension-attribute names.

        Example:
            >>> DigitalAssetMetadata(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty when supplied.
        """

        for field_name, value in (
            ("name", self.name),
            ("media_type", self.media_type),
            ("original_name", self.original_name),
        ):
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be empty when supplied.")
        names = [name for name, _ in self.attributes]
        if any(not name.strip() for name in names):
            raise ValueError("asset attribute names must not be empty.")
        if len(names) != len(set(names)):
            raise ValueError("asset attribute names must be unique.")


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetSpec:
    """Input for declaring a known atomic byte sequence as a Digital Asset.

    A specification has no database identifier or persistence revision. It is
    therefore not a partially populated record.

    Example:
        >>> spec = DigitalAssetSpec(4, (Digest("sha256", "abcd"),))
        >>> spec.size_bytes
        4
    """

    size_bytes: int
    digests: tuple[Digest, ...]
    metadata: DigitalAssetMetadata = dataclasses.field(
        default_factory=DigitalAssetMetadata
    )
    replication_policy_id: ReplicationPolicyID | None = None
    backup_policy_id: BackupPolicyID | None = None

    def __post_init__(self) -> None:
        """Require a non-negative size and at least one unambiguous digest.

        Example:
            >>> DigitalAssetSpec(1, ())
            Traceback (most recent call last):
            ...
            ValueError: a Digital Asset requires at least one digest.
        """

        _validate_asset_identity(self.size_bytes, self.digests)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAsset:
    """Immutable domain snapshot identifying one expected byte sequence.

    A Digital Asset is neither its persistence record nor its byte stream.
    Repositories load and save snapshots; Replicas say where readable copies
    should exist; ``open_read`` supplies the actual bytes.

    Example:
        >>> asset = DigitalAsset(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> asset.digital_asset_id
        7
    """

    digital_asset_id: DigitalAssetID
    size_bytes: int
    digests: tuple[Digest, ...]
    metadata: DigitalAssetMetadata = dataclasses.field(
        default_factory=DigitalAssetMetadata
    )
    replication_policy_id: ReplicationPolicyID | None = None
    backup_policy_id: BackupPolicyID | None = None
    revision: str | None = None

    def __post_init__(self) -> None:
        """Validate domain identity and optional optimistic-lock revision.

        Example:
            >>> DigitalAsset(DigitalAssetID(0), 1, (Digest("sha256", "a"),))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        _validate_asset_identity(self.size_bytes, self.digests)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaObservation:
    """Latest observed physical state for a Replica claim.

    Expected size and digests remain on the Digital Asset. These values record
    what was actually observed at the Replica's Location.

    Example:
        >>> observation = ReplicaObservation(
        ...     ReplicaState.VERIFIED, observed_size_bytes=4,
        ...     observed_digests=(Digest("sha256", "abcd"),),
        ... )
        >>> observation.state
        <ReplicaState.VERIFIED: 'verified'>
    """

    state: ReplicaState
    observed_size_bytes: int | None = None
    observed_digests: tuple[Digest, ...] = ()
    checked_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate observation size, digest algorithms, and timestamp.

        Example:
            >>> ReplicaObservation(ReplicaState.PRESENT, observed_size_bytes=-1)
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        _validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")
        if self.failure_reason is not None and not self.failure_reason.strip():
            raise ValueError("failure_reason must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaSpec:
    """Input for registering a concrete copy of a Digital Asset.

    Example:
        >>> spec = ReplicaSpec(
        ...     DigitalAssetID(7), Location(UUID(int=1), "objects/7"),
        ... )
        >>> spec.mode is ReplicaMode.ACTIVE
        True
    """

    digital_asset_id: DigitalAssetID
    location: Location
    mode: ReplicaMode = ReplicaMode.ACTIVE
    observation: ReplicaObservation = dataclasses.field(
        default_factory=lambda: ReplicaObservation(ReplicaState.UNVERIFIED)
    )

    def __post_init__(self) -> None:
        """Require a positive Digital Asset identifier.

        Example:
            >>> ReplicaSpec(DigitalAssetID(0), Location(UUID(int=1), "bad"))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")


@dataclasses.dataclass(slots=True, frozen=True)
class Replica:
    """Immutable domain snapshot of one claimed stored copy.

    The Replica points to a Digital Asset for expected content identity and to
    a Location for physical addressing. It is not the bytes and is not an ORM
    record.

    Example:
        >>> replica = Replica(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> replica.state
        <ReplicaState.VERIFIED: 'verified'>
    """

    replica_id: ReplicaID
    digital_asset_id: DigitalAssetID
    location: Location
    mode: ReplicaMode
    observation: ReplicaObservation
    revision: str | None = None

    def __post_init__(self) -> None:
        """Validate identifiers and optional optimistic-lock revision.

        Example:
            >>> Replica(
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
class IngestResult:
    """Completed result of one recoverable ingest operation.

    Store publication and repository persistence cannot form one physical
    transaction. ``operation_id`` lets an implementation resume or reconcile
    a failure between those boundaries; the returned Asset and Replica are
    domain snapshots, not database records.

    Example:
        >>> asset = DigitalAsset(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> replica = Replica(
        ...     ReplicaID(12), asset.digital_asset_id,
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> IngestResult(UUID(int=2), asset, replica, True, True).location.key
        'objects/7'
    """

    operation_id: UUID
    asset: DigitalAsset
    replica: Replica
    asset_created: bool
    replica_created: bool
    deduplicated: bool = False
    verified: bool = False
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Require a UUID and an Asset/Replica pair with matching identity.

        Example:
            >>> IngestResult(UUID(int=2), asset, replica, True, True)  # doctest: +SKIP
        """

        if not isinstance(self.operation_id, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("operation_id must be a UUID.")
        if self.replica.digital_asset_id != self.asset.digital_asset_id:
            raise ValueError("ingested Replica does not belong to the Asset.")

    @property
    def location(self) -> Location:
        """Return the concrete Location carried by the resulting Replica.

        Example:
            >>> result.location == result.replica.location  # doctest: +SKIP
            True
        """

        return self.replica.location


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaVerificationResult:
    """Detailed comparison between one Replica and its Digital Asset.

    Example:
        >>> result = ReplicaVerificationResult(
        ...     ReplicaID(12), DigitalAssetID(7), ReplicaState.VERIFIED,
        ...     True, size_matches=True, digest_matches=True,
        ... )
        >>> result.healthy
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
        """Validate observed size and verification timestamp.

        Example:
            >>> ReplicaVerificationResult(
            ...     ReplicaID(1), DigitalAssetID(2), ReplicaState.PRESENT,
            ...     True, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        _validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")

    @property
    def healthy(self) -> bool:
        """Return whether verification confirmed the expected bytes.

        Example:
            >>> ReplicaVerificationResult(
            ...     ReplicaID(12), DigitalAssetID(7),
            ...     ReplicaState.VERIFIED, True,
            ... ).healthy
            True
        """

        return self.state is ReplicaState.VERIFIED and not self.errors


@dataclasses.dataclass(slots=True, frozen=True)
class AssetVerificationResult:
    """Aggregate verification result for one Digital Asset.

    Example:
        >>> verified = ReplicaVerificationResult(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     ReplicaState.VERIFIED, True,
        ... )
        >>> AssetVerificationResult(DigitalAssetID(7), (verified,)).readable
        True
    """

    digital_asset_id: DigitalAssetID
    replicas: tuple[ReplicaVerificationResult, ...]

    @property
    def readable(self) -> bool:
        """Return whether at least one Replica was verified readable.

        Example:
            >>> result.readable  # doctest: +SKIP
            True
        """

        return any(replica.healthy for replica in self.replicas)


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRemovalResult:
    """Outcome of coordinated byte deletion and Replica-state mutation.

    Example:
        >>> result = ReplicaRemovalResult(
        ...     ReplicaID(12), bytes_deleted=True,
        ...     replica_forgotten=False, tombstone_retained=True,
        ... )
        >>> result.tombstone_retained
        True
    """

    replica_id: ReplicaID
    bytes_deleted: bool
    replica_forgotten: bool
    tombstone_retained: bool
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeMemberSpec:
    """One ordered atomic member of a Composite Digital Asset.

    Example:
        >>> member = CompositeMemberSpec(
        ...     DigitalAssetID(7), 0, role="cover", logical_name="cover.jpg",
        ... )
        >>> member.required
        True
    """

    digital_asset_id: DigitalAssetID
    sequence_number: int
    role: str | None = None
    logical_name: str | None = None
    logical_path: str | None = None
    title: str | None = None
    required: bool = True

    def __post_init__(self) -> None:
        """Reject invalid member positions and empty optional labels.

        Example:
            >>> CompositeMemberSpec(DigitalAssetID(7), -1)
            Traceback (most recent call last):
            ...
            ValueError: sequence_number must not be negative.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if self.sequence_number < 0:
            raise ValueError("sequence_number must not be negative.")
        for field_name, value in (
            ("role", self.role),
            ("logical_name", self.logical_name),
            ("logical_path", self.logical_path),
            ("title", self.title),
        ):
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be empty when supplied.")
            if value is not None and "\x00" in value:
                raise ValueError(f"{field_name} must not contain NUL characters.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetSpec:
    """Input for declaring an ordered logical assembly of atomic Assets.

    Example:
        >>> spec = CompositeDigitalAssetSpec(
        ...     members=(CompositeMemberSpec(DigitalAssetID(7), 0),),
        ...     name="disc one",
        ... )
        >>> len(spec.members)
        1
    """

    members: tuple[CompositeMemberSpec, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Require unique, contiguous sequence numbers.

        Example:
            >>> CompositeDigitalAssetSpec(())
            Traceback (most recent call last):
            ...
            ValueError: a Composite Digital Asset requires at least one member.
        """

        _validate_composite_members(self.members)
        if self.name is not None and not self.name.strip():
            raise ValueError("name must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAsset:
    """Immutable domain snapshot of one logical multipart Asset.

    Composite Digital Assets do not directly contain bytes or own Replicas;
    their atomic members do.

    Example:
        >>> composite = CompositeDigitalAsset(
        ...     CompositeDigitalAssetID(3),
        ...     (CompositeMemberSpec(DigitalAssetID(7), 0),),
        ... )
        >>> composite.composite_id
        3
    """

    composite_id: CompositeDigitalAssetID
    members: tuple[CompositeMemberSpec, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()
    revision: str | None = None

    def __post_init__(self) -> None:
        """Validate identity, members, and optional revision.

        Example:
            >>> CompositeDigitalAsset(
            ...     CompositeDigitalAssetID(0),
            ...     (CompositeMemberSpec(DigitalAssetID(7), 0),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: composite_id must be positive.
        """

        if self.composite_id <= 0:
            raise ValueError("composite_id must be positive.")
        _validate_composite_members(self.members)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ResolvedAsset:
    """One Digital Asset paired with the selected readable Replica.

    Example:
        >>> resolved.location == resolved.replica.location  # doctest: +SKIP
        True
    """

    asset: DigitalAsset
    replica: Replica

    def __post_init__(self) -> None:
        """Require the selected Replica to belong to the paired Asset.

        Example:
            >>> ResolvedAsset(asset, wrong_replica)  # doctest: +SKIP
            Traceback (most recent call last):
            ...
            ValueError: Replica does not belong to the resolved Digital Asset.
        """

        if self.asset.digital_asset_id != self.replica.digital_asset_id:
            raise ValueError(
                "Replica does not belong to the resolved Digital Asset."
            )

    @property
    def location(self) -> Location:
        """Return the selected Replica Location.

        Example:
            >>> location = resolved.location  # doctest: +SKIP
        """

        return self.replica.location


@dataclasses.dataclass(slots=True, frozen=True)
class ResolvedCompositeMember:
    """One named Composite member paired with its resolved Asset and Replica.

    Example:
        >>> member.location == member.resolved.location  # doctest: +SKIP
        True
    """

    member: CompositeMemberSpec
    resolved: ResolvedAsset

    def __post_init__(self) -> None:
        """Require resolution of the member declared by the relationship.

        Example:
            >>> ResolvedCompositeMember(member, wrong)  # doctest: +SKIP
            Traceback (most recent call last):
            ...
            ValueError: resolved Asset does not match the Composite member.
        """

        if self.member.digital_asset_id != self.resolved.asset.digital_asset_id:
            raise ValueError(
                "resolved Asset does not match the Composite member."
            )

    @property
    def location(self) -> Location:
        """Return the selected Location for this member.

        Example:
            >>> location = member.location  # doctest: +SKIP
        """

        return self.resolved.location


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeAssetHealth:
    """Completeness and member readability for one Composite Asset.

    Example:
        >>> health = CompositeAssetHealth(
        ...     CompositeDigitalAssetID(3), 2, 2, 2,
        ... )
        >>> health.readable
        True
    """

    composite_id: CompositeDigitalAssetID
    expected_members: int
    resolved_members: int
    readable_members: int
    missing_member_ids: tuple[DigitalAssetID, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def readable(self) -> bool:
        """Return whether every required member has a readable Replica.

        Example:
            >>> CompositeAssetHealth(CompositeDigitalAssetID(3), 2, 2, 2).readable
            True
        """

        return (
            self.expected_members
            == self.resolved_members
            == self.readable_members
            and not self.missing_member_ids
            and not self.errors
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ItemAssetSelection:
    """Resolved atomic or Composite Asset selected for one Item role.

    Exactly one atomic or Composite selection is present. Composite resolution
    retains each membership relationship rather than flattening it to an
    unlabelled list of Locations.

    Example:
        >>> selection = ItemAssetSelection(
        ...     ItemID(9), "cover", resolved_asset=resolved,
        ... )  # doctest: +SKIP
    """

    item_id: ItemID
    role: str
    resolved_asset: ResolvedAsset | None = None
    composite: CompositeDigitalAsset | None = None
    resolved_members: tuple[ResolvedCompositeMember, ...] = ()

    def __post_init__(self) -> None:
        """Require exactly one selected Asset and consistent resolutions.

        Example:
            >>> ItemAssetSelection(ItemID(9), "cover")
            Traceback (most recent call last):
            ...
            ValueError: exactly one atomic or Composite Asset is required.
        """

        if (self.resolved_asset is None) == (self.composite is None):
            raise ValueError(
                "exactly one atomic or Composite Asset is required."
            )
        if not self.role.strip():
            raise ValueError("role must not be empty.")
        if self.item_id <= 0:
            raise ValueError("item_id must be positive.")
        if self.resolved_asset is not None and self.resolved_members:
            raise ValueError(
                "an atomic Item selection must not contain Composite members."
            )
        if self.composite is not None:
            declared_members = set(self.composite.members)
            resolved_relationships = {
                member.member
                for member in self.resolved_members
            }
            if not resolved_relationships <= declared_members:
                raise ValueError(
                    "resolved member does not belong to the selected Composite."
                )
            required_members = {
                member
                for member in self.composite.members
                if member.required
            }
            if not required_members <= resolved_relationships:
                raise ValueError(
                    "a required Composite member has not been resolved."
                )

    @property
    def locations(self) -> tuple[Location, ...]:
        """Return the selected readable Locations in delivery order.

        Example:
            >>> locations = selection.locations  # doctest: +SKIP
        """

        if self.resolved_asset is not None:
            return (self.resolved_asset.location,)
        return tuple(member.location for member in self.resolved_members)


def _validate_asset_identity(size_bytes: int, digests: tuple[Digest, ...]) -> None:
    """Validate the size-and-digest identity shared by Asset input and output.

    Example:
        >>> _validate_asset_identity(1, (Digest("sha256", "aa"),))
    """

    if size_bytes < 0:
        raise ValueError("size_bytes must not be negative.")
    if not digests:
        raise ValueError("a Digital Asset requires at least one digest.")
    _validate_unique_digests(digests)


def _validate_unique_digests(digests: tuple[Digest, ...]) -> None:
    """Require at most one digest value per algorithm.

    Example:
        >>> _validate_unique_digests((Digest("sha256", "aa"),))
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


def _validate_composite_members(
    members: tuple[CompositeMemberSpec, ...],
) -> None:
    """Require a non-empty, uniquely ordered Composite membership.

    Example:
        >>> _validate_composite_members(
        ...     (CompositeMemberSpec(DigitalAssetID(7), 0),),
        ... )
    """

    if not members:
        raise ValueError(
            "a Composite Digital Asset requires at least one member."
        )
    positions = sorted(member.sequence_number for member in members)
    if positions != list(range(len(members))):
        raise ValueError(
            "Composite member sequence numbers must be unique and contiguous."
        )


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
    "AssetVerificationResult",
    "CompositeAssetHealth",
    "CompositeDigitalAsset",
    "CompositeDigitalAssetSpec",
    "CompositeMemberSpec",
    "DigitalAsset",
    "DigitalAssetMetadata",
    "DigitalAssetSpec",
    "IngestResult",
    "ItemAssetSelection",
    "Replica",
    "ReplicaMode",
    "ReplicaObservation",
    "ReplicaRemovalResult",
    "ReplicaSpec",
    "ReplicaState",
    "ReplicaVerificationResult",
    "ResolvedAsset",
    "ResolvedCompositeMember",
]
