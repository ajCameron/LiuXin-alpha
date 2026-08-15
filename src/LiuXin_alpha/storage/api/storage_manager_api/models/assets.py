"""
Asset, Replica, ingest, verification, and composite domain values.
"""

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
    """
    Operational purpose of one concrete Replica.

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
    """
    Observed or expected availability state of one Replica claim.

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
    """
    Descriptive and technical metadata belonging to a Digital Asset.

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
        """
        Reject empty labels and duplicate extension-attribute names.

        Example:
            >>> DigitalAssetMetadata(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty when supplied.


        :return:
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
class DigitalAssetDeclaration:
    """
    Input for declaring a known atomic byte sequence as a Digital Asset.

    A declaration has no manager identifier or persistence revision.
    It is therefore not a partially populated record.

    Example:
        >>> declaration = DigitalAssetDeclaration(
        ...     4, (Digest("sha256", "abcd"),),
        ... )
        >>> declaration.size_bytes
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
        """
        Require a non-negative size and at least one unambiguous digest.

        Example:
            >>> DigitalAssetDeclaration(1, ())
            Traceback (most recent call last):
            ...
            ValueError: a Digital Asset requires at least one digest.


        :return:
        """

        _validate_asset_identity(self.size_bytes, self.digests)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetRecord:
    """
    Manager-maintained facts about one Digital Asset.

    This record is neither the Digital Asset's byte stream nor a database row.
    Repositories translate their private persistence representation into this
    public value; Replica records say where copies should exist and
    ``open_read`` supplies the actual bytes.

    Example:
        >>> record = DigitalAssetRecord(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> record.digital_asset_id
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
        """
        Validate domain identity and optional optimistic-lock revision.

        Example:
            >>> DigitalAssetRecord(DigitalAssetID(0), 1, (Digest("sha256", "a"),))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return:
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        _validate_asset_identity(self.size_bytes, self.digests)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaObservation:
    """
    Latest observed physical state for a Replica claim.

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
        """
        Validate observation size, digest algorithms, and timestamp.

        Example:
            >>> ReplicaObservation(ReplicaState.PRESENT, observed_size_bytes=-1)
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.


        :return:
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        _validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")
        if self.failure_reason is not None and not self.failure_reason.strip():
            raise ValueError("failure_reason must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaDeclaration:
    """
    Input for registering a concrete copy of a Digital Asset.

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

    def __post_init__(self) -> None:
        """
        Require a positive Digital Asset identifier.

        Example:
            >>> ReplicaDeclaration(
            ...     DigitalAssetID(0), Location(UUID(int=1), "bad"),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return:
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRecord:
    """
    Manager-maintained claim about one concrete Replica.

    The record links a Digital Asset identity to a Location and records the
    latest physical observation.
    It is neither the stored bytes nor a database row.

    Example:
        >>> record = ReplicaRecord(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> record.state
        <ReplicaState.VERIFIED: 'verified'>
    """

    replica_id: ReplicaID
    digital_asset_id: DigitalAssetID
    location: Location
    mode: ReplicaMode
    observation: ReplicaObservation
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Validate identifiers and optional optimistic-lock revision.

        Example:
            >>> ReplicaRecord(
            ...     ReplicaID(0), DigitalAssetID(7),
            ...     Location(UUID(int=1), "bad"), ReplicaMode.ACTIVE,
            ...     ReplicaObservation(ReplicaState.UNVERIFIED),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: replica_id must be positive.


        :return:
        """

        if self.replica_id <= 0:
            raise ValueError("replica_id must be positive.")
        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")

    @property
    def state(self) -> ReplicaState:
        """
        Return the latest observed state of this Replica.

        Example:
            >>> replica.state is replica.observation.state  # doctest: +SKIP
            True


        :return:
        """

        return self.observation.state


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetIngestResult:
    """
    Outcome of publishing bytes and registering their manager records.

    Store publication and repository persistence cannot form one physical
    transaction. ``operation_id`` lets an implementation resume or reconcile
    a failure between those boundaries.

    Example:
        >>> asset_record = DigitalAssetRecord(
        ...     DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ... )
        >>> replica_record = ReplicaRecord(
        ...     ReplicaID(12), asset_record.digital_asset_id,
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> DigitalAssetIngestResult(
        ...     UUID(int=2), asset_record, replica_record, True, True,
        ... ).location.key
        'objects/7'
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
        """
        Require a UUID and matching Asset and Replica records.

        Example:
            >>> DigitalAssetIngestResult(  # doctest: +SKIP
            ...     UUID(int=2), asset_record, replica_record, True, True,
            ... )


        :return:
        """

        if not isinstance(self.operation_id, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("operation_id must be a UUID.")
        if (
            self.replica_record.digital_asset_id
            != self.asset_record.digital_asset_id
        ):
            raise ValueError("ingested Replica does not belong to the Asset.")

    @property
    def location(self) -> Location:
        """
        Return the concrete Location carried by the resulting Replica.

        Example:
            >>> result.location == result.replica_record.location  # doctest: +SKIP
            True


        :return:
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaVerificationReport:
    """
    Observed comparison of one Replica with its Digital Asset record.

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
        """
        Validate observed size and verification timestamp.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(1), DigitalAssetID(2), ReplicaState.PRESENT,
            ...     True, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.


        :return:
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        _validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")

    @property
    def healthy(self) -> bool:
        """
        Return whether verification confirmed the expected bytes.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(12), DigitalAssetID(7),
            ...     ReplicaState.VERIFIED, True,
            ... ).healthy
            True


        :return:
        """

        return self.state is ReplicaState.VERIFIED and not self.errors


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetVerificationReport:
    """
    Aggregate verification report for one Digital Asset.

    Example:
        >>> verified = ReplicaVerificationReport(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     ReplicaState.VERIFIED, True,
        ... )
        >>> DigitalAssetVerificationReport(
        ...     DigitalAssetID(7), (verified,),
        ... ).readable
        True
    """

    digital_asset_id: DigitalAssetID
    replica_reports: tuple[ReplicaVerificationReport, ...]

    @property
    def readable(self) -> bool:
        """
        Return whether at least one Replica was verified readable.

        Example:
            >>> report.readable  # doctest: +SKIP
            True


        :return:
        """

        return any(report.healthy for report in self.replica_reports)


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRemovalReport:
    """
    Outcome of coordinated byte deletion and Replica-record mutation.

    A retained tombstone is a deliberately preserved Replica record whose
    state is ``DELETED``. It prevents later reconciliation from mistaking an
    intentional deletion for an unexplained missing copy.

    Example:
        >>> report = ReplicaRemovalReport(
        ...     ReplicaID(12), bytes_deleted=True,
        ...     replica_forgotten=False, tombstone_retained=True,
        ... )
        >>> report.tombstone_retained
        True
    """

    replica_id: ReplicaID
    bytes_deleted: bool
    replica_forgotten: bool
    tombstone_retained: bool
    warnings: tuple[str, ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMembership:
    """
    One ordered atomic member of a Composite Digital Asset.

    Example:
        >>> member = CompositeDigitalAssetMembership(
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
        """
        Reject invalid member positions and empty optional labels.

        Example:
            >>> CompositeDigitalAssetMembership(DigitalAssetID(7), -1)
            Traceback (most recent call last):
            ...
            ValueError: sequence_number must not be negative.


        :return:
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
class CompositeDigitalAssetDeclaration:
    """
    Input for declaring an ordered logical assembly of atomic Assets.

    Example:
        >>> declaration = CompositeDigitalAssetDeclaration(
        ...     members=(CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ...     name="disc one",
        ... )
        >>> len(declaration.members)
        1
    """

    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Require unique, contiguous sequence numbers.

        Example:
            >>> CompositeDigitalAssetDeclaration(())
            Traceback (most recent call last):
            ...
            ValueError: a Composite Digital Asset requires at least one member.


        :return:
        """

        _validate_composite_members(self.members)
        if self.name is not None and not self.name.strip():
            raise ValueError("name must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetRecord:
    """
    Manager-maintained facts about one Composite Digital Asset.

    Composite Digital Assets do not directly contain bytes or own Replicas;
    their atomic members do.

    Example:
        >>> record = CompositeDigitalAssetRecord(
        ...     CompositeDigitalAssetID(3),
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> record.composite_digital_asset_id
        3
    """

    composite_digital_asset_id: CompositeDigitalAssetID
    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Validate identity, members, and optional revision.

        Example:
            >>> CompositeDigitalAssetRecord(
            ...     CompositeDigitalAssetID(0),
            ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: composite_digital_asset_id must be positive.


        :return:
        """

        if self.composite_digital_asset_id <= 0:
            raise ValueError("composite_digital_asset_id must be positive.")
        _validate_composite_members(self.members)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


# Todo: This does not seem to actually do what it claims...
@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetResolution:
    """
    Digital Asset and Replica records selected for readable access.

    Example:
        >>> resolution.location == resolution.replica_record.location  # doctest: +SKIP
        True
    """

    asset_record: DigitalAssetRecord
    replica_record: ReplicaRecord

    def __post_init__(self) -> None:
        """
        Require the selected Replica to belong to the paired Asset.

        Example:
            >>> DigitalAssetResolution(  # doctest: +SKIP
            ...     asset_record, wrong_replica_record,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: Replica does not belong to the resolved Digital Asset.


        :return:
        """

        if (
            self.asset_record.digital_asset_id
            != self.replica_record.digital_asset_id
        ):
            raise ValueError(
                "Replica does not belong to the resolved Digital Asset."
            )

    @property
    def location(self) -> Location:
        """
        Return the selected Replica Location.

        Example:
            >>> location = resolved.location  # doctest: +SKIP


        :return:
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMemberResolution:
    """
    One Composite membership paired with readable Asset and Replica records.

    Example:
        >>> member.location == member.resolution.location  # doctest: +SKIP
        True
    """

    membership: CompositeDigitalAssetMembership
    resolution: DigitalAssetResolution

    def __post_init__(self) -> None:
        """
        Require resolution of the member declared by the relationship.

        Example:
            >>> CompositeDigitalAssetMemberResolution(  # doctest: +SKIP
            ...     membership, wrong_resolution,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: resolved Asset does not match the Composite member.


        :return:
        """

        if (
            self.membership.digital_asset_id
            != self.resolution.asset_record.digital_asset_id
        ):
            raise ValueError(
                "resolved Asset does not match the Composite member."
            )

    @property
    def location(self) -> Location:
        """
        Return the selected Location for this member.

        Example:
            >>> location = member.location  # doctest: +SKIP


        :return:
        """

        return self.resolution.location


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetAvailabilityAssessment:
    """
    Completeness and required-member readability for one Composite Asset.

    The three member counts and failure details cover required memberships.
    Optional members do not make the Composite unreadable when absent.

    Example:
        >>> assessment = CompositeDigitalAssetAvailabilityAssessment(
        ...     CompositeDigitalAssetID(3), 2, 2, 2,
        ... )
        >>> assessment.readable
        True
    """

    composite_digital_asset_id: CompositeDigitalAssetID
    expected_members: int
    resolved_members: int
    readable_members: int
    missing_digital_asset_ids: tuple[DigitalAssetID, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def readable(self) -> bool:
        """
        Return whether every required member has a readable Replica.

        Example:
            >>> CompositeDigitalAssetAvailabilityAssessment(
            ...     CompositeDigitalAssetID(3), 2, 2, 2,
            ... ).readable
            True


        :return:
        """

        return (
            self.expected_members
            == self.resolved_members
            == self.readable_members
            and not self.missing_digital_asset_ids
            and not self.errors
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ItemDigitalAssetResolution:
    """
    Resolved atomic or Composite Asset selected for one Item role.

    Exactly one atomic or Composite selection is present. Composite resolution
    retains each membership relationship rather than flattening it to an
    unlabelled list of Locations.

    Example:
        >>> selection = ItemDigitalAssetResolution(
        ...     ItemID(9), "cover", digital_asset_resolution=resolution,
        ... )  # doctest: +SKIP
    """

    item_id: ItemID
    role: str
    digital_asset_resolution: DigitalAssetResolution | None = None
    composite_digital_asset_record: CompositeDigitalAssetRecord | None = None
    composite_member_resolutions: tuple[
        CompositeDigitalAssetMemberResolution, ...
    ] = ()

    def __post_init__(self) -> None:
        """
        Require exactly one selected Asset and consistent resolutions.

        Example:
            >>> ItemDigitalAssetResolution(ItemID(9), "cover")
            Traceback (most recent call last):
            ...
            ValueError: exactly one atomic or Composite Asset is required.


        :return:
        """

        if (self.digital_asset_resolution is None) == (
            self.composite_digital_asset_record is None
        ):
            raise ValueError(
                "exactly one atomic or Composite Asset is required."
            )
        if not self.role.strip():
            raise ValueError("role must not be empty.")
        if self.item_id <= 0:
            raise ValueError("item_id must be positive.")
        if (
            self.digital_asset_resolution is not None
            and self.composite_member_resolutions
        ):
            raise ValueError(
                "an atomic Item selection must not contain Composite members."
            )
        if self.composite_digital_asset_record is not None:
            declared_members = set(self.composite_digital_asset_record.members)
            resolved_relationships = {
                member.membership
                for member in self.composite_member_resolutions
            }
            if not resolved_relationships <= declared_members:
                raise ValueError(
                    "resolved member does not belong to the selected Composite."
                )
            required_members = {
                member
                for member in self.composite_digital_asset_record.members
                if member.required
            }
            if not required_members <= resolved_relationships:
                raise ValueError(
                    "a required Composite member has not been resolved."
                )

    @property
    def locations(self) -> tuple[Location, ...]:
        """
        Return the selected readable Locations in delivery order.

        Example:
            >>> locations = selection.locations  # doctest: +SKIP


        :return:
        """

        if self.digital_asset_resolution is not None:
            return (self.digital_asset_resolution.location,)
        return tuple(
            member.location for member in self.composite_member_resolutions
        )


def _validate_asset_identity(size_bytes: int, digests: tuple[Digest, ...]) -> None:
    """
    Validate the size-and-digest identity shared by Asset input and output.

    Example:
        >>> _validate_asset_identity(1, (Digest("sha256", "aa"),))


    :param size_bytes:
    :param digests:
    :return:
    """

    if size_bytes < 0:
        raise ValueError("size_bytes must not be negative.")
    if not digests:
        raise ValueError("a Digital Asset requires at least one digest.")
    _validate_unique_digests(digests)


def _validate_unique_digests(digests: tuple[Digest, ...]) -> None:
    """
    Require at most one digest value per algorithm.

    Example:
        >>> _validate_unique_digests((Digest("sha256", "aa"),))


    :param digests:
    :return:
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


def _validate_composite_members(
    members: tuple[CompositeDigitalAssetMembership, ...],
) -> None:
    """
    Require a non-empty, uniquely ordered Composite membership.

    Example:
        >>> _validate_composite_members(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )


    :param members:
    :return:
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
    """
    Reject a timestamp without an unambiguous timezone.

    Example:
        >>> _require_aware_datetime(None, "checked_at")


    :param value:
    :param field_name:
    :return:
    """

    if value is not None and (
        value.tzinfo is None or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware.")


__all__ = [
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetRecord",
    "CompositeDigitalAssetDeclaration",
    "DigitalAssetRecord",
    "DigitalAssetMetadata",
    "DigitalAssetDeclaration",
    "DigitalAssetIngestResult",
    "DigitalAssetVerificationReport",
    "ItemDigitalAssetResolution",
    "ReplicaRecord",
    "ReplicaMode",
    "ReplicaObservation",
    "ReplicaRemovalReport",
    "ReplicaDeclaration",
    "ReplicaState",
    "ReplicaVerificationReport",
    "DigitalAssetResolution",
    "CompositeDigitalAssetMemberResolution",
]
