"""
Represent Replica claims, recorded observations, and lifecycle result values.

Constructors validate selected fields without probing storage. Report predicates
consume the supplied states and errors; they do not independently verify bytes or
enforce consistency between all evidence fields.
"""

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
from LiuXin_alpha.storage.api.storage_manager_api.models.composites import (
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetID,
    ReplicaID,
)


class ReplicaMode(StrEnum):
    """
    Name the operational purpose of a concrete Asset copy.

    Active, backup, archive, cache, transient, and unmanaged are string-valued policy labels.
    Choosing a mode does not itself establish availability, verification, placement eligibility, or
    lifecycle behavior.

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
    Name a recorded or expected availability state for a Replica claim.

    The values distinguish staging, presence without complete verification, verified bytes,
    missing/corrupt/unavailable observations, and deleted claims. An enum value alone is recorded
    evidence, not a fresh Store probe; consumers decide which states qualify for selection or policy
    counts.

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
    """
    Retain the latest supplied physical-state evidence for a Replica.

    Construction checks optional size, unique digest algorithms, timestamp awareness, and nonblank
    failure text. State/evidence consistency and state type are not checked. Original values remain
    shared; creating VERIFIED evidence does not perform verification.

    Example:
        >>> observation = ReplicaObservation(
        ...     ReplicaState.VERIFIED, observed_size_bytes=4,
        ... )
        >>> observation.state is ReplicaState.VERIFIED
        True


    :ivar state: Supplied lifecycle state; construction does not coerce it to ReplicaState.
    :ivar observed_size_bytes: Observed byte count, or None; only values comparing below zero reject.
    :ivar observed_digests: Possibly empty digest evidence with unique algorithm attributes.
    :ivar checked_at: Optional aware timestamp, retained without conversion to UTC.
    :ivar failure_reason: Optional nonblank explanation, retained without stripping.
    """

    state: ReplicaState
    observed_size_bytes: int | None = None
    observed_digests: tuple[Digest, ...] = ()
    checked_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """
        Check size negativity, duplicate digest algorithms, timestamp awareness, and blank failure
        text.

        The state is not validated or reconciled with evidence. No integer/type coercion or
        defensive copying occurs; malformed objects can fail their comparison, attribute, or string
        operations.

        Example:
            >>> ReplicaObservation(
            ...     ReplicaState.PRESENT, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.


        :return: None after the selected evidence checks pass; validation and malformed-input errors propagate.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")
        if self.failure_reason is not None and not self.failure_reason.strip():
            raise ValueError("failure_reason must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaDeclaration:
    """
    Describe a Replica claim to register for an existing Asset at a concrete Location.

    The default observation is a new UNVERIFIED record. Rechecking deliberately remains a manager
    operation (``verify_replica``), rather than a callback retained by this serializable immutable
    value. Direct construction only checks Asset-ID
    positivity; Asset/Store existence, Location ownership, mode, and observation validity belong to
    later operations. Placement hints remain caller-supplied advisory data, included in equality but
    excluded from generated hashing.

    Example:
        >>> declaration = ReplicaDeclaration(
        ...     DigitalAssetID(7), Location(UUID(int=1), "objects/7"),
        ... )
        >>> declaration.mode is ReplicaMode.ACTIVE
        True

    :ivar digital_asset_id: Asset ID rejected when it compares at or below zero, without integer-type or repository checks.
    :ivar location: Claimed concrete Store Location; this constructor does not inspect it.
    :ivar mode: Requested operational purpose, defaulting to ACTIVE without coercion.
    :ivar observation: Initial evidence, defaulting to a new UNVERIFIED observation.
    :ivar placement_hints: Optional retained placement metadata; construction does not snapshot nested values.
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
        """
        Reject an Asset ID that compares at or below zero, leaving all other fields unchecked.

        This is a direct comparison, not integer coercion or a catalogue existence check.

        Example:
            >>> ReplicaDeclaration(
            ...     DigitalAssetID(0), Location(UUID(int=1), "bad"),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return: None for an ID that does not compare at or below zero; invalid comparisons or nonpositive values raise.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRecord:
    """
    Retain a manager-assigned claim linking one Asset identity to one Store Location.

    Placement hints carry the advisory metadata recorded for that placement and can seed later
    replication. They remain separate from byte identity, participate in equality, and are excluded
    from generated hashing. The frozen record does not copy nested values or probe physical storage.

    Example:
        >>> record = ReplicaRecord(
        ...     ReplicaID(12), DigitalAssetID(7),
        ...     Location(UUID(int=1), "objects/7"), ReplicaMode.ACTIVE,
        ...     ReplicaObservation(ReplicaState.VERIFIED),
        ... )
        >>> record.state is ReplicaState.VERIFIED
        True


    :ivar replica_id: Manager-assigned Replica ID, rejected when it compares at or below zero.
    :ivar digital_asset_id: Claimed owning Asset ID, rejected when it compares at or below zero.
    :ivar location: Retained concrete Location without constructor-level ownership validation.
    :ivar mode: Retained operational purpose without enum coercion.
    :ivar observation: Retained state/evidence object exposed by the state property.
    :ivar revision: Optional truthy optimistic-lock token; whitespace is retained.
    :ivar placement_hints: Optional advisory placement metadata retained without a defensive copy.
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
        """
        Reject Replica/Asset IDs that compare at or below zero and false revisions when supplied.

        No integer-type, Location, mode, observation, placement, or repository consistency checks
        occur here. A whitespace-only revision remains accepted.

        Example:
            >>> ReplicaRecord(
            ...     ReplicaID(0), DigitalAssetID(7),
            ...     Location(UUID(int=1), "bad"), ReplicaMode.ACTIVE,
            ...     ReplicaObservation(ReplicaState.UNVERIFIED),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: replica_id must be positive.


        :return: None when the selected ID/revision checks pass; nonpositive IDs or a false supplied revision reject.
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
        Return the state attribute of the retained observation without probing storage or validating
        its type.

        Example:
            >>> replica.state is replica.observation.state  # doctest: +SKIP
            True


        :return: The exact state value currently exposed by observation.
        """

        return self.observation.state


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetIngestResult:
    """
    Collect publication outcome records and flags for one ingest operation.

    Construction checks the operation UUID and agreement of the Asset IDs carried by both records.
    Creation, deduplication, verification, and warning values are retained as reported; the
    constructor does not establish their consistency or repeat any ingest work.

    Example:
        >>> result.location == result.replica_record.location  # doctest: +SKIP
        True


    :ivar operation_id: Required UUID identifying the reported ingest operation.
    :ivar asset_record: Resulting Asset record whose ID must match the Replica owner.
    :ivar replica_record: Resulting Replica record supplying the concrete Location.
    :ivar asset_created: Reported indication that the operation created the Asset record.
    :ivar replica_created: Reported indication that the operation created the Replica record.
    :ivar deduplicated: Reported reuse outcome, not recomputed from the records.
    :ivar verified: Reported verification outcome, independent of constructor validation.
    :ivar warnings: Retained nonfatal diagnostics without content validation.
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
        Require a UUID operation ID and equal owning Asset IDs on the two records.

        Record attributes are read directly without record-type checks. Flags, warnings, and
        relationships between reported outcomes remain unchecked.

        Example:
            >>> result = DigitalAssetIngestResult(  # doctest: +SKIP
            ...     UUID(int=2), asset, replica, True, True,
            ... )


        :return: None when UUID and Asset ownership checks pass; invalid identity or record access raises.
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
        """
        Project the Location from the retained result Replica without lookup or copying.

        Example:
            >>> result.location.key  # doctest: +SKIP
            'objects/7'


        :return: The exact Location held by replica_record.
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaVerificationReport:
    """
    Retain a comparison report for one Replica and its expected Asset identity.

    Only observed-size negativity, digest algorithm uniqueness, and optional timestamp awareness are
    validated. IDs, state, tri-state comparison flags, and their consistency are not checked. The
    healthy property trusts VERIFIED enum identity and an empty errors collection rather than
    recomputing agreement from those flags.

    Example:
        >>> report = ReplicaVerificationReport(
        ...     ReplicaID(12), DigitalAssetID(7), ReplicaState.VERIFIED,
        ...     True, size_matches=True, digest_matches=True,
        ... )
        >>> report.healthy
        True

    :ivar replica_id: Attributed Replica ID, without positivity or repository validation here.
    :ivar digital_asset_id: Attributed Asset ID, without ownership verification here.
    :ivar state: Reported Replica state used by the healthy predicate.
    :ivar exists: Reported existence: True, False, or None when unknown.
    :ivar size_matches: Reported size comparison, or None when not established.
    :ivar digest_matches: Reported digest comparison, or None when not established.
    :ivar observed_size_bytes: Optional observed byte count checked only for negativity.
    :ivar observed_digests: Possibly empty observed digests with unique algorithm attributes.
    :ivar checked_at: Optional aware observation time, retained without timezone conversion.
    :ivar errors: Reported failure messages; any nonempty collection prevents healthy.
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
        Validate selected evidence fields without interpreting the reported outcome.

        Reject negative-comparing observed sizes, duplicate digest algorithms, and naive supplied
        timestamps. IDs, existence/comparison flags, state, and error contents remain unchecked.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(1), DigitalAssetID(2), ReplicaState.PRESENT,
            ...     True, observed_size_bytes=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: observed_size_bytes must not be negative.


        :return: None after the evidence checks succeed; validation or malformed-input errors propagate.
        """

        if self.observed_size_bytes is not None and self.observed_size_bytes < 0:
            raise ValueError("observed_size_bytes must not be negative.")
        validate_unique_digests(self.observed_digests)
        _require_aware_datetime(self.checked_at, "checked_at")

    @property
    def healthy(self) -> bool:
        """
        Return whether state is the VERIFIED enum singleton and errors is empty.

        Existence, size/digest agreement, timestamps, and evidence are not consulted. An equal
        string state is insufficient because the comparison uses identity.

        Example:
            >>> ReplicaVerificationReport(
            ...     ReplicaID(12), DigitalAssetID(7),
            ...     ReplicaState.VERIFIED, True,
            ... ).healthy
            True


        :return: True only for ReplicaState.VERIFIED with no errors, independent of the other report fields.
        """

        return self.state is ReplicaState.VERIFIED and not self.errors


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetVerificationReport:
    """
    Group the reports produced by one Asset verification request.

    The tuple may describe a selected subset, an early-stopped scan, or no Replicas. Construction
    adds no validation that the reports belong to the named Asset. Readability is derived only from
    their healthy properties.

    Example:
        >>> report = DigitalAssetVerificationReport(
        ...     DigitalAssetID(7), (),
        ... )
        >>> report.readable
        False

    :ivar digital_asset_id: Asset attributed to the aggregate without constructor validation.
    :ivar replica_reports: Ordered retained reports, potentially representing only part of the Asset population.
    """

    digital_asset_id: DigitalAssetID
    replica_reports: tuple[ReplicaVerificationReport, ...]

    @property
    def readable(self) -> bool:
        """
        Return whether any retained report is healthy, stopping at the first true result.

        An empty report collection returns False. The property does not inspect unreported Replicas
        or perform a new read.

        Example:
            >>> DigitalAssetVerificationReport(
            ...     DigitalAssetID(7), (),
            ... ).readable
            False


        :return: True when at least one contained report has a truthy healthy property.
        """

        return any(report.healthy for report in self.replica_reports)


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMemberVerificationReport:
    """
    Retain one Composite membership relationship and its atomic verification result.

    The same atomic report may be retained by several values when an Asset appears more than once
    in a Composite. Construction checks only that the relationship and report name the same Asset;
    it does not prove that the relationship belongs to a particular Composite or repeat any Store
    inspection.

    Example:
        >>> member_report.readable == member_report.verification_report.readable  # doctest: +SKIP
        True

    :ivar membership: Complete relationship metadata for one Composite position.
    :ivar verification_report: Atomic verification result whose Asset ID must match membership.
    """

    membership: CompositeDigitalAssetMembership
    verification_report: DigitalAssetVerificationReport

    def __post_init__(self) -> None:
        """
        Require the relationship and atomic report to identify the same Asset.

        :return: None for matching Asset IDs; disagreement raises ValueError.
        """

        if (
            self.membership.digital_asset_id
            != self.verification_report.digital_asset_id
        ):
            raise ValueError(
                "verification report does not match the Composite member."
            )

    @property
    def readable(self) -> bool:
        """
        Project whether the atomic verification produced at least one healthy Replica report.

        :return: The retained verification report's readable result without a new storage probe.
        """

        return self.verification_report.readable


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetVerificationReport:
    """
    Retain one Composite record and verification evidence for every membership occurrence.

    Member-report order and relationships must exactly match the Composite record, including
    repeated Asset memberships and optional members. The ``readable`` predicate requires every
    required occurrence to have a readable atomic report; unreadable optional members do not make
    the Composite unreadable. This value captures completed observations rather than an atomic
    cross-Store snapshot.

    Example:
        >>> report.readable  # doctest: +SKIP
        True

    :ivar composite_digital_asset_record: Composite identity and declared membership sequence.
    :ivar member_reports: Verification evidence in exactly the declared membership order.
    """

    composite_digital_asset_record: CompositeDigitalAssetRecord
    member_reports: tuple[CompositeDigitalAssetMemberVerificationReport, ...]

    def __post_init__(self) -> None:
        """
        Require exact ordered coverage of the Composite's membership relationships.

        Exact tuple equality prevents missing, duplicated, reordered, or foreign relationship
        evidence while still allowing the same Asset identity at several declared positions.

        :return: None for exact membership coverage; disagreement raises ValueError.
        """

        if tuple(report.membership for report in self.member_reports) != (
            self.composite_digital_asset_record.members
        ):
            raise ValueError(
                "verification reports must exactly cover Composite members in order."
            )

    @property
    def readable(self) -> bool:
        """
        Return whether every required relationship has readable atomic verification evidence.

        Optional unreadable members and the number of Replica reports inspected do not affect this
        predicate. A Composite record is always nonempty under its own constructor validation.

        :return: True when each required member report is readable.
        """

        return all(
            not report.membership.required or report.readable
            for report in self.member_reports
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ReplicaRemovalReport:
    """
    Carry the reported outcome of byte deletion and Replica-record mutation.

    This value adds no post-construction validation or outcome consistency checks. Flags reflect the
    producing workflow; bytes_deleted is not independent proof of storage absence, and a retained
    tombstone can coexist with preserved bytes.

    Example:
        >>> report = ReplicaRemovalReport(
        ...     ReplicaID(12), True, False, True,
        ... )
        >>> report.tombstone_retained
        True

    :ivar replica_id: Replica identity attributed to the removal.
    :ivar bytes_deleted: Whether the workflow reports executing its byte-deletion step.
    :ivar replica_forgotten: Whether the workflow reports removing the record.
    :ivar tombstone_retained: Whether a deleted-state record was requested and retained.
    :ivar warnings: Reported nonfatal diagnostics, retained without validation.
    """

    replica_id: ReplicaID
    bytes_deleted: bool
    replica_forgotten: bool
    tombstone_retained: bool
    warnings: tuple[str, ...] = ()


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """
    Accept None or require both a timezone attribute and a non-None UTC offset.

    The supplied value is not converted to UTC or checked with isinstance(datetime). Missing
    attributes and offset-call failures propagate.

    Example:
        >>> _require_aware_datetime(None, "checked_at")


    :param value: Optional timestamp-like object to inspect without copying or conversion.
    :param field_name: Field label interpolated into the naive-timestamp ValueError.
    :return: None for absent or aware values; naive values raise ValueError and malformed objects can raise their own errors.
    """

    if value is not None and (
        value.tzinfo is None or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware.")


__all__ = [
    "CompositeDigitalAssetMemberVerificationReport",
    "CompositeDigitalAssetVerificationReport",
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
