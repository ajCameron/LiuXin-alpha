"""
Represent Composite membership relationships, catalogue records, and availability counts.

Values preserve supplied membership order and metadata. Constructors validate
selected comparisons and labels without resolving Assets or probing Store bytes;
manager workflows determine which members must be readable.
"""

from __future__ import annotations

import dataclasses

from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    CompositeDigitalAssetID,
    DigitalAssetID,
)


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMembership:
    """
    Describe one ordered relationship between a Composite and an atomic Asset.

    The same Asset can occupy multiple positions with different labels or requirements. Construction
    checks selected ID/position comparisons and optional label text, without resolving the Asset or
    normalizing an export path. Frozen fields retain their supplied values.

    Example:
        >>> member = CompositeDigitalAssetMembership(
        ...     DigitalAssetID(7), 0, role="cover",
        ... )
        >>> member.required
        True


    :ivar digital_asset_id: Member Asset ID, rejected when it compares at or below zero.
    :ivar sequence_number: Intended zero-based position, rejected here only when it compares below zero.
    :ivar role: Optional relationship role, retained without stripping; blank or NUL-containing text rejects.
    :ivar logical_name: Optional member label, distinct from any physical Store key.
    :ivar logical_path: Optional logical delivery path; nonblank/NUL checks do not establish safe filesystem traversal.
    :ivar title: Optional relationship title, subject to the same text checks as the other labels.
    :ivar required: Whether consuming workflows require this membership to resolve; the constructor does not enforce a bool type.
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
        Reject IDs at or below zero, positions below zero, and blank or NUL-containing optional
        labels.

        Comparisons do not enforce integer types or finiteness. Labels are tested in their original
        spelling and remain unchanged; path traversal, sequence uniqueness, Asset existence, and
        required-flag type are not checked here.

        Example:
            >>> CompositeDigitalAssetMembership(DigitalAssetID(7), -1)
            Traceback (most recent call last):
            ...
            ValueError: sequence_number must not be negative.


        :return: None after the selected value checks pass; comparison, string-operation, and validation errors propagate.
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
    Describe the members and metadata for a new or replacement logical assembly.

    Members must be nonempty with positions covering zero through length minus one, but the supplied
    sequence is retained in its original order. An optional name must be nonblank. Attributes are
    neither validated nor copied, and member Asset existence is checked by the manager rather than
    this value.

    Example:
        >>> declaration = CompositeDigitalAssetDeclaration(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> len(declaration.members)
        1


    :ivar members: Retained membership sequence whose position values must be unique and contiguous.
    :ivar name: Optional nonblank display name, retained without stripping.
    :ivar attributes: Retained extension name/value pairs without name, value, or uniqueness checks.
    """

    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate contiguous membership positions and a nonblank name when supplied.

        The shared helper does not reorder or copy members. Name is optional and retained unchanged;
        attributes are not examined.

        Example:
            >>> CompositeDigitalAssetDeclaration(())
            Traceback (most recent call last):
            ...
            ValueError: a Composite Digital Asset requires at least one member.


        :return: None when membership and optional-name checks succeed; their errors propagate.
        """

        _validate_composite_members(self.members)
        if self.name is not None and not self.name.strip():
            raise ValueError("name must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetRecord:
    """
    Retain a manager-assigned Composite identity, membership sequence, and descriptive metadata.

    The value contains references to atomic Assets rather than bytes or direct Replica claims.
    Direct construction validates the ID comparison, contiguous positions, and a truthy supplied
    revision. Unlike the declaration, it does not validate name; attributes and nested values remain
    unchecked and shared.

    Example:
        >>> record = CompositeDigitalAssetRecord(
        ...     CompositeDigitalAssetID(3),
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> record.composite_digital_asset_id
        3


    :ivar composite_digital_asset_id: Manager identity rejected when it compares at or below zero.
    :ivar members: Nonempty membership sequence retained without sorting or copying.
    :ivar name: Optional display name, not validated by this record constructor.
    :ivar attributes: Retained descriptive extension values without constructor validation.
    :ivar revision: Optional optimistic-lock token; false supplied values reject, but whitespace is retained.
    """

    composite_digital_asset_id: CompositeDigitalAssetID
    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Check the Composite ID comparison, member positions, and a truthy revision when supplied.

        Name and attributes are not checked, and the method performs no catalogue lookup. Comparison
        and truthiness checks do not enforce the annotated integer/string types.

        Example:
            >>> CompositeDigitalAssetRecord(
            ...     CompositeDigitalAssetID(0),
            ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: composite_digital_asset_id must be positive.


        :return: None after these record checks pass; invalid comparisons, membership positions, or false revisions raise.
        """

        if self.composite_digital_asset_id <= 0:
            raise ValueError("composite_digital_asset_id must be positive.")
        _validate_composite_members(self.members)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetAvailabilityAssessment:
    """
    Carry counts and diagnostics reported by a Composite availability assessment.

    The manager normally counts required membership occurrences, so repeated Assets can contribute
    more than once and optional members can be omitted. This value adds no constructor validation of
    IDs, counts, or evidence consistency. Its readable property compares the supplied totals and
    diagnostics without consulting storage.

    Example:
        >>> assessment = CompositeDigitalAssetAvailabilityAssessment(
        ...     CompositeDigitalAssetID(3), 2, 2, 2,
        ... )
        >>> assessment.readable
        True


    :ivar composite_digital_asset_id: Composite identity attributed to the assessment, without lookup here.
    :ivar expected_members: Number of membership occurrences considered required by the producer.
    :ivar resolved_members: Count whose atomic Asset records were resolved.
    :ivar readable_members: Count for which the producer selected a readable Replica.
    :ivar missing_digital_asset_ids: Reported missing or unreadable Asset identities.
    :ivar errors: Reported diagnostic messages; any nonempty collection prevents readable.
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
        Return whether all three counts are equal and both missing-ID and error collections are
        empty.

        Zero equal counts can be readable, as for a Composite with no required members. Counts are
        not independently validated and the predicate does not inspect any member or physical bytes.

        Example:
            >>> CompositeDigitalAssetAvailabilityAssessment(
            ...     CompositeDigitalAssetID(3), 1, 1, 1,
            ... ).readable
            True


        :return: True when expected, resolved, and readable totals agree with no listed missing IDs or errors.
        """

        return (
            self.expected_members
            == self.resolved_members
            == self.readable_members
            and not self.missing_digital_asset_ids
            and not self.errors
        )


def _validate_composite_members(
    members: tuple[CompositeDigitalAssetMembership, ...],
) -> None:
    """
    Require a nonempty sequence whose sorted position values equal zero through length minus one.

    The comparison enforces contiguous distinct positions without sorting the retained members.
    Asset IDs may repeat. Membership types, labels, and exact integer types of positions are not
    checked; malformed attributes, sorting, or length operations can raise.

    Example:
        >>> _validate_composite_members(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )


    :param members: Membership sequence inspected through each sequence_number attribute and its length.
    :return: None for a nonempty contiguous position set; empty or noncontiguous sequences raise ValueError.
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


__all__ = [
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetDeclaration",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetRecord",
]
