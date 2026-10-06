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
    checks ID/position types and ranges, the required flag, and optional label text, without
    resolving the Asset or normalizing an export path. Frozen fields retain their supplied values.

    Example:
        >>> member = CompositeDigitalAssetMembership(
        ...     DigitalAssetID(7), 0, role="cover",
        ... )
        >>> member.required
        True


    :ivar digital_asset_id: Positive integer member Asset ID.
    :ivar sequence_number: Nonnegative integer intended zero-based position.
    :ivar role: Optional relationship role, retained without stripping; blank or NUL-containing text rejects.
    :ivar logical_name: Optional member label, distinct from any physical Store key.
    :ivar logical_path: Optional logical delivery path; nonblank/NUL checks do not establish safe filesystem traversal.
    :ivar title: Optional relationship title, subject to the same text checks as the other labels.
    :ivar required: Boolean selecting whether consuming workflows require this membership to resolve.
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
        Reject invalid ID/position types and ranges, a nonboolean required flag, and blank,
        NUL-containing, or nonstring optional labels.

        Labels are tested in their original spelling and remain unchanged; path traversal, sequence
        uniqueness, and Asset existence are not checked here.

        Example:
            >>> CompositeDigitalAssetMembership(DigitalAssetID(7), -1)
            Traceback (most recent call last):
            ...
            ValueError: sequence_number must not be negative.


        :return: None after the selected value checks pass; comparison, string-operation, and validation errors propagate.
        """

        if isinstance(self.digital_asset_id, bool) or not isinstance(
            self.digital_asset_id, int
        ):
            raise TypeError("digital_asset_id must be an integer.")
        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if isinstance(self.sequence_number, bool) or not isinstance(
            self.sequence_number, int
        ):
            raise TypeError("sequence_number must be an integer.")
        if self.sequence_number < 0:
            raise ValueError("sequence_number must not be negative.")
        if not isinstance(self.required, bool):
            raise TypeError("required must be a bool.")
        for field_name, value in (
            ("role", self.role),
            ("logical_name", self.logical_name),
            ("logical_path", self.logical_path),
            ("title", self.title),
        ):
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None.")
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be empty when supplied.")
            if value is not None and "\x00" in value:
                raise ValueError(f"{field_name} must not contain NUL characters.")



@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetDeclaration:
    """
    Describe caller-supplied members and metadata before persistence assigns identity and revision.

    Members must be nonempty with positions covering zero through length minus one, but the supplied
    sequence is retained in its original order. An optional name must be nonblank. Attribute names
    must be nonblank and unique and both names and values must be strings. Member Asset existence is
    checked by the manager rather than this value.

    Example:
        >>> declaration = CompositeDigitalAssetDeclaration(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> len(declaration.members)
        1


    :ivar members: Retained membership sequence whose position values must be unique and contiguous.
    :ivar name: Optional nonblank display name, retained without stripping.
    :ivar attributes: Retained extension string pairs with nonblank unique names.
    """

    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate contiguous membership positions and a nonblank name when supplied.

        The shared helper does not reorder or copy members. Name and attributes are retained
        unchanged after their local type, text, and uniqueness checks.

        Example:
            >>> CompositeDigitalAssetDeclaration(())
            Traceback (most recent call last):
            ...
            ValueError: a Composite Digital Asset requires at least one member.


        :return: None when membership and optional-name checks succeed; their errors propagate.
        """

        _validate_composite_members(self.members)
        _validate_optional_name(self.name)
        _validate_attributes(self.attributes)


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetRecord:
    """
    Retain a manager-assigned Composite identity, membership sequence, and descriptive metadata.

    The value contains references to atomic Assets rather than bytes or direct Replica claims.
    Direct construction validates the ID comparison, contiguous positions, and a truthy supplied
    revision. Name and attributes use the same local validation as a declaration; nested membership
    values are validated by their own constructors.

    Example:
        >>> record = CompositeDigitalAssetRecord(
        ...     CompositeDigitalAssetID(3),
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> record.composite_digital_asset_id
        3


    :ivar composite_digital_asset_id: Manager identity rejected when it compares at or below zero.
    :ivar members: Nonempty membership sequence retained without sorting or copying.
    :ivar name: Optional nonblank display name.
    :ivar attributes: Retained descriptive string pairs with nonblank unique names.
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

        Name and attributes receive local value checks, but the method performs no catalogue lookup.
        The Composite ID comparison and revision truthiness do not fully enforce their annotations.

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
        _validate_optional_name(self.name)
        _validate_attributes(self.attributes)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetAvailabilityAssessment:
    """
    Carry counts and diagnostics reported by a Composite availability assessment.

    The manager normally counts required membership occurrences, so repeated Assets can contribute
    more than once and optional members can be omitted. Construction requires a positive Composite
    ID, nonnegative integer counts ordered as readable <= resolved <= expected, positive missing
    Asset IDs, and nonblank error messages. Its readable property compares the validated totals and
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

    def __post_init__(self) -> None:
        """Validate identity, monotonic counts, missing IDs, and diagnostic text."""
        if isinstance(self.composite_digital_asset_id, bool) or not isinstance(
            self.composite_digital_asset_id, int
        ):
            raise TypeError("composite_digital_asset_id must be an integer.")
        if self.composite_digital_asset_id <= 0:
            raise ValueError("composite_digital_asset_id must be positive.")
        for field_name, value in (
            ("expected_members", self.expected_members),
            ("resolved_members", self.resolved_members),
            ("readable_members", self.readable_members),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer.")
            if value < 0:
                raise ValueError(f"{field_name} must not be negative.")
        if self.resolved_members > self.expected_members:
            raise ValueError("resolved_members must not exceed expected_members.")
        if self.readable_members > self.resolved_members:
            raise ValueError("readable_members must not exceed resolved_members.")
        for digital_asset_id in self.missing_digital_asset_ids:
            if isinstance(digital_asset_id, bool) or not isinstance(
                digital_asset_id, int
            ):
                raise TypeError("missing Digital Asset IDs must be integers.")
            if digital_asset_id <= 0:
                raise ValueError("missing Digital Asset IDs must be positive.")
        for error in self.errors:
            if not isinstance(error, str):
                raise TypeError("availability errors must be strings.")
            if not error.strip():
                raise ValueError("availability errors must not be blank.")

    @property
    def readable(self) -> bool:
        """
        Return whether all three counts are equal and both missing-ID and error collections are
        empty.

        Zero equal counts can be readable, as for a Composite with no required members. The
        predicate does not inspect any member or physical bytes beyond the validated summary.

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
    Asset IDs may repeat. Membership types are checked here; each membership constructor owns label,
    ID, flag, and exact position validation.

    Example:
        >>> _validate_composite_members(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )


    :param members: Membership sequence inspected through each sequence_number attribute and its length.
    :return: None for a nonempty contiguous position set; empty or noncontiguous sequences raise ValueError.
    """

    if not isinstance(members, tuple):
        raise TypeError("Composite members must be a tuple.")
    if not members:
        raise ValueError(
            "a Composite Digital Asset requires at least one member."
        )
    if not all(
        isinstance(member, CompositeDigitalAssetMembership)
        for member in members
    ):
        raise TypeError(
            "Composite members must be CompositeDigitalAssetMembership values."
        )
    positions = sorted(member.sequence_number for member in members)
    if positions != list(range(len(members))):
        raise ValueError(
            "Composite member sequence numbers must be unique and contiguous."
        )


def _validate_optional_name(name: str | None) -> None:
    """Require an optional Composite display name to be a nonblank string."""
    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string or None.")
    if name is not None and not name.strip():
        raise ValueError("name must not be empty when supplied.")


def _validate_attributes(attributes: tuple[tuple[str, str], ...]) -> None:
    """Require a tuple of string pairs with nonblank unique attribute names."""
    if not isinstance(attributes, tuple):
        raise TypeError("attributes must be a tuple.")
    names: set[str] = set()
    for attribute in attributes:
        if not isinstance(attribute, tuple) or len(attribute) != 2:
            raise TypeError("Composite attributes must be string pairs.")
        name, value = attribute
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("Composite attributes must be string pairs.")
        if not name.strip():
            raise ValueError("Composite attribute names must not be blank.")
        if name in names:
            raise ValueError(f"duplicate Composite attribute name: {name!r}.")
        names.add(name)


__all__ = [
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetDeclaration",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetRecord",
]
