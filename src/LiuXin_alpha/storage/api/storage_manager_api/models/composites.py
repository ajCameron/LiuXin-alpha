"""Composite Digital Asset declarations, records, and assessments."""

from __future__ import annotations

import dataclasses

from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    CompositeDigitalAssetID,
    DigitalAssetID,
)


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMembership:
    """One ordered atomic member of a Composite Digital Asset.

    Example:
        >>> member = CompositeDigitalAssetMembership(
        ...     DigitalAssetID(7), 0, role="cover",
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
        """Reject invalid positions and empty optional labels.

        Example:
            >>> CompositeDigitalAssetMembership(DigitalAssetID(7), -1)
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
class CompositeDigitalAssetDeclaration:
    """Input for declaring an ordered logical assembly of atomic Assets.

    Example:
        >>> declaration = CompositeDigitalAssetDeclaration(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
        ... )
        >>> len(declaration.members)
        1
    """

    members: tuple[CompositeDigitalAssetMembership, ...]
    name: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Require a named, contiguous member sequence.

        Example:
            >>> CompositeDigitalAssetDeclaration(())
            Traceback (most recent call last):
            ...
            ValueError: a Composite Digital Asset requires at least one member.
        """

        _validate_composite_members(self.members)
        if self.name is not None and not self.name.strip():
            raise ValueError("name must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetRecord:
    """Manager-maintained facts about one Composite Digital Asset.

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
        """Validate identity, members, and optional revision.

        Example:
            >>> CompositeDigitalAssetRecord(
            ...     CompositeDigitalAssetID(0),
            ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: composite_digital_asset_id must be positive.
        """

        if self.composite_digital_asset_id <= 0:
            raise ValueError("composite_digital_asset_id must be positive.")
        _validate_composite_members(self.members)
        if self.revision is not None and not self.revision:
            raise ValueError("revision must not be empty when supplied.")


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetAvailabilityAssessment:
    """Completeness and required-member readability assessment.

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
        """Return whether every required member has a readable Replica.

        Example:
            >>> CompositeDigitalAssetAvailabilityAssessment(
            ...     CompositeDigitalAssetID(3), 1, 1, 1,
            ... ).readable
            True
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
    """Require a non-empty, uniquely ordered Composite membership.

    Example:
        >>> _validate_composite_members(
        ...     (CompositeDigitalAssetMembership(DigitalAssetID(7), 0),),
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


__all__ = [
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetDeclaration",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetRecord",
]
