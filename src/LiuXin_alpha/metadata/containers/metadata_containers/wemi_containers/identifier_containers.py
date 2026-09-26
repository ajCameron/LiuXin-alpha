"""
Model editable scheme-qualified identifiers attached to WEMI targets.

Records carry display and optional normalized values, provenance, ordering and
validity metadata. Target containers group them by allowed scheme. These values
describe attachments rather than live database row proxies.

Example:
    >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
    >>> identifier.validate()
    >>> identifier.value
    'local-1'
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, TypeVar, Literal

from LiuXin_alpha.databases.db_types import (
    IdentifierScheme,
    WORK_IDENTIFIER_SCHEMES,
    EXPRESSION_IDENTIFIER_SCHEMES,
    MANIFESTATION_IDENTIFIER_SCHEMES,
    ITEM_IDENTIFIER_SCHEMES,
)
from LiuXin_alpha.metadata.constants.container_vocabularies import IdentifierStatus
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)


IdentifierT = TypeVar("IdentifierT", bound="IdentifierBase")
SchemeContainerT = TypeVar("SchemeContainerT", bound="SchemeIdentifiersContainer")



@dataclass(slots=True, kw_only=True)
class IdentifierBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold shared identifier value, status, provenance and association fields.

    Concrete keyword-only dataclasses add a WEMI target and level context. Construction
    stores supplied values; validate checks basic field constraints without validating
    identifier syntax or setting is_validated.

    Example:
        >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
        >>> identifier.is_validated
        False
    """

    scheme: IdentifierScheme
    value: str
    normalized_value: str | None = None

    qualifier: str | None = None
    assigning_body: str | None = None

    position: int | None = None
    is_primary: bool = False

    status: IdentifierStatus = IdentifierStatus.ACTIVE
    is_validated: bool = False
    source: str = "user_set"
    notes: str | None = None

    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None
    STRING_DISPLAY_KEYS: ClassVar[tuple[str, ...]] = (
        "value",
        "scheme",
        "status",
    )

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this identifier.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this identifier.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def scheme_key(self) -> "IdentifierScheme":
        """
        Return the stored scheme used to group this identifier.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier.scheme_key == IdentifierScheme.UUID
            True


        :return: Stored IdentifierScheme value, without canonicalization.
        """
        return self.scheme

    def validate(self) -> None:
        """
        Reject blank scheme/value text, negative position and a reversed association interval.

        Scheme is stringified before checking blankness; value is stripped only for the
        check. Equal interval endpoints are allowed. This method neither enforces the target
        allowed-scheme set nor checks identifier syntax, status or normalized-value
        consistency. Failures raise ValueError.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier.association_start_ep_k = 20
            >>> identifier.association_end_ep_k = 10
            >>> identifier.validate()
            Traceback (most recent call last):
            ...
            ValueError: association_end_ep_k cannot be earlier than association_start_ep_k


        :return: None.
        """
        if not str(self.scheme).strip():
            raise ValueError("scheme cannot be blank")

        if not self.value.strip():
            raise ValueError("value cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

        if (
            self.association_start_ep_k is not None
            and self.association_end_ep_k is not None
            and self.association_end_ep_k < self.association_start_ep_k
        ):
            raise ValueError(
                "association_end_ep_k cannot be earlier than association_start_ep_k"
            )

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared identifier fields without target additions, validation or normalization.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier._common_write_payload()['value']
            'local-1'


        :return: New dictionary retaining stored scheme and status enum values.
        """
        return {
            "scheme": self.scheme,
            "value": self.value,
            "normalized_value": self.normalized_value,
            "qualifier": self.qualifier,
            "assigning_body": self.assigning_body,
            "position": self.position,
            "is_primary": self.is_primary,
            "status": self.status,
            "is_validated": self.is_validated,
            "source": self.source,
            "notes": self.notes,
            "association_start_ep_k": self.association_start_ep_k,
            "association_end_ep_k": self.association_end_ep_k,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared fields and concrete target context.

        Example:
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifier.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkIdentifier(IdentifierBase):
    """
    Attach a scheme-qualified identifier to a work, including the canonical-for-work flag.

    Construction retains the supplied values and does not enforce scheme eligibility.
    Use the target container for its allowed-scheme policy and validate explicitly for
    record constraints.

    Example:
        >>> identifier = WorkIdentifier(work_id=3, scheme=IdentifierScheme.UUID, value='local-3')
        >>> identifier.target_id, identifier.value
        (3, 'local-3')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this identifier.

        Example:
            >>> identifier = WorkIdentifier(work_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> str:
        """
        Identify this identifier as attached to a work.

        Example:
            >>> identifier = WorkIdentifier(work_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared identifier fields with the work id and the canonical-for-work flag.

        Scheme and status values are retained. No validation, normalization or persistence
        occurs.

        Example:
            >>> identifier = WorkIdentifier(work_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.as_write_payload()['work_id']
            3


        :return: New dictionary containing shared and work-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "work_id": self.work_id,
                "canonical_for_work": self.canonical_for_work,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionIdentifier(IdentifierBase):
    """
    Attach a scheme-qualified identifier to a expression, including an optional language id.

    Construction retains the supplied values and does not enforce scheme eligibility.
    Use the target container for its allowed-scheme policy and validate explicitly for
    record constraints.

    Example:
        >>> identifier = ExpressionIdentifier(expression_id=3, scheme=IdentifierScheme.UUID, value='local-3')
        >>> identifier.target_id, identifier.value
        (3, 'local-3')
    """
    expression_id: ExpressionID
    language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this identifier.

        Example:
            >>> identifier = ExpressionIdentifier(expression_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> str:
        """
        Identify this identifier as attached to a expression.

        Example:
            >>> identifier = ExpressionIdentifier(expression_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared identifier fields with the expression id and an optional language id.

        Scheme and status values are retained. No validation, normalization or persistence
        occurs.

        Example:
            >>> identifier = ExpressionIdentifier(expression_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.as_write_payload()['expression_id']
            3


        :return: New dictionary containing shared and expression-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "expression_id": self.expression_id,
                "language_id": self.language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationIdentifier(IdentifierBase):
    """
    Attach a scheme-qualified identifier to a manifestation, including an optional edition note.

    Construction retains the supplied values and does not enforce scheme eligibility.
    Use the target container for its allowed-scheme policy and validate explicitly for
    record constraints.

    Example:
        >>> identifier = ManifestationIdentifier(manifestation_id=3, scheme=IdentifierScheme.UUID, value='local-3')
        >>> identifier.target_id, identifier.value
        (3, 'local-3')
    """
    manifestation_id: ManifestationID
    edition_note: str | None = None

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this identifier.

        Example:
            >>> identifier = ManifestationIdentifier(manifestation_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        """
        Identify this identifier as attached to a manifestation.

        Example:
            >>> identifier = ManifestationIdentifier(manifestation_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared identifier fields with the manifestation id and an optional edition note.

        Scheme and status values are retained. No validation, normalization or persistence
        occurs.

        Example:
            >>> identifier = ManifestationIdentifier(manifestation_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.as_write_payload()['manifestation_id']
            3


        :return: New dictionary containing shared and manifestation-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_note": self.edition_note,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemIdentifier(IdentifierBase):
    """
    Attach a scheme-qualified identifier to a item, including the copy-specific flag and optional physical marking.

    Construction retains the supplied values and does not enforce scheme eligibility.
    Use the target container for its allowed-scheme policy and validate explicitly for
    record constraints.

    Example:
        >>> identifier = ItemIdentifier(item_id=3, scheme=IdentifierScheme.UUID, value='local-3')
        >>> identifier.target_id, identifier.value
        (3, 'local-3')
    """
    item_id: ItemID
    copy_specific: bool = True
    physical_marking: str | None = None

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this identifier.

        Example:
            >>> identifier = ItemIdentifier(item_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> str:
        """
        Identify this identifier as attached to a item.

        Example:
            >>> identifier = ItemIdentifier(item_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared identifier fields with the item id and the copy-specific flag and optional physical marking.

        Scheme and status values are retained. No validation, normalization or persistence
        occurs.

        Example:
            >>> identifier = ItemIdentifier(item_id=3, scheme=IdentifierScheme.UUID, value='local-3')
            >>> identifier.as_write_payload()['item_id']
            3


        :return: New dictionary containing shared and item-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
                "physical_marking": self.physical_marking,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class SchemeIdentifiersContainer(
    MetadataSequenceStringMixin,
    Generic[IdentifierT],
    abc.ABC,
):
    """
    Maintain an ordered editable identifier list for one scheme and target.

    The keyword-only dataclass constructor retains a supplied _identifiers list, or
    creates a fresh default list. Identifier objects remain shared. Record insertion
    checks shape and renumbers positions; complete validation is explicit.

    Example:
        >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
        >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
        >>> identifiers.add_identifier(identifier)
        >>> identifiers.values()
        ('local-1',)
    """

    scheme: IdentifierScheme
    target_id: int
    _identifiers: list[IdentifierT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL: ClassVar[str] = "identifiers"

    def __iter__(self) -> Iterator[IdentifierT]:
        """
        Iterate over shared identifier records in list order.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> next(iter(identifiers)) is identifier
            True


        :return: Iterator over stored records.
        """
        return iter(self._identifiers)

    def __len__(self) -> int:
        """
        Count records in this scheme bucket.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> len(identifiers)
            1


        :return: Number of stored identifiers.
        """
        return len(self._identifiers)

    def __getitem__(self, index: int) -> IdentifierT:
        """
        Read a record by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers[-1] is identifier
            True


        :param index: List index of the identifier to read.
        :return: Stored identifier object.
        """
        return self._identifiers[index]

    def identifiers(self) -> tuple[IdentifierT, ...]:
        """
        Take a tuple snapshot of record order with shared mutable identifiers.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.identifiers()[0] is identifier
            True


        :return: Tuple of stored identifier references.
        """
        return tuple(self._identifiers)

    def values(self) -> tuple[str, ...]:
        """
        Collect raw identifier values in list order, retaining duplicates.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.values()
            ('local-1',)


        :return: Tuple of stored value strings.
        """
        return tuple(identifier.value for identifier in self._identifiers)

    def normalized_values(self) -> tuple[str, ...]:
        """
        Prefer each truthy normalized value, falling back to its raw value.

        No normalization is performed here, and duplicates are retained.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifier.normalized_value = ''
            >>> identifiers.normalized_values()
            ('local-1',)
            >>> identifier.normalized_value = 'LOCAL-1'
            >>> identifiers.normalized_values()
            ('LOCAL-1',)


        :return: Tuple of selected value strings in list order.
        """
        return tuple(
            identifier.normalized_value or identifier.value
            for identifier in self._identifiers
        )

    def to_text(self, sep: str = " / ") -> str:
        """
        Join raw identifier values using the requested separator.

        Normalized values and status flags do not affect this rendering.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.to_text(sep='; ')
            'local-1'


        :param sep: Separator between raw values.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.values())

    def add_identifier(self, identifier: IdentifierT) -> None:
        """
        Check target and scheme, append the shared record, and renumber positions.

        Shape errors raise ValueError before insertion. Full record validation remains
        explicit.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifier.position
            0


        :param identifier: Record matching the bucket target kind, id and scheme.
        :return: None.
        """
        self._validate_identifier_shape(identifier)
        self._identifiers.append(identifier)
        self.normalize_positions()

    def replace_identifier(self, index: int, identifier: IdentifierT) -> None:
        """
        Check shape, replace the indexed record and renumber all positions.

        Shape errors raise ValueError; invalid indices raise IndexError.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> replacement = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-2')
            >>> identifiers.replace_identifier(0, replacement)
            >>> identifiers.values(), replacement.position
            (('local-2',), 0)


        :param index: List index of the record to replace.
        :param identifier: Replacement matching the bucket target and scheme.
        :return: None.
        """
        self._validate_identifier_shape(identifier)
        self._identifiers[index] = identifier
        self.normalize_positions()

    def remove_identifier_at(self, index: int) -> IdentifierT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its field values. Invalid indices raise IndexError.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.remove_identifier_at(0) is identifier
            True


        :param index: List index to remove, including negative indices.
        :return: Removed identifier object.
        """
        removed = self._identifiers.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.clear()
            >>> identifiers.values()
            ()


        :return: None.
        """
        self._identifiers.clear()

    def move_identifier(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber positions.

        Source indices follow list.pop rules; destinations follow list.insert rules.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.move_identifier(0, 99)
            >>> identifiers[0] is identifier and identifier.position == 0
            True


        :param old_index: Source list index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal, clipped by list.insert when out of
            range.
        :return: None.
        """
        identifier = self._identifiers.pop(old_index)
        self._identifiers.insert(new_index, identifier)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the record whose enumerated index matches.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.set_primary(0)
            >>> identifier.is_primary
            True
            >>> identifiers.set_primary(-1)
            >>> identifier.is_primary
            False


        :param index: Nonnegative index to designate, or an unmatched index to clear all
            flags.
        :return: None.
        """
        for i, identifier in enumerate(self._identifiers):
            identifier.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifier.position = 8
            >>> identifiers.normalize_positions()
            >>> identifier.position
            0


        :return: None.
        """
        for index, identifier in enumerate(self._identifiers):
            identifier.position = index

    def primary_identifier(self) -> IdentifierT | None:
        """
        Select the first flagged record, falling back to the first stored identifier.

        Selection does not set the fallback primary flag or filter by status or
        is_validated.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.primary_identifier() is identifier, identifier.is_primary
            (True, False)


        :return: Shared selected identifier, or None when empty.
        """
        for identifier in self._identifiers:
            if identifier.is_primary:
                return identifier
        return self._identifiers[0] if self._identifiers else None

    def validate(self) -> None:
        """
        Check record shape and values, contiguous positions and at most one primary identifier.

        Raise ValueError on the first failure without repairing values. This bucket does not
        enforce the top-level target allowed-scheme set.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.validate()
            >>> identifier.position = 2
            >>> identifiers.validate()
            Traceback (most recent call last):
            ...
            ValueError: Identifier position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0

        for expected_index, identifier in enumerate(self._identifiers):
            self._validate_identifier_shape(identifier)
            identifier.validate()

            if identifier.position != expected_index:
                raise ValueError(
                    f"Identifier position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {identifier.position}"
                )

            if identifier.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary identifier is allowed for "
                f"{self.target_kind} {self.target_id} scheme {self.scheme}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.as_write_payload()[0]['value']
            'local-1'


        :return: New list of per-identifier dictionaries.
        """
        return [identifier.as_write_payload() for identifier in self._identifiers]

    def _validate_identifier_shape(self, identifier: IdentifierT) -> None:
        """
        Require matching target kind, target id and scheme.

        Raise ValueError on the first mismatch; other identifier fields are not checked.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers._validate_identifier_shape(identifier)


        :param identifier: Candidate record whose target and scheme must match this bucket.
        :return: None.
        """
        if identifier.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {identifier.target_kind} identifier to "
                f"{self.target_kind} container"
            )

        if identifier.target_id != self.target_id:
            raise ValueError(
                f"Identifier target_id {identifier.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if identifier.scheme_key != self.scheme:
            raise ValueError(
                f"Identifier scheme {identifier.scheme_key} does not match "
                f"container scheme {self.scheme}"
            )


@dataclass(slots=True, kw_only=True)
class WorkSchemeIdentifiersContainer(SchemeIdentifiersContainer[WorkIdentifier]):
    """
    Collect ordered identifiers of one scheme on a work.

    Construction retains a supplied list without checking records or scheme eligibility.
    The target-level container owns the allowed-scheme policy.

    Example:
        >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
        >>> identifiers.work_id, len(identifiers)
        (3, 0)
    """
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id under its work-specific name.

        Example:
            >>> identifiers = WorkSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
            >>> identifiers.work_id
            3


        :return: Stored work row id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionSchemeIdentifiersContainer(
    SchemeIdentifiersContainer[ExpressionIdentifier]
):
    """
    Collect ordered identifiers of one scheme on a expression.

    Construction retains a supplied list without checking records or scheme eligibility.
    The target-level container owns the allowed-scheme policy.

    Example:
        >>> identifiers = ExpressionSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
        >>> identifiers.expression_id, len(identifiers)
        (3, 0)
    """
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id under its expression-specific name.

        Example:
            >>> identifiers = ExpressionSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
            >>> identifiers.expression_id
            3


        :return: Stored expression row id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationSchemeIdentifiersContainer(
    SchemeIdentifiersContainer[ManifestationIdentifier]
):
    """
    Collect ordered identifiers of one scheme on a manifestation.

    Construction retains a supplied list without checking records or scheme eligibility.
    The target-level container owns the allowed-scheme policy.

    Example:
        >>> identifiers = ManifestationSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
        >>> identifiers.manifestation_id, len(identifiers)
        (3, 0)
    """
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id under its manifestation-specific name.

        Example:
            >>> identifiers = ManifestationSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
            >>> identifiers.manifestation_id
            3


        :return: Stored manifestation row id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemSchemeIdentifiersContainer(SchemeIdentifiersContainer[ItemIdentifier]):
    """
    Collect ordered identifiers of one scheme on a item.

    Construction retains a supplied list without checking records or scheme eligibility.
    The target-level container owns the allowed-scheme policy.

    Example:
        >>> identifiers = ItemSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
        >>> identifiers.item_id, len(identifiers)
        (3, 0)
    """
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id under its item-specific name.

        Example:
            >>> identifiers = ItemSchemeIdentifiersContainer(scheme=IdentifierScheme.UUID, target_id=3)
            >>> identifiers.item_id
            3


        :return: Stored item row id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetIdentifiersContainer(
    MetadataSequenceStringMixin,
    Generic[IdentifierT, SchemeContainerT],
    abc.ABC,
):
    """
    Group editable identifier buckets by scheme for one WEMI target.

    The dataclass constructor retains a supplied _by_scheme dictionary, or creates a
    fresh one. Buckets preserve registration order. ensure_scheme enforces
    ALLOWED_SCHEMES; nonmutating lookups simply inspect the mapping.

    Example:
        >>> identifiers = WorkIdentifiersContainer(work_id=1)
        >>> identifiers.schemes()
        ()
    """

    _by_scheme: dict[IdentifierScheme, SchemeContainerT] = field(default_factory=dict)

    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = frozenset()
    STRING_COUNT_LABEL: ClassVar[str] = "identifiers"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the row id represented by this target container.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.target_id
            1


        :return: Concrete WEMI target id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """
        Require the WEMI level represented by this target container.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_scheme_container(self, scheme: IdentifierScheme) -> SchemeContainerT:
        """
        Require creation of an empty scheme bucket using this target id.

        Concrete factories do not register the bucket or check scheme eligibility;
        ensure_scheme handles those steps.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> bucket = identifiers._make_scheme_container(IdentifierScheme.UUID)
            >>> bucket.target_id, identifiers.schemes()
            (1, ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: New target-specific scheme bucket.
        """

    def _validate_scheme_allowed(self, scheme: IdentifierScheme) -> None:
        """
        Require membership in this target class ALLOWED_SCHEMES set.

        Unsupported schemes raise ValueError; names are not canonicalized.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers._validate_scheme_allowed(IdentifierScheme.URI)
            Traceback (most recent call last):
            ...
            ValueError: Identifier scheme uri is not allowed for work


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: None.
        """
        if scheme not in self.ALLOWED_SCHEMES:
            raise ValueError(
                f"Identifier scheme {scheme} is not allowed for {self.target_kind}"
            )

    def schemes(self) -> tuple[IdentifierScheme, ...]:
        """
        Return registered scheme keys in insertion order, including empty buckets.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.schemes()
            ()


        :return: Tuple of registered schemes.
        """
        return tuple(self._by_scheme.keys())

    def has_scheme(self, scheme: IdentifierScheme) -> bool:
        """
        Check bucket registration without validating eligibility or inspecting its contents.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.has_scheme(IdentifierScheme.UUID)
            False


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: True when the scheme key exists.
        """
        return scheme in self._by_scheme

    def get_scheme(self, scheme: IdentifierScheme) -> SchemeContainerT | None:
        """
        Look up a scheme without checking eligibility or creating a bucket.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.get_scheme(IdentifierScheme.URI) is None
            True


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_scheme.get(scheme)

    def ensure_scheme(self, scheme: IdentifierScheme) -> SchemeContainerT:
        """
        Check eligibility and return the scheme bucket, creating and registering it if absent.

        Eligibility is checked even for an already registered bucket. Unsupported schemes
        raise ValueError.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> bucket = identifiers.ensure_scheme(IdentifierScheme.UUID)
            >>> identifiers.ensure_scheme(IdentifierScheme.UUID) is bucket
            True


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: Live bucket for the requested allowed scheme.
        """
        self._validate_scheme_allowed(scheme)
        container = self._by_scheme.get(scheme)
        if container is None:
            container = self._make_scheme_container(scheme)
            self._by_scheme[scheme] = container
        return container

    def add_identifier(self, identifier: IdentifierT) -> None:
        """
        Check target id, ensure an allowed scheme bucket and delegate shape checks and insertion.

        An id mismatch or disallowed scheme raises ValueError before bucket creation. A
        later shape failure can leave an empty bucket registered. Successful insertion
        renumbers positions; full validation remains explicit.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.add_identifier(WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1'))
            >>> identifiers.scheme_values(IdentifierScheme.UUID)
            ('local-1',)


        :param identifier: Shared identifier record to insert into its scheme bucket.
        :return: None.
        """
        if identifier.target_id != self.target_id:
            raise ValueError(
                f"Identifier target_id {identifier.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_scheme(identifier.scheme_key).add_identifier(identifier)

    def iter_all_identifiers(self) -> Iterator[IdentifierT]:
        """
        Yield shared records in scheme registration order and then bucket order.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> next(identifiers.iter_all_identifiers()) is identifier
            True


        :return: Iterator over every stored identifier.
        """
        for container in self._by_scheme.values():
            yield from container

    def all_values(self) -> tuple[str, ...]:
        """
        Collect raw values across all buckets in scheme and record order.

        Duplicates and all statuses are retained.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.all_values()
            ('local-1',)


        :return: Tuple of raw identifier values.
        """
        return tuple(identifier.value for identifier in self.iter_all_identifiers())

    def scheme_values(self, scheme: IdentifierScheme) -> tuple[str, ...]:
        """
        Read raw values for a scheme without validating or creating its bucket.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.scheme_values(IdentifierScheme.UUID), identifiers.schemes()
            ((), ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: Ordered value tuple, or an empty tuple when absent.
        """
        container = self.get_scheme(scheme)
        if container is None:
            return tuple()
        return container.values()

    def scheme_normalized_values(self, scheme: IdentifierScheme) -> tuple[str, ...]:
        """
        Read stored normalized values with raw-value fallback for a scheme.

        No normalization, eligibility check or bucket creation occurs.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifier.normalized_value = 'LOCAL-1'
            >>> identifiers.scheme_normalized_values(IdentifierScheme.UUID)
            ('LOCAL-1',)


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: Ordered selected values, or an empty tuple when absent.
        """
        container = self.get_scheme(scheme)
        if container is None:
            return tuple()
        return container.normalized_values()

    def scheme_text(
        self,
        scheme: IdentifierScheme,
        sep: str = " / ",
    ) -> str:
        """
        Join raw values for a scheme without creating or validating the bucket.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.scheme_text(IdentifierScheme.UUID), identifiers.schemes()
            ('', ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :param sep: Separator between raw identifier values.
        :return: Joined text, or an empty string for an absent or empty bucket.
        """
        container = self.get_scheme(scheme)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_identifier_for_scheme(
        self,
        scheme: IdentifierScheme,
    ) -> IdentifierT | None:
        """
        Select a scheme primary record, falling back to that bucket first record.

        No eligibility check or bucket creation occurs. Status and is_validated do not
        affect selection.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.primary_identifier_for_scheme(IdentifierScheme.UUID) is identifier
            True
            >>> identifier.is_primary
            False


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: Shared selected record, or None for an absent or empty bucket.
        """
        container = self.get_scheme(scheme)
        if container is None:
            return None
        return container.primary_identifier()

    def primary_identifiers(self) -> dict[IdentifierScheme, IdentifierT]:
        """
        Select one primary-or-first record from each nonempty registered bucket.

        Selection does not set flags or filter statuses; empty buckets are omitted.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.primary_identifiers()[IdentifierScheme.UUID] is identifier
            True


        :return: New scheme-to-record dictionary containing shared identifier objects.
        """
        result: dict[IdentifierScheme, IdentifierT] = {}
        for scheme, container in self._by_scheme.items():
            primary = container.primary_identifier()
            if primary is not None:
                result[scheme] = primary
        return result

    def validate(self) -> None:
        """
        Check every registered scheme key for eligibility, then validate its bucket.

        The first error propagates. No values or bucket mappings are repaired.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifiers.validate()


        :return: None.
        """
        for scheme, container in self._by_scheme.items():
            self._validate_scheme_allowed(scheme)
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten bucket payloads in scheme registration order without validation or persistence.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=1)
            >>> identifier = WorkIdentifier(work_id=1, scheme=IdentifierScheme.UUID, value='local-1')
            >>> identifiers.add_identifier(identifier)
            >>> identifiers.as_write_payload()[0]['value']
            'local-1'


        :return: New list of identifier payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_scheme.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        WorkIdentifier,
        WorkSchemeIdentifiersContainer,
    ]
):
    """
    Group identifiers by the schemes allowed for a work.

    The allowed set is defined by WORK_IDENTIFIER_SCHEMES. Construction retains a
    supplied bucket mapping; ensure_scheme and validate enforce eligibility.

    Example:
        >>> identifiers = WorkIdentifiersContainer(work_id=3)
        >>> identifiers.ensure_scheme(IdentifierScheme.UUID).target_id
        3
    """
    work_id: WorkID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = WORK_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this identifier container.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=3)
            >>> identifiers.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> str:
        """
        Identify the identifier-container target as a work.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=3)
            >>> identifiers.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_scheme_container(self, scheme: IdentifierScheme) -> WorkSchemeIdentifiersContainer:
        """
        Build an empty work scheme bucket without registration or eligibility checks.

        Use ensure_scheme to enforce the allowed set and register the result.

        Example:
            >>> identifiers = WorkIdentifiersContainer(work_id=3)
            >>> bucket = identifiers._make_scheme_container(IdentifierScheme.UUID)
            >>> bucket.target_id, identifiers.schemes()
            (3, ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: New WorkSchemeIdentifiersContainer using the stored target id.
        """
        return WorkSchemeIdentifiersContainer(scheme=scheme, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ExpressionIdentifier,
        ExpressionSchemeIdentifiersContainer,
    ]
):
    """
    Group identifiers by the schemes allowed for a expression.

    The allowed set is defined by EXPRESSION_IDENTIFIER_SCHEMES. Construction retains a
    supplied bucket mapping; ensure_scheme and validate enforce eligibility.

    Example:
        >>> identifiers = ExpressionIdentifiersContainer(expression_id=3)
        >>> identifiers.ensure_scheme(IdentifierScheme.UUID).target_id
        3
    """
    expression_id: ExpressionID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = EXPRESSION_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this identifier container.

        Example:
            >>> identifiers = ExpressionIdentifiersContainer(expression_id=3)
            >>> identifiers.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> str:
        """
        Identify the identifier-container target as a expression.

        Example:
            >>> identifiers = ExpressionIdentifiersContainer(expression_id=3)
            >>> identifiers.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_scheme_container(
        self,
        scheme: IdentifierScheme,
    ) -> ExpressionSchemeIdentifiersContainer:
        """
        Build an empty expression scheme bucket without registration or eligibility checks.

        Use ensure_scheme to enforce the allowed set and register the result.

        Example:
            >>> identifiers = ExpressionIdentifiersContainer(expression_id=3)
            >>> bucket = identifiers._make_scheme_container(IdentifierScheme.UUID)
            >>> bucket.target_id, identifiers.schemes()
            (3, ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: New ExpressionSchemeIdentifiersContainer using the stored target id.
        """
        return ExpressionSchemeIdentifiersContainer(
            scheme=scheme,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ManifestationIdentifier,
        ManifestationSchemeIdentifiersContainer,
    ]
):
    """
    Group identifiers by the schemes allowed for a manifestation.

    The allowed set is defined by MANIFESTATION_IDENTIFIER_SCHEMES. Construction retains
    a supplied bucket mapping; ensure_scheme and validate enforce eligibility.

    Example:
        >>> identifiers = ManifestationIdentifiersContainer(manifestation_id=3)
        >>> identifiers.ensure_scheme(IdentifierScheme.UUID).target_id
        3
    """
    manifestation_id: ManifestationID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = MANIFESTATION_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this identifier container.

        Example:
            >>> identifiers = ManifestationIdentifiersContainer(manifestation_id=3)
            >>> identifiers.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        """
        Identify the identifier-container target as a manifestation.

        Example:
            >>> identifiers = ManifestationIdentifiersContainer(manifestation_id=3)
            >>> identifiers.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_scheme_container(
        self,
        scheme: IdentifierScheme,
    ) -> ManifestationSchemeIdentifiersContainer:
        """
        Build an empty manifestation scheme bucket without registration or eligibility checks.

        Use ensure_scheme to enforce the allowed set and register the result.

        Example:
            >>> identifiers = ManifestationIdentifiersContainer(manifestation_id=3)
            >>> bucket = identifiers._make_scheme_container(IdentifierScheme.UUID)
            >>> bucket.target_id, identifiers.schemes()
            (3, ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: New ManifestationSchemeIdentifiersContainer using the stored target id.
        """
        return ManifestationSchemeIdentifiersContainer(
            scheme=scheme,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ItemIdentifier,
        ItemSchemeIdentifiersContainer,
    ]
):
    """
    Group identifiers by the schemes allowed for a item.

    The allowed set is defined by ITEM_IDENTIFIER_SCHEMES. Construction retains a
    supplied bucket mapping; ensure_scheme and validate enforce eligibility.

    Example:
        >>> identifiers = ItemIdentifiersContainer(item_id=3)
        >>> identifiers.ensure_scheme(IdentifierScheme.UUID).target_id
        3
    """
    item_id: ItemID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = ITEM_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this identifier container.

        Example:
            >>> identifiers = ItemIdentifiersContainer(item_id=3)
            >>> identifiers.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> str:
        """
        Identify the identifier-container target as a item.

        Example:
            >>> identifiers = ItemIdentifiersContainer(item_id=3)
            >>> identifiers.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_scheme_container(self, scheme: IdentifierScheme) -> ItemSchemeIdentifiersContainer:
        """
        Build an empty item scheme bucket without registration or eligibility checks.

        Use ensure_scheme to enforce the allowed set and register the result.

        Example:
            >>> identifiers = ItemIdentifiersContainer(item_id=3)
            >>> bucket = identifiers._make_scheme_container(IdentifierScheme.UUID)
            >>> bucket.target_id, identifiers.schemes()
            (3, ())


        :param scheme: Identifier scheme selecting a bucket for this target.
        :return: New ItemSchemeIdentifiersContainer using the stored target id.
        """
        return ItemSchemeIdentifiersContainer(scheme=scheme, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Identifier convenience layer
# ---------------------------------------------------------------------------


def _install_scheme_convenience_properties(
    cls: type[BaseTargetIdentifiersContainer],
    schemes: frozenset[IdentifierScheme],
) -> None:
    """
    Install per-scheme convenience properties and methods on a container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.

    For a scheme stem of 'isbn_13', this creates:
    - .isbn_13               -> SchemeIdentifiersContainer
    - .isbn_13_values        -> tuple[str, ...]
    - .isbn_13_normalized_values -> tuple[str, ...]
    - .isbn_13_text           -> str   (default " / " separator)
    - .isbn_13_to_text(sep=" / ") -> str
    - .isbn_13_primary       -> IdentifierBase | None
    """
    for scheme in sorted(schemes, key=lambda s: s.value):
        stem = scheme.value

        def scheme_container_getter(self, _scheme=scheme):
            return self.ensure_scheme(_scheme)

        def scheme_values_getter(self, _scheme=scheme):
            return self.scheme_values(_scheme)

        def scheme_normalized_values_getter(self, _scheme=scheme):
            return self.scheme_normalized_values(_scheme)

        def scheme_text_getter(self, _scheme=scheme):
            return self.scheme_text(_scheme)

        def scheme_text_method(self, sep: str = " / ", _scheme=scheme) -> str:
            return self.scheme_text(_scheme, sep=sep)

        def scheme_primary_getter(self, _scheme=scheme):
            return self.primary_identifier_for_scheme(_scheme)

        setattr(cls, stem, property(scheme_container_getter))
        setattr(cls, f"{stem}_values", property(scheme_values_getter))
        setattr(cls, f"{stem}_normalized_values", property(scheme_normalized_values_getter))
        setattr(cls, f"{stem}_text", property(scheme_text_getter))
        setattr(cls, f"{stem}_to_text", scheme_text_method)
        setattr(cls, f"{stem}_primary", property(scheme_primary_getter))


_install_scheme_convenience_properties(WorkIdentifiersContainer, WORK_IDENTIFIER_SCHEMES)
_install_scheme_convenience_properties(ExpressionIdentifiersContainer, EXPRESSION_IDENTIFIER_SCHEMES)
_install_scheme_convenience_properties(
    ManifestationIdentifiersContainer,
    MANIFESTATION_IDENTIFIER_SCHEMES,
)
_install_scheme_convenience_properties(ItemIdentifiersContainer, ITEM_IDENTIFIER_SCHEMES)


__all__ = [
    "IdentifierStatus",
    "IdentifierBase",
    "WorkIdentifier",
    "ExpressionIdentifier",
    "ManifestationIdentifier",
    "ItemIdentifier",
    "SchemeIdentifiersContainer",
    "WorkSchemeIdentifiersContainer",
    "ExpressionSchemeIdentifiersContainer",
    "ManifestationSchemeIdentifiersContainer",
    "ItemSchemeIdentifiersContainer",
    "BaseTargetIdentifiersContainer",
    "WorkIdentifiersContainer",
    "ExpressionIdentifiersContainer",
    "ManifestationIdentifiersContainer",
    "ItemIdentifiersContainer",
]
