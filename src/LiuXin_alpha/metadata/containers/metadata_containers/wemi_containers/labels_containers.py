"""
Represent editable label assertions attached to WEMI entities.

Records retain identity/context hints, ordering and provenance. Target containers
group shared records by kind; construction and serialization do not automatically
perform full validation.

Example:
    >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import LabelKind
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


LabelT = TypeVar("LabelT", bound="LabelBase")
KindContainerT = TypeVar("KindContainerT", bound="KindLabelsContainer")



@dataclass(slots=True, kw_only=True)
class LabelBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a label attachment with display, normalized and sort text with optional language and authority references.

    Concrete keyword-only dataclasses add the target id and WEMI-level context. Values
    are retained as supplied, with explicit validate checks rather than database lookup.

    Example:
        >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
        >>> record.target_kind
        'work'
    """

    label_kind: LabelKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("text", "label_kind", "source")

    # Optional glue to authority / external classification systems.
    authority_record_id: int | None = None
    external_key: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this label assertion.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this label assertion.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def kind_key(self) -> LabelKind:
        """
        Return the stored label kind used for bucket grouping.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.kind_key == LabelKind.TAG
            True


        :return: Stored LabelKind value without normalization.
        """
        return self.label_kind

    def validate(self) -> None:
        """
        Reject blank label-kind text, blank display text and negative positions.

        Kind is stringified before its blankness check. Text is stripped only for
        validation, leaving stored values unchanged. No authority, language or normalization
        consistency checks occur; failures raise ValueError.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.text = ' '
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: text cannot be blank


        :return: None.
        """
        if not str(self.label_kind).strip():
            raise ValueError("label_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared label fields without validation, normalization or target additions.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record._common_write_payload()['label_kind'] == LabelKind.TAG
            True


        :return: New dictionary retaining stored values, including LabelKind.
        """
        return {
            "label_kind": self.label_kind,
            "text": self.text,
            "normalized_text": self.normalized_text,
            "sort_text": self.sort_text,
            "language_id": self.language_id,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
            "authority_record_id": self.authority_record_id,
            "external_key": self.external_key,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared label fields and concrete target context.

        Example:
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkLabel(LabelBase):
    """
    Attach a typed label assertion to a work, including the canonical-for-work flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = WorkLabel(work_id=3, label_kind=LabelKind.TAG, text='Favorite')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this label.

        Example:
            >>> record = WorkLabel(work_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this label as attached to a work.

        Example:
            >>> record = WorkLabel(work_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared label fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkLabel(work_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific fields.
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
class ExpressionLabel(LabelBase):
    """
    Attach a typed label assertion to a expression, including an optional applies-to language id.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ExpressionLabel(expression_id=3, label_kind=LabelKind.TAG, text='Favorite')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """
    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this label.

        Example:
            >>> record = ExpressionLabel(expression_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this label as attached to a expression.

        Example:
            >>> record = ExpressionLabel(expression_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared label fields with the expression id and an optional applies-to language id.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionLabel(expression_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "expression_id": self.expression_id,
                "applies_to_language_id": self.applies_to_language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationLabel(LabelBase):
    """
    Attach a typed label assertion to a manifestation, including the edition-specific flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ManifestationLabel(manifestation_id=3, label_kind=LabelKind.TAG, text='Favorite')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this label.

        Example:
            >>> record = ManifestationLabel(manifestation_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this label as attached to a manifestation.

        Example:
            >>> record = ManifestationLabel(manifestation_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared label fields with the manifestation id and the edition-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationLabel(manifestation_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_specific": self.edition_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemLabel(LabelBase):
    """
    Attach a typed label assertion to a item, including the copy-specific flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ItemLabel(item_id=3, label_kind=LabelKind.TAG, text='Favorite')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this label.

        Example:
            >>> record = ItemLabel(item_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this label as attached to a item.

        Example:
            >>> record = ItemLabel(item_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared label fields with the item id and the copy-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemLabel(item_id=3, label_kind=LabelKind.TAG, text='Favorite')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindLabelsContainer(MetadataSequenceStringMixin, Generic[LabelT], abc.ABC):
    """
    Maintain an ordered editable label list for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _labels list; otherwise it
    creates a fresh list. Records remain shared. Insertion checks shape and renumbers
    positions, while full validation is explicit.

    Example:
        >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
        >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
        >>> records.add_label(record)
        >>> records.texts()
        ('Favorite',)
    """

    label_kind: LabelKind
    target_id: int
    _labels: list[LabelT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL = "labels"

    def __iter__(self) -> Iterator[LabelT]:
        """
        Iterate over shared label records in list order.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._labels)

    def __len__(self) -> int:
        """
        Count the stored label assertions.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> len(records)
            1


        :return: Number of records.
        """
        return len(self._labels)

    def __getitem__(self, index: int) -> LabelT:
        """
        Read a label by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._labels[index]

    def labels(self) -> tuple[LabelT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.labels()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._labels)

    def texts(self) -> tuple[str, ...]:
        """
        Collect stored label text in list order.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.texts()
            ('Favorite',)


        :return: Tuple of display strings, retaining duplicates.
        """
        return tuple(label.text for label in self._labels)

    def sort_texts(self) -> tuple[str, ...]:
        """
        Prefer each truthy sort_text value, falling back to its display text.

        The records are not sorted and no normalization occurs.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> record.sort_text = ''
            >>> records.sort_texts()
            ('Favorite',)
            >>> record.sort_text = 'favorite'
            >>> records.sort_texts()
            ('favorite',)


        :return: Tuple of selected text values in list order.
        """
        return tuple(label.sort_text or label.text for label in self._labels)

    def to_text(self, sep: str = ", ") -> str:
        """
        Join every display string with the requested separator.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.to_text(sep=' / ')
            'Favorite'


        :param sep: Separator between consecutive display strings.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_label(self, label: LabelT) -> None:
        """
        Check target and label kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> record.position
            0


        :param label: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_label_shape(label)
        self._labels.append(label)
        self.normalize_positions()

    def replace_label(self, index: int, label: LabelT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.replace_label(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param label: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_label_shape(label)
        self._labels[index] = label
        self.normalize_positions()

    def remove_label_at(self, index: int) -> LabelT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.remove_label_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._labels.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._labels.clear()

    def move_label(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.move_label(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        label = self._labels.pop(old_index)
        self._labels.insert(new_index, label)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.set_primary(0)
            >>> record.is_primary
            True
            >>> records.set_primary(-1)
            >>> record.is_primary
            False


        :param index: Nonnegative index to designate, or an unmatched index to clear all
            flags.
        :return: None.
        """
        for i, label in enumerate(self._labels):
            label.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, label in enumerate(self._labels):
            label.position = index

    def primary_label(self) -> LabelT | None:
        """
        Select the first flagged label, falling back to the first stored record.

        The fallback is not marked primary and competing flags are not validated here.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.primary_label() is record, record.is_primary
            (True, False)


        :return: Shared selected label, or None for an empty bucket.
        """
        for label in self._labels:
            if label.is_primary:
                return label
        return self._labels[0] if self._labels else None

    def validate(self) -> None:
        """
        Check label shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Label position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0

        for expected_index, label in enumerate(self._labels):
            self._validate_label_shape(label)
            label.validate()

            if label.position != expected_index:
                raise ValueError(
                    f"Label position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {label.position}"
                )

            if label.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary label is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.label_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [label.as_write_payload() for label in self._labels]

    def _validate_label_shape(self, label: LabelT) -> None:
        """
        Require matching target kind, target id and label kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records._validate_label_shape(record)


        :param label: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if label.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {label.target_kind} label to {self.target_kind} container"
            )

        if label.target_id != self.target_id:
            raise ValueError(
                f"Label target_id {label.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if label.kind_key != self.label_kind:
            raise ValueError(
                f"Label kind {label.kind_key} does not match "
                f"container kind {self.label_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindLabelsContainer(KindLabelsContainer[WorkLabel]):
    """
    Collect ordered label assertions of one kind on a work.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id using its work-specific name.

        Example:
            >>> records = WorkKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
            >>> records.work_id
            3


        :return: Stored work id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindLabelsContainer(KindLabelsContainer[ExpressionLabel]):
    """
    Collect ordered label assertions of one kind on a expression.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ExpressionKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id using its expression-specific name.

        Example:
            >>> records = ExpressionKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
            >>> records.expression_id
            3


        :return: Stored expression id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindLabelsContainer(KindLabelsContainer[ManifestationLabel]):
    """
    Collect ordered label assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ManifestationKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id using its manifestation-specific name.

        Example:
            >>> records = ManifestationKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
            >>> records.manifestation_id
            3


        :return: Stored manifestation id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindLabelsContainer(KindLabelsContainer[ItemLabel]):
    """
    Collect ordered label assertions of one kind on a item.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ItemKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id using its item-specific name.

        Example:
            >>> records = ItemKindLabelsContainer(label_kind=LabelKind.TAG, target_id=3)
            >>> records.item_id
            3


        :return: Stored item id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetLabelsContainer(
    MetadataSequenceStringMixin,
    Generic[LabelT, KindContainerT],
    abc.ABC,
):
    """
    Group editable label buckets by kind for a WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkLabelsContainer(work_id=1)
        >>> records.kinds()
        ()
    """

    _by_kind: dict[LabelKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "labels"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.target_id
            1


        :return: Concrete target id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level represented by this container.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, label_kind: LabelKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> bucket = records._make_kind_container(LabelKind.TAG)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[LabelKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def has_kind(self, label_kind: LabelKind) -> bool:
        """
        Check whether a bucket is registered, regardless of its contents.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.has_kind(LabelKind.TAG)
            False


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: True when the kind key exists.
        """
        return label_kind in self._by_kind

    def get_kind(self, label_kind: LabelKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.get_kind(LabelKind.TAG) is None
            True


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(label_kind)

    def ensure_kind(self, label_kind: LabelKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> bucket = records.ensure_kind(LabelKind.TAG)
            >>> records.ensure_kind(LabelKind.TAG) is bucket
            True


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(label_kind)
        if container is None:
            container = self._make_kind_container(label_kind)
            self._by_kind[label_kind] = container
        return container

    def add_label(self, label: LabelT) -> None:
        """
        Check target id, ensure the label kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later target-kind or
        assertion-kind mismatch can leave a new empty bucket. Successful insertion renumbers
        positions; full validation remains explicit.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.add_label(WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite'))
            >>> records.kind_text(LabelKind.TAG)
            'Favorite'


        :param label: Shared record to insert into its kind bucket.
        :return: None.
        """
        if label.target_id != self.target_id:
            raise ValueError(
                f"Label target_id {label.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(label.kind_key).add_label(label)

    def iter_all_labels(self) -> Iterator[LabelT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> next(records.iter_all_labels()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        """
        Collect every label text in kind registration order and then bucket order.

        Duplicates are retained.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.all_texts()
            ('Favorite',)


        :return: Tuple of stored label text values.
        """
        return tuple(label.text for label in self.iter_all_labels())

    def kind_text(self, label_kind: LabelKind, sep: str = ", ") -> str:
        """
        Join display text for a kind without creating a missing bucket.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.kind_text(LabelKind.TAG), records.kinds()
            ('', ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :param sep: Separator between display strings.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(label_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_labels(self) -> dict[LabelKind, LabelT]:
        """
        Select the primary-or-first label from each nonempty registered kind bucket.

        Empty buckets are omitted; selection does not set any flags.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.primary_labels()[LabelKind.TAG] is record
            True


        :return: New kind-to-label dictionary retaining shared record references.
        """
        result: dict[LabelKind, LabelT] = {}
        for label_kind, container in self._by_kind.items():
            primary = container.primary_label()
            if primary is not None:
                result[label_kind] = primary
        return result

    def primary_label_for_kind(self, label_kind: LabelKind) -> LabelT | None:
        """
        Select the primary-or-first label for a kind without creating a bucket.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.primary_label_for_kind(LabelKind.TAG) is record, record.is_primary
            (True, False)


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: Shared selected label, or None for an absent or empty kind.
        """
        container = self.get_kind(label_kind)
        if container is None:
            return None
        return container.primary_label()

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing or normalizing data.

        The first bucket error propagates.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkLabelsContainer(work_id=1)
            >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
            >>> records.add_label(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkLabelsContainer(
    BaseTargetLabelsContainer[
        WorkLabel,
        WorkKindLabelsContainer,
    ]
):
    """
    Group all label assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = WorkLabelsContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this label container.

        Example:
            >>> records = WorkLabelsContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the label-container target as a work.

        Example:
            >>> records = WorkLabelsContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, label_kind: LabelKind) -> WorkKindLabelsContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkLabelsContainer(work_id=3)
            >>> bucket = records._make_kind_container(LabelKind.TAG)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: New WorkKindLabelsContainer using the stored target id.
        """
        return WorkKindLabelsContainer(label_kind=label_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionLabelsContainer(
    BaseTargetLabelsContainer[
        ExpressionLabel,
        ExpressionKindLabelsContainer,
    ]
):
    """
    Group all label assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ExpressionLabelsContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this label container.

        Example:
            >>> records = ExpressionLabelsContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the label-container target as a expression.

        Example:
            >>> records = ExpressionLabelsContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(
        self,
        label_kind: LabelKind,
    ) -> ExpressionKindLabelsContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionLabelsContainer(expression_id=3)
            >>> bucket = records._make_kind_container(LabelKind.TAG)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: New ExpressionKindLabelsContainer using the stored target id.
        """
        return ExpressionKindLabelsContainer(
            label_kind=label_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationLabelsContainer(
    BaseTargetLabelsContainer[
        ManifestationLabel,
        ManifestationKindLabelsContainer,
    ]
):
    """
    Group all label assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ManifestationLabelsContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this label container.

        Example:
            >>> records = ManifestationLabelsContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the label-container target as a manifestation.

        Example:
            >>> records = ManifestationLabelsContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(
        self,
        label_kind: LabelKind,
    ) -> ManifestationKindLabelsContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationLabelsContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(LabelKind.TAG)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: New ManifestationKindLabelsContainer using the stored target id.
        """
        return ManifestationKindLabelsContainer(
            label_kind=label_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemLabelsContainer(
    BaseTargetLabelsContainer[
        ItemLabel,
        ItemKindLabelsContainer,
    ]
):
    """
    Group all label assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ItemLabelsContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this label container.

        Example:
            >>> records = ItemLabelsContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the label-container target as a item.

        Example:
            >>> records = ItemLabelsContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, label_kind: LabelKind) -> ItemKindLabelsContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemLabelsContainer(item_id=3)
            >>> bucket = records._make_kind_container(LabelKind.TAG)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param label_kind: LabelKind value selecting a bucket for this target.
        :return: New ItemKindLabelsContainer using the stored target id.
        """
        return ItemKindLabelsContainer(label_kind=label_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Label-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(label_kind: LabelKind) -> str:
    """
    Look up the fixed convenience-property stem for a label kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(LabelKind.TAG)
        'tags'


    :param label_kind: LabelKind value selecting a bucket for this target.
    :return: Attribute stem for the selected kind.
    """
    stems: dict[LabelKind, str] = {
        LabelKind.TAG: "tags",
        LabelKind.GENRE: "genres",
        LabelKind.FORM: "forms",
        LabelKind.TOPIC: "topics",
        LabelKind.CHARACTER: "characters",
        LabelKind.PLACE: "places",
        LabelKind.PERIOD: "periods",
        LabelKind.AUDIENCE: "audiences",
        LabelKind.AWARD: "awards",
        LabelKind.COLLECTION: "collections",
        LabelKind.INTERNAL: "internal_labels",
    }
    return stems[label_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetLabelsContainer],
) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a label container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkLabelsContainer(work_id=1)
        >>> records.tags_text, records.kinds()
        ('', ())
        >>> bucket = records.tags
        >>> records.get_kind(LabelKind.TAG) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """

    for label_kind in LabelKind:
        stem = _kind_property_stem(label_kind)

        def kind_container_getter(self, _kind=label_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkLabelsContainer(work_id=1)
                >>> bucket = records.tags
                >>> records.get_kind(LabelKind.TAG) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Label kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=label_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkLabelsContainer(work_id=1)
                >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
                >>> records.add_label(record)
                >>> records.tags_text
                'Favorite'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Label kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=label_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkLabelsContainer(work_id=1)
                >>> record = WorkLabel(work_id=1, label_kind=LabelKind.TAG, text='Favorite')
                >>> records.add_label(record)
                >>> records.tags_to_text(sep=' / ')
                'Favorite'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Label kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkLabelsContainer)
_install_kind_convenience_properties(ExpressionLabelsContainer)
_install_kind_convenience_properties(ManifestationLabelsContainer)
_install_kind_convenience_properties(ItemLabelsContainer)


__all__ = [
    "LabelKind",
    "LabelBase",
    "WorkLabel",
    "ExpressionLabel",
    "ManifestationLabel",
    "ItemLabel",
    "KindLabelsContainer",
    "WorkKindLabelsContainer",
    "ExpressionKindLabelsContainer",
    "ManifestationKindLabelsContainer",
    "ItemKindLabelsContainer",
    "BaseTargetLabelsContainer",
    "WorkLabelsContainer",
    "ExpressionLabelsContainer",
    "ManifestationLabelsContainer",
    "ItemLabelsContainer",
]
