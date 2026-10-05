"""
Represent descriptive subject access points attached to WEMI targets.

Topics, characters, places and periods are grouped by kind. Records retain display,
sort and normalization hints without performing authority lookup or normalization;
containers share those records.

Example:
    >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import SubjectKind
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


SubjectT = TypeVar("SubjectT", bound="SubjectBase")
KindContainerT = TypeVar("KindContainerT", bound="KindSubjectsContainer")



@dataclass(slots=True, kw_only=True)
class SubjectBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a subject attachment with display, normalized and sort text with optional language and authority references.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied. Validation and referenced-row resolution are
    separate operations.

    Example:
        >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
        >>> record.target_kind
        'work'
    """

    subject_kind: SubjectKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("text", "subject_kind", "source")

    authority_record_id: int | None = None
    external_key: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this subject attachment.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this subject attachment.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def kind_key(self) -> SubjectKind:
        """
        Return the stored subject kind used for bucket grouping.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.kind_key == SubjectKind.TOPIC
            True


        :return: Stored SubjectKind value without normalization.
        """
        return self.subject_kind

    def validate(self) -> None:
        """
        Reject blank subject-kind text, blank display text and negative record positions.

        Kind is stringified before its blankness check. Text is stripped only for
        validation, leaving the stored value unchanged. No checks are made on authority and
        language references or normalized/sort text. Failures raise ValueError.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.text = ' '
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: text cannot be blank


        :return: None.
        """
        if not str(self.subject_kind).strip():
            raise ValueError("subject_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared subject fields without target additions, validation or persistence.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record._common_write_payload()['subject_kind'] == SubjectKind.TOPIC
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "subject_kind": self.subject_kind,
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
        Require serialization of shared subject fields and concrete target context.

        Example:
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkSubject(SubjectBase):
    """
    Attach a subject record to a work, including the canonical-for-work flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = WorkSubject(work_id=3, subject_kind=SubjectKind.TOPIC, text='History')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """

    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this subject.

        Example:
            >>> record = WorkSubject(work_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this subject as attached to a work.

        Example:
            >>> record = WorkSubject(work_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared subject fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkSubject(work_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific values.
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
class ExpressionSubject(SubjectBase):
    """
    Attach a subject record to a expression, including an optional applies-to language id.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ExpressionSubject(expression_id=3, subject_kind=SubjectKind.TOPIC, text='History')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """

    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this subject.

        Example:
            >>> record = ExpressionSubject(expression_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this subject as attached to a expression.

        Example:
            >>> record = ExpressionSubject(expression_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared subject fields with the expression id and an optional applies-to language id.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionSubject(expression_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific values.
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
class ManifestationSubject(SubjectBase):
    """
    Attach a subject record to a manifestation, including the edition-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ManifestationSubject(manifestation_id=3, subject_kind=SubjectKind.TOPIC, text='History')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """

    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this subject.

        Example:
            >>> record = ManifestationSubject(manifestation_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this subject as attached to a manifestation.

        Example:
            >>> record = ManifestationSubject(manifestation_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared subject fields with the manifestation id and the edition-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationSubject(manifestation_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific values.
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
class ItemSubject(SubjectBase):
    """
    Attach a subject record to a item, including the copy-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ItemSubject(item_id=3, subject_kind=SubjectKind.TOPIC, text='History')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """

    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this subject.

        Example:
            >>> record = ItemSubject(item_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this subject as attached to a item.

        Example:
            >>> record = ItemSubject(item_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared subject fields with the item id and the copy-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemSubject(item_id=3, subject_kind=SubjectKind.TOPIC, text='History')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
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
class KindSubjectsContainer(MetadataSequenceStringMixin, Generic[SubjectT], abc.ABC):
    """
    Maintain an ordered editable subject list for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _subjects list; otherwise it
    creates a fresh list. Records remain shared. Insertion checks shape and renumbers
    positions, while full validation is explicit.

    Example:
        >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
        >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
        >>> records.add_subject(record)
        >>> records.texts()
        ('History',)
    """

    subject_kind: SubjectKind
    target_id: int
    _subjects: list[SubjectT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL = "subjects"

    def __iter__(self) -> Iterator[SubjectT]:
        """
        Iterate over shared subject records in list order.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._subjects)

    def __len__(self) -> int:
        """
        Count the stored subject assertions.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> len(records)
            1


        :return: Number of records.
        """
        return len(self._subjects)

    def __getitem__(self, index: int) -> SubjectT:
        """
        Read a subject by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._subjects[index]

    def subjects(self) -> tuple[SubjectT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.subjects()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._subjects)

    def texts(self) -> tuple[str, ...]:
        """
        Collect stored subject text in list order.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.texts()
            ('History',)


        :return: Tuple of display strings, retaining duplicates.
        """
        return tuple(subject.text for subject in self._subjects)

    def sort_texts(self) -> tuple[str, ...]:
        """
        Prefer each truthy sort_text value, falling back to its display text.

        The records are not sorted and no normalization occurs.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> record.sort_text = ''
            >>> records.sort_texts()
            ('History',)
            >>> record.sort_text = 'history'
            >>> records.sort_texts()
            ('history',)


        :return: Tuple of selected text values in list order.
        """
        return tuple(subject.sort_text or subject.text for subject in self._subjects)

    def to_text(self, sep: str = ", ") -> str:
        """
        Join every display string with the requested separator.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.to_text(sep=' / ')
            'History'


        :param sep: Separator between consecutive display strings.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_subject(self, subject: SubjectT) -> None:
        """
        Check target and subject kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> record.position
            0


        :param subject: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_subject_shape(subject)
        self._subjects.append(subject)
        self.normalize_positions()

    def replace_subject(self, index: int, subject: SubjectT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.replace_subject(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param subject: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_subject_shape(subject)
        self._subjects[index] = subject
        self.normalize_positions()

    def remove_subject_at(self, index: int) -> SubjectT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.remove_subject_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._subjects.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._subjects.clear()

    def move_subject(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.move_subject(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        subject = self._subjects.pop(old_index)
        self._subjects.insert(new_index, subject)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
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
        for i, subject in enumerate(self._subjects):
            subject.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, subject in enumerate(self._subjects):
            subject.position = index

    def primary_subject(self) -> SubjectT | None:
        """
        Select the first flagged subject, falling back to the first stored record.

        The fallback is not marked primary and competing flags are not validated here.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.primary_subject() is record, record.is_primary
            (True, False)


        :return: Shared selected subject, or None for an empty bucket.
        """
        for subject in self._subjects:
            if subject.is_primary:
                return subject
        return self._subjects[0] if self._subjects else None

    def validate(self) -> None:
        """
        Check subject shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Subject position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0

        for expected_index, subject in enumerate(self._subjects):
            self._validate_subject_shape(subject)
            subject.validate()

            if subject.position != expected_index:
                raise ValueError(
                    f"Subject position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {subject.position}"
                )

            if subject.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary subject is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.subject_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [subject.as_write_payload() for subject in self._subjects]

    def _validate_subject_shape(self, subject: SubjectT) -> None:
        """
        Require matching target kind, target id and subject kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records._validate_subject_shape(record)


        :param subject: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if subject.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {subject.target_kind} subject to {self.target_kind} container"
            )

        if subject.target_id != self.target_id:
            raise ValueError(
                f"Subject target_id {subject.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if subject.kind_key != self.subject_kind:
            raise ValueError(
                f"Subject kind {subject.kind_key} does not match "
                f"container kind {self.subject_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindSubjectsContainer(KindSubjectsContainer[WorkSubject]):
    """
    Collect ordered subject assertions of one kind on a work.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id using its work-specific name.

        Example:
            >>> records = WorkKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
            >>> records.work_id
            3


        :return: Stored work id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindSubjectsContainer(KindSubjectsContainer[ExpressionSubject]):
    """
    Collect ordered subject assertions of one kind on a expression.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ExpressionKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id using its expression-specific name.

        Example:
            >>> records = ExpressionKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
            >>> records.expression_id
            3


        :return: Stored expression id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindSubjectsContainer(KindSubjectsContainer[ManifestationSubject]):
    """
    Collect ordered subject assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ManifestationKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id using its manifestation-specific name.

        Example:
            >>> records = ManifestationKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
            >>> records.manifestation_id
            3


        :return: Stored manifestation id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindSubjectsContainer(KindSubjectsContainer[ItemSubject]):
    """
    Collect ordered subject assertions of one kind on a item.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ItemKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id using its item-specific name.

        Example:
            >>> records = ItemKindSubjectsContainer(subject_kind=SubjectKind.TOPIC, target_id=3)
            >>> records.item_id
            3


        :return: Stored item id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetSubjectsContainer(
    MetadataSequenceStringMixin,
    Generic[SubjectT, KindContainerT],
    abc.ABC,
):
    """
    Group editable subject buckets by kind for a WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkSubjectsContainer(work_id=1)
        >>> records.kinds()
        ()
    """

    _by_kind: dict[SubjectKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "subjects"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
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
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, subject_kind: SubjectKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> bucket = records._make_kind_container(SubjectKind.TOPIC)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[SubjectKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def has_kind(self, subject_kind: SubjectKind) -> bool:
        """
        Check whether a bucket is registered, regardless of its contents.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.has_kind(SubjectKind.TOPIC)
            False


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: True when the kind key exists.
        """
        return subject_kind in self._by_kind

    def get_kind(self, subject_kind: SubjectKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.get_kind(SubjectKind.TOPIC) is None
            True


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(subject_kind)

    def ensure_kind(self, subject_kind: SubjectKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> bucket = records.ensure_kind(SubjectKind.TOPIC)
            >>> records.ensure_kind(SubjectKind.TOPIC) is bucket
            True


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(subject_kind)
        if container is None:
            container = self._make_kind_container(subject_kind)
            self._by_kind[subject_kind] = container
        return container

    def add_subject(self, subject: SubjectT) -> None:
        """
        Check target id, ensure the subject kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later target-kind or
        assertion-kind mismatch can leave a new empty bucket. Successful insertion renumbers
        positions; full validation remains explicit.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.add_subject(WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History'))
            >>> records.kind_text(SubjectKind.TOPIC)
            'History'


        :param subject: Shared record to insert into its kind bucket.
        :return: None.
        """
        if subject.target_id != self.target_id:
            raise ValueError(
                f"Subject target_id {subject.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(subject.kind_key).add_subject(subject)

    def iter_all_subjects(self) -> Iterator[SubjectT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> next(records.iter_all_subjects()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        """
        Collect every subject text in kind registration order and then bucket order.

        Duplicates are retained.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.all_texts()
            ('History',)


        :return: Tuple of stored subject text values.
        """
        return tuple(subject.text for subject in self.iter_all_subjects())

    def kind_text(self, subject_kind: SubjectKind, sep: str = ", ") -> str:
        """
        Join display text for a kind without creating a missing bucket.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.kind_text(SubjectKind.TOPIC), records.kinds()
            ('', ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :param sep: Separator between display strings.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(subject_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_subjects(self) -> dict[SubjectKind, SubjectT]:
        """
        Select the primary-or-first subject from each nonempty registered kind bucket.

        Empty buckets are omitted; selection does not set any flags.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.primary_subjects()[SubjectKind.TOPIC] is record
            True


        :return: New kind-to-subject dictionary retaining shared record references.
        """
        result: dict[SubjectKind, SubjectT] = {}
        for subject_kind, container in self._by_kind.items():
            primary = container.primary_subject()
            if primary is not None:
                result[subject_kind] = primary
        return result

    def primary_subject_for_kind(self, subject_kind: SubjectKind) -> SubjectT | None:
        """
        Select the primary-or-first subject for a kind without creating a bucket.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.primary_subject_for_kind(SubjectKind.TOPIC) is record, record.is_primary
            (True, False)


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: Shared selected subject, or None for an absent or empty kind.
        """
        container = self.get_kind(subject_kind)
        if container is None:
            return None
        return container.primary_subject()

    def validate(self) -> None:
        """
        Validate each registered subject bucket without repairing data or checking outer ownership.

        Each bucket checks its own target, kind and records. Supplied bucket mappings are
        not checked against this outer target or their dictionary keys; the first bucket
        error propagates.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkSubjectsContainer(work_id=1)
            >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
            >>> records.add_subject(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkSubjectsContainer(
    BaseTargetSubjectsContainer[
        WorkSubject,
        WorkKindSubjectsContainer,
    ]
):
    """
    Group all subject assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = WorkSubjectsContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this subject container.

        Example:
            >>> records = WorkSubjectsContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the subject-container target as a work.

        Example:
            >>> records = WorkSubjectsContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, subject_kind: SubjectKind) -> WorkKindSubjectsContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkSubjectsContainer(work_id=3)
            >>> bucket = records._make_kind_container(SubjectKind.TOPIC)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: New WorkKindSubjectsContainer using the stored target id.
        """
        return WorkKindSubjectsContainer(subject_kind=subject_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionSubjectsContainer(
    BaseTargetSubjectsContainer[
        ExpressionSubject,
        ExpressionKindSubjectsContainer,
    ]
):
    """
    Group all subject assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ExpressionSubjectsContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this subject container.

        Example:
            >>> records = ExpressionSubjectsContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the subject-container target as a expression.

        Example:
            >>> records = ExpressionSubjectsContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(
        self,
        subject_kind: SubjectKind,
    ) -> ExpressionKindSubjectsContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionSubjectsContainer(expression_id=3)
            >>> bucket = records._make_kind_container(SubjectKind.TOPIC)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: New ExpressionKindSubjectsContainer using the stored target id.
        """
        return ExpressionKindSubjectsContainer(
            subject_kind=subject_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationSubjectsContainer(
    BaseTargetSubjectsContainer[
        ManifestationSubject,
        ManifestationKindSubjectsContainer,
    ]
):
    """
    Group all subject assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ManifestationSubjectsContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this subject container.

        Example:
            >>> records = ManifestationSubjectsContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the subject-container target as a manifestation.

        Example:
            >>> records = ManifestationSubjectsContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(
        self,
        subject_kind: SubjectKind,
    ) -> ManifestationKindSubjectsContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationSubjectsContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(SubjectKind.TOPIC)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: New ManifestationKindSubjectsContainer using the stored target id.
        """
        return ManifestationKindSubjectsContainer(
            subject_kind=subject_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemSubjectsContainer(
    BaseTargetSubjectsContainer[
        ItemSubject,
        ItemKindSubjectsContainer,
    ]
):
    """
    Group all subject assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ItemSubjectsContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this subject container.

        Example:
            >>> records = ItemSubjectsContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the subject-container target as a item.

        Example:
            >>> records = ItemSubjectsContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, subject_kind: SubjectKind) -> ItemKindSubjectsContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemSubjectsContainer(item_id=3)
            >>> bucket = records._make_kind_container(SubjectKind.TOPIC)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param subject_kind: SubjectKind value selecting a bucket for this target.
        :return: New ItemKindSubjectsContainer using the stored target id.
        """
        return ItemKindSubjectsContainer(subject_kind=subject_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Subject-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(subject_kind: SubjectKind) -> str:
    """
    Look up the fixed convenience-property stem for a subject kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(SubjectKind.TOPIC)
        'topics'


    :param subject_kind: SubjectKind value selecting a convenience stem.
    :return: Attribute stem for the selected kind.
    """
    stems: dict[SubjectKind, str] = {
        SubjectKind.TOPIC: "topics",
        SubjectKind.CHARACTER: "characters",
        SubjectKind.PLACE: "places",
        SubjectKind.PERIOD: "periods",
    }
    return stems[subject_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetSubjectsContainer],
) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a subject container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkSubjectsContainer(work_id=1)
        >>> records.topics_text, records.kinds()
        ('', ())
        >>> bucket = records.topics
        >>> records.get_kind(SubjectKind.TOPIC) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for subject_kind in SubjectKind:
        stem = _kind_property_stem(subject_kind)

        def kind_container_getter(self, _kind=subject_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkSubjectsContainer(work_id=1)
                >>> bucket = records.topics
                >>> records.get_kind(SubjectKind.TOPIC) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Subject kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=subject_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkSubjectsContainer(work_id=1)
                >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
                >>> records.add_subject(record)
                >>> records.topics_text
                'History'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Subject kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=subject_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkSubjectsContainer(work_id=1)
                >>> record = WorkSubject(work_id=1, subject_kind=SubjectKind.TOPIC, text='History')
                >>> records.add_subject(record)
                >>> records.topics_to_text(sep=' / ')
                'History'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Subject kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkSubjectsContainer)
_install_kind_convenience_properties(ExpressionSubjectsContainer)
_install_kind_convenience_properties(ManifestationSubjectsContainer)
_install_kind_convenience_properties(ItemSubjectsContainer)


__all__ = [
    "SubjectKind",
    "SubjectBase",
    "WorkSubject",
    "ExpressionSubject",
    "ManifestationSubject",
    "ItemSubject",
    "KindSubjectsContainer",
    "WorkKindSubjectsContainer",
    "ExpressionKindSubjectsContainer",
    "ManifestationKindSubjectsContainer",
    "ItemKindSubjectsContainer",
    "BaseTargetSubjectsContainer",
    "WorkSubjectsContainer",
    "ExpressionSubjectsContainer",
    "ManifestationSubjectsContainer",
    "ItemSubjectsContainer",
]
