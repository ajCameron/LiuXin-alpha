"""
Represent editable note assertions and ordered kind buckets for WEMI targets.

Records carry target attachment, ordering and provenance data. Containers share
record objects and validate explicitly; payload creation does not write to a
database.

Example:
    >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import (
    NoteKind,
    NoteFormat,
    NoteVisibility,
)
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


NoteT = TypeVar("NoteT", bound="NoteBase")
KindContainerT = TypeVar("KindContainerT", bound="KindNotesContainer")


@dataclass(slots=True, kw_only=True)
class NoteBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a note attachment with a body, format, optional title/language, visibility and association interval.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied, with no automatic validation, normalization or
    referenced-row lookup.

    Example:
        >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> record.target_kind
        'work'
    """

    note_kind: NoteKind
    body: str
    body_format: NoteFormat = NoteFormat.PLAIN_TEXT
    title: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    visibility: NoteVisibility = NoteVisibility.PRIVATE
    notes: str | None = None

    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None
    STRING_DISPLAY_KEYS: ClassVar[tuple[str, ...]] = (
        "title",
        "body",
        "note_kind",
    )

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this note.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this note.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def kind_key(self) -> NoteKind:
        """
        Return the stored note kind used for bucket grouping.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.kind_key == NoteKind.DESCRIPTION
            True


        :return: Stored NoteKind value without conversion.
        """
        return self.note_kind

    def validate(self) -> None:
        """
        Reject blank kind/body text, negative positions and reversed association intervals.

        The kind is stringified and stripped for its check; body whitespace is checked
        without changing the stored body. Interval ordering is checked only when both
        endpoints are present. Format, visibility and referenced ids are not validated.
        Failures raise ValueError.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.association_start_ep_k = 20
            >>> record.association_end_ep_k = 10
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: association_end_ep_k cannot be earlier than association_start_ep_k


        :return: None.
        """
        if not str(self.note_kind).strip():
            raise ValueError("note_kind cannot be blank")

        if not self.body.strip():
            raise ValueError("body cannot be blank")

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
        Collect shared note fields without target additions or validation.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record._common_write_payload()['note_kind'] == NoteKind.DESCRIPTION
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "note_kind": self.note_kind,
            "body": self.body,
            "body_format": self.body_format,
            "title": self.title,
            "language_id": self.language_id,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "visibility": self.visibility,
            "notes": self.notes,
            "association_start_ep_k": self.association_start_ep_k,
            "association_end_ep_k": self.association_end_ep_k,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared note fields and concrete target context.

        Example:
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkNote(NoteBase):
    """
    Attach a note assertion to a work, including the canonical-for-work flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = WorkNote(work_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """

    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this note.

        Example:
            >>> record = WorkNote(work_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this note as attached to a work.

        Example:
            >>> record = WorkNote(work_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared note fields with the work id and the canonical-for-work flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = WorkNote(work_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
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
class ExpressionNote(NoteBase):
    """
    Attach a note assertion to a expression, including an optional applies-to language id.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ExpressionNote(expression_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """

    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this note.

        Example:
            >>> record = ExpressionNote(expression_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this note as attached to a expression.

        Example:
            >>> record = ExpressionNote(expression_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared note fields with the expression id and an optional applies-to language id.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ExpressionNote(expression_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
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
class ManifestationNote(NoteBase):
    """
    Attach a note assertion to a manifestation, including the edition-specific flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ManifestationNote(manifestation_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """

    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this note.

        Example:
            >>> record = ManifestationNote(manifestation_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this note as attached to a manifestation.

        Example:
            >>> record = ManifestationNote(manifestation_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared note fields with the manifestation id and the edition-specific flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ManifestationNote(manifestation_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
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
class ItemNote(NoteBase):
    """
    Attach a note assertion to a item, including the copy-specific and physical-observation flags.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ItemNote(item_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """

    item_id: ItemID
    copy_specific: bool = True
    physical_observation: bool = False

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this note.

        Example:
            >>> record = ItemNote(item_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this note as attached to a item.

        Example:
            >>> record = ItemNote(item_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared note fields with the item id and the copy-specific and physical-observation flags.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ItemNote(item_id=3, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
                "physical_observation": self.physical_observation,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindNotesContainer(MetadataSequenceStringMixin, Generic[NoteT], abc.ABC):
    """
    Maintain ordered note records for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _notes list, or creates a
    fresh list by default. Records remain shared. Insertion checks shape and renumbers
    positions; full validation is explicit.

    Example:
        >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
        >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
        >>> records.add_note(record)
        >>> records.bodies()
        ('A description',)
    """

    note_kind: NoteKind
    target_id: int
    _notes: list[NoteT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL: ClassVar[str] = "notes"

    def __iter__(self) -> Iterator[NoteT]:
        """
        Iterate over shared note records in list order.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._notes)

    def __len__(self) -> int:
        """
        Count the stored note records.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> len(records)
            1


        :return: Number of stored records.
        """
        return len(self._notes)

    def __getitem__(self, index: int) -> NoteT:
        """
        Read a note by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._notes[index]

    def notes(self) -> tuple[NoteT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.notes()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._notes)

    def titles(self) -> tuple[str | None, ...]:
        """
        Collect optional note titles in list order, retaining None, blanks and duplicates.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.titles()
            (None,)


        :return: Tuple of stored title values.
        """
        return tuple(note.title for note in self._notes)

    def bodies(self) -> tuple[str, ...]:
        """
        Collect stored note bodies in list order without format conversion.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.bodies()
            ('A description',)


        :return: Tuple of strings, retaining duplicates.
        """
        return tuple(note.body for note in self._notes)

    def to_text(self, sep: str = "\n\n") -> str:
        """
        Join every note body verbatim using the separator, without interpreting body_format.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.to_text(sep=' / ')
            'A description'


        :param sep: Separator between consecutive bodies; defaults to two newline
            characters.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.bodies())

    def add_note(self, note: NoteT) -> None:
        """
        Check target and note kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> record.position
            0


        :param note: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_note_shape(note)
        self._notes.append(note)
        self.normalize_positions()

    def replace_note(self, index: int, note: NoteT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.replace_note(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param note: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_note_shape(note)
        self._notes[index] = note
        self.normalize_positions()

    def remove_note_at(self, index: int) -> NoteT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.remove_note_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._notes.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._notes.clear()

    def move_note(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.move_note(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        note = self._notes.pop(old_index)
        self._notes.insert(new_index, note)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
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
        for i, note in enumerate(self._notes):
            note.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, note in enumerate(self._notes):
            note.position = index

    def primary_note(self) -> NoteT | None:
        """
        Select the first flagged note, falling back to the first stored record.

        The fallback is not marked primary and competing flags are not validated here.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.primary_note() is record, record.is_primary
            (True, False)


        :return: Shared selected note, or None for an empty bucket.
        """
        for note in self._notes:
            if note.is_primary:
                return note
        return self._notes[0] if self._notes else None

    def validate(self) -> None:
        """
        Check note shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Note position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0

        for expected_index, note in enumerate(self._notes):
            self._validate_note_shape(note)
            note.validate()

            if note.position != expected_index:
                raise ValueError(
                    f"Note position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {note.position}"
                )

            if note.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary note is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.note_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [note.as_write_payload() for note in self._notes]

    def _validate_note_shape(self, note: NoteT) -> None:
        """
        Require matching target kind, target id and note kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records._validate_note_shape(record)


        :param note: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if note.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {note.target_kind} note to {self.target_kind} container"
            )

        if note.target_id != self.target_id:
            raise ValueError(
                f"Note target_id {note.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if note.kind_key != self.note_kind:
            raise ValueError(
                f"Note kind {note.kind_key} does not match "
                f"container kind {self.note_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindNotesContainer(KindNotesContainer[WorkNote]):
    """
    Collect ordered note assertions of one kind on a work.

    Construction retains a supplied list without validation. The target kind is a class
    constant. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id using its work-specific name.

        Example:
            >>> records = WorkKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
            >>> records.work_id
            3


        :return: Stored work id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindNotesContainer(KindNotesContainer[ExpressionNote]):
    """
    Collect ordered note assertions of one kind on a expression.

    Construction retains a supplied list without validation. The target kind is a class
    constant. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ExpressionKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id using its expression-specific name.

        Example:
            >>> records = ExpressionKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
            >>> records.expression_id
            3


        :return: Stored expression id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindNotesContainer(KindNotesContainer[ManifestationNote]):
    """
    Collect ordered note assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. The target kind is a class
    constant. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ManifestationKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id using its manifestation-specific name.

        Example:
            >>> records = ManifestationKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
            >>> records.manifestation_id
            3


        :return: Stored manifestation id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindNotesContainer(KindNotesContainer[ItemNote]):
    """
    Collect ordered note assertions of one kind on a item.

    Construction retains a supplied list without validation. The target kind is a class
    constant. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ItemKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id using its item-specific name.

        Example:
            >>> records = ItemKindNotesContainer(note_kind=NoteKind.DESCRIPTION, target_id=3)
            >>> records.item_id
            3


        :return: Stored item id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetNotesContainer(
    MetadataSequenceStringMixin,
    Generic[NoteT, KindContainerT],
    abc.ABC,
):
    """
    Group editable note buckets by kind for one WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkNotesContainer(work_id=1)
        >>> records.kinds()
        ()
    """

    _by_kind: dict[NoteKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL: ClassVar[str] = "notes"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
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
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, note_kind: NoteKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> bucket = records._make_kind_container(NoteKind.DESCRIPTION)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[NoteKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def has_kind(self, note_kind: NoteKind) -> bool:
        """
        Check whether a note bucket is registered, regardless of its contents.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.has_kind(NoteKind.DESCRIPTION)
            False


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: True when the kind key exists.
        """
        return note_kind in self._by_kind

    def get_kind(self, note_kind: NoteKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.get_kind(NoteKind.DESCRIPTION) is None
            True


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(note_kind)

    def ensure_kind(self, note_kind: NoteKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> bucket = records.ensure_kind(NoteKind.DESCRIPTION)
            >>> records.ensure_kind(NoteKind.DESCRIPTION) is bucket
            True


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(note_kind)
        if container is None:
            container = self._make_kind_container(note_kind)
            self._by_kind[note_kind] = container
        return container

    def add_note(self, note: NoteT) -> None:
        """
        Check target id, ensure the note kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later shape failure can
        leave a new empty bucket. Successful insertion renumbers positions; full validation
        remains explicit.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.add_note(WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description'))
            >>> records.kind_text(NoteKind.DESCRIPTION)
            'A description'


        :param note: Shared record to insert into its kind bucket.
        :return: None.
        """
        if note.target_id != self.target_id:
            raise ValueError(
                f"Note target_id {note.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(note.kind_key).add_note(note)

    def iter_all_notes(self) -> Iterator[NoteT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> next(records.iter_all_notes()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def all_titles(self) -> tuple[str, ...]:
        """
        Collect truthy note titles in kind registration order and then bucket order.

        None and empty strings are omitted; whitespace and duplicates remain.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> record.title = 'Overview'
            >>> records.all_titles()
            ('Overview',)


        :return: Tuple of stored nonempty title values.
        """
        return tuple(note.title for note in self.iter_all_notes() if note.title)

    def kind_text(self, note_kind: NoteKind, sep: str = "\n\n") -> str:
        """
        Join note bodies verbatim without creating a missing bucket or interpreting their formats.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.kind_text(NoteKind.DESCRIPTION), records.kinds()
            ('', ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :param sep: Separator between bodies; defaults to two newline characters.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(note_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_notes(self) -> dict[NoteKind, NoteT]:
        """
        Select the primary-or-first note from each nonempty registered kind bucket.

        Empty buckets are omitted and selection changes no flags.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.primary_notes()[NoteKind.DESCRIPTION] is record, record.is_primary
            (True, False)


        :return: New kind-to-note dictionary retaining shared record references.
        """
        result: dict[NoteKind, NoteT] = {}
        for note_kind, container in self._by_kind.items():
            primary = container.primary_note()
            if primary is not None:
                result[note_kind] = primary
        return result

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing data or checking bucket ownership against this container.

        Each bucket checks its own target, kind and records. The first bucket error
        propagates.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkNotesContainer(work_id=1)
            >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
            >>> records.add_note(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkNotesContainer(
    BaseTargetNotesContainer[
        WorkNote,
        WorkKindNotesContainer,
    ]
):
    """
    Group all note assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = WorkNotesContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this note container.

        Example:
            >>> records = WorkNotesContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the note-container target as a work.

        Example:
            >>> records = WorkNotesContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, note_kind: NoteKind) -> WorkKindNotesContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkNotesContainer(work_id=3)
            >>> bucket = records._make_kind_container(NoteKind.DESCRIPTION)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: New WorkKindNotesContainer using the stored target id.
        """
        return WorkKindNotesContainer(note_kind=note_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionNotesContainer(
    BaseTargetNotesContainer[
        ExpressionNote,
        ExpressionKindNotesContainer,
    ]
):
    """
    Group all note assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ExpressionNotesContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this note container.

        Example:
            >>> records = ExpressionNotesContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the note-container target as a expression.

        Example:
            >>> records = ExpressionNotesContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(
        self,
        note_kind: NoteKind,
    ) -> ExpressionKindNotesContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionNotesContainer(expression_id=3)
            >>> bucket = records._make_kind_container(NoteKind.DESCRIPTION)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: New ExpressionKindNotesContainer using the stored target id.
        """
        return ExpressionKindNotesContainer(
            note_kind=note_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationNotesContainer(
    BaseTargetNotesContainer[
        ManifestationNote,
        ManifestationKindNotesContainer,
    ]
):
    """
    Group all note assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ManifestationNotesContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this note container.

        Example:
            >>> records = ManifestationNotesContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the note-container target as a manifestation.

        Example:
            >>> records = ManifestationNotesContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(
        self,
        note_kind: NoteKind,
    ) -> ManifestationKindNotesContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationNotesContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(NoteKind.DESCRIPTION)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: New ManifestationKindNotesContainer using the stored target id.
        """
        return ManifestationKindNotesContainer(
            note_kind=note_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemNotesContainer(
    BaseTargetNotesContainer[
        ItemNote,
        ItemKindNotesContainer,
    ]
):
    """
    Group all note assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ItemNotesContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this note container.

        Example:
            >>> records = ItemNotesContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the note-container target as a item.

        Example:
            >>> records = ItemNotesContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, note_kind: NoteKind) -> ItemKindNotesContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemNotesContainer(item_id=3)
            >>> bucket = records._make_kind_container(NoteKind.DESCRIPTION)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param note_kind: NoteKind value selecting a bucket for this target.
        :return: New ItemKindNotesContainer using the stored target id.
        """
        return ItemKindNotesContainer(note_kind=note_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Note-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(note_kind: NoteKind) -> str:
    """
    Look up the fixed convenience-property stem for a note kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(NoteKind.DESCRIPTION)
        'descriptions'


    :param note_kind: NoteKind value selecting a bucket for this target.
    :return: Attribute stem for the selected kind.
    """
    stems: dict[NoteKind, str] = {
        NoteKind.DESCRIPTION: "descriptions",
        NoteKind.REVIEW: "reviews",
        NoteKind.ANNOTATION: "annotations",
        NoteKind.SUMMARY: "summaries",
        NoteKind.TRANSCRIPTION: "transcriptions",
        NoteKind.PROVENANCE: "provenance_notes",
        NoteKind.CONDITION: "condition_notes",
        NoteKind.ACQUISITION: "acquisition_notes",
        NoteKind.CONTENTS: "contents_notes",
        NoteKind.CITATION: "citation_notes",
        NoteKind.INTERNAL: "internal_notes",
    }
    return stems[note_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetNotesContainer],
) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a note container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkNotesContainer(work_id=1)
        >>> records.descriptions_text, records.kinds()
        ('', ())
        >>> bucket = records.descriptions
        >>> records.get_kind(NoteKind.DESCRIPTION) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """

    for note_kind in NoteKind:
        stem = _kind_property_stem(note_kind)

        def kind_container_getter(self, _kind=note_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkNotesContainer(work_id=1)
                >>> bucket = records.descriptions
                >>> records.get_kind(NoteKind.DESCRIPTION) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Note kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=note_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkNotesContainer(work_id=1)
                >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
                >>> records.add_note(record)
                >>> records.descriptions_text
                'A description'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Note kind captured as the default argument during installation.
            :return: Joined note bodies, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = "\n\n", _kind=note_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkNotesContainer(work_id=1)
                >>> record = WorkNote(work_id=1, note_kind=NoteKind.DESCRIPTION, body='A description')
                >>> records.add_note(record)
                >>> records.descriptions_to_text(sep=' / ')
                'A description'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Note kind captured as the default argument during installation.
            :return: Joined note bodies, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkNotesContainer)
_install_kind_convenience_properties(ExpressionNotesContainer)
_install_kind_convenience_properties(ManifestationNotesContainer)
_install_kind_convenience_properties(ItemNotesContainer)


__all__ = [
    "NoteKind",
    "NoteFormat",
    "NoteVisibility",
    "NoteBase",
    "WorkNote",
    "ExpressionNote",
    "ManifestationNote",
    "ItemNote",
    "KindNotesContainer",
    "WorkKindNotesContainer",
    "ExpressionKindNotesContainer",
    "ManifestationKindNotesContainer",
    "ItemKindNotesContainer",
    "BaseTargetNotesContainer",
    "WorkNotesContainer",
    "ExpressionNotesContainer",
    "ManifestationNotesContainer",
    "ItemNotesContainer",
]
