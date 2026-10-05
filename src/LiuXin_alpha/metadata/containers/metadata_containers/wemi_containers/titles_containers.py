"""
Represent editable WEMI title attachments, kind buckets and an item-centred read-side slice.

Title records carry language/script, ordering and provenance hints rather than
proxying database rows. ItemWemiTitleSlice provides the read-side combination of
title containers; record construction and serialization do not validate.

Example:
    >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import TitleKind
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


TitleT = TypeVar("TitleT", bound="TitleBase")
KindContainerT = TypeVar("KindContainerT", bound="KindTitlesContainer")



@dataclass(slots=True, kw_only=True)
class TitleBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a title attachment with display, normalized and sort text with optional language and script references.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied. Validation and referenced-row resolution are
    separate operations.

    Example:
        >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
        >>> record.target_kind
        'work'
    """

    title_kind: TitleKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None
    script_code: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS: ClassVar[tuple[str, ...]] = (
        "text",
        "title_kind",
        "source",
    )

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this title attachment.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this title attachment.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def kind_key(self) -> TitleKind:
        """
        Return the stored title kind used for bucket grouping.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record.kind_key == TitleKind.MAIN
            True


        :return: Stored TitleKind value without normalization.
        """
        return self.title_kind

    def validate(self) -> None:
        """
        Reject blank title-kind text, blank display text and negative record positions.

        Kind is stringified before its blankness check. Text is stripped only for
        validation, leaving the stored value unchanged. No checks are made on
        language/script references or normalized/sort text. Failures raise ValueError.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record.text = ' '
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: text cannot be blank


        :return: None.
        """
        if not str(self.title_kind).strip():
            raise ValueError("title_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared title fields without target additions, validation or persistence.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record._common_write_payload()['title_kind'] == TitleKind.MAIN
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "title_kind": self.title_kind,
            "text": self.text,
            "normalized_text": self.normalized_text,
            "sort_text": self.sort_text,
            "language_id": self.language_id,
            "script_code": self.script_code,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared title fields and concrete target context.

        Example:
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkTitle(TitleBase):
    """
    Attach a title record to a work, including the canonical-for-work flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = WorkTitle(work_id=3, title_kind=TitleKind.MAIN, text='Example')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """

    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this title.

        Example:
            >>> record = WorkTitle(work_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this title as attached to a work.

        Example:
            >>> record = WorkTitle(work_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared title fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkTitle(work_id=3, title_kind=TitleKind.MAIN, text='Example')
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
class ExpressionTitle(TitleBase):
    """
    Attach a title record to a expression, including optional applies-to and transliteration-source language ids.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ExpressionTitle(expression_id=3, title_kind=TitleKind.MAIN, text='Example')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """

    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None
    transliterated_from_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this title.

        Example:
            >>> record = ExpressionTitle(expression_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this title as attached to a expression.

        Example:
            >>> record = ExpressionTitle(expression_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared title fields with the expression id and optional applies-to and transliteration-source language ids.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionTitle(expression_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific values.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "expression_id": self.expression_id,
                "applies_to_language_id": self.applies_to_language_id,
                "transliterated_from_language_id": self.transliterated_from_language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationTitle(TitleBase):
    """
    Attach a title record to a manifestation, including edition-specific and title-page-transcription flags.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ManifestationTitle(manifestation_id=3, title_kind=TitleKind.MAIN, text='Example')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """

    manifestation_id: ManifestationID
    edition_specific: bool = True
    transcribed_from_title_page: bool = False

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this title.

        Example:
            >>> record = ManifestationTitle(manifestation_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this title as attached to a manifestation.

        Example:
            >>> record = ManifestationTitle(manifestation_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared title fields with the manifestation id and edition-specific and title-page-transcription flags.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationTitle(manifestation_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific values.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_specific": self.edition_specific,
                "transcribed_from_title_page": self.transcribed_from_title_page,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemTitle(TitleBase):
    """
    Attach a title record to a item, including copy-specific and cataloguer-supplied flags.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ItemTitle(item_id=3, title_kind=TitleKind.MAIN, text='Example')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """

    item_id: ItemID
    copy_specific: bool = True
    supplied_by_cataloguer: bool = False

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this title.

        Example:
            >>> record = ItemTitle(item_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this title as attached to a item.

        Example:
            >>> record = ItemTitle(item_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared title fields with the item id and copy-specific and cataloguer-supplied flags.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemTitle(item_id=3, title_kind=TitleKind.MAIN, text='Example')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
                "supplied_by_cataloguer": self.supplied_by_cataloguer,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindTitlesContainer(MetadataSequenceStringMixin, Generic[TitleT], abc.ABC):
    """
    Maintain an ordered editable title list for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _titles list; otherwise it
    creates a fresh list. Records remain shared. Insertion checks shape and renumbers
    positions, while full validation is explicit.

    Example:
        >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
        >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
        >>> records.add_title(record)
        >>> records.texts()
        ('Example',)
    """

    title_kind: TitleKind
    target_id: int
    _titles: list[TitleT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL: ClassVar[str] = "titles"

    def __iter__(self) -> Iterator[TitleT]:
        """
        Iterate over shared title records in list order.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._titles)

    def __len__(self) -> int:
        """
        Count the stored title assertions.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> len(records)
            1


        :return: Number of records.
        """
        return len(self._titles)

    def __getitem__(self, index: int) -> TitleT:
        """
        Read a title by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._titles[index]

    def titles(self) -> tuple[TitleT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.titles()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._titles)

    def texts(self) -> tuple[str, ...]:
        """
        Collect stored title text in list order.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.texts()
            ('Example',)


        :return: Tuple of display strings, retaining duplicates.
        """
        return tuple(title.text for title in self._titles)

    def sort_texts(self) -> tuple[str, ...]:
        """
        Prefer each truthy sort_text value, falling back to its display text.

        The records are not sorted and no normalization occurs.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> record.sort_text = ''
            >>> records.sort_texts()
            ('Example',)
            >>> record.sort_text = 'example'
            >>> records.sort_texts()
            ('example',)


        :return: Tuple of selected text values in list order.
        """
        return tuple(title.sort_text or title.text for title in self._titles)

    def to_text(self, sep: str = " ; ") -> str:
        """
        Join every display string with the requested separator.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.to_text(sep=' / ')
            'Example'


        :param sep: Separator between consecutive display strings.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_title(self, title: TitleT) -> None:
        """
        Check target and title kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> record.position
            0


        :param title: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_title_shape(title)
        self._titles.append(title)
        self.normalize_positions()

    def replace_title(self, index: int, title: TitleT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.replace_title(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param title: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_title_shape(title)
        self._titles[index] = title
        self.normalize_positions()

    def remove_title_at(self, index: int) -> TitleT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.remove_title_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._titles.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._titles.clear()

    def move_title(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.move_title(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        title = self._titles.pop(old_index)
        self._titles.insert(new_index, title)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
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
        for i, title in enumerate(self._titles):
            title.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, title in enumerate(self._titles):
            title.position = index

    def primary_title(self) -> TitleT | None:
        """
        Select the first flagged title, falling back to the first stored record.

        The fallback is not marked primary and competing flags are not validated here.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.primary_title() is record, record.is_primary
            (True, False)


        :return: Shared selected title, or None for an empty bucket.
        """
        for title in self._titles:
            if title.is_primary:
                return title
        return self._titles[0] if self._titles else None

    def validate(self) -> None:
        """
        Check title shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Title position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0

        for expected_index, title in enumerate(self._titles):
            self._validate_title_shape(title)
            title.validate()

            if title.position != expected_index:
                raise ValueError(
                    f"Title position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {title.position}"
                )

            if title.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary title is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.title_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [title.as_write_payload() for title in self._titles]

    def _validate_title_shape(self, title: TitleT) -> None:
        """
        Require matching target kind, target id and title kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records._validate_title_shape(record)


        :param title: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if title.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {title.target_kind} title to {self.target_kind} container"
            )

        if title.target_id != self.target_id:
            raise ValueError(
                f"Title target_id {title.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if title.kind_key != self.title_kind:
            raise ValueError(
                f"Title kind {title.kind_key} does not match "
                f"container kind {self.title_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindTitlesContainer(KindTitlesContainer[WorkTitle]):
    """
    Collect ordered title assertions of one kind on a work.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id using its work-specific name.

        Example:
            >>> records = WorkKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
            >>> records.work_id
            3


        :return: Stored work id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindTitlesContainer(KindTitlesContainer[ExpressionTitle]):
    """
    Collect ordered title assertions of one kind on a expression.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ExpressionKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id using its expression-specific name.

        Example:
            >>> records = ExpressionKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
            >>> records.expression_id
            3


        :return: Stored expression id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindTitlesContainer(KindTitlesContainer[ManifestationTitle]):
    """
    Collect ordered title assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ManifestationKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id using its manifestation-specific name.

        Example:
            >>> records = ManifestationKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
            >>> records.manifestation_id
            3


        :return: Stored manifestation id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindTitlesContainer(KindTitlesContainer[ItemTitle]):
    """
    Collect ordered title assertions of one kind on a item.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ItemKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id using its item-specific name.

        Example:
            >>> records = ItemKindTitlesContainer(title_kind=TitleKind.MAIN, target_id=3)
            >>> records.item_id
            3


        :return: Stored item id.
        """
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetTitlesContainer(
    MetadataSequenceStringMixin,
    Generic[TitleT, KindContainerT],
    abc.ABC,
):
    """
    Group editable title buckets by kind for a WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkTitlesContainer(work_id=1)
        >>> records.kinds()
        ()
    """

    _by_kind: dict[TitleKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL: ClassVar[str] = "titles"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
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
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, title_kind: TitleKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> bucket = records._make_kind_container(TitleKind.MAIN)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[TitleKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def has_kind(self, title_kind: TitleKind) -> bool:
        """
        Check whether a bucket is registered, regardless of its contents.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.has_kind(TitleKind.MAIN)
            False


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: True when the kind key exists.
        """
        return title_kind in self._by_kind

    def get_kind(self, title_kind: TitleKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.get_kind(TitleKind.MAIN) is None
            True


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(title_kind)

    def ensure_kind(self, title_kind: TitleKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> bucket = records.ensure_kind(TitleKind.MAIN)
            >>> records.ensure_kind(TitleKind.MAIN) is bucket
            True


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(title_kind)
        if container is None:
            container = self._make_kind_container(title_kind)
            self._by_kind[title_kind] = container
        return container

    def add_title(self, title: TitleT) -> None:
        """
        Check target id, ensure the title kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later target-kind or
        assertion-kind mismatch can leave a new empty bucket. Successful insertion renumbers
        positions; full validation remains explicit.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.add_title(WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example'))
            >>> records.kind_text(TitleKind.MAIN)
            'Example'


        :param title: Shared record to insert into its kind bucket.
        :return: None.
        """
        if title.target_id != self.target_id:
            raise ValueError(
                f"Title target_id {title.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(title.kind_key).add_title(title)

    def iter_all_titles(self) -> Iterator[TitleT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> next(records.iter_all_titles()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        """
        Collect every title text in kind registration order and then bucket order.

        Duplicates are retained.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.all_texts()
            ('Example',)


        :return: Tuple of stored title text values.
        """
        return tuple(title.text for title in self.iter_all_titles())

    def kind_text(self, title_kind: TitleKind, sep: str = " ; ") -> str:
        """
        Join display text for a kind without creating a missing bucket.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.kind_text(TitleKind.MAIN), records.kinds()
            ('', ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :param sep: Separator between display strings.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(title_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_titles(self) -> dict[TitleKind, TitleT]:
        """
        Select the primary-or-first title from each nonempty registered kind bucket.

        Empty buckets are omitted; selection does not set any flags.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.primary_titles()[TitleKind.MAIN] is record
            True


        :return: New kind-to-title dictionary retaining shared record references.
        """
        result: dict[TitleKind, TitleT] = {}
        for title_kind, container in self._by_kind.items():
            primary = container.primary_title()
            if primary is not None:
                result[title_kind] = primary
        return result

    def primary_title_for_kind(self, title_kind: TitleKind) -> TitleT | None:
        """
        Select the primary-or-first title for a kind without creating a bucket.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.primary_title_for_kind(TitleKind.MAIN) is record, record.is_primary
            (True, False)


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: Shared selected title, or None for an absent or empty kind.
        """
        container = self.get_kind(title_kind)
        if container is None:
            return None
        return container.primary_title()

    @property
    def main_title(self) -> TitleT | None:
        """
        Select the primary or first record in the MAIN title bucket without creating a bucket.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> titles.main_title is title
            True


        :return: Shared main-title record, or None.
        """
        return self.primary_title_for_kind(TitleKind.MAIN)

    @property
    def display_title(self) -> str | None:
        """
        Choose display text by preferred title kind, then by insertion order.

        Try MAIN, TRANSLATED, ALTERNATIVE and SUPPLIED, using each bucket's primary or first
        record. A found record returns its raw text, including an empty string. If none is
        found, use the first record across registered buckets. No buckets are created.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> titles.display_title
            'Example'
            >>> title.text = ''
            >>> titles.display_title
            ''


        :return: Selected text, or None when every bucket is empty.
        """
        for kind in (
            TitleKind.MAIN,
            TitleKind.TRANSLATED,
            TitleKind.ALTERNATIVE,
            TitleKind.SUPPLIED,
        ):
            title = self.primary_title_for_kind(kind)
            if title is not None:
                return title.text
        first = next(self.iter_all_titles(), None)
        return first.text if first is not None else None

    @property
    def sort_title(self) -> str | None:
        """
        Prefer explicit SORT text, then main-title sort hints, then display text.

        An explicit SORT record returns its text even when empty. Otherwise a main record
        uses its first truthy sort_text, normalized_text or text; without a main record, use
        display_title. This selects text without sorting records.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> title.sort_text = 'Example, The'
            >>> titles.sort_title
            'Example, The'


        :return: Chosen sort/display text, or None without titles.
        """
        sort_title = self.primary_title_for_kind(TitleKind.SORT)
        if sort_title is not None:
            return sort_title.text

        main_title = self.main_title
        if main_title is not None:
            return main_title.sort_text or main_title.normalized_text or main_title.text

        return self.display_title

    def validate(self) -> None:
        """
        Validate each registered title bucket without repairing data or checking outer ownership.

        Each bucket checks its own target, kind and records. Supplied bucket mappings are
        not checked against this outer target or their dictionary keys; the first bucket
        error propagates.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkTitlesContainer(work_id=1)
            >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> records.add_title(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkTitlesContainer(
    BaseTargetTitlesContainer[
        WorkTitle,
        WorkKindTitlesContainer,
    ]
):
    """
    Group all title assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = WorkTitlesContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this title container.

        Example:
            >>> records = WorkTitlesContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the title-container target as a work.

        Example:
            >>> records = WorkTitlesContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, title_kind: TitleKind) -> WorkKindTitlesContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkTitlesContainer(work_id=3)
            >>> bucket = records._make_kind_container(TitleKind.MAIN)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: New WorkKindTitlesContainer using the stored target id.
        """
        return WorkKindTitlesContainer(title_kind=title_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionTitlesContainer(
    BaseTargetTitlesContainer[
        ExpressionTitle,
        ExpressionKindTitlesContainer,
    ]
):
    """
    Group all title assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ExpressionTitlesContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this title container.

        Example:
            >>> records = ExpressionTitlesContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the title-container target as a expression.

        Example:
            >>> records = ExpressionTitlesContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(
        self,
        title_kind: TitleKind,
    ) -> ExpressionKindTitlesContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionTitlesContainer(expression_id=3)
            >>> bucket = records._make_kind_container(TitleKind.MAIN)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: New ExpressionKindTitlesContainer using the stored target id.
        """
        return ExpressionKindTitlesContainer(
            title_kind=title_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationTitlesContainer(
    BaseTargetTitlesContainer[
        ManifestationTitle,
        ManifestationKindTitlesContainer,
    ]
):
    """
    Group all title assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ManifestationTitlesContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this title container.

        Example:
            >>> records = ManifestationTitlesContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the title-container target as a manifestation.

        Example:
            >>> records = ManifestationTitlesContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(
        self,
        title_kind: TitleKind,
    ) -> ManifestationKindTitlesContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationTitlesContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(TitleKind.MAIN)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: New ManifestationKindTitlesContainer using the stored target id.
        """
        return ManifestationKindTitlesContainer(
            title_kind=title_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemTitlesContainer(
    BaseTargetTitlesContainer[
        ItemTitle,
        ItemKindTitlesContainer,
    ]
):
    """
    Group all title assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ItemTitlesContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """

    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this title container.

        Example:
            >>> records = ItemTitlesContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the title-container target as a item.

        Example:
            >>> records = ItemTitlesContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, title_kind: TitleKind) -> ItemKindTitlesContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemTitlesContainer(item_id=3)
            >>> bucket = records._make_kind_container(TitleKind.MAIN)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param title_kind: TitleKind value selecting a bucket for this target.
        :return: New ItemKindTitlesContainer using the stored target id.
        """
        return ItemKindTitlesContainer(title_kind=title_kind, target_id=self.item_id)


@dataclass(slots=True, kw_only=True)
class ItemWemiTitleSlice:
    """
    Combine optional title containers along an item-centred WEMI path.

    The keyword-only dataclass retains the work, expression, manifestation and item
    containers by reference. Rendering reads their current display titles without
    fetching or validating metadata.

    Example:
        >>> titles = WorkTitlesContainer(work_id=1)
        >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
        >>> titles.add_title(title)
        >>> slice_ = ItemWemiTitleSlice(work_titles=titles)
        >>> slice_.title_parts()
        ('Example',)
    """

    work_titles: WorkTitlesContainer | None = None
    expression_titles: ExpressionTitlesContainer | None = None
    manifestation_titles: ManifestationTitlesContainer | None = None
    item_titles: ItemTitlesContainer | None = None

    def title_parts(self, *, dedupe: bool = True) -> tuple[str, ...]:
        """
        Collect truthy display titles in work, expression, manifestation and item order.

        Empty values are omitted but whitespace-only text is retained. Optional
        deduplication compares exact text and preserves its first occurrence; it performs no
        case or whitespace normalization.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> slice_ = ItemWemiTitleSlice(work_titles=titles)
            >>> slice_.expression_titles = titles
            >>> slice_.title_parts()
            ('Example',)
            >>> slice_.title_parts(dedupe=False)
            ('Example', 'Example')


        :param dedupe: Suppress later exact duplicates when True.
        :return: Tuple of selected title strings.
        """
        raw_parts = [
            self.work_titles.display_title if self.work_titles is not None else None,
            self.expression_titles.display_title if self.expression_titles is not None else None,
            self.manifestation_titles.display_title if self.manifestation_titles is not None else None,
            self.item_titles.display_title if self.item_titles is not None else None,
        ]

        parts: list[str] = []
        seen: set[str] = set()
        for part in raw_parts:
            if not part:
                continue
            if dedupe and part in seen:
                continue
            parts.append(part)
            seen.add(part)
        return tuple(parts)

    def full_title(self, sep: str = " — ", *, dedupe: bool = True) -> str:
        """
        Join the current title parts with the supplied separator.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> slice_ = ItemWemiTitleSlice(work_titles=titles)
            >>> slice_.full_title(sep=' / ')
            'Example'


        :param sep: Literal separator placed between parts; defaults to a spaced em dash.
        :param dedupe: Suppress later exact duplicate parts when True.
        :return: Joined string, or an empty string when no parts exist.
        """
        return sep.join(self.title_parts(dedupe=dedupe))

    def __str__(self) -> str:
        """
        Render the full title using default deduplication and a spaced em dash.

        Example:
            >>> titles = WorkTitlesContainer(work_id=1)
            >>> title = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
            >>> titles.add_title(title)
            >>> slice_ = ItemWemiTitleSlice(work_titles=titles)
            >>> str(slice_)
            'Example'


        :return: Joined title string.
        """
        return self.full_title()


# ---------------------------------------------------------------------------
# Title-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(title_kind: TitleKind) -> str:
    """
    Look up the fixed convenience-property stem for a title kind.

    The twelve title kinds have explicit stems; unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(TitleKind.MAIN)
        'main_titles'


    :param title_kind: TitleKind value selecting a convenience stem.
    :return: Attribute stem for the selected kind.
    """
    stems: dict[TitleKind, str] = {
        TitleKind.MAIN: "main_titles",
        TitleKind.SUBTITLE: "subtitles",
        TitleKind.ALTERNATIVE: "alternative_titles",
        TitleKind.SHORT: "short_titles",
        TitleKind.SORT: "sort_titles",
        TitleKind.UNIFORM: "uniform_titles",
        TitleKind.TRANSLATED: "translated_titles",
        TitleKind.TRANSLITERATED: "transliterated_titles",
        TitleKind.COVER: "cover_titles",
        TitleKind.SPINE: "spine_titles",
        TitleKind.RUNNING: "running_titles",
        TitleKind.SUPPLIED: "supplied_titles",
    }
    return stems[title_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetTitlesContainer],
) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a title container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkTitlesContainer(work_id=1)
        >>> records.main_titles_text, records.kinds()
        ('', ())
        >>> bucket = records.main_titles
        >>> records.get_kind(TitleKind.MAIN) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """

    for title_kind in TitleKind:
        stem = _kind_property_stem(title_kind)

        def kind_container_getter(self, _kind=title_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkTitlesContainer(work_id=1)
                >>> bucket = records.main_titles
                >>> records.get_kind(TitleKind.MAIN) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Title kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=title_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkTitlesContainer(work_id=1)
                >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
                >>> records.add_title(record)
                >>> records.main_titles_text
                'Example'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Title kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = " ; ", _kind=title_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkTitlesContainer(work_id=1)
                >>> record = WorkTitle(work_id=1, title_kind=TitleKind.MAIN, text='Example')
                >>> records.add_title(record)
                >>> records.main_titles_to_text(sep=' / ')
                'Example'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Title kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkTitlesContainer)
_install_kind_convenience_properties(ExpressionTitlesContainer)
_install_kind_convenience_properties(ManifestationTitlesContainer)
_install_kind_convenience_properties(ItemTitlesContainer)


__all__ = [
    "TitleKind",
    "TitleBase",
    "WorkTitle",
    "ExpressionTitle",
    "ManifestationTitle",
    "ItemTitle",
    "KindTitlesContainer",
    "WorkKindTitlesContainer",
    "ExpressionKindTitlesContainer",
    "ManifestationKindTitlesContainer",
    "ItemKindTitlesContainer",
    "BaseTargetTitlesContainer",
    "WorkTitlesContainer",
    "ExpressionTitlesContainer",
    "ManifestationTitlesContainer",
    "ItemTitlesContainer",
    "ItemWemiTitleSlice",
]
