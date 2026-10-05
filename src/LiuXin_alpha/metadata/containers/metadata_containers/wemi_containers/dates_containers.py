"""
Model editable date assertions attached to work, expression, manifestation and item targets.

Date records carry optional ep_k endpoints, display text and provenance. Containers
group and order those records by date kind. These are attached metadata values
rather than independent identity objects or joined query views.

Example:
    >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
    >>> date.validate()
    >>> date.rendered_text
    '10'
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import DateKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID, LanguageID

DateT = TypeVar("DateT", bound="DateBase")
KindContainerT = TypeVar("KindContainerT", bound="KindDatesContainer")


@dataclass(slots=True, kw_only=True)
class DateBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold common date assertion fields and require a concrete WEMI target.

    Concrete keyword-only dataclasses add their target id and level-specific flags or
    language context. Construction stores values; validation is explicit, and
    rendered_text does not parse or format a calendar date.

    Example:
        >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
        >>> date.date_kind == DateKind.CREATED
        True
    """

    date_kind: DateKind
    start_ep_k: int | None = None
    end_ep_k: int | None = None
    display_text: str | None = None
    is_approximate: bool = False
    calendar: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("display_text", "start_ep_k", "date_kind")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the row id of the WEMI entity receiving this date assertion.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this date assertion.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def rendered_text(self) -> str:
        """
        Prefer truthy display text, then render the stored endpoints as plain numbers.

        Unequal paired endpoints are joined with an en dash. Otherwise the start, then the
        end, is stringified; missing endpoints produce empty text. Approximation and
        calendar fields do not alter this rendering.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date.end_ep_k = 20
            >>> date.rendered_text
            '10–20'
            >>> date.display_text = 'circa 1900'
            >>> date.rendered_text
            'circa 1900'


        :return: Display text, numeric endpoint text or an empty string.
        """
        if self.display_text:
            return self.display_text
        if self.start_ep_k is not None and self.end_ep_k is not None and self.start_ep_k != self.end_ep_k:
            return f"{self.start_ep_k}–{self.end_ep_k}"
        if self.start_ep_k is not None:
            return str(self.start_ep_k)
        if self.end_ep_k is not None:
            return str(self.end_ep_k)
        return ""

    def validate(self) -> None:
        """
        Require an endpoint or truthy display text, a nonnegative position and an ordered interval.

        Raise ValueError on the first failed constraint. Display text is tested for
        truthiness without stripping, equal endpoints are allowed, and date kind, calendar
        and approximation are not validated.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date.end_ep_k = 5
            >>> date.validate()
            Traceback (most recent call last):
            ...
            ValueError: end_ep_k cannot be earlier than start_ep_k


        :return: None.
        """
        if self.start_ep_k is None and self.end_ep_k is None and not self.display_text:
            raise ValueError("date record must provide at least one of start_ep_k, end_ep_k, or display_text")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if self.start_ep_k is not None and self.end_ep_k is not None and self.end_ep_k < self.start_ep_k:
            raise ValueError("end_ep_k cannot be earlier than start_ep_k")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect the shared date fields without validation or target-specific additions.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date._common_write_payload()['start_ep_k']
            10


        :return: New dictionary retaining stored values, including the DateKind enum.
        """
        return {
            "date_kind": self.date_kind,
            "start_ep_k": self.start_ep_k,
            "end_ep_k": self.end_ep_k,
            "display_text": self.display_text,
            "is_approximate": self.is_approximate,
            "calendar": self.calendar,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require a write-layer dictionary containing shared and target-specific date fields.

        Example:
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> date.as_write_payload()['work_id']
            1


        :return: New payload dictionary; serialization does not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkDate(DateBase):
    """
    Represent a typed date assertion on a work, including the canonical-for-work flag.

    Endpoints and display text are stored as supplied. Call validate explicitly to check
    the shared date constraints.

    Example:
        >>> date = WorkDate(work_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
        >>> date.rendered_text
        'circa 1900'
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this date.

        Example:
            >>> date = WorkDate(work_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this date as attached to a work.

        Example:
            >>> date = WorkDate(work_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared date fields with the work id and the canonical-for-work flag.

        Values are retained without validation, calendar conversion or persistence.

        Example:
            >>> date = WorkDate(work_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionDate(DateBase):
    """
    Represent a typed date assertion on a expression, including an optional applies-to language id.

    Endpoints and display text are stored as supplied. Call validate explicitly to check
    the shared date constraints.

    Example:
        >>> date = ExpressionDate(expression_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
        >>> date.rendered_text
        'circa 1900'
    """
    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this date.

        Example:
            >>> date = ExpressionDate(expression_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this date as attached to a expression.

        Example:
            >>> date = ExpressionDate(expression_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared date fields with the expression id and an optional applies-to language id.

        Values are retained without validation, calendar conversion or persistence.

        Example:
            >>> date = ExpressionDate(expression_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_language_id": self.applies_to_language_id})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationDate(DateBase):
    """
    Represent a typed date assertion on a manifestation, including the edition-specific flag.

    Endpoints and display text are stored as supplied. Call validate explicitly to check
    the shared date constraints.

    Example:
        >>> date = ManifestationDate(manifestation_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
        >>> date.rendered_text
        'circa 1900'
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this date.

        Example:
            >>> date = ManifestationDate(manifestation_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this date as attached to a manifestation.

        Example:
            >>> date = ManifestationDate(manifestation_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared date fields with the manifestation id and the edition-specific flag.

        Values are retained without validation, calendar conversion or persistence.

        Example:
            >>> date = ManifestationDate(manifestation_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemDate(DateBase):
    """
    Represent a typed date assertion on a item, including the copy-specific flag.

    Endpoints and display text are stored as supplied. Call validate explicitly to check
    the shared date constraints.

    Example:
        >>> date = ItemDate(item_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
        >>> date.rendered_text
        'circa 1900'
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this date.

        Example:
            >>> date = ItemDate(item_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this date as attached to a item.

        Example:
            >>> date = ItemDate(item_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared date fields with the item id and the copy-specific flag.

        Values are retained without validation, calendar conversion or persistence.

        Example:
            >>> date = ItemDate(item_id=3, date_kind=DateKind.CREATED, display_text='circa 1900')
            >>> date.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindDatesContainer(MetadataSequenceStringMixin, Generic[DateT], abc.ABC):
    """
    Maintain an ordered editable date list for one kind and WEMI target.

    The generated constructor retains a supplied _dates list by reference and otherwise
    creates a fresh list. Date objects remain shared. Insertion checks shape and
    renumbers positions; full validation is explicit.

    Example:
        >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
        >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
        >>> dates.add_date(date)
        >>> dates.texts()
        ('10',)
    """
    date_kind: DateKind
    target_id: int
    _dates: list[DateT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "dates"

    def __iter__(self) -> Iterator[DateT]:
        """
        Iterate over shared date objects in list order.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> next(iter(dates)) is date
            True


        :return: Iterator over stored dates.
        """
        return iter(self._dates)

    def __len__(self) -> int:
        """
        Count dates currently stored in this kind bucket.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> len(dates)
            1


        :return: Number of stored dates.
        """
        return len(self._dates)

    def __getitem__(self, index: int) -> DateT:
        """
        Read a date by list index, including negative indices; invalid indices raise IndexError.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates[-1] is date
            True


        :param index: List index of the date to read.
        :return: Stored date object.
        """
        return self._dates[index]

    def dates(self) -> tuple[DateT, ...]:
        """
        Take a tuple snapshot of date order while retaining shared mutable objects.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.dates()[0] is date
            True


        :return: Tuple of stored date references.
        """
        return tuple(self._dates)

    def texts(self) -> tuple[str, ...]:
        """
        Render every date in list order, including empty rendered text.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.texts()
            ('10',)


        :return: Tuple of rendered date strings.
        """
        return tuple(date.rendered_text for date in self._dates)

    def to_text(self, sep: str = "; ") -> str:
        """
        Join nonempty rendered date strings with the requested separator.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.to_text(sep=' / ')
            '10'


        :param sep: Separator between nonempty date strings.
        :return: Joined date text, or an empty string.
        """
        return sep.join(text for text in self.texts() if text)

    def add_date(self, date: DateT) -> None:
        """
        Check target kind, id and date kind, append the shared date, and renumber positions.

        Shape mismatches raise ValueError before insertion; other fields are checked only by
        validate.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> date.position
            0


        :param date: Date object matching this bucket target and kind.
        :return: None.
        """
        self._validate_date_shape(date)
        self._dates.append(date)
        self.normalize_positions()

    def replace_date(self, index: int, date: DateT) -> None:
        """
        Check shape, replace the indexed date and renumber all positions.

        Shape errors raise ValueError; invalid list indices raise IndexError.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.replace_date(0, WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=20))
            >>> dates.texts()
            ('20',)


        :param index: List index of the date to replace.
        :param date: Replacement date with matching target and kind.
        :return: None.
        """
        self._validate_date_shape(date)
        self._dates[index] = date
        self.normalize_positions()

    def remove_date_at(self, index: int) -> DateT:
        """
        Pop the indexed date and renumber the remaining positions.

        Invalid indices raise IndexError; the removed object keeps its own field values.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.remove_date_at(0) is date
            True
            >>> len(dates)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed date object.
        """
        removed = self._dates.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned date objects.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.clear()
            >>> dates.dates()
            ()


        :return: None.
        """
        self._dates.clear()

    def move_date(self, old_index: int, new_index: int) -> None:
        """
        Pop a date, insert it at the destination and renumber all positions.

        The source follows list.pop rules; the destination follows list.insert rules,
        including clipping out-of-range indices.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.move_date(0, 99)
            >>> dates[0] is date and date.position == 0
            True


        :param old_index: Source list index; invalid indices raise IndexError.
        :param new_index: Insertion index after removing the source date.
        :return: None.
        """
        date = self._dates.pop(old_index)
        self._dates.insert(new_index, date)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set the primary flag only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.set_primary(0)
            >>> date.is_primary
            True
            >>> dates.set_primary(-1)
            >>> date.is_primary
            False


        :param index: Nonnegative index to make primary, or an unmatched index to clear all
            flags.
        :return: None.
        """
        for i, date in enumerate(self._dates):
            date.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared date position with its zero-based list index.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> date.position = 8
            >>> dates.normalize_positions()
            >>> date.position
            0


        :return: None.
        """
        for index, date in enumerate(self._dates):
            date.position = index

    def validate(self) -> None:
        """
        Check date shapes and values, contiguous positions, and at most one primary date.

        Raise ValueError on the first failed constraint; values are not repaired and empty
        buckets are valid.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.validate()
            >>> date.position = 2
            >>> dates.validate()
            Traceback (most recent call last):
            ...
            ValueError: Date position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0
        for expected_index, date in enumerate(self._dates):
            self._validate_date_shape(date)
            date.validate()
            if date.position != expected_index:
                raise ValueError(f"Date position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {date.position}")
            if date.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary date is allowed for {self.target_kind} {self.target_id} kind {self.date_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize dates in list order without validation or persistence.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates.as_write_payload()[0]['start_ep_k']
            10


        :return: New list of per-date dictionaries.
        """
        return [date.as_write_payload() for date in self._dates]

    def _validate_date_shape(self, date: DateT) -> None:
        """
        Require matching target kind, target id and date kind.

        Raise ValueError for the first mismatch; other date fields are not inspected.

        Example:
            >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=1)
            >>> date = WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10)
            >>> dates.add_date(date)
            >>> dates._validate_date_shape(date)


        :param date: Candidate date whose target and kind must match this bucket.
        :return: None.
        """
        if date.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {date.target_kind} date to {self.target_kind} container")
        if date.target_id != self.target_id:
            raise ValueError(f"Date target_id {date.target_id} does not match container target_id {self.target_id}")
        if date.date_kind != self.date_kind:
            raise ValueError(f"Date kind {date.date_kind} does not match container kind {self.date_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindDatesContainer(KindDatesContainer[WorkDate]):
    """
    Collect shared date assertions of one kind for a work.

    The keyword-only dataclass constructor retains a supplied list; otherwise each
    instance starts with a fresh empty list. Call validate explicitly for full
    constraints.

    Example:
        >>> dates = WorkKindDatesContainer(date_kind=DateKind.CREATED, target_id=3)
        >>> dates.target_kind, len(dates)
        ('work', 0)
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindDatesContainer(KindDatesContainer[ExpressionDate]):
    """
    Collect shared date assertions of one kind for a expression.

    The keyword-only dataclass constructor retains a supplied list; otherwise each
    instance starts with a fresh empty list. Call validate explicitly for full
    constraints.

    Example:
        >>> dates = ExpressionKindDatesContainer(date_kind=DateKind.CREATED, target_id=3)
        >>> dates.target_kind, len(dates)
        ('expression', 0)
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindDatesContainer(KindDatesContainer[ManifestationDate]):
    """
    Collect shared date assertions of one kind for a manifestation.

    The keyword-only dataclass constructor retains a supplied list; otherwise each
    instance starts with a fresh empty list. Call validate explicitly for full
    constraints.

    Example:
        >>> dates = ManifestationKindDatesContainer(date_kind=DateKind.CREATED, target_id=3)
        >>> dates.target_kind, len(dates)
        ('manifestation', 0)
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindDatesContainer(KindDatesContainer[ItemDate]):
    """
    Collect shared date assertions of one kind for a item.

    The keyword-only dataclass constructor retains a supplied list; otherwise each
    instance starts with a fresh empty list. Call validate explicitly for full
    constraints.

    Example:
        >>> dates = ItemKindDatesContainer(date_kind=DateKind.CREATED, target_id=3)
        >>> dates.target_kind, len(dates)
        ('item', 0)
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetDatesContainer(
    MetadataSequenceStringMixin,
    Generic[DateT, KindContainerT],
    abc.ABC,
):
    """
    Group date buckets by kind for a single WEMI target.

    Kinds retain registration order, including empty buckets. The dataclass constructor
    retains an explicitly supplied _by_kind dictionary; the default creates an
    independent dictionary.

    Example:
        >>> dates = WorkDatesContainer(work_id=1)
        >>> dates.kinds()
        ()
    """
    _by_kind: dict[DateKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "dates"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the row id shared by all date buckets.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.target_id
            1


        :return: Concrete WEMI row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level represented by this container.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, date_kind: DateKind) -> KindContainerT:
        """
        Require an empty kind bucket bound to this target.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> bucket = dates._make_kind_container(DateKind.CREATED)
            >>> bucket.target_id, dates.kinds()
            (1, ())


        :param date_kind: Date kind for the new bucket.
        :return: New bucket, not registered until ensure_kind stores it.
        """

    def kinds(self) -> tuple[DateKind, ...]:
        """
        Return registered date kinds in insertion order, including empty buckets.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def get_kind(self, date_kind: DateKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.get_kind(DateKind.CREATED) is None
            True


        :param date_kind: Kind whose bucket should be returned.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(date_kind)

    def ensure_kind(self, date_kind: DateKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one when absent.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> bucket = dates.ensure_kind(DateKind.CREATED)
            >>> dates.ensure_kind(DateKind.CREATED) is bucket
            True


        :param date_kind: Kind whose bucket should exist.
        :return: Live bucket bound to this target.
        """
        container = self._by_kind.get(date_kind)
        if container is None:
            container = self._make_kind_container(date_kind)
            self._by_kind[date_kind] = container
        return container

    def add_date(self, date: DateT) -> None:
        """
        Check the target id, ensure a kind bucket, then delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later kind or
        target-level mismatch can leave a new empty bucket. Successful insertion renumbers
        positions; full validation remains explicit.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.add_date(WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10))
            >>> dates.kind_text(DateKind.CREATED)
            '10'


        :param date: Shared date record to insert into its kind bucket.
        :return: None.
        """
        if date.target_id != self.target_id:
            raise ValueError(f"Date target_id {date.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(date.date_kind).add_date(date)

    def iter_all_dates(self) -> Iterator[DateT]:
        """
        Yield shared dates in kind registration order and then bucket order.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> list(dates.iter_all_dates())
            []


        :return: Iterator over all stored date objects.
        """
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, date_kind: DateKind, sep: str = "; ") -> str:
        """
        Render a kind bucket without creating it when absent.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.kind_text(DateKind.CREATED), dates.kinds()
            ('', ())


        :param date_kind: Kind whose dates should be rendered.
        :param sep: Separator between nonempty rendered dates.
        :return: Joined nonempty date text, or an empty string for an absent or empty
            bucket.
        """
        container = self.get_kind(date_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate each registered bucket without repairing its values.

        The first bucket validation error propagates.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten date payloads in kind registration order, without validation or persistence.

        Example:
            >>> dates = WorkDatesContainer(work_id=1)
            >>> dates.as_write_payload()
            []


        :return: New list of date dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkDatesContainer(BaseTargetDatesContainer[WorkDate, WorkKindDatesContainer]):
    """
    Group all date kinds attached to one work.

    Explicit kind methods provide the core API; generated date-kind properties expose
    the same buckets and rendered text.

    Example:
        >>> dates = WorkDatesContainer(work_id=3)
        >>> dates.target_id, dates.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this date container.

        Example:
            >>> dates = WorkDatesContainer(work_id=3)
            >>> dates.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the date-container target as a work.

        Example:
            >>> dates = WorkDatesContainer(work_id=3)
            >>> dates.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, date_kind: DateKind) -> WorkKindDatesContainer:
        """
        Build an empty work date bucket without registering it.

        Example:
            >>> dates = WorkDatesContainer(work_id=3)
            >>> bucket = dates._make_kind_container(DateKind.CREATED)
            >>> bucket.target_id, len(bucket), dates.kinds()
            (3, 0, ())


        :param date_kind: Kind assigned to the new bucket.
        :return: New WorkKindDatesContainer using the stored target id.
        """
        return WorkKindDatesContainer(date_kind=date_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionDatesContainer(BaseTargetDatesContainer[ExpressionDate, ExpressionKindDatesContainer]):
    """
    Group all date kinds attached to one expression.

    Explicit kind methods provide the core API; generated date-kind properties expose
    the same buckets and rendered text.

    Example:
        >>> dates = ExpressionDatesContainer(expression_id=3)
        >>> dates.target_id, dates.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this date container.

        Example:
            >>> dates = ExpressionDatesContainer(expression_id=3)
            >>> dates.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the date-container target as a expression.

        Example:
            >>> dates = ExpressionDatesContainer(expression_id=3)
            >>> dates.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(self, date_kind: DateKind) -> ExpressionKindDatesContainer:
        """
        Build an empty expression date bucket without registering it.

        Example:
            >>> dates = ExpressionDatesContainer(expression_id=3)
            >>> bucket = dates._make_kind_container(DateKind.CREATED)
            >>> bucket.target_id, len(bucket), dates.kinds()
            (3, 0, ())


        :param date_kind: Kind assigned to the new bucket.
        :return: New ExpressionKindDatesContainer using the stored target id.
        """
        return ExpressionKindDatesContainer(date_kind=date_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationDatesContainer(BaseTargetDatesContainer[ManifestationDate, ManifestationKindDatesContainer]):
    """
    Group all date kinds attached to one manifestation.

    Explicit kind methods provide the core API; generated date-kind properties expose
    the same buckets and rendered text.

    Example:
        >>> dates = ManifestationDatesContainer(manifestation_id=3)
        >>> dates.target_id, dates.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this date container.

        Example:
            >>> dates = ManifestationDatesContainer(manifestation_id=3)
            >>> dates.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the date-container target as a manifestation.

        Example:
            >>> dates = ManifestationDatesContainer(manifestation_id=3)
            >>> dates.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(self, date_kind: DateKind) -> ManifestationKindDatesContainer:
        """
        Build an empty manifestation date bucket without registering it.

        Example:
            >>> dates = ManifestationDatesContainer(manifestation_id=3)
            >>> bucket = dates._make_kind_container(DateKind.CREATED)
            >>> bucket.target_id, len(bucket), dates.kinds()
            (3, 0, ())


        :param date_kind: Kind assigned to the new bucket.
        :return: New ManifestationKindDatesContainer using the stored target id.
        """
        return ManifestationKindDatesContainer(date_kind=date_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemDatesContainer(BaseTargetDatesContainer[ItemDate, ItemKindDatesContainer]):
    """
    Group all date kinds attached to one item.

    Explicit kind methods provide the core API; generated date-kind properties expose
    the same buckets and rendered text.

    Example:
        >>> dates = ItemDatesContainer(item_id=3)
        >>> dates.target_id, dates.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this date container.

        Example:
            >>> dates = ItemDatesContainer(item_id=3)
            >>> dates.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the date-container target as a item.

        Example:
            >>> dates = ItemDatesContainer(item_id=3)
            >>> dates.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, date_kind: DateKind) -> ItemKindDatesContainer:
        """
        Build an empty item date bucket without registering it.

        Example:
            >>> dates = ItemDatesContainer(item_id=3)
            >>> bucket = dates._make_kind_container(DateKind.CREATED)
            >>> bucket.target_id, len(bucket), dates.kinds()
            (3, 0, ())


        :param date_kind: Kind assigned to the new bucket.
        :return: New ItemKindDatesContainer using the stored target id.
        """
        return ItemKindDatesContainer(date_kind=date_kind, target_id=self.item_id)


def _kind_property_stem(date_kind: DateKind) -> str:
    """
    Look up the fixed convenience-property stem for a supported date kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(DateKind.CREATED)
        'created_dates'


    :param date_kind: DateKind enum member to translate.
    :return: Property stem ending in _dates.
    """
    stems = {
        DateKind.CREATED: "created_dates",
        DateKind.ISSUED: "issued_dates",
        DateKind.PUBLISHED: "published_dates",
        DateKind.RELEASED: "released_dates",
        DateKind.RECORDED: "recorded_dates",
        DateKind.PERFORMED: "performed_dates",
        DateKind.ACQUIRED: "acquired_dates",
        DateKind.MODIFIED: "modified_dates",
        DateKind.DIGITIZED: "digitized_dates",
        DateKind.COPYRIGHT: "copyright_dates",
    }
    return stems[date_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetDatesContainer]) -> None:
    """
    Install per-kind bucket and rendered-text accessors on a date container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing attributes with those names
    are overwritten. Explicit kind methods remain the canonical surface under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> dates = WorkDatesContainer(work_id=1)
        >>> dates.created_dates_text, dates.kinds()
        ('', ())
        >>> bucket = dates.created_dates
        >>> dates.get_kind(DateKind.CREATED) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for date_kind in DateKind:
        stem = _kind_property_stem(date_kind)

        def kind_container_getter(self, _kind=date_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> dates = WorkDatesContainer(work_id=1)
                >>> bucket = dates.created_dates
                >>> dates.get_kind(DateKind.CREATED) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Date kind captured as the default argument during installation.
            :return: Live per-kind date container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=date_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> dates = WorkDatesContainer(work_id=1)
                >>> dates.add_date(WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10))
                >>> dates.created_dates_text
                '10'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Date kind captured as the default argument during installation.
            :return: Joined nonempty date text, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = "; ", _kind=date_kind) -> str:
            """
            Render the captured kind using a caller-selected separator.

            Example:
                >>> dates = WorkDatesContainer(work_id=1)
                >>> dates.add_date(WorkDate(work_id=1, date_kind=DateKind.CREATED, start_ep_k=10))
                >>> dates.created_dates_to_text(sep=' / ')
                '10'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Date kind captured as the default argument during installation.
            :return: Joined nonempty date text, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkDatesContainer)
_install_kind_convenience_properties(ExpressionDatesContainer)
_install_kind_convenience_properties(ManifestationDatesContainer)
_install_kind_convenience_properties(ItemDatesContainer)


__all__ = [
    "DateKind",
    "DateBase",
    "WorkDate",
    "ExpressionDate",
    "ManifestationDate",
    "ItemDate",
    "KindDatesContainer",
    "WorkKindDatesContainer",
    "ExpressionKindDatesContainer",
    "ManifestationKindDatesContainer",
    "ItemKindDatesContainer",
    "BaseTargetDatesContainer",
    "WorkDatesContainer",
    "ExpressionDatesContainer",
    "ManifestationDatesContainer",
    "ItemDatesContainer",
]
