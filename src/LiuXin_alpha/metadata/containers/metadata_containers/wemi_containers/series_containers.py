"""
Represent series memberships and editable kind buckets for WEMI targets.

Records carry names, numbering, authority hints and target context. Record order
within a bucket is separate from position_in_series. Construction and serialization
do not validate.

Example:
    >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import SeriesKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID, LanguageID

SeriesT = TypeVar("SeriesT", bound="SeriesEntryBase")
KindContainerT = TypeVar("KindContainerT", bound="KindSeriesEntriesContainer")


@dataclass(slots=True, kw_only=True)
class SeriesEntryBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a series attachment with a name, optional sort name, text/numeric numbering and authority hints.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied. Validation and referenced-row resolution are
    separate operations.

    Example:
        >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> record.target_kind
        'work'
    """

    series_kind: SeriesKind
    name: str
    sort_name: str | None = None
    numbering_text: str | None = None
    position_in_series: float | None = None
    language_id: LanguageID | None = None

    authority_scheme: str | None = None
    authority_identifier: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("display_text", "name", "series_kind")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this series attachment.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this series attachment.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def display_text(self) -> str:
        """
        Render the series name with preferred text or numeric numbering.

        Truthy numbering_text wins and is appended after a hash sign. Otherwise a non-None
        position_in_series uses general numeric formatting, including zero. Without either,
        return the stored name. No trimming or validation occurs.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.position_in_series = 0
            >>> record.display_text
            'Cycle #0'
            >>> record.numbering_text = 'II'
            >>> record.display_text
            'Cycle #II'


        :return: Series name with optional numbering suffix.
        """
        if self.numbering_text:
            return f"{self.name} #{self.numbering_text}"
        if self.position_in_series is not None:
            return f"{self.name} #{self.position_in_series:g}"
        return self.name

    def validate(self) -> None:
        """
        Reject blank names, negative record positions and a half-specified authority pair.

        Authority fields must both be truthy or both falsey; whitespace is not stripped for
        that check. position_in_series and numbering consistency are not validated. Stored
        text is unchanged and failures raise ValueError.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.authority_scheme = 'local'
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: authority_scheme and authority_identifier must either both be set or both be empty


        :return: None.
        """
        if not self.name.strip():
            raise ValueError("name cannot be blank")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if bool(self.authority_scheme) ^ bool(self.authority_identifier):
            raise ValueError("authority_scheme and authority_identifier must either both be set or both be empty")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared series fields without target additions, validation or persistence.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record._common_write_payload()['series_kind'] == SeriesKind.SERIES
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "series_kind": self.series_kind,
            "name": self.name,
            "sort_name": self.sort_name,
            "numbering_text": self.numbering_text,
            "position_in_series": self.position_in_series,
            "language_id": self.language_id,
            "authority_scheme": self.authority_scheme,
            "authority_identifier": self.authority_identifier,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared series fields and concrete target context.

        Example:
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkSeriesEntry(SeriesEntryBase):
    """
    Attach a series record to a work, including the canonical-for-work flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = WorkSeriesEntry(work_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this series.

        Example:
            >>> record = WorkSeriesEntry(work_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this series as attached to a work.

        Example:
            >>> record = WorkSeriesEntry(work_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared series fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkSeriesEntry(work_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionSeriesEntry(SeriesEntryBase):
    """
    Attach a series record to a expression, including the applies-to-realisation flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ExpressionSeriesEntry(expression_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """
    expression_id: ExpressionID
    applies_to_realisation: bool = True

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this series.

        Example:
            >>> record = ExpressionSeriesEntry(expression_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this series as attached to a expression.

        Example:
            >>> record = ExpressionSeriesEntry(expression_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared series fields with the expression id and the applies-to-realisation flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionSeriesEntry(expression_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_realisation": self.applies_to_realisation})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationSeriesEntry(SeriesEntryBase):
    """
    Attach a series record to a manifestation, including the edition-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ManifestationSeriesEntry(manifestation_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this series.

        Example:
            >>> record = ManifestationSeriesEntry(manifestation_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this series as attached to a manifestation.

        Example:
            >>> record = ManifestationSeriesEntry(manifestation_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared series fields with the manifestation id and the edition-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationSeriesEntry(manifestation_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemSeriesEntry(SeriesEntryBase):
    """
    Attach a series record to a item, including the copy-specific flag.

    The keyword-only dataclass retains values as supplied. Construction does not
    validate values or resolve referenced ids.

    Example:
        >>> record = ItemSeriesEntry(item_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this series.

        Example:
            >>> record = ItemSeriesEntry(item_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this series as attached to a item.

        Example:
            >>> record = ItemSeriesEntry(item_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared series fields with the item id and the copy-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemSeriesEntry(item_id=3, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindSeriesEntriesContainer(MetadataSequenceStringMixin, Generic[SeriesT], abc.ABC):
    """
    Maintain ordered entry records for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _entries list, or creates a
    fresh list by default. Records remain shared. Insertion checks shape and renumbers
    positions; full validation is explicit.

    Example:
        >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
        >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
        >>> records.add_entry(record)
        >>> records.texts()
        ('Cycle',)
    """
    series_kind: SeriesKind
    target_id: int
    _entries: list[SeriesT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "series entries"

    def __iter__(self) -> Iterator[SeriesT]:
        """
        Iterate over shared entry records in list order.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """
        Count the stored entry records.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> len(records)
            1


        :return: Number of stored records.
        """
        return len(self._entries)

    def __getitem__(self, index: int) -> SeriesT:
        """
        Read a entry by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._entries[index]

    def entries(self) -> tuple[SeriesT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.entries()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._entries)

    def texts(self) -> tuple[str, ...]:
        """
        Collect entry display_text values in list order without validation.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.texts()
            ('Cycle',)


        :return: Tuple of strings, retaining duplicates.
        """
        return tuple(entry.display_text for entry in self._entries)

    def to_text(self, sep: str = "; ") -> str:
        """
        Join every formatted entry using the separator.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.to_text(sep=' / ')
            'Cycle'


        :param sep: Separator between entry displays; defaults to a semicolon and space.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_entry(self, entry: SeriesT) -> None:
        """
        Check target and entry kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> record.position
            0


        :param entry: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_entry_shape(entry)
        self._entries.append(entry)
        self.normalize_positions()

    def replace_entry(self, index: int, entry: SeriesT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.replace_entry(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param entry: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_entry_shape(entry)
        self._entries[index] = entry
        self.normalize_positions()

    def remove_entry_at(self, index: int) -> SeriesT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.remove_entry_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._entries.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._entries.clear()

    def move_entry(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.move_entry(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        entry = self._entries.pop(old_index)
        self._entries.insert(new_index, entry)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
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
        for i, entry in enumerate(self._entries):
            entry.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, entry in enumerate(self._entries):
            entry.position = index

    def validate(self) -> None:
        """
        Check entry shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Series-entry position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0
        for expected_index, entry in enumerate(self._entries):
            self._validate_entry_shape(entry)
            entry.validate()
            if entry.position != expected_index:
                raise ValueError(f"Series-entry position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {entry.position}")
            if entry.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary series entry is allowed for {self.target_kind} {self.target_id} kind {self.series_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [entry.as_write_payload() for entry in self._entries]

    def _validate_entry_shape(self, entry: SeriesT) -> None:
        """
        Require matching target kind, target id and entry kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records._validate_entry_shape(record)


        :param entry: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if entry.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {entry.target_kind} series entry to {self.target_kind} container")
        if entry.target_id != self.target_id:
            raise ValueError(f"Series entry target_id {entry.target_id} does not match container target_id {self.target_id}")
        if entry.series_kind != self.series_kind:
            raise ValueError(f"Series kind {entry.series_kind} does not match container kind {self.series_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindSeriesEntriesContainer(KindSeriesEntriesContainer[WorkSeriesEntry]):
    """
    Collect ordered entry assertions of one kind on a work.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'work' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = WorkKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindSeriesEntriesContainer(KindSeriesEntriesContainer[ExpressionSeriesEntry]):
    """
    Collect ordered entry assertions of one kind on a expression.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'expression' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ExpressionKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindSeriesEntriesContainer(KindSeriesEntriesContainer[ManifestationSeriesEntry]):
    """
    Collect ordered entry assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'manifestation' and can be supplied explicitly; construction does
    not validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ManifestationKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindSeriesEntriesContainer(KindSeriesEntriesContainer[ItemSeriesEntry]):
    """
    Collect ordered entry assertions of one kind on a item.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'item' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ItemKindSeriesEntriesContainer(series_kind=SeriesKind.SERIES, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetSeriesEntriesContainer(
    MetadataSequenceStringMixin,
    Generic[SeriesT, KindContainerT],
    abc.ABC,
):
    """
    Group editable entry buckets by kind for one WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkSeriesEntriesContainer(work_id=1)
        >>> records.kinds()
        ()
    """
    _by_kind: dict[SeriesKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "series entries"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
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
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, series_kind: SeriesKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> bucket = records._make_kind_container(SeriesKind.SERIES)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[SeriesKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def get_kind(self, series_kind: SeriesKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.get_kind(SeriesKind.SERIES) is None
            True


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(series_kind)

    def ensure_kind(self, series_kind: SeriesKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> bucket = records.ensure_kind(SeriesKind.SERIES)
            >>> records.ensure_kind(SeriesKind.SERIES) is bucket
            True


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(series_kind)
        if container is None:
            container = self._make_kind_container(series_kind)
            self._by_kind[series_kind] = container
        return container

    def add_entry(self, entry: SeriesT) -> None:
        """
        Check target id, ensure the entry kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later shape failure can
        leave a new empty bucket. Successful insertion renumbers positions; full validation
        remains explicit.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.add_entry(WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle'))
            >>> records.kind_text(SeriesKind.SERIES)
            'Cycle'


        :param entry: Shared record to insert into its kind bucket.
        :return: None.
        """
        if entry.target_id != self.target_id:
            raise ValueError(f"Series entry target_id {entry.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(entry.series_kind).add_entry(entry)

    def iter_all_entries(self) -> Iterator[SeriesT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> next(records.iter_all_entries()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, series_kind: SeriesKind, sep: str = "; ") -> str:
        """
        Join entry displays without creating a missing bucket.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.kind_text(SeriesKind.SERIES), records.kinds()
            ('', ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :param sep: Separator between entry displays; defaults to a semicolon and space.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(series_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing data or checking bucket ownership against this container.

        Each bucket checks its own target, kind and records. The first bucket error
        propagates.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=1)
            >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
            >>> records.add_entry(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[WorkSeriesEntry, WorkKindSeriesEntriesContainer]):
    """
    Group all entry assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = WorkSeriesEntriesContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this entry container.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the entry-container target as a work.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, series_kind: SeriesKind) -> WorkKindSeriesEntriesContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkSeriesEntriesContainer(work_id=3)
            >>> bucket = records._make_kind_container(SeriesKind.SERIES)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: New WorkKindSeriesEntriesContainer using the stored target id.
        """
        return WorkKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ExpressionSeriesEntry, ExpressionKindSeriesEntriesContainer]):
    """
    Group all entry assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ExpressionSeriesEntriesContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this entry container.

        Example:
            >>> records = ExpressionSeriesEntriesContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the entry-container target as a expression.

        Example:
            >>> records = ExpressionSeriesEntriesContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(self, series_kind: SeriesKind) -> ExpressionKindSeriesEntriesContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionSeriesEntriesContainer(expression_id=3)
            >>> bucket = records._make_kind_container(SeriesKind.SERIES)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: New ExpressionKindSeriesEntriesContainer using the stored target id.
        """
        return ExpressionKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ManifestationSeriesEntry, ManifestationKindSeriesEntriesContainer]):
    """
    Group all entry assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ManifestationSeriesEntriesContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this entry container.

        Example:
            >>> records = ManifestationSeriesEntriesContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the entry-container target as a manifestation.

        Example:
            >>> records = ManifestationSeriesEntriesContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(self, series_kind: SeriesKind) -> ManifestationKindSeriesEntriesContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationSeriesEntriesContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(SeriesKind.SERIES)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: New ManifestationKindSeriesEntriesContainer using the stored target id.
        """
        return ManifestationKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ItemSeriesEntry, ItemKindSeriesEntriesContainer]):
    """
    Group all entry assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ItemSeriesEntriesContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this entry container.

        Example:
            >>> records = ItemSeriesEntriesContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the entry-container target as a item.

        Example:
            >>> records = ItemSeriesEntriesContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, series_kind: SeriesKind) -> ItemKindSeriesEntriesContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemSeriesEntriesContainer(item_id=3)
            >>> bucket = records._make_kind_container(SeriesKind.SERIES)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param series_kind: SeriesKind value selecting a bucket for this target.
        :return: New ItemKindSeriesEntriesContainer using the stored target id.
        """
        return ItemKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.item_id)


def _kind_property_stem(series_kind: SeriesKind) -> str:
    """
    Look up the fixed convenience-property stem for a series kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(SeriesKind.SERIES)
        'series_entries'


    :param series_kind: SeriesKind value selecting a convenience stem.
    :return: Attribute stem for the selected kind.
    """
    stems = {
        SeriesKind.SERIES: "series_entries",
        SeriesKind.SUBSERIES: "subseries_entries",
        SeriesKind.ARC: "arc_entries",
        SeriesKind.COLLECTION: "collection_entries",
    }
    return stems[series_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetSeriesEntriesContainer]) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a entry container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkSeriesEntriesContainer(work_id=1)
        >>> records.series_entries_text, records.kinds()
        ('', ())
        >>> bucket = records.series_entries
        >>> records.get_kind(SeriesKind.SERIES) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for series_kind in SeriesKind:
        stem = _kind_property_stem(series_kind)

        def kind_container_getter(self, _kind=series_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkSeriesEntriesContainer(work_id=1)
                >>> bucket = records.series_entries
                >>> records.get_kind(SeriesKind.SERIES) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Series kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=series_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkSeriesEntriesContainer(work_id=1)
                >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
                >>> records.add_entry(record)
                >>> records.series_entries_text
                'Cycle'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Series kind captured as the default argument during installation.
            :return: Joined entry displays, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = "; ", _kind=series_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkSeriesEntriesContainer(work_id=1)
                >>> record = WorkSeriesEntry(work_id=1, series_kind=SeriesKind.SERIES, name='Cycle')
                >>> records.add_entry(record)
                >>> records.series_entries_to_text(sep=' / ')
                'Cycle'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Series kind captured as the default argument during installation.
            :return: Joined entry displays, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkSeriesEntriesContainer)
_install_kind_convenience_properties(ExpressionSeriesEntriesContainer)
_install_kind_convenience_properties(ManifestationSeriesEntriesContainer)
_install_kind_convenience_properties(ItemSeriesEntriesContainer)


__all__ = [
    "SeriesKind",
    "SeriesEntryBase",
    "WorkSeriesEntry",
    "ExpressionSeriesEntry",
    "ManifestationSeriesEntry",
    "ItemSeriesEntry",
    "KindSeriesEntriesContainer",
    "WorkKindSeriesEntriesContainer",
    "ExpressionKindSeriesEntriesContainer",
    "ManifestationKindSeriesEntriesContainer",
    "ItemKindSeriesEntriesContainer",
    "BaseTargetSeriesEntriesContainer",
    "WorkSeriesEntriesContainer",
    "ExpressionSeriesEntriesContainer",
    "ManifestationSeriesEntriesContainer",
    "ItemSeriesEntriesContainer",
]
