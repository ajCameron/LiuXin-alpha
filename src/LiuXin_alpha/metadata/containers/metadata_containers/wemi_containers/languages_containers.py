"""
Represent editable language assertions attached to WEMI entities.

Records retain identity/context hints, ordering and provenance. Target containers
group shared records by kind; construction and serialization do not automatically
perform full validation.

Example:
    >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import LanguageKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID, LanguageID


LanguageT = TypeVar("LanguageT", bound="LanguageBase")
KindContainerT = TypeVar("KindContainerT", bound="KindLanguagesContainer")


@dataclass(slots=True, kw_only=True)
class LanguageBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a language attachment with an optional language id, code and name with ordering and provenance.

    Concrete keyword-only dataclasses add the target id and WEMI-level context. Values
    are retained as supplied, with explicit validate checks rather than database lookup.

    Example:
        >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> record.target_kind
        'work'
    """

    language_kind: LanguageKind
    language_id: LanguageID | None = None
    language_code: str | None = None
    language_name: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("language_name", "language_code", "display_text")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this language assertion.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this language assertion.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def display_text(self) -> str:
        """
        Prefer a truthy language name, then a truthy code, then a language-id label.

        The fallback is language:<id>, including language:None for an unvalidated empty
        record. No lookup or whitespace stripping occurs.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.display_text
            'en'
            >>> record.language_name = 'English'
            >>> record.display_text
            'English'
            >>> WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT).display_text
            'language:None'


        :return: Chosen name/code or formatted id label.
        """
        if self.language_name:
            return self.language_name
        if self.language_code:
            return self.language_code
        return f"language:{self.language_id}"

    def validate(self) -> None:
        """
        Require a non-None language id or truthy code/name and reject a negative position.

        Code and name are not stripped, so whitespace is truthy. No vocabulary lookup, kind
        validation or consistency check is performed. Failures raise ValueError.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.position = -1
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: position cannot be negative


        :return: None.
        """
        if self.language_id is None and not (self.language_code or self.language_name):
            raise ValueError("language record must provide at least one of language_id, language_code, or language_name")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared language fields without validation, normalization or target additions.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record._common_write_payload()['language_kind'] == LanguageKind.CONTENT
            True


        :return: New dictionary retaining stored values, including LanguageKind.
        """
        return {
            "language_kind": self.language_kind,
            "language_id": self.language_id,
            "language_code": self.language_code,
            "language_name": self.language_name,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared language fields and concrete target context.

        Example:
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkLanguage(LanguageBase):
    """
    Attach a typed language assertion to a work, including the canonical-for-work flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = WorkLanguage(work_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> record.target_id, record.target_kind
        (3, 'work')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this language.

        Example:
            >>> record = WorkLanguage(work_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this language as attached to a work.

        Example:
            >>> record = WorkLanguage(work_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared language fields with the work id and the canonical-for-work flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = WorkLanguage(work_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionLanguage(LanguageBase):
    """
    Attach a typed language assertion to a expression, including an optional applies-to language id.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ExpressionLanguage(expression_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """
    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this language.

        Example:
            >>> record = ExpressionLanguage(expression_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this language as attached to a expression.

        Example:
            >>> record = ExpressionLanguage(expression_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared language fields with the expression id and an optional applies-to language id.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ExpressionLanguage(expression_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_language_id": self.applies_to_language_id})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationLanguage(LanguageBase):
    """
    Attach a typed language assertion to a manifestation, including the edition-specific flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ManifestationLanguage(manifestation_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this language.

        Example:
            >>> record = ManifestationLanguage(manifestation_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this language as attached to a manifestation.

        Example:
            >>> record = ManifestationLanguage(manifestation_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared language fields with the manifestation id and the edition-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ManifestationLanguage(manifestation_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemLanguage(LanguageBase):
    """
    Attach a typed language assertion to a item, including the copy-specific flag.

    The keyword-only dataclass stores values as supplied. Validation and any
    referenced-row resolution are separate operations.

    Example:
        >>> record = ItemLanguage(item_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> record.target_id, record.target_kind
        (3, 'item')
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this language.

        Example:
            >>> record = ItemLanguage(item_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this language as attached to a item.

        Example:
            >>> record = ItemLanguage(item_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared language fields with the item id and the copy-specific flag.

        No validation, reference lookup, normalization or persistence occurs.

        Example:
            >>> record = ItemLanguage(item_id=3, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific fields.
        """
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindLanguagesContainer(MetadataSequenceStringMixin, Generic[LanguageT], abc.ABC):
    """
    Maintain an ordered editable language list for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _languages list; otherwise
    it creates a fresh list. Records remain shared. Insertion checks shape and renumbers
    positions, while full validation is explicit.

    Example:
        >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
        >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
        >>> records.add_language(record)
        >>> records.texts()
        ('en',)
    """

    language_kind: LanguageKind
    target_id: int
    _languages: list[LanguageT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "languages"

    def __iter__(self) -> Iterator[LanguageT]:
        """
        Iterate over shared language records in list order.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._languages)

    def __len__(self) -> int:
        """
        Count the stored language assertions.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> len(records)
            1


        :return: Number of records.
        """
        return len(self._languages)

    def __getitem__(self, index: int) -> LanguageT:
        """
        Read a language by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._languages[index]

    def languages(self) -> tuple[LanguageT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.languages()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._languages)

    def texts(self) -> tuple[str, ...]:
        """
        Collect language display_text values in list order.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.texts()
            ('en',)


        :return: Tuple of display strings, retaining duplicates.
        """
        return tuple(language.display_text for language in self._languages)

    def to_text(self, sep: str = ", ") -> str:
        """
        Join every display string with the requested separator.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.to_text(sep=' / ')
            'en'


        :param sep: Separator between consecutive display strings.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_language(self, language: LanguageT) -> None:
        """
        Check target and language kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> record.position
            0


        :param language: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_language_shape(language)
        self._languages.append(language)
        self.normalize_positions()

    def replace_language(self, index: int, language: LanguageT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.replace_language(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param language: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_language_shape(language)
        self._languages[index] = language
        self.normalize_positions()

    def remove_language_at(self, index: int) -> LanguageT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.remove_language_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._languages.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._languages.clear()

    def move_language(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.move_language(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        language = self._languages.pop(old_index)
        self._languages.insert(new_index, language)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
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
        for i, language in enumerate(self._languages):
            language.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, language in enumerate(self._languages):
            language.position = index

    def validate(self) -> None:
        """
        Check language shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Language position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0
        for expected_index, language in enumerate(self._languages):
            self._validate_language_shape(language)
            language.validate()
            if language.position != expected_index:
                raise ValueError(f"Language position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {language.position}")
            if language.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary language is allowed for {self.target_kind} {self.target_id} kind {self.language_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [language.as_write_payload() for language in self._languages]

    def _validate_language_shape(self, language: LanguageT) -> None:
        """
        Require matching target kind, target id and language kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records._validate_language_shape(record)


        :param language: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if language.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {language.target_kind} language to {self.target_kind} container")
        if language.target_id != self.target_id:
            raise ValueError(f"Language target_id {language.target_id} does not match container target_id {self.target_id}")
        if language.language_kind != self.language_kind:
            raise ValueError(f"Language kind {language.language_kind} does not match container kind {self.language_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindLanguagesContainer(KindLanguagesContainer[WorkLanguage]):
    """
    Collect ordered language assertions of one kind on a work.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = WorkKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindLanguagesContainer(KindLanguagesContainer[ExpressionLanguage]):
    """
    Collect ordered language assertions of one kind on a expression.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ExpressionKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindLanguagesContainer(KindLanguagesContainer[ManifestationLanguage]):
    """
    Collect ordered language assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ManifestationKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindLanguagesContainer(KindLanguagesContainer[ItemLanguage]):
    """
    Collect ordered language assertions of one kind on a item.

    Construction retains a supplied list without validation. Shape checks accompany
    mutation; full record validation is explicit.

    Example:
        >>> records = ItemKindLanguagesContainer(language_kind=LanguageKind.CONTENT, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetLanguagesContainer(
    MetadataSequenceStringMixin,
    Generic[LanguageT, KindContainerT],
    abc.ABC,
):
    """
    Group editable language buckets by kind for a WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkLanguagesContainer(work_id=1)
        >>> records.kinds()
        ()
    """

    _by_kind: dict[LanguageKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "languages"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
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
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, language_kind: LanguageKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> bucket = records._make_kind_container(LanguageKind.CONTENT)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[LanguageKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def has_kind(self, language_kind: LanguageKind) -> bool:
        """
        Check whether a bucket is registered, regardless of its contents.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.has_kind(LanguageKind.CONTENT)
            False


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: True when the kind key exists.
        """
        return language_kind in self._by_kind

    def get_kind(self, language_kind: LanguageKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.get_kind(LanguageKind.CONTENT) is None
            True


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(language_kind)

    def ensure_kind(self, language_kind: LanguageKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> bucket = records.ensure_kind(LanguageKind.CONTENT)
            >>> records.ensure_kind(LanguageKind.CONTENT) is bucket
            True


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(language_kind)
        if container is None:
            container = self._make_kind_container(language_kind)
            self._by_kind[language_kind] = container
        return container

    def add_language(self, language: LanguageT) -> None:
        """
        Check target id, ensure the language kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later target-kind or
        assertion-kind mismatch can leave a new empty bucket. Successful insertion renumbers
        positions; full validation remains explicit.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.add_language(WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en'))
            >>> records.kind_text(LanguageKind.CONTENT)
            'en'


        :param language: Shared record to insert into its kind bucket.
        :return: None.
        """
        if language.target_id != self.target_id:
            raise ValueError(f"Language target_id {language.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(language.language_kind).add_language(language)

    def iter_all_languages(self) -> Iterator[LanguageT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> next(records.iter_all_languages()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, language_kind: LanguageKind, sep: str = ", ") -> str:
        """
        Join display text for a kind without creating a missing bucket.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.kind_text(LanguageKind.CONTENT), records.kinds()
            ('', ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :param sep: Separator between display strings.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(language_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing or normalizing data.

        The first bucket error propagates.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkLanguagesContainer(work_id=1)
            >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
            >>> records.add_language(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkLanguagesContainer(BaseTargetLanguagesContainer[WorkLanguage, WorkKindLanguagesContainer]):
    """
    Group all language assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = WorkLanguagesContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this language container.

        Example:
            >>> records = WorkLanguagesContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the language-container target as a work.

        Example:
            >>> records = WorkLanguagesContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, language_kind: LanguageKind) -> WorkKindLanguagesContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkLanguagesContainer(work_id=3)
            >>> bucket = records._make_kind_container(LanguageKind.CONTENT)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: New WorkKindLanguagesContainer using the stored target id.
        """
        return WorkKindLanguagesContainer(language_kind=language_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionLanguagesContainer(BaseTargetLanguagesContainer[ExpressionLanguage, ExpressionKindLanguagesContainer]):
    """
    Group all language assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ExpressionLanguagesContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this language container.

        Example:
            >>> records = ExpressionLanguagesContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the language-container target as a expression.

        Example:
            >>> records = ExpressionLanguagesContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(self, language_kind: LanguageKind) -> ExpressionKindLanguagesContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionLanguagesContainer(expression_id=3)
            >>> bucket = records._make_kind_container(LanguageKind.CONTENT)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: New ExpressionKindLanguagesContainer using the stored target id.
        """
        return ExpressionKindLanguagesContainer(language_kind=language_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationLanguagesContainer(BaseTargetLanguagesContainer[ManifestationLanguage, ManifestationKindLanguagesContainer]):
    """
    Group all language assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ManifestationLanguagesContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this language container.

        Example:
            >>> records = ManifestationLanguagesContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the language-container target as a manifestation.

        Example:
            >>> records = ManifestationLanguagesContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(self, language_kind: LanguageKind) -> ManifestationKindLanguagesContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationLanguagesContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(LanguageKind.CONTENT)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: New ManifestationKindLanguagesContainer using the stored target id.
        """
        return ManifestationKindLanguagesContainer(language_kind=language_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemLanguagesContainer(BaseTargetLanguagesContainer[ItemLanguage, ItemKindLanguagesContainer]):
    """
    Group all language assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors
    reference the same stored records.

    Example:
        >>> records = ItemLanguagesContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this language container.

        Example:
            >>> records = ItemLanguagesContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the language-container target as a item.

        Example:
            >>> records = ItemLanguagesContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, language_kind: LanguageKind) -> ItemKindLanguagesContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemLanguagesContainer(item_id=3)
            >>> bucket = records._make_kind_container(LanguageKind.CONTENT)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param language_kind: LanguageKind value selecting a bucket for this target.
        :return: New ItemKindLanguagesContainer using the stored target id.
        """
        return ItemKindLanguagesContainer(language_kind=language_kind, target_id=self.item_id)


def _kind_property_stem(language_kind: LanguageKind) -> str:
    """
    Look up the fixed convenience-property stem for a language kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(LanguageKind.CONTENT)
        'content_languages'


    :param language_kind: LanguageKind value selecting a bucket for this target.
    :return: Attribute stem for the selected kind.
    """
    stems = {
        LanguageKind.CONTENT: "content_languages",
        LanguageKind.ORIGINAL: "original_languages",
        LanguageKind.SOURCE: "source_languages",
        LanguageKind.TARGET: "target_languages",
        LanguageKind.SUBTITLE: "subtitle_languages",
        LanguageKind.SUMMARY: "summary_languages",
        LanguageKind.INTERFACE: "interface_languages",
    }
    return stems[language_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetLanguagesContainer]) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a language container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkLanguagesContainer(work_id=1)
        >>> records.content_languages_text, records.kinds()
        ('', ())
        >>> bucket = records.content_languages
        >>> records.get_kind(LanguageKind.CONTENT) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for language_kind in LanguageKind:
        stem = _kind_property_stem(language_kind)

        def kind_container_getter(self, _kind=language_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkLanguagesContainer(work_id=1)
                >>> bucket = records.content_languages
                >>> records.get_kind(LanguageKind.CONTENT) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Language kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=language_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkLanguagesContainer(work_id=1)
                >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
                >>> records.add_language(record)
                >>> records.content_languages_text
                'en'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Language kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=language_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkLanguagesContainer(work_id=1)
                >>> record = WorkLanguage(work_id=1, language_kind=LanguageKind.CONTENT, language_code='en')
                >>> records.add_language(record)
                >>> records.content_languages_to_text(sep=' / ')
                'en'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Language kind captured as the default argument during installation.
            :return: Joined display text, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkLanguagesContainer)
_install_kind_convenience_properties(ExpressionLanguagesContainer)
_install_kind_convenience_properties(ManifestationLanguagesContainer)
_install_kind_convenience_properties(ItemLanguagesContainer)


__all__ = [
    "LanguageKind",
    "LanguageBase",
    "WorkLanguage",
    "ExpressionLanguage",
    "ManifestationLanguage",
    "ItemLanguage",
    "KindLanguagesContainer",
    "WorkKindLanguagesContainer",
    "ExpressionKindLanguagesContainer",
    "ManifestationKindLanguagesContainer",
    "ItemKindLanguagesContainer",
    "BaseTargetLanguagesContainer",
    "WorkLanguagesContainer",
    "ExpressionLanguagesContainer",
    "ManifestationLanguagesContainer",
    "ItemLanguagesContainer",
]
