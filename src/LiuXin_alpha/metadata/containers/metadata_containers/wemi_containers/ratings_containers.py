"""
Represent editable rating assertions and ordered kind buckets for WEMI targets.

Records carry target attachment, ordering and provenance data. Containers share
record objects and validate explicitly; payload creation does not write to a
database.

Example:
    >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
    >>> record.validate()
    >>> record.target_id
    1
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import RatingKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID

RatingT = TypeVar("RatingT", bound="RatingBase")
KindContainerT = TypeVar("KindContainerT", bound="KindRatingsContainer")


@dataclass(slots=True, kw_only=True)
class RatingBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a rating attachment with a numeric value, scale bounds, optional normalized value and agency.

    Concrete keyword-only dataclasses add the target id and level-specific context.
    Values are retained as supplied, with no automatic validation, normalization or
    referenced-row lookup.

    Example:
        >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
        >>> record.target_kind
        'work'
    """

    rating_kind: RatingKind
    value: float
    scale_max: float = 5.0
    scale_min: float = 0.0
    normalized_value: float | None = None
    agency: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("display_text", "value", "rating_kind")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id receiving this rating.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this rating.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    def display_text(self) -> str:
        """
        Format the stored value and upper scale bound with general numeric formatting.

        The display omits scale_min, agency and normalized_value and does not validate
        first.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.display_text
            '4/5'


        :return: Text in the form 'value/scale_max'.
        """
        return f"{self.value:g}/{self.scale_max:g}"

    def validate(self) -> None:
        """
        Check position, increasing scale bounds, in-range value and optional normalized bounds.

        Position must be nonnegative when present; value must lie inclusively between
        scale_min and scale_max. A supplied normalized_value must lie between zero and one,
        but is neither calculated nor checked against value. Kind and agency are not
        validated. Failures raise ValueError.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.value = 6
            >>> record.validate()
            Traceback (most recent call last):
            ...
            ValueError: value must lie within the rating scale


        :return: None.
        """
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if self.scale_max <= self.scale_min:
            raise ValueError("scale_max must be greater than scale_min")
        if not (self.scale_min <= self.value <= self.scale_max):
            raise ValueError("value must lie within the rating scale")
        if self.normalized_value is not None and not (0.0 <= self.normalized_value <= 1.0):
            raise ValueError("normalized_value must be between 0.0 and 1.0")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared rating fields without target additions or validation.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record._common_write_payload()['rating_kind'] == RatingKind.OVERALL
            True


        :return: New dictionary retaining stored values and enum members.
        """
        return {
            "rating_kind": self.rating_kind,
            "value": self.value,
            "scale_max": self.scale_max,
            "scale_min": self.scale_min,
            "normalized_value": self.normalized_value,
            "agency": self.agency,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Require serialization of shared rating fields and concrete target context.

        Example:
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations do not persist data.
        """


@dataclass(slots=True, kw_only=True)
class WorkRating(RatingBase):
    """
    Attach a rating assertion to a work, including the canonical-for-work flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = WorkRating(work_id=3, rating_kind=RatingKind.OVERALL, value=4)
        >>> record.target_id, record.target_kind
        (3, 'work')
    """
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this rating.

        Example:
            >>> record = WorkRating(work_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this rating as attached to a work.

        Example:
            >>> record = WorkRating(work_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared rating fields with the work id and the canonical-for-work flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = WorkRating(work_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionRating(RatingBase):
    """
    Attach a rating assertion to a expression, including the applies-to-realisation flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ExpressionRating(expression_id=3, rating_kind=RatingKind.OVERALL, value=4)
        >>> record.target_id, record.target_kind
        (3, 'expression')
    """
    expression_id: ExpressionID
    applies_to_realisation: bool = True

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this rating.

        Example:
            >>> record = ExpressionRating(expression_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this rating as attached to a expression.

        Example:
            >>> record = ExpressionRating(expression_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared rating fields with the expression id and the applies-to-realisation flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ExpressionRating(expression_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_realisation": self.applies_to_realisation})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationRating(RatingBase):
    """
    Attach a rating assertion to a manifestation, including the edition-specific flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ManifestationRating(manifestation_id=3, rating_kind=RatingKind.OVERALL, value=4)
        >>> record.target_id, record.target_kind
        (3, 'manifestation')
    """
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this rating.

        Example:
            >>> record = ManifestationRating(manifestation_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this rating as attached to a manifestation.

        Example:
            >>> record = ManifestationRating(manifestation_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared rating fields with the manifestation id and the edition-specific flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ManifestationRating(manifestation_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemRating(RatingBase):
    """
    Attach a rating assertion to a item, including the copy-specific flag.

    The keyword-only dataclass stores values as supplied. Record validation and any
    database reference checks are separate operations.

    Example:
        >>> record = ItemRating(item_id=3, rating_kind=RatingKind.OVERALL, value=4)
        >>> record.target_id, record.target_kind
        (3, 'item')
    """
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this rating.

        Example:
            >>> record = ItemRating(item_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this rating as attached to a item.

        Example:
            >>> record = ItemRating(item_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared rating fields with the item id and the copy-specific flag.

        No validation, normalization, reference lookup or persistence occurs.

        Example:
            >>> record = ItemRating(item_id=3, rating_kind=RatingKind.OVERALL, value=4)
            >>> record.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific values.
        """
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindRatingsContainer(MetadataSequenceStringMixin, Generic[RatingT], abc.ABC):
    """
    Maintain ordered rating records for one kind and WEMI target.

    The generated constructor retains an explicitly supplied _ratings list, or creates a
    fresh list by default. Records remain shared. Insertion checks shape and renumbers
    positions; full validation is explicit.

    Example:
        >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
        >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
        >>> records.add_rating(record)
        >>> records.texts()
        ('4/5',)
    """
    rating_kind: RatingKind
    target_id: int
    _ratings: list[RatingT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "ratings"

    def __iter__(self) -> Iterator[RatingT]:
        """
        Iterate over shared rating records in list order.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> next(iter(records)) is record
            True


        :return: Iterator over stored references.
        """
        return iter(self._ratings)

    def __len__(self) -> int:
        """
        Count the stored rating records.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> len(records)
            1


        :return: Number of stored records.
        """
        return len(self._ratings)

    def __getitem__(self, index: int) -> RatingT:
        """
        Read a rating by list index, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records[-1] is record
            True


        :param index: List index of the record to read.
        :return: Stored record object.
        """
        return self._ratings[index]

    def ratings(self) -> tuple[RatingT, ...]:
        """
        Take a tuple snapshot of record order while retaining shared mutable objects.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.ratings()[0] is record
            True


        :return: Tuple of stored record references.
        """
        return tuple(self._ratings)

    def texts(self) -> tuple[str, ...]:
        """
        Collect rating display_text values in list order without validation.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.texts()
            ('4/5',)


        :return: Tuple of strings, retaining duplicates.
        """
        return tuple(rating.display_text for rating in self._ratings)

    def to_text(self, sep: str = ", ") -> str:
        """
        Join every formatted rating using the separator.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.to_text(sep=' / ')
            '4/5'


        :param sep: Separator between rating displays; defaults to a comma and space.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.texts())

    def add_rating(self, rating: RatingT) -> None:
        """
        Check target and rating kind, append the shared record and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record fields are checked
        only by validate.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> record.position
            0


        :param rating: Record matching this bucket target kind, id and assertion kind.
        :return: None.
        """
        self._validate_rating_shape(rating)
        self._ratings.append(rating)
        self.normalize_positions()

    def replace_rating(self, index: int, rating: RatingT) -> None:
        """
        Check shape, replace the indexed record and renumber positions.

        Shape mismatches raise ValueError; invalid indices raise IndexError.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.replace_rating(0, record)
            >>> records[0] is record and record.position == 0
            True


        :param index: List index of the record to replace.
        :param rating: Replacement record matching this bucket target and kind.
        :return: None.
        """
        self._validate_rating_shape(rating)
        self._ratings[index] = rating
        self.normalize_positions()

    def remove_rating_at(self, index: int) -> RatingT:
        """
        Pop the indexed record and renumber survivors.

        The removed object retains its own fields. Invalid indices raise IndexError.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.remove_rating_at(0) is record
            True
            >>> len(records)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed record object.
        """
        removed = self._ratings.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned records.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.clear()
            >>> len(records)
            0


        :return: None.
        """
        self._ratings.clear()

    def move_rating(self, old_index: int, new_index: int) -> None:
        """
        Pop a record, insert it at the destination and renumber all positions.

        The source follows list.pop rules and the destination follows list.insert rules.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.move_rating(0, 99)
            >>> records[0] is record and record.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after removal; out-of-range destinations are
            clipped.
        :return: None.
        """
        rating = self._ratings.pop(old_index)
        self._ratings.insert(new_index, rating)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        Negative and out-of-range indices clear every primary flag.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
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
        for i, rating in enumerate(self._ratings):
            rating.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared record position with its zero-based list index.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> record.position = 9
            >>> records.normalize_positions()
            >>> record.position
            0


        :return: None.
        """
        for index, rating in enumerate(self._ratings):
            rating.position = index

    def validate(self) -> None:
        """
        Check rating shapes and values, contiguous positions and at most one primary record.

        Raise ValueError on the first failed constraint. Empty buckets are valid; values are
        not repaired.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.validate()
            >>> record.position = 2
            >>> records.validate()
            Traceback (most recent call last):
            ...
            ValueError: Rating position mismatch for work 1: expected 0, got 2


        :return: None.
        """
        primary_count = 0
        for expected_index, rating in enumerate(self._ratings):
            self._validate_rating_shape(rating)
            rating.validate()
            if rating.position != expected_index:
                raise ValueError(f"Rating position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {rating.position}")
            if rating.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary rating is allowed for {self.target_kind} {self.target_id} kind {self.rating_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Serialize records in list order without validation or persistence.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of per-record payload dictionaries.
        """
        return [rating.as_write_payload() for rating in self._ratings]

    def _validate_rating_shape(self, rating: RatingT) -> None:
        """
        Require matching target kind, target id and rating kind.

        Raise ValueError on the first mismatch; other record fields are not inspected.

        Example:
            >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records._validate_rating_shape(record)


        :param rating: Candidate record whose target and assertion kind must match this
            bucket.
        :return: None.
        """
        if rating.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {rating.target_kind} rating to {self.target_kind} container")
        if rating.target_id != self.target_id:
            raise ValueError(f"Rating target_id {rating.target_id} does not match container target_id {self.target_id}")
        if rating.rating_kind != self.rating_kind:
            raise ValueError(f"Rating kind {rating.rating_kind} does not match container kind {self.rating_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindRatingsContainer(KindRatingsContainer[WorkRating]):
    """
    Collect ordered rating assertions of one kind on a work.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'work' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = WorkKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=3)
        >>> records.target_kind, len(records)
        ('work', 0)
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindRatingsContainer(KindRatingsContainer[ExpressionRating]):
    """
    Collect ordered rating assertions of one kind on a expression.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'expression' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ExpressionKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=3)
        >>> records.target_kind, len(records)
        ('expression', 0)
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindRatingsContainer(KindRatingsContainer[ManifestationRating]):
    """
    Collect ordered rating assertions of one kind on a manifestation.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'manifestation' and can be supplied explicitly; construction does
    not validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ManifestationKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=3)
        >>> records.target_kind, len(records)
        ('manifestation', 0)
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindRatingsContainer(KindRatingsContainer[ItemRating]):
    """
    Collect ordered rating assertions of one kind on a item.

    Construction retains a supplied list without validation. The dataclass target_kind
    field defaults to 'item' and can be supplied explicitly; construction does not
    validate it. Shape checks accompany mutation; full validation is explicit.

    Example:
        >>> records = ItemKindRatingsContainer(rating_kind=RatingKind.OVERALL, target_id=3)
        >>> records.target_kind, len(records)
        ('item', 0)
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetRatingsContainer(
    MetadataSequenceStringMixin,
    Generic[RatingT, KindContainerT],
    abc.ABC,
):
    """
    Group editable rating buckets by kind for one WEMI target.

    Kind registration order is retained, including empty buckets. The dataclass
    constructor retains a supplied _by_kind dictionary, or creates an independent empty
    mapping.

    Example:
        >>> records = WorkRatingsContainer(work_id=1)
        >>> records.kinds()
        ()
    """
    _by_kind: dict[RatingKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "ratings"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id represented by this container.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
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
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_kind_container(self, rating_kind: RatingKind) -> KindContainerT:
        """
        Require creation of an empty kind bucket using this target id.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> bucket = records._make_kind_container(RatingKind.OVERALL)
            >>> bucket.target_id, records.kinds()
            (1, ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: New target-specific bucket; registration belongs to ensure_kind.
        """

    def kinds(self) -> tuple[RatingKind, ...]:
        """
        Return registered kind keys in insertion order, including empty buckets.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.kinds()
            ()


        :return: Tuple of kind keys.
        """
        return tuple(self._by_kind.keys())

    def get_kind(self, rating_kind: RatingKind) -> KindContainerT | None:
        """
        Read a kind bucket without creating one.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.get_kind(RatingKind.OVERALL) is None
            True


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: Live bucket, or None when absent.
        """
        return self._by_kind.get(rating_kind)

    def ensure_kind(self, rating_kind: RatingKind) -> KindContainerT:
        """
        Return a kind bucket, creating and registering an empty one if absent.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> bucket = records.ensure_kind(RatingKind.OVERALL)
            >>> records.ensure_kind(RatingKind.OVERALL) is bucket
            True


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: Live bucket using this target id.
        """
        container = self._by_kind.get(rating_kind)
        if container is None:
            container = self._make_kind_container(rating_kind)
            self._by_kind[rating_kind] = container
        return container

    def add_rating(self, rating: RatingT) -> None:
        """
        Check target id, ensure the rating kind bucket and delegate shape checks and insertion.

        An id mismatch raises ValueError before bucket creation. A later shape failure can
        leave a new empty bucket. Successful insertion renumbers positions; full validation
        remains explicit.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.add_rating(WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4))
            >>> records.kind_text(RatingKind.OVERALL)
            '4/5'


        :param rating: Shared record to insert into its kind bucket.
        :return: None.
        """
        if rating.target_id != self.target_id:
            raise ValueError(f"Rating target_id {rating.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(rating.rating_kind).add_rating(rating)

    def iter_all_ratings(self) -> Iterator[RatingT]:
        """
        Yield shared records in kind registration order and then bucket order.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> next(records.iter_all_ratings()) is record
            True


        :return: Iterator over all stored records.
        """
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, rating_kind: RatingKind, sep: str = ", ") -> str:
        """
        Join rating displays without creating a missing bucket.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.kind_text(RatingKind.OVERALL), records.kinds()
            ('', ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :param sep: Separator between rating displays; defaults to a comma and space.
        :return: Rendered text, or an empty string for an absent or empty bucket.
        """
        container = self.get_kind(rating_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate every registered bucket without repairing data or checking bucket ownership against this container.

        Each bucket checks its own target, kind and records. The first bucket error
        propagates.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> records.validate()


        :return: None.
        """
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-bucket payloads in kind registration order without validation or persistence.

        Example:
            >>> records = WorkRatingsContainer(work_id=1)
            >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
            >>> records.add_rating(record)
            >>> records.as_write_payload()[0]['work_id']
            1


        :return: New list of record payload dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkRatingsContainer(BaseTargetRatingsContainer[WorkRating, WorkKindRatingsContainer]):
    """
    Group all rating assertions on a work by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = WorkRatingsContainer(work_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this rating container.

        Example:
            >>> records = WorkRatingsContainer(work_id=3)
            >>> records.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the rating-container target as a work.

        Example:
            >>> records = WorkRatingsContainer(work_id=3)
            >>> records.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def _make_kind_container(self, rating_kind: RatingKind) -> WorkKindRatingsContainer:
        """
        Build an empty work kind bucket without registering it.

        Example:
            >>> records = WorkRatingsContainer(work_id=3)
            >>> bucket = records._make_kind_container(RatingKind.OVERALL)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: New WorkKindRatingsContainer using the stored target id.
        """
        return WorkKindRatingsContainer(rating_kind=rating_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionRatingsContainer(BaseTargetRatingsContainer[ExpressionRating, ExpressionKindRatingsContainer]):
    """
    Group all rating assertions on a expression by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ExpressionRatingsContainer(expression_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this rating container.

        Example:
            >>> records = ExpressionRatingsContainer(expression_id=3)
            >>> records.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the rating-container target as a expression.

        Example:
            >>> records = ExpressionRatingsContainer(expression_id=3)
            >>> records.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def _make_kind_container(self, rating_kind: RatingKind) -> ExpressionKindRatingsContainer:
        """
        Build an empty expression kind bucket without registering it.

        Example:
            >>> records = ExpressionRatingsContainer(expression_id=3)
            >>> bucket = records._make_kind_container(RatingKind.OVERALL)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: New ExpressionKindRatingsContainer using the stored target id.
        """
        return ExpressionKindRatingsContainer(rating_kind=rating_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationRatingsContainer(BaseTargetRatingsContainer[ManifestationRating, ManifestationKindRatingsContainer]):
    """
    Group all rating assertions on a manifestation by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ManifestationRatingsContainer(manifestation_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this rating container.

        Example:
            >>> records = ManifestationRatingsContainer(manifestation_id=3)
            >>> records.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the rating-container target as a manifestation.

        Example:
            >>> records = ManifestationRatingsContainer(manifestation_id=3)
            >>> records.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def _make_kind_container(self, rating_kind: RatingKind) -> ManifestationKindRatingsContainer:
        """
        Build an empty manifestation kind bucket without registering it.

        Example:
            >>> records = ManifestationRatingsContainer(manifestation_id=3)
            >>> bucket = records._make_kind_container(RatingKind.OVERALL)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: New ManifestationKindRatingsContainer using the stored target id.
        """
        return ManifestationKindRatingsContainer(rating_kind=rating_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemRatingsContainer(BaseTargetRatingsContainer[ItemRating, ItemKindRatingsContainer]):
    """
    Group all rating assertions on a item by kind.

    The explicit kind methods are the core API. Generated bucket and text accessors use
    the same stored records.

    Example:
        >>> records = ItemRatingsContainer(item_id=3)
        >>> records.target_id, records.kinds()
        (3, ())
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this rating container.

        Example:
            >>> records = ItemRatingsContainer(item_id=3)
            >>> records.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the rating-container target as a item.

        Example:
            >>> records = ItemRatingsContainer(item_id=3)
            >>> records.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def _make_kind_container(self, rating_kind: RatingKind) -> ItemKindRatingsContainer:
        """
        Build an empty item kind bucket without registering it.

        Example:
            >>> records = ItemRatingsContainer(item_id=3)
            >>> bucket = records._make_kind_container(RatingKind.OVERALL)
            >>> bucket.target_id, len(bucket), records.kinds()
            (3, 0, ())


        :param rating_kind: RatingKind value selecting a bucket for this target.
        :return: New ItemKindRatingsContainer using the stored target id.
        """
        return ItemKindRatingsContainer(rating_kind=rating_kind, target_id=self.item_id)


def _kind_property_stem(rating_kind: RatingKind) -> str:
    """
    Look up the fixed convenience-property stem for a rating kind.

    Unsupported keys raise KeyError.

    Example:
        >>> _kind_property_stem(RatingKind.OVERALL)
        'overall_ratings'


    :param rating_kind: RatingKind value selecting a bucket for this target.
    :return: Attribute stem for the selected kind.
    """
    stems = {
        RatingKind.OVERALL: "overall_ratings",
        RatingKind.USER: "user_ratings",
        RatingKind.CRITIC: "critic_ratings",
        RatingKind.INTERNAL: "internal_ratings",
        RatingKind.COMMUNITY: "community_ratings",
    }
    return stems[rating_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetRatingsContainer]) -> None:
    """
    Install per-kind bucket, text and configurable text accessors on a rating container class.

    Each kind receives stem, stem_text and stem_to_text attributes. Bucket access
    creates missing buckets; text access does not. Existing names are overwritten. The
    explicit kind methods remain canonical under
    metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> records = WorkRatingsContainer(work_id=1)
        >>> records.overall_ratings_text, records.kinds()
        ('', ())
        >>> bucket = records.overall_ratings
        >>> records.get_kind(RatingKind.OVERALL) is bucket
        True


    :param cls: Target container class receiving generated properties and methods.
    :return: None.
    """
    for rating_kind in RatingKind:
        stem = _kind_property_stem(rating_kind)

        def kind_container_getter(self, _kind=rating_kind):
            """
            Return the captured kind bucket, registering an empty bucket when absent.

            Example:
                >>> records = WorkRatingsContainer(work_id=1)
                >>> bucket = records.overall_ratings
                >>> records.get_kind(RatingKind.OVERALL) is bucket
                True


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Rating kind captured as the default argument during installation.
            :return: Live per-kind container.
            """
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=rating_kind):
            """
            Render the captured kind with the default separator without creating a bucket.

            Example:
                >>> records = WorkRatingsContainer(work_id=1)
                >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
                >>> records.add_rating(record)
                >>> records.overall_ratings_text
                '4/5'


            :param self: Target container instance receiving the generated accessor.
            :param _kind: Rating kind captured as the default argument during installation.
            :return: Joined rating displays, or an empty string.
            """
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=rating_kind) -> str:
            """
            Render the captured kind with a caller-selected separator.

            Example:
                >>> records = WorkRatingsContainer(work_id=1)
                >>> record = WorkRating(work_id=1, rating_kind=RatingKind.OVERALL, value=4)
                >>> records.add_rating(record)
                >>> records.overall_ratings_to_text(sep=' / ')
                '4/5'


            :param self: Target container instance receiving the generated accessor.
            :param sep: Separator forwarded to kind_text.
            :param _kind: Rating kind captured as the default argument during installation.
            :return: Joined rating displays, or an empty string.
            """
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkRatingsContainer)
_install_kind_convenience_properties(ExpressionRatingsContainer)
_install_kind_convenience_properties(ManifestationRatingsContainer)
_install_kind_convenience_properties(ItemRatingsContainer)


__all__ = [
    "RatingKind",
    "RatingBase",
    "WorkRating",
    "ExpressionRating",
    "ManifestationRating",
    "ItemRating",
    "KindRatingsContainer",
    "WorkKindRatingsContainer",
    "ExpressionKindRatingsContainer",
    "ManifestationKindRatingsContainer",
    "ItemKindRatingsContainer",
    "BaseTargetRatingsContainer",
    "WorkRatingsContainer",
    "ExpressionRatingsContainer",
    "ManifestationRatingsContainer",
    "ItemRatingsContainer",
]
