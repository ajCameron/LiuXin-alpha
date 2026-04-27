"""Rating metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import RatingKind
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID

RatingT = TypeVar("RatingT", bound="RatingBase")
KindContainerT = TypeVar("KindContainerT", bound="KindRatingsContainer")


@dataclass(slots=True, kw_only=True)
class RatingBase(abc.ABC):
    """Shared relation data for one rating record attached to a bibliographic entity."""

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

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this rating attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def display_text(self) -> str:
        return f"{self.value:g}/{self.scale_max:g}"

    def validate(self) -> None:
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if self.scale_max <= self.scale_min:
            raise ValueError("scale_max must be greater than scale_min")
        if not (self.scale_min <= self.value <= self.scale_max):
            raise ValueError("value must lie within the rating scale")
        if self.normalized_value is not None and not (0.0 <= self.normalized_value <= 1.0):
            raise ValueError("normalized_value must be between 0.0 and 1.0")

    def _common_write_payload(self) -> dict[str, object]:
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
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkRating(RatingBase):
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionRating(RatingBase):
    expression_id: ExpressionID
    applies_to_realisation: bool = True

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_realisation": self.applies_to_realisation})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationRating(RatingBase):
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemRating(RatingBase):
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindRatingsContainer(Generic[RatingT], abc.ABC):
    rating_kind: RatingKind
    target_id: int
    _ratings: list[RatingT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]

    def __iter__(self) -> Iterator[RatingT]:
        return iter(self._ratings)

    def __len__(self) -> int:
        return len(self._ratings)

    def __getitem__(self, index: int) -> RatingT:
        return self._ratings[index]

    def ratings(self) -> tuple[RatingT, ...]:
        return tuple(self._ratings)

    def texts(self) -> tuple[str, ...]:
        return tuple(rating.display_text for rating in self._ratings)

    def to_text(self, sep: str = ", ") -> str:
        return sep.join(self.texts())

    def add_rating(self, rating: RatingT) -> None:
        self._validate_rating_shape(rating)
        self._ratings.append(rating)
        self.normalize_positions()

    def replace_rating(self, index: int, rating: RatingT) -> None:
        self._validate_rating_shape(rating)
        self._ratings[index] = rating
        self.normalize_positions()

    def remove_rating_at(self, index: int) -> RatingT:
        removed = self._ratings.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._ratings.clear()

    def move_rating(self, old_index: int, new_index: int) -> None:
        rating = self._ratings.pop(old_index)
        self._ratings.insert(new_index, rating)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, rating in enumerate(self._ratings):
            rating.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, rating in enumerate(self._ratings):
            rating.position = index

    def validate(self) -> None:
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
        return [rating.as_write_payload() for rating in self._ratings]

    def _validate_rating_shape(self, rating: RatingT) -> None:
        if rating.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {rating.target_kind} rating to {self.target_kind} container")
        if rating.target_id != self.target_id:
            raise ValueError(f"Rating target_id {rating.target_id} does not match container target_id {self.target_id}")
        if rating.rating_kind != self.rating_kind:
            raise ValueError(f"Rating kind {rating.rating_kind} does not match container kind {self.rating_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindRatingsContainer(KindRatingsContainer[WorkRating]):
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindRatingsContainer(KindRatingsContainer[ExpressionRating]):
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindRatingsContainer(KindRatingsContainer[ManifestationRating]):
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindRatingsContainer(KindRatingsContainer[ItemRating]):
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetRatingsContainer(Generic[RatingT, KindContainerT], abc.ABC):
    _by_kind: dict[RatingKind, KindContainerT] = field(default_factory=dict)

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, rating_kind: RatingKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[RatingKind, ...]:
        return tuple(self._by_kind.keys())

    def get_kind(self, rating_kind: RatingKind) -> KindContainerT | None:
        return self._by_kind.get(rating_kind)

    def ensure_kind(self, rating_kind: RatingKind) -> KindContainerT:
        container = self._by_kind.get(rating_kind)
        if container is None:
            container = self._make_kind_container(rating_kind)
            self._by_kind[rating_kind] = container
        return container

    def add_rating(self, rating: RatingT) -> None:
        if rating.target_id != self.target_id:
            raise ValueError(f"Rating target_id {rating.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(rating.rating_kind).add_rating(rating)

    def iter_all_ratings(self) -> Iterator[RatingT]:
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, rating_kind: RatingKind, sep: str = ", ") -> str:
        container = self.get_kind(rating_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkRatingsContainer(BaseTargetRatingsContainer[WorkRating, WorkKindRatingsContainer]):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, rating_kind: RatingKind) -> WorkKindRatingsContainer:
        return WorkKindRatingsContainer(rating_kind=rating_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionRatingsContainer(BaseTargetRatingsContainer[ExpressionRating, ExpressionKindRatingsContainer]):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(self, rating_kind: RatingKind) -> ExpressionKindRatingsContainer:
        return ExpressionKindRatingsContainer(rating_kind=rating_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationRatingsContainer(BaseTargetRatingsContainer[ManifestationRating, ManifestationKindRatingsContainer]):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(self, rating_kind: RatingKind) -> ManifestationKindRatingsContainer:
        return ManifestationKindRatingsContainer(rating_kind=rating_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemRatingsContainer(BaseTargetRatingsContainer[ItemRating, ItemKindRatingsContainer]):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, rating_kind: RatingKind) -> ItemKindRatingsContainer:
        return ItemKindRatingsContainer(rating_kind=rating_kind, target_id=self.item_id)


def _kind_property_stem(rating_kind: RatingKind) -> str:
    stems = {
        RatingKind.OVERALL: "overall_ratings",
        RatingKind.USER: "user_ratings",
        RatingKind.CRITIC: "critic_ratings",
        RatingKind.INTERNAL: "internal_ratings",
        RatingKind.COMMUNITY: "community_ratings",
    }
    return stems[rating_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetRatingsContainer]) -> None:
    """Install per-kind convenience properties and methods on a ratings container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    for rating_kind in RatingKind:
        stem = _kind_property_stem(rating_kind)

        def kind_container_getter(self, _kind=rating_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=rating_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=rating_kind) -> str:
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
