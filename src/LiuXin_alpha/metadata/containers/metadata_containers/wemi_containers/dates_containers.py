"""Date metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import DateKind
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID, LanguageID

DateT = TypeVar("DateT", bound="DateBase")
KindContainerT = TypeVar("KindContainerT", bound="KindDatesContainer")


@dataclass(slots=True, kw_only=True)
class DateBase(abc.ABC):
    """Shared relation data for one date record attached to a bibliographic entity."""

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

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this date attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def rendered_text(self) -> str:
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
        if self.start_ep_k is None and self.end_ep_k is None and not self.display_text:
            raise ValueError("date record must provide at least one of start_ep_k, end_ep_k, or display_text")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if self.start_ep_k is not None and self.end_ep_k is not None and self.end_ep_k < self.start_ep_k:
            raise ValueError("end_ep_k cannot be earlier than start_ep_k")

    def _common_write_payload(self) -> dict[str, object]:
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
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkDate(DateBase):
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
class ExpressionDate(DateBase):
    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_language_id": self.applies_to_language_id})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationDate(DateBase):
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
class ItemDate(DateBase):
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
class KindDatesContainer(Generic[DateT], abc.ABC):
    date_kind: DateKind
    target_id: int
    _dates: list[DateT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]

    def __iter__(self) -> Iterator[DateT]:
        return iter(self._dates)

    def __len__(self) -> int:
        return len(self._dates)

    def __getitem__(self, index: int) -> DateT:
        return self._dates[index]

    def dates(self) -> tuple[DateT, ...]:
        return tuple(self._dates)

    def texts(self) -> tuple[str, ...]:
        return tuple(date.rendered_text for date in self._dates)

    def to_text(self, sep: str = "; ") -> str:
        return sep.join(text for text in self.texts() if text)

    def add_date(self, date: DateT) -> None:
        self._validate_date_shape(date)
        self._dates.append(date)
        self.normalize_positions()

    def replace_date(self, index: int, date: DateT) -> None:
        self._validate_date_shape(date)
        self._dates[index] = date
        self.normalize_positions()

    def remove_date_at(self, index: int) -> DateT:
        removed = self._dates.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._dates.clear()

    def move_date(self, old_index: int, new_index: int) -> None:
        date = self._dates.pop(old_index)
        self._dates.insert(new_index, date)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, date in enumerate(self._dates):
            date.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, date in enumerate(self._dates):
            date.position = index

    def validate(self) -> None:
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
        return [date.as_write_payload() for date in self._dates]

    def _validate_date_shape(self, date: DateT) -> None:
        if date.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {date.target_kind} date to {self.target_kind} container")
        if date.target_id != self.target_id:
            raise ValueError(f"Date target_id {date.target_id} does not match container target_id {self.target_id}")
        if date.date_kind != self.date_kind:
            raise ValueError(f"Date kind {date.date_kind} does not match container kind {self.date_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindDatesContainer(KindDatesContainer[WorkDate]):
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindDatesContainer(KindDatesContainer[ExpressionDate]):
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindDatesContainer(KindDatesContainer[ManifestationDate]):
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindDatesContainer(KindDatesContainer[ItemDate]):
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetDatesContainer(Generic[DateT, KindContainerT], abc.ABC):
    _by_kind: dict[DateKind, KindContainerT] = field(default_factory=dict)

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, date_kind: DateKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[DateKind, ...]:
        return tuple(self._by_kind.keys())

    def get_kind(self, date_kind: DateKind) -> KindContainerT | None:
        return self._by_kind.get(date_kind)

    def ensure_kind(self, date_kind: DateKind) -> KindContainerT:
        container = self._by_kind.get(date_kind)
        if container is None:
            container = self._make_kind_container(date_kind)
            self._by_kind[date_kind] = container
        return container

    def add_date(self, date: DateT) -> None:
        if date.target_id != self.target_id:
            raise ValueError(f"Date target_id {date.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(date.date_kind).add_date(date)

    def iter_all_dates(self) -> Iterator[DateT]:
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, date_kind: DateKind, sep: str = "; ") -> str:
        container = self.get_kind(date_kind)
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
class WorkDatesContainer(BaseTargetDatesContainer[WorkDate, WorkKindDatesContainer]):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, date_kind: DateKind) -> WorkKindDatesContainer:
        return WorkKindDatesContainer(date_kind=date_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionDatesContainer(BaseTargetDatesContainer[ExpressionDate, ExpressionKindDatesContainer]):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(self, date_kind: DateKind) -> ExpressionKindDatesContainer:
        return ExpressionKindDatesContainer(date_kind=date_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationDatesContainer(BaseTargetDatesContainer[ManifestationDate, ManifestationKindDatesContainer]):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(self, date_kind: DateKind) -> ManifestationKindDatesContainer:
        return ManifestationKindDatesContainer(date_kind=date_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemDatesContainer(BaseTargetDatesContainer[ItemDate, ItemKindDatesContainer]):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, date_kind: DateKind) -> ItemKindDatesContainer:
        return ItemKindDatesContainer(date_kind=date_kind, target_id=self.item_id)


def _kind_property_stem(date_kind: DateKind) -> str:
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
    """Install per-kind convenience properties and methods on a dates container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    for date_kind in DateKind:
        stem = _kind_property_stem(date_kind)

        def kind_container_getter(self, _kind=date_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=date_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = "; ", _kind=date_kind) -> str:
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
