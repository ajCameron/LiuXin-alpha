"""Series metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
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
    """Shared relation data for one series-like attachment."""

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
        """ID of the W/E/M/I entity this series entry attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def display_text(self) -> str:
        if self.numbering_text:
            return f"{self.name} #{self.numbering_text}"
        if self.position_in_series is not None:
            return f"{self.name} #{self.position_in_series:g}"
        return self.name

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("name cannot be blank")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")
        if bool(self.authority_scheme) ^ bool(self.authority_identifier):
            raise ValueError("authority_scheme and authority_identifier must either both be set or both be empty")

    def _common_write_payload(self) -> dict[str, object]:
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
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkSeriesEntry(SeriesEntryBase):
    """
    Represent one numbered Series membership attached to a Work.
    """
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
class ExpressionSeriesEntry(SeriesEntryBase):
    """
    Represent one numbered Series membership attached to an Expression.
    """
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
class ManifestationSeriesEntry(SeriesEntryBase):
    """
    Represent one numbered Series membership attached to a Manifestation.
    """
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
class ItemSeriesEntry(SeriesEntryBase):
    """
    Represent one numbered Series membership attached to an Item.
    """
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
class KindSeriesEntriesContainer(MetadataSequenceStringMixin, Generic[SeriesT], abc.ABC):
    """
    Manage ordered Series memberships of one kind for a WEMI target.
    """
    series_kind: SeriesKind
    target_id: int
    _entries: list[SeriesT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "series entries"

    def __iter__(self) -> Iterator[SeriesT]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __getitem__(self, index: int) -> SeriesT:
        return self._entries[index]

    def entries(self) -> tuple[SeriesT, ...]:
        return tuple(self._entries)

    def texts(self) -> tuple[str, ...]:
        return tuple(entry.display_text for entry in self._entries)

    def to_text(self, sep: str = "; ") -> str:
        return sep.join(self.texts())

    def add_entry(self, entry: SeriesT) -> None:
        self._validate_entry_shape(entry)
        self._entries.append(entry)
        self.normalize_positions()

    def replace_entry(self, index: int, entry: SeriesT) -> None:
        self._validate_entry_shape(entry)
        self._entries[index] = entry
        self.normalize_positions()

    def remove_entry_at(self, index: int) -> SeriesT:
        removed = self._entries.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._entries.clear()

    def move_entry(self, old_index: int, new_index: int) -> None:
        entry = self._entries.pop(old_index)
        self._entries.insert(new_index, entry)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, entry in enumerate(self._entries):
            entry.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, entry in enumerate(self._entries):
            entry.position = index

    def validate(self) -> None:
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
        return [entry.as_write_payload() for entry in self._entries]

    def _validate_entry_shape(self, entry: SeriesT) -> None:
        if entry.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {entry.target_kind} series entry to {self.target_kind} container")
        if entry.target_id != self.target_id:
            raise ValueError(f"Series entry target_id {entry.target_id} does not match container target_id {self.target_id}")
        if entry.series_kind != self.series_kind:
            raise ValueError(f"Series kind {entry.series_kind} does not match container kind {self.series_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindSeriesEntriesContainer(KindSeriesEntriesContainer[WorkSeriesEntry]):
    """
    Collect Series memberships of one kind for a Work.
    """
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindSeriesEntriesContainer(KindSeriesEntriesContainer[ExpressionSeriesEntry]):
    """
    Collect Series memberships of one kind for an Expression.
    """
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindSeriesEntriesContainer(KindSeriesEntriesContainer[ManifestationSeriesEntry]):
    """
    Collect Series memberships of one kind for a Manifestation.
    """
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindSeriesEntriesContainer(KindSeriesEntriesContainer[ItemSeriesEntry]):
    """
    Collect Series memberships of one kind for an Item.
    """
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetSeriesEntriesContainer(
    MetadataSequenceStringMixin,
    Generic[SeriesT, KindContainerT],
    abc.ABC,
):
    """
    Group all Series memberships for one WEMI target by relationship kind.
    """
    _by_kind: dict[SeriesKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "series entries"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, series_kind: SeriesKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[SeriesKind, ...]:
        return tuple(self._by_kind.keys())

    def get_kind(self, series_kind: SeriesKind) -> KindContainerT | None:
        return self._by_kind.get(series_kind)

    def ensure_kind(self, series_kind: SeriesKind) -> KindContainerT:
        container = self._by_kind.get(series_kind)
        if container is None:
            container = self._make_kind_container(series_kind)
            self._by_kind[series_kind] = container
        return container

    def add_entry(self, entry: SeriesT) -> None:
        if entry.target_id != self.target_id:
            raise ValueError(f"Series entry target_id {entry.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(entry.series_kind).add_entry(entry)

    def iter_all_entries(self) -> Iterator[SeriesT]:
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, series_kind: SeriesKind, sep: str = "; ") -> str:
        container = self.get_kind(series_kind)
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
class WorkSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[WorkSeriesEntry, WorkKindSeriesEntriesContainer]):
    """
    Group every Series membership attached to a Work.
    """
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, series_kind: SeriesKind) -> WorkKindSeriesEntriesContainer:
        return WorkKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ExpressionSeriesEntry, ExpressionKindSeriesEntriesContainer]):
    """
    Group every Series membership attached to an Expression.
    """
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(self, series_kind: SeriesKind) -> ExpressionKindSeriesEntriesContainer:
        return ExpressionKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ManifestationSeriesEntry, ManifestationKindSeriesEntriesContainer]):
    """
    Group every Series membership attached to a Manifestation.
    """
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(self, series_kind: SeriesKind) -> ManifestationKindSeriesEntriesContainer:
        return ManifestationKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemSeriesEntriesContainer(BaseTargetSeriesEntriesContainer[ItemSeriesEntry, ItemKindSeriesEntriesContainer]):
    """
    Group every Series membership attached to an Item.
    """
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, series_kind: SeriesKind) -> ItemKindSeriesEntriesContainer:
        return ItemKindSeriesEntriesContainer(series_kind=series_kind, target_id=self.item_id)


def _kind_property_stem(series_kind: SeriesKind) -> str:
    stems = {
        SeriesKind.SERIES: "series_entries",
        SeriesKind.SUBSERIES: "subseries_entries",
        SeriesKind.ARC: "arc_entries",
        SeriesKind.COLLECTION: "collection_entries",
    }
    return stems[series_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetSeriesEntriesContainer]) -> None:
    """Install per-kind convenience properties and methods on a series container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    for series_kind in SeriesKind:
        stem = _kind_property_stem(series_kind)

        def kind_container_getter(self, _kind=series_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=series_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = "; ", _kind=series_kind) -> str:
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
