"""Label metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import LabelKind
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


LabelT = TypeVar("LabelT", bound="LabelBase")
KindContainerT = TypeVar("KindContainerT", bound="KindLabelsContainer")



@dataclass(slots=True, kw_only=True)
class LabelBase(MetadataValueStringMixin, abc.ABC):
    """
    Shared relation data for a label attached to a bibliographic entity.

    This models the label-link, not a database row proxy.
    """

    label_kind: LabelKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("text", "label_kind", "source")

    # Optional glue to authority / external classification systems.
    authority_record_id: int | None = None
    external_key: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this label attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @property
    def kind_key(self) -> LabelKind:
        """
        The kind this label is grouped by.

        :return:
        """
        return self.label_kind

    def validate(self) -> None:
        """
        Validate that the label is internally consistent.

        :return:
        """
        if not str(self.label_kind).strip():
            raise ValueError("label_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "label_kind": self.label_kind,
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
        Serialise to a write-layer payload.
        """


@dataclass(slots=True, kw_only=True)
class WorkLabel(LabelBase):
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
        payload.update(
            {
                "work_id": self.work_id,
                "canonical_for_work": self.canonical_for_work,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionLabel(LabelBase):
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
        payload.update(
            {
                "expression_id": self.expression_id,
                "applies_to_language_id": self.applies_to_language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationLabel(LabelBase):
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
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_specific": self.edition_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemLabel(LabelBase):
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
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindLabelsContainer(MetadataSequenceStringMixin, Generic[LabelT], abc.ABC):
    """
    Ordered editable container for all labels of one kind on one target entity.
    """

    label_kind: LabelKind
    target_id: int
    _labels: list[LabelT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL = "labels"

    def __iter__(self) -> Iterator[LabelT]:
        return iter(self._labels)

    def __len__(self) -> int:
        return len(self._labels)

    def __getitem__(self, index: int) -> LabelT:
        return self._labels[index]

    def labels(self) -> tuple[LabelT, ...]:
        return tuple(self._labels)

    def texts(self) -> tuple[str, ...]:
        return tuple(label.text for label in self._labels)

    def sort_texts(self) -> tuple[str, ...]:
        return tuple(label.sort_text or label.text for label in self._labels)

    def to_text(self, sep: str = ", ") -> str:
        return sep.join(self.texts())

    def add_label(self, label: LabelT) -> None:
        self._validate_label_shape(label)
        self._labels.append(label)
        self.normalize_positions()

    def replace_label(self, index: int, label: LabelT) -> None:
        self._validate_label_shape(label)
        self._labels[index] = label
        self.normalize_positions()

    def remove_label_at(self, index: int) -> LabelT:
        removed = self._labels.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._labels.clear()

    def move_label(self, old_index: int, new_index: int) -> None:
        label = self._labels.pop(old_index)
        self._labels.insert(new_index, label)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, label in enumerate(self._labels):
            label.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, label in enumerate(self._labels):
            label.position = index

    def primary_label(self) -> LabelT | None:
        for label in self._labels:
            if label.is_primary:
                return label
        return self._labels[0] if self._labels else None

    def validate(self) -> None:
        primary_count = 0

        for expected_index, label in enumerate(self._labels):
            self._validate_label_shape(label)
            label.validate()

            if label.position != expected_index:
                raise ValueError(
                    f"Label position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {label.position}"
                )

            if label.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary label is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.label_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [label.as_write_payload() for label in self._labels]

    def _validate_label_shape(self, label: LabelT) -> None:
        if label.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {label.target_kind} label to {self.target_kind} container"
            )

        if label.target_id != self.target_id:
            raise ValueError(
                f"Label target_id {label.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if label.kind_key != self.label_kind:
            raise ValueError(
                f"Label kind {label.kind_key} does not match "
                f"container kind {self.label_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindLabelsContainer(KindLabelsContainer[WorkLabel]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindLabelsContainer(KindLabelsContainer[ExpressionLabel]):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindLabelsContainer(KindLabelsContainer[ManifestationLabel]):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindLabelsContainer(KindLabelsContainer[ItemLabel]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetLabelsContainer(
    MetadataSequenceStringMixin,
    Generic[LabelT, KindContainerT],
    abc.ABC,
):
    """
    Top-level editable label container for one target entity.
    Holds one KindLabelsContainer per label kind.
    """

    _by_kind: dict[LabelKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "labels"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the target object.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @abc.abstractmethod
    def _make_kind_container(self, label_kind: LabelKind) -> KindContainerT:
        """
        Build the correct per-kind container for this target type.
        """

    def kinds(self) -> tuple[LabelKind, ...]:
        return tuple(self._by_kind.keys())

    def has_kind(self, label_kind: LabelKind) -> bool:
        return label_kind in self._by_kind

    def get_kind(self, label_kind: LabelKind) -> KindContainerT | None:
        return self._by_kind.get(label_kind)

    def ensure_kind(self, label_kind: LabelKind) -> KindContainerT:
        container = self._by_kind.get(label_kind)
        if container is None:
            container = self._make_kind_container(label_kind)
            self._by_kind[label_kind] = container
        return container

    def add_label(self, label: LabelT) -> None:
        if label.target_id != self.target_id:
            raise ValueError(
                f"Label target_id {label.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(label.kind_key).add_label(label)

    def iter_all_labels(self) -> Iterator[LabelT]:
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        return tuple(label.text for label in self.iter_all_labels())

    def kind_text(self, label_kind: LabelKind, sep: str = ", ") -> str:
        container = self.get_kind(label_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_labels(self) -> dict[LabelKind, LabelT]:
        result: dict[LabelKind, LabelT] = {}
        for label_kind, container in self._by_kind.items():
            primary = container.primary_label()
            if primary is not None:
                result[label_kind] = primary
        return result

    def primary_label_for_kind(self, label_kind: LabelKind) -> LabelT | None:
        container = self.get_kind(label_kind)
        if container is None:
            return None
        return container.primary_label()

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkLabelsContainer(
    BaseTargetLabelsContainer[
        WorkLabel,
        WorkKindLabelsContainer,
    ]
):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, label_kind: LabelKind) -> WorkKindLabelsContainer:
        return WorkKindLabelsContainer(label_kind=label_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionLabelsContainer(
    BaseTargetLabelsContainer[
        ExpressionLabel,
        ExpressionKindLabelsContainer,
    ]
):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(
        self,
        label_kind: LabelKind,
    ) -> ExpressionKindLabelsContainer:
        return ExpressionKindLabelsContainer(
            label_kind=label_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationLabelsContainer(
    BaseTargetLabelsContainer[
        ManifestationLabel,
        ManifestationKindLabelsContainer,
    ]
):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(
        self,
        label_kind: LabelKind,
    ) -> ManifestationKindLabelsContainer:
        return ManifestationKindLabelsContainer(
            label_kind=label_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemLabelsContainer(
    BaseTargetLabelsContainer[
        ItemLabel,
        ItemKindLabelsContainer,
    ]
):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, label_kind: LabelKind) -> ItemKindLabelsContainer:
        return ItemKindLabelsContainer(label_kind=label_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Label-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(label_kind: LabelKind) -> str:
    stems: dict[LabelKind, str] = {
        LabelKind.TAG: "tags",
        LabelKind.GENRE: "genres",
        LabelKind.FORM: "forms",
        LabelKind.TOPIC: "topics",
        LabelKind.CHARACTER: "characters",
        LabelKind.PLACE: "places",
        LabelKind.PERIOD: "periods",
        LabelKind.AUDIENCE: "audiences",
        LabelKind.AWARD: "awards",
        LabelKind.COLLECTION: "collections",
        LabelKind.INTERNAL: "internal_labels",
    }
    return stems[label_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetLabelsContainer],
) -> None:
    """
    Install per-kind convenience properties and methods on a labels container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.

    For a kind stem of 'tags', this creates:
    - .tags
    - .tags_text
    - .tags_to_text(sep=", ")
    """

    for label_kind in LabelKind:
        stem = _kind_property_stem(label_kind)

        def kind_container_getter(self, _kind=label_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=label_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=label_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkLabelsContainer)
_install_kind_convenience_properties(ExpressionLabelsContainer)
_install_kind_convenience_properties(ManifestationLabelsContainer)
_install_kind_convenience_properties(ItemLabelsContainer)


__all__ = [
    "LabelKind",
    "LabelBase",
    "WorkLabel",
    "ExpressionLabel",
    "ManifestationLabel",
    "ItemLabel",
    "KindLabelsContainer",
    "WorkKindLabelsContainer",
    "ExpressionKindLabelsContainer",
    "ManifestationKindLabelsContainer",
    "ItemKindLabelsContainer",
    "BaseTargetLabelsContainer",
    "WorkLabelsContainer",
    "ExpressionLabelsContainer",
    "ManifestationLabelsContainer",
    "ItemLabelsContainer",
]
