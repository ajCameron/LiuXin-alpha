"""External resource metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import ResourceKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID

ResourceT = TypeVar("ResourceT", bound="ResourceBase")
KindContainerT = TypeVar("KindContainerT", bound="KindResourcesContainer")


@dataclass(slots=True, kw_only=True)
class ResourceBase(MetadataValueStringMixin, abc.ABC):
    """Shared relation data for one external resource attached to a bibliographic entity."""

    resource_kind: ResourceKind
    uri: str
    label: str | None = None
    mime_type: str | None = None
    access_note: str | None = None

    position: int | None = None
    is_primary: bool = False
    is_public: bool = True

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("label", "uri", "resource_kind")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this resource attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def display_text(self) -> str:
        return self.label or self.uri

    def validate(self) -> None:
        if not self.uri.strip():
            raise ValueError("uri cannot be blank")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "resource_kind": self.resource_kind,
            "uri": self.uri,
            "label": self.label,
            "mime_type": self.mime_type,
            "access_note": self.access_note,
            "position": self.position,
            "is_primary": self.is_primary,
            "is_public": self.is_public,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkResource(ResourceBase):
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
class ExpressionResource(ResourceBase):
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
class ManifestationResource(ResourceBase):
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
class ItemResource(ResourceBase):
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
class KindResourcesContainer(MetadataSequenceStringMixin, Generic[ResourceT], abc.ABC):
    resource_kind: ResourceKind
    target_id: int
    _resources: list[ResourceT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "resources"

    def __iter__(self) -> Iterator[ResourceT]:
        return iter(self._resources)

    def __len__(self) -> int:
        return len(self._resources)

    def __getitem__(self, index: int) -> ResourceT:
        return self._resources[index]

    def resources(self) -> tuple[ResourceT, ...]:
        return tuple(self._resources)

    def texts(self) -> tuple[str, ...]:
        return tuple(resource.display_text for resource in self._resources)

    def to_text(self, sep: str = ", ") -> str:
        return sep.join(self.texts())

    def add_resource(self, resource: ResourceT) -> None:
        self._validate_resource_shape(resource)
        self._resources.append(resource)
        self.normalize_positions()

    def replace_resource(self, index: int, resource: ResourceT) -> None:
        self._validate_resource_shape(resource)
        self._resources[index] = resource
        self.normalize_positions()

    def remove_resource_at(self, index: int) -> ResourceT:
        removed = self._resources.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._resources.clear()

    def move_resource(self, old_index: int, new_index: int) -> None:
        resource = self._resources.pop(old_index)
        self._resources.insert(new_index, resource)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, resource in enumerate(self._resources):
            resource.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, resource in enumerate(self._resources):
            resource.position = index

    def validate(self) -> None:
        primary_count = 0
        for expected_index, resource in enumerate(self._resources):
            self._validate_resource_shape(resource)
            resource.validate()
            if resource.position != expected_index:
                raise ValueError(f"Resource position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {resource.position}")
            if resource.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary resource is allowed for {self.target_kind} {self.target_id} kind {self.resource_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        return [resource.as_write_payload() for resource in self._resources]

    def _validate_resource_shape(self, resource: ResourceT) -> None:
        if resource.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {resource.target_kind} resource to {self.target_kind} container")
        if resource.target_id != self.target_id:
            raise ValueError(f"Resource target_id {resource.target_id} does not match container target_id {self.target_id}")
        if resource.resource_kind != self.resource_kind:
            raise ValueError(f"Resource kind {resource.resource_kind} does not match container kind {self.resource_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindResourcesContainer(KindResourcesContainer[WorkResource]):
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindResourcesContainer(KindResourcesContainer[ExpressionResource]):
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindResourcesContainer(KindResourcesContainer[ManifestationResource]):
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindResourcesContainer(KindResourcesContainer[ItemResource]):
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetResourcesContainer(
    MetadataSequenceStringMixin,
    Generic[ResourceT, KindContainerT],
    abc.ABC,
):
    _by_kind: dict[ResourceKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "resources"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, resource_kind: ResourceKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[ResourceKind, ...]:
        return tuple(self._by_kind.keys())

    def get_kind(self, resource_kind: ResourceKind) -> KindContainerT | None:
        return self._by_kind.get(resource_kind)

    def ensure_kind(self, resource_kind: ResourceKind) -> KindContainerT:
        container = self._by_kind.get(resource_kind)
        if container is None:
            container = self._make_kind_container(resource_kind)
            self._by_kind[resource_kind] = container
        return container

    def add_resource(self, resource: ResourceT) -> None:
        if resource.target_id != self.target_id:
            raise ValueError(f"Resource target_id {resource.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(resource.resource_kind).add_resource(resource)

    def iter_all_resources(self) -> Iterator[ResourceT]:
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, resource_kind: ResourceKind, sep: str = ", ") -> str:
        container = self.get_kind(resource_kind)
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
class WorkResourcesContainer(BaseTargetResourcesContainer[WorkResource, WorkKindResourcesContainer]):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, resource_kind: ResourceKind) -> WorkKindResourcesContainer:
        return WorkKindResourcesContainer(resource_kind=resource_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionResourcesContainer(BaseTargetResourcesContainer[ExpressionResource, ExpressionKindResourcesContainer]):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ExpressionKindResourcesContainer:
        return ExpressionKindResourcesContainer(resource_kind=resource_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationResourcesContainer(BaseTargetResourcesContainer[ManifestationResource, ManifestationKindResourcesContainer]):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ManifestationKindResourcesContainer:
        return ManifestationKindResourcesContainer(resource_kind=resource_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemResourcesContainer(BaseTargetResourcesContainer[ItemResource, ItemKindResourcesContainer]):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, resource_kind: ResourceKind) -> ItemKindResourcesContainer:
        return ItemKindResourcesContainer(resource_kind=resource_kind, target_id=self.item_id)


def _kind_property_stem(resource_kind: ResourceKind) -> str:
    stems = {
        ResourceKind.AUTHORITY: "authority_resources",
        ResourceKind.CATALOGUE: "catalogue_resources",
        ResourceKind.FULL_TEXT: "full_text_resources",
        ResourceKind.PREVIEW: "preview_resources",
        ResourceKind.DOWNLOAD: "download_resources",
        ResourceKind.COVER_IMAGE: "cover_image_resources",
        ResourceKind.MIRROR: "mirror_resources",
        ResourceKind.PUBLISHER: "publisher_resources",
        ResourceKind.PURCHASE: "purchase_resources",
    }
    return stems[resource_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetResourcesContainer]) -> None:
    """Install per-kind convenience properties and methods on a resources container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    for resource_kind in ResourceKind:
        stem = _kind_property_stem(resource_kind)

        def kind_container_getter(self, _kind=resource_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=resource_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=resource_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkResourcesContainer)
_install_kind_convenience_properties(ExpressionResourcesContainer)
_install_kind_convenience_properties(ManifestationResourcesContainer)
_install_kind_convenience_properties(ItemResourcesContainer)


__all__ = [
    "ResourceKind",
    "ResourceBase",
    "WorkResource",
    "ExpressionResource",
    "ManifestationResource",
    "ItemResource",
    "KindResourcesContainer",
    "WorkKindResourcesContainer",
    "ExpressionKindResourcesContainer",
    "ManifestationKindResourcesContainer",
    "ItemKindResourcesContainer",
    "BaseTargetResourcesContainer",
    "WorkResourcesContainer",
    "ExpressionResourcesContainer",
    "ManifestationResourcesContainer",
    "ItemResourcesContainer",
]
