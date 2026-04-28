"""Core WEMI metadata-bundle API contract for manifestation entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a
manifestation. It is not the manifestation identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias


from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api.asset_replica_api import AssetReplicaIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api.digital_asset_api import DigitalAssetIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ManifestationRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | AssetReplicaIdentityAPI
    | DigitalAssetIdentityAPI
    | ExpressionIdentityAPI
    | ItemIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ManifestationRelationLink:
    target: ManifestationRelationTarget
    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int | str] = None
    extra: MutableMetadataRecord = dataclasses.field(default_factory=dict)


class ManifestationMetadataAPI(abc.ABC):
    """Rich metadata bundle centred on one manifestation."""

    RELATION_KEYS: ClassVar[tuple[str, ...]] = (
        "works",
        "expressions",
        "items",
        "agents",
        "identifiers",
        "titles",
        "genres",
        "labels",
        "languages",
        "notes",
        "comments",
        "files",
        "images",
        "digital_assets",
        "asset_replicas",
    )
    RELATION_ALIASES: ClassVar[Mapping[str, str]] = {
        "work": "works",
        "expression": "expressions",
        "item": "items",
        "agent": "agents",
        "creator": "agents",
        "identifier": "identifiers",
        "title": "titles",
        "genre": "genres",
        "label": "labels",
        "language": "languages",
        "note": "notes",
        "comment": "comments",
        "file": "files",
        "image": "images",
        "digital_asset": "digital_assets",
        "replica": "asset_replicas",
        "asset_replica": "asset_replicas",
    }

    @classmethod
    def relation_names(cls) -> tuple[str, ...]:
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation: str) -> str:
        normalized = str(relation).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(f"Unknown manifestation-metadata relation {relation!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
        return normalized

    @property
    @abc.abstractmethod
    def manifestation(self) -> Optional[ManifestationIdentityAPI]:
        raise NotImplementedError

    @manifestation.setter
    @abc.abstractmethod
    def manifestation(self, value: Optional[ManifestationIdentityAPI]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[ManifestationRelationLink]:
        raise NotImplementedError

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[ManifestationRelationLink]) -> None:
        raise NotImplementedError

    def add_relation_link(self, relation: str, link: ManifestationRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, links)

    def remove_relation_link(self, relation: str, link: ManifestationRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation: str) -> list[ManifestationRelationTarget]:
        relation_key = self.validate_relation_name(relation)
        return [link.target for link in self.get_relation_links(relation_key)]

    def set_related(self, relation: str, values: Iterable[ManifestationRelationTarget]) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(
            relation_key,
            [ManifestationRelationLink(target=value) for value in values],
        )

    def add_related(self, relation: str, value: ManifestationRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(relation_key, ManifestationRelationLink(target=value))

    def clear_related(self, relation: str) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(relation_key, [])

    @property
    def works(self) -> list[ManifestationRelationTarget]:
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ManifestationRelationTarget]:
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("expressions", values)

    @property
    def items(self) -> list[ManifestationRelationTarget]:
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("items", values)

    @property
    def agents(self) -> list[ManifestationRelationTarget]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ManifestationRelationTarget]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ManifestationRelationTarget]:
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ManifestationRelationTarget]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("genres", values)

    @property
    def labels(self) -> list[ManifestationRelationTarget]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ManifestationRelationTarget]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ManifestationRelationTarget]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ManifestationRelationTarget]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("comments", values)

    @property
    def files(self) -> list[ManifestationRelationTarget]:
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("files", values)

    @property
    def images(self) -> list[ManifestationRelationTarget]:
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("images", values)

    @property
    def digital_assets(self) -> list[ManifestationRelationTarget]:
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("digital_assets", values)

    @property
    def asset_replicas(self) -> list[ManifestationRelationTarget]:
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("asset_replicas", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        raise NotImplementedError

__all__ = [
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "ManifestationMetadataAPI",
]
