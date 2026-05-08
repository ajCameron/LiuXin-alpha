"""Core WEMI metadata-bundle API contract for manifestation entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a
manifestation. It is not the manifestation identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias


from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_edge_api import (
    RelationCardinality,
    RelationEdge,
    RelationEdgeID,
    validate_relation_edge_cardinality,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ManifestationRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ExpressionIdentityAPI
    | ItemIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ManifestationRelationEdge(RelationEdge[ManifestationRelationTarget]):
    """Edge from a manifestation-metadata container to a related entity."""

    target: ManifestationRelationTarget


ManifestationRelationLink: TypeAlias = ManifestationRelationEdge


class ManifestationMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with one manifestation.

    Implementations should expose the core manifestation row, WEMI context,
    attached assets/files, and relation-edge metadata.
    """

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
    RELATION_CARDINALITIES: ClassVar[Mapping[str, RelationCardinality]] = {
        "items": RelationCardinality.ONE_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
        "files": RelationCardinality.ONE_TO_MANY,
        "images": RelationCardinality.ONE_TO_MANY,
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

    @classmethod
    def relation_cardinality(cls, relation: str) -> RelationCardinality:
        relation_key = cls.validate_relation_name(relation)
        return cls.RELATION_CARDINALITIES.get(
            relation_key,
            RelationCardinality.MANY_TO_MANY,
        )

    @classmethod
    def validate_relation_links(
        cls,
        relation: str,
        links: Iterable[ManifestationRelationLink],
    ) -> list[ManifestationRelationLink]:
        relation_key = cls.validate_relation_name(relation)
        return validate_relation_edge_cardinality(
            relation_key,
            links,
            cls.relation_cardinality(relation_key),
        )

    @property
    @abc.abstractmethod
    def manifestation(self) -> Optional[ManifestationIdentityAPI]:
        """Primary manifestation row for this metadata bundle."""

    @manifestation.setter
    @abc.abstractmethod
    def manifestation(self, value: Optional[ManifestationIdentityAPI]) -> None:
        """Set primary manifestation row."""

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[ManifestationRelationLink]:
        """Get edge metadata links for one relation type."""

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[ManifestationRelationLink]) -> None:
        """Replace edge metadata links for one relation type."""

    @abc.abstractmethod
    def write_to_database(
        self,
        database: MetadataWriteDatabaseAPI,
        *,
        fields: Iterable[str] | None = None,
        item_id: int | None = None,
        target_row: MetadataWriteTargetRow | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> MetadataWriteReportAPI:
        """Persist supported relation changes for this manifestation metadata bundle."""

    def add_relation_link(self, relation: str, link: ManifestationRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

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
            [
                ManifestationRelationEdge(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation: str, value: ManifestationRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(
            relation_key,
            ManifestationRelationEdge(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_edges(self, relation: str) -> list[ManifestationRelationEdge]:
        return self.get_relation_links(relation)

    def set_relation_edges(
        self,
        relation: str,
        edges: Iterable[ManifestationRelationEdge],
    ) -> None:
        self.set_relation_links(relation, edges)

    def add_relation_edge(self, relation: str, edge: ManifestationRelationEdge) -> None:
        self.add_relation_link(relation, edge)

    def remove_relation_edge(self, relation: str, edge: ManifestationRelationEdge) -> bool:
        return self.remove_relation_link(relation, edge)

    def get_relation_edge_by_id(
        self,
        relation: str,
        edge_id: RelationEdgeID,
    ) -> Optional[ManifestationRelationEdge]:
        for edge in self.get_relation_edges(relation):
            if edge.edge_id == edge_id:
                return edge
        return None

    def upsert_relation_edge(
        self,
        relation: str,
        edge: ManifestationRelationEdge,
    ) -> None:
        relation_key = self.validate_relation_name(relation)
        if edge.edge_id is None:
            self.add_relation_edge(relation_key, edge)
            return

        edges = list(self.get_relation_edges(relation_key))
        for index, existing_edge in enumerate(edges):
            if existing_edge.edge_id == edge.edge_id:
                edges[index] = edge
                self.set_relation_edges(relation_key, edges)
                return
        self.add_relation_edge(relation_key, edge)

    def remove_relation_edge_by_id(
        self,
        relation: str,
        edge_id: RelationEdgeID,
    ) -> bool:
        relation_key = self.validate_relation_name(relation)
        edges = list(self.get_relation_edges(relation_key))
        for index, edge in enumerate(edges):
            if edge.edge_id == edge_id:
                del edges[index]
                self.set_relation_edges(relation_key, edges)
                return True
        return False

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
        """
        Serialize this manifestation metadata bundle to a metadata record.

        :param include_related:
        :return:
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Build a manifestation metadata bundle from a metadata record.

        :param payload:
        :return:
        """

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

__all__ = [
    "ManifestationRelationEdge",
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "ManifestationMetadataAPI",
]
