"""Core WEMI metadata-bundle API contract for item entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around an
item. It is not the item identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias


from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ItemRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ItemRelationEdge(RelationEdge[ItemRelationTarget]):
    """
    Relation edge used by item metadata containers.

    This intentionally mirrors the standard generated interlink metadata used by
    the FRBR schema: ``priority``, ``primary``, ``type``, ``origin``,
    ``policy``, ``data``, and ``index``.
    """

    target: ItemRelationTarget


ItemRelationLink: TypeAlias = ItemRelationEdge


class ItemMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with one item.

    Implementations should expose:
    - the core ``item`` row container
    - bibliographic context above the item (WEMI + agents + topical metadata)
    - storage context below/alongside the item (digital assets, replicas,
      stores, folders)
    """

    RELATION_KEYS: ClassVar[tuple[str, ...]] = (
        "works",
        "expressions",
        "manifestations",
        "agents",
        "digital_assets",
        "composite_digital_assets",
        "asset_replicas",
        "stores",
        "folders",
        "files",
        "images",
        "identifiers",
        "annotations",
        "genres",
        "subjects",
        "series",
        "tags",
        "labels",
        "languages",
        "notes",
        "comments",
    )

    RELATION_ALIASES: ClassVar[Mapping[str, str]] = {
        "work": "works",
        "expression": "expressions",
        "manifestation": "manifestations",
        "agent": "agents",
        "creator": "agents",
        "creators": "agents",
        "publisher": "agents",
        "publishers": "agents",
        "organization": "agents",
        "organisation": "agents",
        "org": "agents",
        "orgs": "agents",
        "digital_asset": "digital_assets",
        "asset": "digital_assets",
        "assets": "digital_assets",
        "composite_asset": "composite_digital_assets",
        "composite_assets": "composite_digital_assets",
        "composite_digital_asset": "composite_digital_assets",
        "asset_replica": "asset_replicas",
        "replica": "asset_replicas",
        "replicas": "asset_replicas",
        "store": "stores",
        "folder": "folders",
        "file": "files",
        "image": "images",
        "cover": "images",
        "covers": "images",
        "identifier": "identifiers",
        "annotation": "annotations",
        "genre": "genres",
        "subject": "subjects",
        "tag": "tags",
        "label": "labels",
        "language": "languages",
        "note": "notes",
        "comment": "comments",
    }
    RELATION_CARDINALITIES: ClassVar[Mapping[str, RelationCardinality]] = {
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "annotations": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[str, ...]:
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation: str) -> str:
        normalized = str(relation).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(
                "Unknown item-metadata relation {!r}. Expected one of {}.".format(
                    relation,
                    ", ".join(cls.RELATION_KEYS),
                )
            )
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
        links: Iterable[ItemRelationLink],
    ) -> list[ItemRelationLink]:
        relation_key = cls.validate_relation_name(relation)
        return validate_relation_edge_cardinality(
            relation_key,
            links,
            cls.relation_cardinality(relation_key),
        )

    @property
    @abc.abstractmethod
    def item(self) -> Optional[ItemIdentityAPI]:
        """Primary item row for this metadata bundle."""

    @item.setter
    @abc.abstractmethod
    def item(self, value: Optional[ItemIdentityAPI]) -> None:
        """Set primary item row."""

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[ItemRelationLink]:
        """Get edge metadata links for one relation type."""

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[ItemRelationLink]) -> None:
        """Replace edge metadata links for one relation type."""

    def add_relation_link(self, relation: str, link: ItemRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation: str, link: ItemRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation: str) -> list[ItemRelationTarget]:
        relation_key = self.validate_relation_name(relation)
        links = self.get_relation_links(relation_key)
        return [link.target for link in links]

    def set_related(self, relation: str, values: Iterable[ItemRelationTarget]) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(
            relation_key,
            [
                ItemRelationEdge(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation: str, value: ItemRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(
            relation_key,
            ItemRelationEdge(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_edges(self, relation: str) -> list[ItemRelationEdge]:
        return self.get_relation_links(relation)

    def set_relation_edges(self, relation: str, edges: Iterable[ItemRelationEdge]) -> None:
        self.set_relation_links(relation, edges)

    def add_relation_edge(self, relation: str, edge: ItemRelationEdge) -> None:
        self.add_relation_link(relation, edge)

    def remove_relation_edge(self, relation: str, edge: ItemRelationEdge) -> bool:
        return self.remove_relation_link(relation, edge)

    def get_relation_edge_by_id(
        self,
        relation: str,
        edge_id: RelationEdgeID,
    ) -> Optional[ItemRelationEdge]:
        for edge in self.get_relation_edges(relation):
            if edge.edge_id == edge_id:
                return edge
        return None

    def upsert_relation_edge(self, relation: str, edge: ItemRelationEdge) -> None:
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
    def works(self) -> list[ItemRelationTarget]:
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ItemRelationTarget]:
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[ItemRelationTarget]:
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("manifestations", values)

    @property
    def agents(self) -> list[ItemRelationTarget]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("agents", values)

    @property
    def digital_assets(self) -> list[ItemRelationTarget]:
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("digital_assets", values)

    @property
    def composite_digital_assets(self) -> list[ItemRelationTarget]:
        return self.get_related("composite_digital_assets")

    @composite_digital_assets.setter
    def composite_digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("composite_digital_assets", values)

    @property
    def asset_replicas(self) -> list[ItemRelationTarget]:
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("asset_replicas", values)

    @property
    def stores(self) -> list[ItemRelationTarget]:
        return self.get_related("stores")

    @stores.setter
    def stores(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("stores", values)

    @property
    def folders(self) -> list[ItemRelationTarget]:
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("folders", values)

    @property
    def files(self) -> list[ItemRelationTarget]:
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("files", values)

    @property
    def images(self) -> list[ItemRelationTarget]:
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[ItemRelationTarget]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("identifiers", values)

    @property
    def annotations(self) -> list[ItemRelationTarget]:
        return self.get_related("annotations")

    @annotations.setter
    def annotations(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("annotations", values)

    @property
    def genres(self) -> list[ItemRelationTarget]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[ItemRelationTarget]:
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("subjects", values)

    @property
    def series(self) -> list[ItemRelationTarget]:
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("series", values)

    @property
    def tags(self) -> list[ItemRelationTarget]:
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("tags", values)

    @property
    def labels(self) -> list[ItemRelationTarget]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ItemRelationTarget]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ItemRelationTarget]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ItemRelationTarget]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ItemRelationTarget]) -> None:
        self.set_related("comments", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """Serialize container into a mapping representation."""

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """Hydrate container from mapping representation."""

__all__ = [
    "ItemMetadataAPI",
    "ItemRelationEdge",
    "ItemRelationLink",
    "ItemRelationTarget",
]
