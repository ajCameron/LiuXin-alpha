"""Core WEMI metadata-bundle API contract for work entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a work.
It is not the work identity object and it is not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

WorkRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | ItemIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class WorkRelationEdge(RelationEdge[WorkRelationTarget]):
    """
    Edge from a work-metadata container to a related entity.

    This mirrors common interlink edge metadata used in the database while
    remaining backend-agnostic for in-memory metadata workflows.
    """

    target: WorkRelationTarget


WorkRelationLink: TypeAlias = WorkRelationEdge


class WorkMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with a work.

    Implementations should expose:
    - the core `work` row container
    - relation collections for associated entities
    - edge metadata for those relations
    """

    RELATION_KEYS: ClassVar[tuple[str, ...]] = (
        "agents",
        "expressions",
        "manifestations",
        "items",
        "files",
        "titles",
        "genres",
        "subjects",
        "series",
        "tags",
        "labels",
        "languages",
        "images",
        "identifiers",
        "ratings",
        "notes",
        "comments",
        "synopses",
        "folders",
    )

    RELATION_ALIASES: ClassVar[Mapping[str, str]] = {
        "agent": "agents",
        "creator": "agents",
        "creators": "agents",
        "organization": "agents",
        "organisation": "agents",
        "org": "agents",
        "orgs": "agents",
        "publisher": "agents",
        "publishers": "agents",
        "expression": "expressions",
        "manifestation": "manifestations",
        "item": "items",
        "file": "files",
        "title": "titles",
        "genre": "genres",
        "subject": "subjects",
        "tag": "tags",
        "label": "labels",
        "language": "languages",
        "image": "images",
        "cover": "images",
        "covers": "images",
        "identifier": "identifiers",
        "rating": "ratings",
        "note": "notes",
        "comment": "comments",
        "synopsis": "synopses",
        "folder": "folders",
    }
    RELATION_CARDINALITIES: ClassVar[Mapping[str, RelationCardinality]] = {
        "expressions": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "ratings": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
        "synopses": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[str, ...]:
        """
        Things this work can relate to.

        :return:
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation: str) -> str:
        normalized = str(relation).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(
                "Unknown work-metadata relation {!r}. Expected one of {}.".format(
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
        links: Iterable[WorkRelationLink],
    ) -> list[WorkRelationLink]:
        relation_key = cls.validate_relation_name(relation)
        return validate_relation_edge_cardinality(
            relation_key,
            links,
            cls.relation_cardinality(relation_key),
        )

    @property
    @abc.abstractmethod
    def work(self) -> Optional[WorkIdentityAPI]:
        """Primary work row for this metadata bundle."""

    @work.setter
    @abc.abstractmethod
    def work(self, value: Optional[WorkIdentityAPI]) -> None:
        """Set primary work row."""

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[WorkRelationLink]:
        """Get edge metadata links for one relation type."""

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[WorkRelationLink]) -> None:
        """Replace edge metadata links for one relation type."""

    def add_relation_link(self, relation: str, link: WorkRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation: str, link: WorkRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation: str) -> list[WorkRelationTarget]:
        relation_key = self.validate_relation_name(relation)
        links = self.get_relation_links(relation_key)
        return [link.target for link in links]

    def set_related(self, relation: str, values: Iterable[WorkRelationTarget]) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(
            relation_key,
            [
                WorkRelationEdge(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation: str, value: WorkRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(
            relation_key,
            WorkRelationEdge(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_edges(self, relation: str) -> list[WorkRelationEdge]:
        return self.get_relation_links(relation)

    def set_relation_edges(self, relation: str, edges: Iterable[WorkRelationEdge]) -> None:
        self.set_relation_links(relation, edges)

    def add_relation_edge(self, relation: str, edge: WorkRelationEdge) -> None:
        self.add_relation_link(relation, edge)

    def remove_relation_edge(self, relation: str, edge: WorkRelationEdge) -> bool:
        return self.remove_relation_link(relation, edge)

    def get_relation_edge_by_id(
        self,
        relation: str,
        edge_id: RelationEdgeID,
    ) -> Optional[WorkRelationEdge]:
        for edge in self.get_relation_edges(relation):
            if edge.edge_id == edge_id:
                return edge
        return None

    def upsert_relation_edge(self, relation: str, edge: WorkRelationEdge) -> None:
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
    def agents(self) -> list[WorkRelationTarget]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("agents", values)

    @property
    def expressions(self) -> list[WorkRelationTarget]:
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[WorkRelationTarget]:
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[WorkRelationTarget]:
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("items", values)

    @property
    def files(self) -> list[WorkRelationTarget]:
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("files", values)

    @property
    def titles(self) -> list[WorkRelationTarget]:
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("titles", values)

    @property
    def genres(self) -> list[WorkRelationTarget]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[WorkRelationTarget]:
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("subjects", values)

    @property
    def series(self) -> list[WorkRelationTarget]:
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("series", values)

    @property
    def tags(self) -> list[WorkRelationTarget]:
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("tags", values)

    @property
    def labels(self) -> list[WorkRelationTarget]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[WorkRelationTarget]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("languages", values)

    @property
    def images(self) -> list[WorkRelationTarget]:
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[WorkRelationTarget]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("identifiers", values)

    @property
    def ratings(self) -> list[WorkRelationTarget]:
        return self.get_related("ratings")

    @ratings.setter
    def ratings(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("ratings", values)

    @property
    def notes(self) -> list[WorkRelationTarget]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[WorkRelationTarget]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("comments", values)

    @property
    def synopses(self) -> list[WorkRelationTarget]:
        return self.get_related("synopses")

    @synopses.setter
    def synopses(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("synopses", values)

    @property
    def folders(self) -> list[WorkRelationTarget]:
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[WorkRelationTarget]) -> None:
        self.set_related("folders", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """Serialize container into a mapping representation."""

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """Hydrate container from mapping representation."""

__all__ = [
    "WorkMetadataAPI",
    "WorkRelationEdge",
    "WorkRelationLink",
    "WorkRelationTarget",
]
