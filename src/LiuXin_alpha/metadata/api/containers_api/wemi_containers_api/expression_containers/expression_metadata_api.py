"""Core WEMI metadata-bundle API contract for expression entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around an
expression. It is not the expression identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias


from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ExpressionRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ItemIdentityAPI
    | ManifestationIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ExpressionRelationEdge(RelationEdge[ExpressionRelationTarget]):
    """Edge from an expression-metadata container to a related entity."""

    target: ExpressionRelationTarget


ExpressionRelationLink: TypeAlias = ExpressionRelationEdge


class ExpressionMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with one expression.

    Implementations should expose the core expression row, parent work context,
    child manifestation/item context, and relation-edge metadata.
    """

    RELATION_KEYS: ClassVar[tuple[str, ...]] = (
        "works",
        "manifestations",
        "items",
        "agents",
        "identifiers",
        "titles",
        "genres",
        "labels",
        "languages",
        "notes",
        "comments",
    )

    RELATION_ALIASES: ClassVar[Mapping[str, str]] = {
        "work": "works",
        "manifestation": "manifestations",
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
    }
    RELATION_CARDINALITIES: ClassVar[Mapping[str, RelationCardinality]] = {
        "works": RelationCardinality.MANY_TO_ONE,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
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
            raise KeyError(f"Unknown expression-metadata relation {relation!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
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
        links: Iterable[ExpressionRelationLink],
    ) -> list[ExpressionRelationLink]:
        relation_key = cls.validate_relation_name(relation)
        return validate_relation_edge_cardinality(
            relation_key,
            links,
            cls.relation_cardinality(relation_key),
        )

    @property
    @abc.abstractmethod
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        """Primary expression row for this metadata bundle."""

    @expression.setter
    @abc.abstractmethod
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        """Set primary expression row."""

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[ExpressionRelationLink]:
        """Get edge metadata links for one relation type."""

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[ExpressionRelationLink]) -> None:
        """Replace edge metadata links for one relation type."""

    def add_relation_link(self, relation: str, link: ExpressionRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation: str, link: ExpressionRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation: str) -> list[ExpressionRelationTarget]:
        relation_key = self.validate_relation_name(relation)
        return [link.target for link in self.get_relation_links(relation_key)]

    def set_related(self, relation: str, values: Iterable[ExpressionRelationTarget]) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(
            relation_key,
            [
                ExpressionRelationEdge(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation: str, value: ExpressionRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(
            relation_key,
            ExpressionRelationEdge(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_edges(self, relation: str) -> list[ExpressionRelationEdge]:
        return self.get_relation_links(relation)

    def set_relation_edges(
        self,
        relation: str,
        edges: Iterable[ExpressionRelationEdge],
    ) -> None:
        self.set_relation_links(relation, edges)

    def add_relation_edge(self, relation: str, edge: ExpressionRelationEdge) -> None:
        self.add_relation_link(relation, edge)

    def remove_relation_edge(self, relation: str, edge: ExpressionRelationEdge) -> bool:
        return self.remove_relation_link(relation, edge)

    def get_relation_edge_by_id(
        self,
        relation: str,
        edge_id: RelationEdgeID,
    ) -> Optional[ExpressionRelationEdge]:
        for edge in self.get_relation_edges(relation):
            if edge.edge_id == edge_id:
                return edge
        return None

    def upsert_relation_edge(
        self,
        relation: str,
        edge: ExpressionRelationEdge,
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
    def works(self) -> list[ExpressionRelationTarget]:
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("works", values)

    @property
    def manifestations(self) -> list[ExpressionRelationTarget]:
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[ExpressionRelationTarget]:
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("items", values)

    @property
    def agents(self) -> list[ExpressionRelationTarget]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ExpressionRelationTarget]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ExpressionRelationTarget]:
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ExpressionRelationTarget]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("genres", values)

    @property
    def labels(self) -> list[ExpressionRelationTarget]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ExpressionRelationTarget]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ExpressionRelationTarget]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ExpressionRelationTarget]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_related("comments", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Serialize this expression metadata bundle to a metadata record.

        :param include_related:
        :return:
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Build an expression metadata bundle from a metadata record.

        :param payload:
        :return:
        """

__all__ = [
    "ExpressionRelationEdge",
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ExpressionMetadataAPI",
]
