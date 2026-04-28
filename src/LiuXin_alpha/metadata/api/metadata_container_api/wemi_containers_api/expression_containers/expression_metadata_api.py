"""Core WEMI metadata-bundle API contract for expression entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around an
expression. It is not the expression identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias


from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ExpressionRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ItemIdentityAPI
    | ManifestationIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ExpressionRelationLink:
    target: ExpressionRelationTarget
    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int | str] = None
    extra: MutableMetadataRecord = dataclasses.field(default_factory=dict)


class ExpressionMetadataAPI(abc.ABC):
    """Rich metadata bundle centred on one expression."""

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

    @property
    @abc.abstractmethod
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        raise NotImplementedError

    @expression.setter
    @abc.abstractmethod
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_relation_links(self, relation: str) -> list[ExpressionRelationLink]:
        raise NotImplementedError

    @abc.abstractmethod
    def set_relation_links(self, relation: str, links: Iterable[ExpressionRelationLink]) -> None:
        raise NotImplementedError

    def add_relation_link(self, relation: str, link: ExpressionRelationLink) -> None:
        relation_key = self.validate_relation_name(relation)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, links)

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
            [ExpressionRelationLink(target=value) for value in values],
        )

    def add_related(self, relation: str, value: ExpressionRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(relation_key, ExpressionRelationLink(target=value))

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
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        raise NotImplementedError

__all__ = [
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ExpressionMetadataAPI",
]
