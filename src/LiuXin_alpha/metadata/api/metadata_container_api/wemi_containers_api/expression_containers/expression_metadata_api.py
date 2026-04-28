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


@dataclasses.dataclass(frozen=True, slots=True)
class ExpressionStorageHints:
    expression_id: Optional[int] = None
    work_id: Optional[int] = None
    title: Optional[str] = None
    label: Optional[str] = None
    expression_type: Optional[str] = None
    language_code: Optional[str] = None
    primary_agents: tuple[str, ...] = ()
    genres: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    extra: MetadataRecord = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> MutableMetadataRecord:
        return {
            "expression_id": self.expression_id,
            "work_id": self.work_id,
            "title": self.title,
            "label": self.label,
            "expression_type": self.expression_type,
            "language_code": self.language_code,
            "primary_agents": self.primary_agents,
            "genres": self.genres,
            "labels": self.labels,
            "identifiers": self.identifiers,
            "extra": dict(self.extra),
        }


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
    def validate_relation_name(cls, relation: str) -> str:
        normalized = cls.RELATION_ALIASES.get(str(relation).strip().lower(), str(relation).strip().lower())
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

    def get_related(self, relation: str) -> list[ExpressionRelationTarget]:
        return [link.target for link in self.get_relation_links(relation)]

    def set_related(self, relation: str, values: Iterable[ExpressionRelationTarget]) -> None:
        self.set_relation_links(relation, [ExpressionRelationLink(target=value) for value in values])

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        raise NotImplementedError

    @abc.abstractmethod
    def storage_hints(self) -> ExpressionStorageHints:
        raise NotImplementedError


__all__ = [
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ExpressionStorageHints",
    "ExpressionMetadataAPI",
]
