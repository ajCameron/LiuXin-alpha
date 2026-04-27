"""Core WEMI metadata-bundle API contract for manifestation entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a
manifestation. It is not the manifestation identity object and not a read-side view.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self


from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
@dataclasses.dataclass(slots=True)
class ManifestationRelationLink:
    target: Any
    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int | str] = None
    extra: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestationStorageHints:
    manifestation_id: Optional[int] = None
    expression_id: Optional[int] = None
    title: Optional[str] = None
    edition_statement: Optional[str] = None
    format_detail: Optional[str] = None
    carrier_type: Optional[str] = None
    publication_year: Optional[int] = None
    primary_agents: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    file_formats: tuple[str, ...] = ()
    extra: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "manifestation_id": self.manifestation_id,
            "expression_id": self.expression_id,
            "title": self.title,
            "edition_statement": self.edition_statement,
            "format_detail": self.format_detail,
            "carrier_type": self.carrier_type,
            "publication_year": self.publication_year,
            "primary_agents": self.primary_agents,
            "identifiers": self.identifiers,
            "file_formats": self.file_formats,
            "extra": dict(self.extra),
        }


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
    def validate_relation_name(cls, relation: str) -> str:
        normalized = cls.RELATION_ALIASES.get(str(relation).strip().lower(), str(relation).strip().lower())
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

    def get_related(self, relation: str) -> list[Any]:
        return [link.target for link in self.get_relation_links(relation)]

    def set_related(self, relation: str, values: Iterable[Any]) -> None:
        self.set_relation_links(relation, [ManifestationRelationLink(target=value) for value in values])

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> Self:
        raise NotImplementedError

    @abc.abstractmethod
    def storage_hints(self) -> ManifestationStorageHints:
        raise NotImplementedError


__all__ = [
    "ManifestationRelationLink",
    "ManifestationStorageHints",
    "ManifestationMetadataAPI",
]
