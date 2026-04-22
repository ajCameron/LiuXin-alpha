"""
API contract for manifestation identity and metadata containers.
"""

from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self


class ManifestationIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """Row-level API for one manifestation."""

    @property
    def id(self) -> Optional[int]:
        return self.manifestation_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.manifestation_id = value

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_id.setter
    @abc.abstractmethod
    def manifestation_id(self, manifestation_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def expression_id(self) -> Optional[int]:
        return self.manifestation_expression_id

    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        self.manifestation_expression_id = value

    @property
    @abc.abstractmethod
    def manifestation_expression_id(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_expression_id.setter
    @abc.abstractmethod
    def manifestation_expression_id(self, manifestation_expression_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_format_detail(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_format_detail.setter
    @abc.abstractmethod
    def manifestation_format_detail(self, manifestation_format_detail: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_carrier_type(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_carrier_type.setter
    @abc.abstractmethod
    def manifestation_carrier_type(self, manifestation_carrier_type: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_edition_statement(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_edition_statement.setter
    @abc.abstractmethod
    def manifestation_edition_statement(self, manifestation_edition_statement: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_pub_year(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_pub_year.setter
    @abc.abstractmethod
    def manifestation_pub_year(self, manifestation_pub_year: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_status(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_status.setter
    @abc.abstractmethod
    def manifestation_status(self, manifestation_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_flags(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_flags.setter
    @abc.abstractmethod
    def manifestation_flags(self, manifestation_flags: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def to_mapping(self) -> Any:
        raise NotImplementedError


class ManifestationIdentityAPI(ManifestationIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete manifestation identity container."""


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
    "ManifestationIdentityPropertiesAPI",
    "ManifestationIdentityAPI",
    "ManifestationRelationLink",
    "ManifestationStorageHints",
    "ManifestationMetadataAPI",
]
