"""
API contract for expression identity and metadata containers.
"""

from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self


class ExpressionIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """Row-level API for one expression."""

    @property
    def id(self) -> Optional[int]:
        return self.expression_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.expression_id = value

    @property
    @abc.abstractmethod
    def expression_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_id.setter
    @abc.abstractmethod
    def expression_id(self, expression_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def work_id(self) -> Optional[int]:
        return self.expression_work_id

    @work_id.setter
    def work_id(self, value: Optional[int]) -> None:
        self.expression_work_id = value

    @property
    @abc.abstractmethod
    def expression_work_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_work_id.setter
    @abc.abstractmethod
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_type(self) -> Optional[str]:
        raise NotImplementedError

    @expression_type.setter
    @abc.abstractmethod
    def expression_type(self, expression_type: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_language_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_language_id.setter
    @abc.abstractmethod
    def expression_language_id(self, expression_language_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_label(self) -> Optional[str]:
        raise NotImplementedError

    @expression_label.setter
    @abc.abstractmethod
    def expression_label(self, expression_label: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_title_override(self) -> Optional[str]:
        raise NotImplementedError

    @expression_title_override.setter
    @abc.abstractmethod
    def expression_title_override(self, expression_title_override: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_subtitle(self) -> Optional[str]:
        raise NotImplementedError

    @expression_subtitle.setter
    @abc.abstractmethod
    def expression_subtitle(self, expression_subtitle: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_flags(self) -> Optional[str]:
        raise NotImplementedError

    @expression_flags.setter
    @abc.abstractmethod
    def expression_flags(self, expression_flags: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_status(self) -> Optional[str]:
        raise NotImplementedError

    @expression_status.setter
    @abc.abstractmethod
    def expression_status(self, expression_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def to_mapping(self) -> Any:
        raise NotImplementedError


class ExpressionIdentityAPI(ExpressionIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete expression identity container."""


@dataclasses.dataclass(slots=True)
class ExpressionRelationLink:
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
    extra: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> dict[str, Any]:
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

    def get_related(self, relation: str) -> list[Any]:
        return [link.target for link in self.get_relation_links(relation)]

    def set_related(self, relation: str, values: Iterable[Any]) -> None:
        self.set_relation_links(relation, [ExpressionRelationLink(target=value) for value in values])

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> Self:
        raise NotImplementedError

    @abc.abstractmethod
    def storage_hints(self) -> ExpressionStorageHints:
        raise NotImplementedError


__all__ = [
    "ExpressionIdentityPropertiesAPI",
    "ExpressionIdentityAPI",
    "ExpressionRelationLink",
    "ExpressionStorageHints",
    "ExpressionMetadataAPI",
]
