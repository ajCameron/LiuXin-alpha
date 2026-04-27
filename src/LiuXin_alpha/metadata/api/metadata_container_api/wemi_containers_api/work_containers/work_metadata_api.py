"""Core WEMI metadata-bundle API contract for work entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a work.
It is not the work identity object and it is not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI
@dataclasses.dataclass(slots=True)
class WorkRelationLink:
    """
    Link from a work-metadata container to a related entity.

    This mirrors common interlink edge metadata used in the database
    (`priority`, `type`, `origin`, `policy`, `data`) while remaining backend-agnostic
    for in-memory metadata workflows.
    """

    target: Any
    priority: Optional[int] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    extra: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True, slots=True)
class WorkStorageHints:
    """
    Storage-facing projection of a work metadata container.

    Store plugins can use this as a typed source of truth for path placement,
    naming, and policy selection.
    """

    work_id: Optional[int] = None
    title: Optional[str] = None
    canonical_title: Optional[str] = None
    sort_title: Optional[str] = None
    work_type: Optional[str] = None
    medium: Optional[str] = None

    primary_agents: tuple[str, ...] = ()
    series: tuple[str, ...] = ()
    genres: tuple[str, ...] = ()
    subjects: tuple[str, ...] = ()
    languages: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    manifestation_types: tuple[str, ...] = ()
    file_formats: tuple[str, ...] = ()

    preferred_folder_tokens: tuple[str, ...] = ()
    preferred_filename_stem: Optional[str] = None

    extra: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def to_mapping(self) -> dict[str, Any]:
        """
        To a dictionary mapping.

        :return:
        """
        return {
            "work_id": self.work_id,
            "title": self.title,
            "canonical_title": self.canonical_title,
            "sort_title": self.sort_title,
            "work_type": self.work_type,
            "medium": self.medium,
            "primary_agents": self.primary_agents,
            "series": self.series,
            "genres": self.genres,
            "subjects": self.subjects,
            "languages": self.languages,
            "labels": self.labels,
            "manifestation_types": self.manifestation_types,
            "file_formats": self.file_formats,
            "preferred_folder_tokens": self.preferred_folder_tokens,
            "preferred_filename_stem": self.preferred_filename_stem,
            "extra": dict(self.extra),
        }


class WorkMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with a work.

    Implementations should expose:
    - the core `work` row container
    - relation collections for associated entities
    - edge metadata for those relations
    - a storage-facing metadata projection (`storage_hints`)
    """

    RELATION_KEYS: ClassVar[tuple[str, ...]] = (
        "agents",
        "expressions",
        "manifestations",
        "items",
        "files",
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
        self.get_relation_links(relation_key).append(link)

    def remove_relation_link(self, relation: str, link: WorkRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation)
        links = self.get_relation_links(relation_key)
        try:
            links.remove(link)
            return True
        except ValueError:
            return False

    def get_related(self, relation: str) -> list[Any]:
        relation_key = self.validate_relation_name(relation)
        links = self.get_relation_links(relation_key)
        return [link.target for link in links]

    def set_related(self, relation: str, values: Iterable[Any]) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(
            relation_key,
            [WorkRelationLink(target=value) for value in values],
        )

    def add_related(self, relation: str, value: Any) -> None:
        relation_key = self.validate_relation_name(relation)
        self.add_relation_link(relation_key, WorkRelationLink(target=value))

    def clear_related(self, relation: str) -> None:
        relation_key = self.validate_relation_name(relation)
        self.set_relation_links(relation_key, [])

    @property
    def agents(self) -> list[Any]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[Any]) -> None:
        self.set_related("agents", values)

    @property
    def expressions(self) -> list[Any]:
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[Any]) -> None:
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[Any]:
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[Any]) -> None:
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[Any]:
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[Any]) -> None:
        self.set_related("items", values)

    @property
    def files(self) -> list[Any]:
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[Any]) -> None:
        self.set_related("files", values)

    @property
    def genres(self) -> list[Any]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[Any]) -> None:
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[Any]:
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[Any]) -> None:
        self.set_related("subjects", values)

    @property
    def series(self) -> list[Any]:
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[Any]) -> None:
        self.set_related("series", values)

    @property
    def tags(self) -> list[Any]:
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[Any]) -> None:
        self.set_related("tags", values)

    @property
    def labels(self) -> list[Any]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[Any]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[Any]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[Any]) -> None:
        self.set_related("languages", values)

    @property
    def images(self) -> list[Any]:
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[Any]) -> None:
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[Any]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[Any]) -> None:
        self.set_related("identifiers", values)

    @property
    def ratings(self) -> list[Any]:
        return self.get_related("ratings")

    @ratings.setter
    def ratings(self, values: Iterable[Any]) -> None:
        self.set_related("ratings", values)

    @property
    def notes(self) -> list[Any]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[Any]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[Any]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[Any]) -> None:
        self.set_related("comments", values)

    @property
    def synopses(self) -> list[Any]:
        return self.get_related("synopses")

    @synopses.setter
    def synopses(self, values: Iterable[Any]) -> None:
        self.set_related("synopses", values)

    @property
    def folders(self) -> list[Any]:
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[Any]) -> None:
        self.set_related("folders", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        """Serialize container into a mapping representation."""

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> Self:
        """Hydrate container from mapping representation."""

    @abc.abstractmethod
    def storage_hints(self) -> WorkStorageHints:
        """Return a storage-oriented projection for store placement logic."""


__all__ = ["WorkMetadataAPI", "WorkRelationLink", "WorkStorageHints"]
