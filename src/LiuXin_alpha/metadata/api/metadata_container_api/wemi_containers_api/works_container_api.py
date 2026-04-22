"""
API contract for a rich "work metadata" container.

`WorkContainerAPI` models one row from the `works` table.
`WorkMetadataContainerAPI` models the full metadata graph around that work
(agents, WEMI descendants, genres, notes, identifiers, etc.).
"""

from __future__ import annotations

import abc
import dataclasses
from abc import abstractmethod

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self, Dict


class WorkContainerPropertiesApi(metaclass=abc.ABCMeta):
    """
    Provides a full interface to the properties of a work row.
    """
    # ------------------------------------------------------------------
    # Primary key
    # ------------------------------------------------------------------

    @property
    def id(self) -> Optional[int]:
        """
        id for the underlying works row.

        :return:
        """
        return self.work_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Set the id for the underlying works row.

        :param value:
        :return:
        """
        self.work_id = value

    @property
    @abstractmethod
    def work_id(self) -> Optional[int]:
        """
        Proxy for the work_id.

        :return:
        """
        raise NotImplementedError

    @work_id.setter
    @abstractmethod
    def work_id(self, work_id: Optional[int]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Core work identity
    # ------------------------------------------------------------------

    @property
    def type(self) -> Optional[str]:
        """
        Return the type of the underlying works row.

        :return:
        """
        return self.work_type

    @type.setter
    def type(self, value: Optional[str]) -> None:
        """
        Validate and set the type of the underlying works row.

        :param value:
        :return:
        """
        self.work_type = value

    @property
    @abstractmethod
    def work_type(self) -> Optional[str]:
        """
        Return the type of the work type.

        :return:
        """
        raise NotImplementedError

    @work_type.setter
    @abstractmethod
    def work_type(self, work_type: Optional[str]) -> None:
        """
        Validate and set the work type.

        :param work_type:
        :return:
        """
        raise NotImplementedError

    @property
    def medium(self) -> Optional[str]:
        """
        Set the medium of the underlying works row.

        :return:
        """
        return self.work_medium

    @medium.setter
    def medium(self, value: Optional[str]) -> None:
        self.work_medium = value

    @property
    @abstractmethod
    def work_medium(self) -> Optional[str]:
        """
        Porxy to set the medium type of the underlying work.

        :return:
        """
        raise NotImplementedError

    @work_medium.setter
    @abstractmethod
    def work_medium(self, work_medium: Optional[str]) -> None:
        raise NotImplementedError

    # - Title methods

    @property
    def title(self) -> Optional[str]:
        """
        The raw title of the work - alias of work_title.

        :return:
        """
        return self.work_title

    @title.setter
    def title(self, value: Optional[str]) -> None:

        self.work_title = value

    @property
    @abstractmethod
    def work_title(self) -> Optional[str]:
        raise NotImplementedError

    @work_title.setter
    @abstractmethod
    def work_title(self, work_title: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def name(self) -> Optional[str]:
        return self.work_name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        self.work_name = value

    @property
    @abstractmethod
    def work_name(self) -> Optional[str]:
        raise NotImplementedError

    @work_name.setter
    @abstractmethod
    def work_name(self, work_name: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def canonical_title(self) -> Optional[str]:
        return self.work_canonical_title

    @canonical_title.setter
    def canonical_title(self, value: Optional[str]) -> None:
        self.work_canonical_title = value

    @property
    @abstractmethod
    def work_canonical_title(self) -> Optional[str]:
        raise NotImplementedError

    @work_canonical_title.setter
    @abstractmethod
    def work_canonical_title(self, work_canonical_title: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def sort_title(self) -> Optional[str]:
        return self.work_sort_title

    @sort_title.setter
    def sort_title(self, value: Optional[str]) -> None:
        self.work_sort_title = value

    @property
    @abstractmethod
    def work_sort_title(self) -> Optional[str]:
        raise NotImplementedError

    @work_sort_title.setter
    @abstractmethod
    def work_sort_title(self, work_sort_title: Optional[str]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # High-level classification
    # ------------------------------------------------------------------

    @property
    def is_fiction(self) -> Optional[int]:
        return self.work_is_fiction

    @is_fiction.setter
    def is_fiction(self, value: Optional[int]) -> None:
        self.work_is_fiction = value

    @property
    @abstractmethod
    def work_is_fiction(self) -> Optional[int]:
        """Stored as SQLite-ish 1/0/NULL."""
        raise NotImplementedError

    @work_is_fiction.setter
    @abstractmethod
    def work_is_fiction(self, work_is_fiction: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def audience(self) -> Optional[str]:
        return self.work_audience

    @audience.setter
    def audience(self, value: Optional[str]) -> None:
        self.work_audience = value

    @property
    @abstractmethod
    def work_audience(self) -> Optional[str]:
        raise NotImplementedError

    @work_audience.setter
    @abstractmethod
    def work_audience(self, work_audience: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def completion_status(self) -> Optional[str]:
        return self.work_completion_status

    @completion_status.setter
    def completion_status(self, value: Optional[str]) -> None:
        self.work_completion_status = value

    @property
    @abstractmethod
    def work_completion_status(self) -> Optional[str]:
        raise NotImplementedError

    @work_completion_status.setter
    @abstractmethod
    def work_completion_status(self, work_completion_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def original_language_id(self) -> Optional[int]:
        return self.work_original_language_id

    @original_language_id.setter
    def original_language_id(self, value: Optional[int]) -> None:
        self.work_original_language_id = value

    @property
    @abstractmethod
    def work_original_language_id(self) -> Optional[int]:
        raise NotImplementedError

    @work_original_language_id.setter
    @abstractmethod
    def work_original_language_id(self, work_original_language_id: Optional[int]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Notes / provenance
    # ------------------------------------------------------------------

    @property
    def discovery_note(self) -> Optional[str]:
        return self.work_discovery_note

    @discovery_note.setter
    def discovery_note(self, value: Optional[str]) -> None:
        self.work_discovery_note = value

    @property
    @abstractmethod
    def work_discovery_note(self) -> Optional[str]:
        raise NotImplementedError

    @work_discovery_note.setter
    @abstractmethod
    def work_discovery_note(self, work_discovery_note: Optional[str]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------

    @property
    def created_timestamp_ep_k(self) -> Optional[int]:
        return self.work_created_timestamp_ep_k

    @created_timestamp_ep_k.setter
    def created_timestamp_ep_k(self, value: Optional[int]) -> None:
        self.work_created_timestamp_ep_k = value

    @property
    def modified_timestamp_ep_k(self) -> Optional[int]:
        return self.work_modified_timestamp_ep_k

    @modified_timestamp_ep_k.setter
    def modified_timestamp_ep_k(self, value: Optional[int]) -> None:
        self.work_modified_timestamp_ep_k = value

    @property
    @abstractmethod
    def work_created_timestamp_ep_k(self) -> Optional[int]:
        raise NotImplementedError

    @work_created_timestamp_ep_k.setter
    @abstractmethod
    def work_created_timestamp_ep_k(self, work_created_timestamp_ep_k: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        raise NotImplementedError

    @work_modified_timestamp_ep_k.setter
    @abstractmethod
    def work_modified_timestamp_ep_k(self, work_modified_timestamp_ep_k: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def original_year(self) -> Optional[int]:
        return self.work_original_year

    @original_year.setter
    def original_year(self, value: Optional[int]) -> None:
        self.work_original_year = value

    @property
    @abstractmethod
    def work_original_year(self) -> Optional[int]:
        raise NotImplementedError

    @work_original_year.setter
    @abstractmethod
    def work_original_year(self, work_original_year: Optional[int]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Scratch / misc
    # ------------------------------------------------------------------

    @property
    def scratch(self) -> Optional[str]:
        return self.work_scratch

    @scratch.setter
    def scratch(self, value: Optional[str]) -> None:
        self.work_scratch = value

    @property
    @abstractmethod
    def work_scratch(self) -> Optional[str]:
        raise NotImplementedError

    @work_scratch.setter
    @abstractmethod
    def work_scratch(self, work_scratch: Optional[str]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> Self:
        raise NotImplementedError

    @abstractmethod
    def to_mapping(self) -> Dict[str, Any]:
        raise NotImplementedError


class WorkContainerAPI(WorkContainerPropertiesApi, metaclass=abc.ABCMeta):
    """Typing interface for a Work container.

    This exists so container implementations can inherit a single ABC with the
    full property + mapping surface.
    """

    pass


# Todo: This feels like it could be a generic thing
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


class WorkMetadataContainerAPI(abc.ABC):
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
    def work(self) -> Optional[WorkContainerAPI]:
        """Primary work row for this metadata bundle."""

    @work.setter
    @abc.abstractmethod
    def work(self, value: Optional[WorkContainerAPI]) -> None:
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


__all__ = ["WorkMetadataContainerAPI", "WorkRelationLink", "WorkStorageHints"]
