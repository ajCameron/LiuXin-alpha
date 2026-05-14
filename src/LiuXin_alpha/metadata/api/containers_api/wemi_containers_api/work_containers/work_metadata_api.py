"""Core WEMI metadata-bundle API contract for work entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a work.
It is not the work identity object and it is not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Literal, Mapping, Optional, Self, TypeAlias, cast

from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
    relation_target_id,
    RelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    RelationCardinality,
    RelationLink,
    RelationLinkID,
    select_primary_relation_link,
    validate_relation_link_cardinality,
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
class WorkRelationLink(RelationLink[WorkRelationTarget]):
    """
    Link from a work-metadata container to a related entity.

    This mirrors common interlink metadata used in the database while
    remaining backend-agnostic for in-memory metadata workflows.
    """

    target: WorkRelationTarget


WorkRelationKey: TypeAlias = Literal[
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
]


class WorkMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with a work.

    Implementations should expose:
    - the core `work` row container
    - relation-keyed collections for associated entities
    - link metadata for those relations

    The ``relation_key`` parameter names one normalized relation bucket from
    ``RELATION_KEYS``. These keys usually mirror related metadata table or
    bucket names, such as ``tags`` or ``agents``, but they are API contract keys
    rather than a guarantee about a physical database table.
    """

    RELATION_KEYS: ClassVar[tuple[WorkRelationKey, ...]] = (
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

    RELATION_ALIASES: ClassVar[Mapping[str, WorkRelationKey]] = {
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
    RELATION_CARDINALITIES: ClassVar[Mapping[WorkRelationKey, RelationCardinality]] = {
        "expressions": RelationCardinality.MANY_TO_MANY,
        "manifestations": RelationCardinality.MANY_TO_MANY,
        "items": RelationCardinality.MANY_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "ratings": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
        "synopses": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[WorkRelationKey, ...]:
        """
        Things this work can relate to.

        :return:
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> WorkRelationKey:
        """Normalize and validate one relation key for this work bundle."""
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(
                "Unknown work-metadata relation key {!r}. Expected one of {}.".format(
                    relation_key,
                    ", ".join(cls.RELATION_KEYS),
                )
            )
        return cast(WorkRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: WorkRelationKey) -> RelationCardinality:
        """Return the cardinality policy for one relation key."""
        relation_key = cls.validate_relation_name(relation_key)
        return cls.RELATION_CARDINALITIES.get(
            relation_key,
            RelationCardinality.MANY_TO_MANY,
        )

    @classmethod
    def validate_relation_links(
        cls,
        relation_key: WorkRelationKey,
        links: Iterable[WorkRelationLink],
    ) -> list[WorkRelationLink]:
        """Validate relation links for one relation key."""
        relation_key = cls.validate_relation_name(relation_key)
        return validate_relation_link_cardinality(
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
    def get_relation_links(self, relation_key: WorkRelationKey) -> list[WorkRelationLink]:
        """Get relation links for one relation key."""

    @abc.abstractmethod
    def set_relation_links(self, relation_key: WorkRelationKey, links: Iterable[WorkRelationLink]) -> None:
        """Replace relation links for one relation key."""

    @abc.abstractmethod
    def write_to_database(
        self,
        database: MetadataWriteDatabaseAPI,
        *,
        fields: Iterable[str] | None = None,
        item_id: int | None = None,
        target_row: MetadataWriteTargetRow | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> MetadataWriteReportAPI:
        """Persist supported relation-backed changes for this work metadata bundle."""

    def add_relation_link(self, relation_key: WorkRelationKey, link: WorkRelationLink) -> None:
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation_key: WorkRelationKey, link: WorkRelationLink) -> bool:
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation_key: WorkRelationKey) -> list[WorkRelationTarget]:
        relation_key = self.validate_relation_name(relation_key)
        links = self.get_relation_links(relation_key)
        return [link.target for link in links]

    def primary_relation_link(self, relation_key: WorkRelationKey) -> Optional[WorkRelationLink]:
        """Return the preferred relation link for one relation key, if any."""

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: WorkRelationKey) -> WorkRelationTarget | None:
        """Return the preferred relation target for one relation key, if any."""

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(self, relation_key: WorkRelationKey, link: WorkRelationLink) -> None:
        """Mark one relation link as the preferred link for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        selected_index: int | None = None
        for index, existing_link in enumerate(links):
            same_link_id = link.link_id is not None and existing_link.link_id == link.link_id
            same_target = link.link_id is None and existing_link.target == link.target
            if existing_link is link or same_link_id or same_target:
                selected_index = index
                links[index] = link
                break

        if selected_index is None:
            selected_index = len(links)
            links.append(link)

        for index, existing_link in enumerate(links):
            existing_link.primary = index == selected_index
        self.set_relation_links(relation_key, links)

    def set_related(self, relation_key: WorkRelationKey, values: Iterable[WorkRelationTarget]) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(
            relation_key,
            [
                WorkRelationLink(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation_key: WorkRelationKey, value: WorkRelationTarget) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self.add_relation_link(
            relation_key,
            WorkRelationLink(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_link_by_id(
        self,
        relation_key: WorkRelationKey,
        link_id: RelationLinkID,
    ) -> Optional[WorkRelationLink]:
        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    def upsert_relation_link(self, relation_key: WorkRelationKey, link: WorkRelationLink) -> None:
        relation_key = self.validate_relation_name(relation_key)
        if link.link_id is None:
            self.add_relation_link(relation_key, link)
            return

        links = list(self.get_relation_links(relation_key))
        for index, existing_link in enumerate(links):
            if existing_link.link_id == link.link_id:
                links[index] = link
                self.set_relation_links(relation_key, links)
                return
        self.add_relation_link(relation_key, link)

    def remove_relation_link_by_id(
        self,
        relation_key: WorkRelationKey,
        link_id: RelationLinkID,
    ) -> bool:
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        for index, link in enumerate(links):
            if link.link_id == link_id:
                del links[index]
                self.set_relation_links(relation_key, links)
                return True
        return False

    def clear_related(self, relation_key: WorkRelationKey) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])

    @property
    def primary_expression(self) -> WorkRelationTarget | None:
        """Preferred expression traversal from this work."""

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        return relation_target_id(self.primary_expression, "expression_id")

    @property
    def primary_manifestation(self) -> WorkRelationTarget | None:
        """Preferred manifestation traversal from this work."""

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        return relation_target_id(self.primary_manifestation, "manifestation_id")

    @property
    def primary_item(self) -> WorkRelationTarget | None:
        """Preferred item traversal from this work."""

        return self.primary_related("items")

    @property
    def primary_item_id(self) -> Optional[int]:
        return relation_target_id(self.primary_item, "item_id")

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

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

__all__ = [
    "WorkMetadataAPI",
    "WorkRelationKey",
    "WorkRelationLink",
    "WorkRelationTarget",
]
