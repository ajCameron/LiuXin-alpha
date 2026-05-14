"""Core WEMI metadata-bundle API contract for expression entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around an
expression. It is not the expression identity object and not a read-side view.
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
class ExpressionRelationLink(RelationLink[ExpressionRelationTarget]):
    """Link from an expression-metadata container to a related entity."""

    target: ExpressionRelationTarget


ExpressionRelationKey: TypeAlias = Literal[
    "works",
    "manifestations",
    "items",
    "agents",
    "identifiers",
    "titles",
    "genres",
    "tags",
    "labels",
    "languages",
    "notes",
    "comments",
]


class ExpressionMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with one expression.

    Implementations should expose the core expression row, work context,
    manifestation/item context, and relation-keyed link metadata.

    The ``relation_key`` parameter names one normalized relation bucket from
    ``RELATION_KEYS``. These keys usually mirror related metadata table or
    bucket names, such as ``tags`` or ``languages``, but they are API contract
    keys rather than a guarantee about a physical database table.
    """

    RELATION_KEYS: ClassVar[tuple[ExpressionRelationKey, ...]] = (
        "works",
        "manifestations",
        "items",
        "agents",
        "identifiers",
        "titles",
        "genres",
        "tags",
        "labels",
        "languages",
        "notes",
        "comments",
    )

    RELATION_ALIASES: ClassVar[Mapping[str, ExpressionRelationKey]] = {
        "work": "works",
        "manifestation": "manifestations",
        "item": "items",
        "agent": "agents",
        "creator": "agents",
        "identifier": "identifiers",
        "title": "titles",
        "genre": "genres",
        "tag": "tags",
        "label": "labels",
        "language": "languages",
        "note": "notes",
        "comment": "comments",
    }
    RELATION_CARDINALITIES: ClassVar[Mapping[ExpressionRelationKey, RelationCardinality]] = {
        "works": RelationCardinality.MANY_TO_MANY,
        "manifestations": RelationCardinality.MANY_TO_MANY,
        "items": RelationCardinality.MANY_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[ExpressionRelationKey, ...]:
        """
        Relation keys this expression metadata bundle can expose.

        :return:
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ExpressionRelationKey:
        """
        Normalize and validate one relation key.

        :param relation_key:
        :return:
        """
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(f"Unknown expression-metadata relation key {relation_key!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
        return cast(ExpressionRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: ExpressionRelationKey) -> RelationCardinality:
        """
        Return the cardinality policy for one relation key.

        ONE-ONE, ONE-MANY .e.t.c.
        :param relation_key:
        :return:
        """
        relation_key = cls.validate_relation_name(relation_key)
        return cls.RELATION_CARDINALITIES.get(
            relation_key,
            RelationCardinality.MANY_TO_MANY,
        )

    @classmethod
    def validate_relation_links(
        cls,
        relation_key: ExpressionRelationKey,
        links: Iterable[ExpressionRelationLink],
    ) -> list[ExpressionRelationLink]:
        """
        Validate relation links for one relation key.

        :param relation_key:
        :param links:
        :return:
        """
        relation_key = cls.validate_relation_name(relation_key)
        return validate_relation_link_cardinality(
            relation_key,
            links,
            cls.relation_cardinality(relation_key),
        )

    @property
    @abc.abstractmethod
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        """
        Primary expression row for this metadata bundle.

        :return:
        """

    @expression.setter
    @abc.abstractmethod
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        """
        Set primary expression identity.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: ExpressionRelationKey) -> list[ExpressionRelationLink]:
        """Get relation links for one relation key."""

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ExpressionRelationKey, links: Iterable[ExpressionRelationLink]) -> None:
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
        """
        Persist supported relation-backed changes for this expression metadata bundle.

        :param database:
        :param fields:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    # Todo: How do you set sidecare data at the same time with this?
    def add_relation_link(self, relation_key: ExpressionRelationKey, link: ExpressionRelationLink) -> None:
        """
        Add a relational link to this expression.

        Link target should be in the form of another object which can be linked to this one.
        :param relation_key:
        :param link:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation_key: ExpressionRelationKey, link: ExpressionRelationLink) -> bool:
        """
        Remove a relation link between this expression and another object.

        :param relation_key:
        :param link:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation_key: ExpressionRelationKey) -> list[ExpressionRelationTarget]:
        """
        Get the related entities for this relation key.

        :param relation_key:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        return [link.target for link in self.get_relation_links(relation_key)]

    def primary_relation_link(self, relation_key: ExpressionRelationKey) -> Optional[ExpressionRelationLink]:
        """Return the preferred relation link for one relation key, if any."""

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: ExpressionRelationKey) -> ExpressionRelationTarget | None:
        """Return the preferred relation target for one relation key, if any."""

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(
        self,
        relation_key: ExpressionRelationKey,
        link: ExpressionRelationLink,
    ) -> None:
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

    def set_related(self, relation_key: ExpressionRelationKey, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set multiple related values with one call.

        :param relation_key:
        :param values:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(
            relation_key,
            [
                ExpressionRelationLink(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation_key: ExpressionRelationKey, value: ExpressionRelationTarget) -> None:
        """
        Add a related object to this metadata.

        :param relation_key:
        :param value:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.add_relation_link(
            relation_key,
            ExpressionRelationLink(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_link_by_id(
        self,
        relation_key: ExpressionRelationKey,
        link_id: RelationLinkID,
    ) -> Optional[ExpressionRelationLink]:
        """
        Get a relation link by its id.

        :param relation_key:
        :param link_id:
        :return:
        """
        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    def upsert_relation_link(
        self,
        relation_key: ExpressionRelationKey,
        link: ExpressionRelationLink,
    ) -> None:
        """
        Upsert a relation link.

        Bringing it to the top of the stack.
        :param relation_key:
        :param link:
        :return:
        """
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
        relation_key: ExpressionRelationKey,
        link_id: RelationLinkID,
    ) -> bool:
        """
        Remove a relation link, if present, from the stack by id.

        :param relation_key:
        :param link_id:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        for index, link in enumerate(links):
            if link.link_id == link_id:
                del links[index]
                self.set_relation_links(relation_key, links)
                return True
        return False

    def clear_related(self, relation_key: ExpressionRelationKey) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])

    @property
    def primary_work(self) -> ExpressionRelationTarget | None:
        """Preferred work traversal from this expression."""

        return self.primary_related("works")

    @property
    def primary_manifestation(self) -> ExpressionRelationTarget | None:
        """Preferred manifestation traversal from this expression."""

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        return relation_target_id(self.primary_manifestation, "manifestation_id")

    @property
    def primary_item(self) -> ExpressionRelationTarget | None:
        """Preferred item traversal from this expression."""

        return self.primary_related("items")

    @property
    def primary_item_id(self) -> Optional[int]:
        return relation_target_id(self.primary_item, "item_id")

    @property
    def work_id(self) -> Optional[int]:
        """
        Legacy/source-row work id hint for this expression.

        The complete graph is exposed through the ``works`` relation links.
        :return:
        """
        return self.expression_work_id

    @work_id.setter
    def work_id(self, value: Optional[int]) -> None:
        """
        Set the legacy/source-row work id hint for this expression.

        :param value:
        :return:
        """
        self.expression_work_id = value

    @property
    @abc.abstractmethod
    def work_ids(self) -> Optional[Iterable[int]]:
        """
        Get all the work ids this expression is linked to.

        :return:
        """

    @work_ids.setter
    @abc.abstractmethod
    def work_ids(self, work_ids: Optional[Iterable[int]]) -> None:
        """
        Set all the work ids this expression is linked to.

        :param work_ids:
        :return:
        """

    @property
    def primary_work_id(self) -> Optional[int]:
        """
        Preferred work id for this expression.

        The selected ``works`` relation link wins when present; otherwise this
        falls back to the legacy/source-row work id hint.
        :return:
        """
        primary_id = relation_target_id(self.primary_work, "work_id")
        if primary_id is not None:
            return primary_id
        return self.expression_work_id

    @primary_work_id.setter
    def primary_work_id(self, value: Optional[int]) -> None:
        """
        Set the legacy/source-row work id hint for this expression.

        Use ``set_primary_relation_link`` to change graph-link preference.

        :param value:
        :return:
        """
        self.expression_work_id = value


    @property
    @abc.abstractmethod
    def expression_work_id(self) -> Optional[int]:
        """
        Legacy/source-row work id hint for this expression.

        :return:
        """

    @expression_work_id.setter
    @abc.abstractmethod
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        """
        Set the legacy/source-row work id hint for this expression.

        :param expression_work_id:
        :return:
        """

    @property
    def works(self) -> list[ExpressionRelationTarget]:
        """
        Get the works linked to this expression.

        :return:
        """
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the works linked to this expression.

        :param values:
        :return:
        """
        self.set_related("works", values)

    @property
    def manifestations(self) -> list[ExpressionRelationTarget]:
        """
        Return the manifestations linked to this expression.

        :return:
        """
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the manifestations linked to this expression.

        :param values:
        :return:
        """
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[ExpressionRelationTarget]:
        """
        Return the items linked to this manifestation.

        :return:
        """
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the items linked to this manifestation.

        :param values:
        :return:
        """
        self.set_related("items", values)

    @property
    def agents(self) -> list[ExpressionRelationTarget]:
        """
        Get the agents responsible for the current expression.

        :return:
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the agents for this container.

        :param values:
        :return:
        """
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ExpressionRelationTarget]:
        """
        Get the identifiers linked to this expression.

        :return:
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Write the identifiers out to the metadata object.

        :param values:
        :return:
        """
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ExpressionRelationTarget]:
        """
        Return the titles linked to this expression.

        :return:
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the titles - through setting their relations - with this expression.

        :param values:
        :return:
        """
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ExpressionRelationTarget]:
        """
        Return the genres linked to this metadata expression.

        :return:
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the genres based on their relations to the expression.

        :param values:
        :return:
        """
        self.set_related("genres", values)

    @property
    def tags(self) -> list[ExpressionRelationTarget]:
        """
        Get the tags based on their relations to the expression.

        :return:
        """
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the tags from a series of tag relations.

        :param values:
        :return:
        """
        self.set_related("tags", values)

    @property
    def labels(self) -> list[ExpressionRelationTarget]:
        """
        Get a list of the labels related to this expression.

        :return:
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the values from a list.

        :param values:
        :return:
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ExpressionRelationTarget]:
        """
        Get the expression-language interactions.

        :return:
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Write the language relation targets out to the metadata object.

        :param values:
        :return:
        """
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ExpressionRelationTarget]:
        """
        Return the notes relation targets.

        :return:
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Set the notes from the Expression relations.

        :param values:
        :return:
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ExpressionRelationTarget]:
        """
        Get the comment relations.

        :return:
        """
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

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

__all__ = [
    "ExpressionRelationKey",
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ExpressionMetadataAPI",
]
