"""Core WEMI metadata-bundle API contract for manifestation entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around a
manifestation. It is not the manifestation identity object and not a read-side view.
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import (
    AgentIdentityAPI)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import (
    ExpressionIdentityAPI)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
    relation_target_id,
    RelationTarget,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.projection_view_api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    RelationCardinality,
    RelationLink,
    RelationLinkID,
    select_primary_relation_link,
    validate_relation_link_cardinality,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.metadata_relations_api import (
    WemiMetadataRelationsAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ManifestationRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ExpressionIdentityAPI
    | ItemIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ManifestationRelationLink(RelationLink[ManifestationRelationTarget]):
    """Link from a manifestation-metadata container to a related entity."""

    target: ManifestationRelationTarget


ManifestationRelationKey: TypeAlias = Literal[
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
]


class ManifestationMetadataAPI(WemiMetadataRelationsAPI[ManifestationRelationKey, ManifestationRelationTarget], abc.ABC):
    """
    API for a container that holds all metadata associated with one manifestation.

    Implementations should expose the core manifestation row, WEMI context,
    attached assets/files, and relation-keyed link metadata.

    The ``relation_key`` parameter names one normalized relation bucket from
    ``RELATION_KEYS``. These keys usually mirror related metadata table or
    bucket names, such as ``files`` or ``agents``, but they are API contract
    keys rather than a guarantee about a physical database table.
    """

    RELATION_KEYS: ClassVar[tuple[ManifestationRelationKey, ...]] = (
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
    RELATION_ALIASES: ClassVar[Mapping[str, ManifestationRelationKey]] = {
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
    RELATION_CARDINALITIES: ClassVar[Mapping[ManifestationRelationKey, RelationCardinality]] = {
        "works": RelationCardinality.MANY_TO_MANY,
        "expressions": RelationCardinality.MANY_TO_MANY,
        "items": RelationCardinality.MANY_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
        "files": RelationCardinality.ONE_TO_MANY,
        "images": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[ManifestationRelationKey, ...]:
        """
        Relation keys this manifestation metadata bundle can expose.

        :return:
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ManifestationRelationKey:
        """
        Normalize and validate one relation key.

        :param relation_key:
        :return:
        """
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(f"Unknown manifestation-metadata relation key {relation_key!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
        return cast(ManifestationRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: ManifestationRelationKey) -> RelationCardinality:
        """
        Return the cardinality policy for one relation key.

        :param relation_key: agents/tags/e.t.c
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
        relation_key: ManifestationRelationKey,
        links: Iterable[ManifestationRelationLink],
    ) -> list[ManifestationRelationLink]:
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
    def manifestation(self) -> Optional[ManifestationIdentityAPI]:
        """
        Primary manifestation row for this metadata bundle.

        :return:
        """

    @manifestation.setter
    @abc.abstractmethod
    def manifestation(self, value: Optional[ManifestationIdentityAPI]) -> None:
        """
        Set primary manifestation row for this metadata bundle.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> MetadataValuesViewAPI:
        """Structured, read-only value projections for this metadata bundle."""

    @property
    @abc.abstractmethod
    def text(self) -> MetadataTextViewAPI:
        """Display/export text projections for this metadata bundle."""

    @abc.abstractmethod
    def get_relation_links(self, relation_key: ManifestationRelationKey) -> list[ManifestationRelationLink]:
        """
        Get relation links for one relation key.

        :param relation_key:
        :return:
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ManifestationRelationKey, links: Iterable[ManifestationRelationLink]) -> None:
        """
        Replace relation links for one relation key.

        :param relation_key:
        :param links:
        :return:
        """

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
        Persist supported relation-backed changes for this manifestation metadata bundle.

        :param database:
        :param fields:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    def add_relation_link(self, relation_key: ManifestationRelationKey, link: ManifestationRelationLink) -> None:
        """
        Add one relation link for a relation key.

        :param relation_key:
        :param link:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation_key: ManifestationRelationKey, link: ManifestationRelationLink) -> bool:
        """
        Remove one relation link for a relation key, if it exists.

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

    def get_related(self, relation_key: ManifestationRelationKey) -> list[ManifestationRelationTarget]:
        """
        Return the related relations for this manifestation-metadata bundle of a particular type.

        :param relation_key:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        return [link.target for link in self.get_relation_links(relation_key)]

    def primary_relation_link(self, relation_key: ManifestationRelationKey) -> Optional[ManifestationRelationLink]:
        """Return the preferred relation link for one relation key, if any."""

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: ManifestationRelationKey) -> ManifestationRelationTarget | None:
        """Return the preferred relation target for one relation key, if any."""

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(
        self,
        relation_key: ManifestationRelationKey,
        link: ManifestationRelationLink,
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

    def set_related(self, relation_key: ManifestationRelationKey, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Set the related values for this manifestation-metadata bundle of a particular type.

        :param relation_key:
        :param values:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(
            relation_key,
            [
                ManifestationRelationLink(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation_key: ManifestationRelationKey, value: ManifestationRelationTarget) -> None:
        """
        Add a related value for this manifestation-metadata bundle of a particular type.

        :param relation_key:
        :param value:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.add_relation_link(
            relation_key,
            ManifestationRelationLink(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_link_by_id(
        self,
        relation_key: ManifestationRelationKey,
        link_id: RelationLinkID,
    ) -> Optional[ManifestationRelationLink]:
        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    def upsert_relation_link(
        self,
        relation_key: ManifestationRelationKey,
        link: ManifestationRelationLink,
    ) -> None:
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
        relation_key: ManifestationRelationKey,
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

    def clear_related(self, relation_key: ManifestationRelationKey) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])

    @property
    def primary_work(self) -> ManifestationRelationTarget | None:
        """Preferred work traversal from this manifestation."""

        return self.primary_related("works")

    @property
    def primary_work_id(self) -> Optional[int]:
        return relation_target_id(self.primary_work, "work_id")

    @property
    def primary_expression(self) -> ManifestationRelationTarget | None:
        """Preferred expression traversal from this manifestation."""

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        primary_id = relation_target_id(self.primary_expression, "expression_id")
        if primary_id is not None:
            return primary_id
        manifestation = self.manifestation
        if manifestation is None:
            return None
        return manifestation.manifestation_expression_id

    @property
    def primary_item(self) -> ManifestationRelationTarget | None:
        """Preferred item traversal from this manifestation."""

        return self.primary_related("items")

    @property
    def primary_item_id(self) -> Optional[int]:
        return relation_target_id(self.primary_item, "item_id")

    @property
    def works(self) -> list[ManifestationRelationTarget]:
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ManifestationRelationTarget]:
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("expressions", values)

    @property
    def items(self) -> list[ManifestationRelationTarget]:
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("items", values)

    @property
    def agents(self) -> list[ManifestationRelationTarget]:
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ManifestationRelationTarget]:
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ManifestationRelationTarget]:
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ManifestationRelationTarget]:
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("genres", values)

    @property
    def labels(self) -> list[ManifestationRelationTarget]:
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ManifestationRelationTarget]:
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ManifestationRelationTarget]:
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ManifestationRelationTarget]:
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("comments", values)

    @property
    def files(self) -> list[ManifestationRelationTarget]:
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("files", values)

    @property
    def images(self) -> list[ManifestationRelationTarget]:
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("images", values)

    @property
    def digital_assets(self) -> list[ManifestationRelationTarget]:
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("digital_assets", values)

    @property
    def asset_replicas(self) -> list[ManifestationRelationTarget]:
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ManifestationRelationTarget]) -> None:
        self.set_related("asset_replicas", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Serialize this manifestation metadata bundle to a metadata record.

        :param include_related:
        :return:
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Build a manifestation metadata bundle from a metadata record.

        :param payload:
        :return:
        """

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

__all__ = [
    "ManifestationRelationKey",
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "ManifestationMetadataAPI",
]
