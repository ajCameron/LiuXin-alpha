"""Core WEMI metadata-bundle API contract for item entities.

Category: core WEMI metadata bundle.
This module defines the editable database-backed metadata surface around an
item. It is not the item identity object and not a read-side view.
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI

ItemRelationTarget: TypeAlias = (
    AgentIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | WorkIdentityAPI
    | RelationTarget
)

@dataclasses.dataclass(slots=True)
class ItemRelationLink(RelationLink[ItemRelationTarget]):
    """
    Relation link used by item metadata containers.

    This intentionally mirrors the standard generated interlink metadata used by
    the FRBR schema: ``priority``, ``primary``, ``type``, ``origin``,
    ``policy``, ``data``, and ``index``.
    """

    target: ItemRelationTarget


ItemRelationKey: TypeAlias = Literal[
    "works",
    "expressions",
    "manifestations",
    "agents",
    "digital_assets",
    "composite_digital_assets",
    "asset_replicas",
    "stores",
    "folders",
    "files",
    "images",
    "identifiers",
    "titles",
    "annotations",
    "genres",
    "subjects",
    "series",
    "tags",
    "labels",
    "languages",
    "notes",
    "comments",
]


class ItemMetadataAPI(abc.ABC):
    """
    API for a container that holds all metadata associated with one item.

    Implementations should expose:
    - the core ``item`` row container
    - bibliographic context above the item (WEMI + agents + topical metadata)
    - storage context below/alongside the item (digital assets, replicas,
      stores, folders)

    The ``relation_key`` parameter names one normalized relation bucket from
    ``RELATION_KEYS``. These keys usually mirror related metadata table or
    bucket names, such as ``files`` or ``tags``, but they are API contract keys
    rather than a guarantee about a physical database table.
    """

    RELATION_KEYS: ClassVar[tuple[ItemRelationKey, ...]] = (
        "works",
        "expressions",
        "manifestations",
        "agents",
        "digital_assets",
        "composite_digital_assets",
        "asset_replicas",
        "stores",
        "folders",
        "files",
        "images",
        "identifiers",
        "titles",
        "annotations",
        "genres",
        "subjects",
        "series",
        "tags",
        "labels",
        "languages",
        "notes",
        "comments",
    )

    RELATION_ALIASES: ClassVar[Mapping[str, ItemRelationKey]] = {
        "work": "works",
        "expression": "expressions",
        "manifestation": "manifestations",
        "agent": "agents",
        "creator": "agents",
        "creators": "agents",
        "publisher": "agents",
        "publishers": "agents",
        "organization": "agents",
        "organisation": "agents",
        "org": "agents",
        "orgs": "agents",
        "digital_asset": "digital_assets",
        "asset": "digital_assets",
        "assets": "digital_assets",
        "composite_asset": "composite_digital_assets",
        "composite_assets": "composite_digital_assets",
        "composite_digital_asset": "composite_digital_assets",
        "asset_replica": "asset_replicas",
        "replica": "asset_replicas",
        "replicas": "asset_replicas",
        "store": "stores",
        "folder": "folders",
        "file": "files",
        "image": "images",
        "cover": "images",
        "covers": "images",
        "identifier": "identifiers",
        "title": "titles",
        "annotation": "annotations",
        "genre": "genres",
        "subject": "subjects",
        "tag": "tags",
        "label": "labels",
        "language": "languages",
        "note": "notes",
        "comment": "comments",
    }
    RELATION_CARDINALITIES: ClassVar[Mapping[ItemRelationKey, RelationCardinality]] = {
        "works": RelationCardinality.MANY_TO_MANY,
        "expressions": RelationCardinality.MANY_TO_MANY,
        "manifestations": RelationCardinality.MANY_TO_MANY,
        "identifiers": RelationCardinality.ONE_TO_MANY,
        "titles": RelationCardinality.ONE_TO_MANY,
        "annotations": RelationCardinality.ONE_TO_MANY,
        "notes": RelationCardinality.ONE_TO_MANY,
        "comments": RelationCardinality.ONE_TO_MANY,
    }

    @classmethod
    def relation_names(cls) -> tuple[ItemRelationKey, ...]:
        """
        Relation keys this item metadata bundle can expose.

        :return:
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ItemRelationKey:
        """
        Normalize and validate one relation key.

        :param relation_key:
        :return:
        """
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(
                "Unknown item-metadata relation key {!r}. Expected one of {}.".format(
                    relation_key,
                    ", ".join(cls.RELATION_KEYS),
                )
            )
        return cast(ItemRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: ItemRelationKey) -> RelationCardinality:
        """
        Return the cardinality policy for one relation key.

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
        relation_key: ItemRelationKey,
        links: Iterable[ItemRelationLink],
    ) -> list[ItemRelationLink]:
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
    def item(self) -> Optional[ItemIdentityAPI]:
        """
        Primary item identity for this metadata bundle.

        :return:
        """

    @item.setter
    @abc.abstractmethod
    def item(self, value: Optional[ItemIdentityAPI]) -> None:
        """
        Set the primary item identity for this metadata bundle.

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
    def get_relation_links(self, relation_key: ItemRelationKey) -> list[ItemRelationLink]:
        """
        Get relation links for one relation key.

        :param relation_key:
        :return:
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ItemRelationKey, links: Iterable[ItemRelationLink]) -> None:
        """
        Entirely replace relation links for one relation key.

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
        Persist supported relation-backed changes for this item metadata bundle.

        This is done by writing out to the database.
        :param database:
        :param fields:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    def add_relation_link(self, relation_key: ItemRelationKey, link: ItemRelationLink) -> None:
        """
        Add an existing relational link to the item metadata.

        :param relation_key:
        :param link:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(relation_key, self.validate_relation_links(relation_key, links))

    def remove_relation_link(self, relation_key: ItemRelationKey, link: ItemRelationLink) -> bool:
        """
        Remove a relational link from this metadata.

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

    def get_related(self, relation_key: ItemRelationKey) -> list[ItemRelationTarget]:
        """
        Get the related objects for this relation key.

        :param relation_key:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        links = self.get_relation_links(relation_key)
        return [link.target for link in links]

    def primary_relation_link(self, relation_key: ItemRelationKey) -> Optional[ItemRelationLink]:
        """Return the preferred relation link for one relation key, if any."""

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: ItemRelationKey) -> ItemRelationTarget | None:
        """Return the preferred relation target for one relation key, if any."""

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(
        self,
        relation_key: ItemRelationKey,
        link: ItemRelationLink,
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

    def set_related(self, relation_key: ItemRelationKey, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the related value of an object to this one.

        :param relation_key:
        :param values:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(
            relation_key,
            [
                ItemRelationLink(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation_key: ItemRelationKey, value: ItemRelationTarget) -> None:
        """
        Add a related object to this metadata for one relation key.

        :param relation_key:
        :param value:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.add_relation_link(
            relation_key,
            ItemRelationLink(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_link_by_id(
        self,
        relation_key: ItemRelationKey,
        link_id: RelationLinkID,
    ) -> Optional[ItemRelationLink]:
        """
        Return a relation link by its ID, if it exists on the system.

        :param relation_key:
        :param link_id:
        :return:
        """
        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    # Todo: Common "WEMIObject" base class for these methods?
    def upsert_relation_link(self, relation_key: ItemRelationKey, link: ItemRelationLink) -> None:
        """
        Upsert a relation link by id when it already exists.

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
        relation_key: ItemRelationKey,
        link_id: RelationLinkID,
    ) -> bool:
        """
        Remove a relation link by its ID, if it exists in the metadata object.

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

    def clear_related(self, relation_key: ItemRelationKey) -> None:
        """
        Clear all related links of a certain type.

        :param relation_key:
        :return:
        """
        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])

    @property
    def primary_work(self) -> ItemRelationTarget | None:
        """Preferred work traversal from this item."""

        return self.primary_related("works")

    @property
    def primary_work_id(self) -> Optional[int]:
        return relation_target_id(self.primary_work, "work_id")

    @property
    def primary_expression(self) -> ItemRelationTarget | None:
        """Preferred expression traversal from this item."""

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        return relation_target_id(self.primary_expression, "expression_id")

    @property
    def primary_manifestation(self) -> ItemRelationTarget | None:
        """Preferred manifestation traversal from this item."""

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        primary_id = relation_target_id(self.primary_manifestation, "manifestation_id")
        if primary_id is not None:
            return primary_id
        item = self.item
        if item is None:
            return None
        return item.item_manifestation_id

    @property
    def works(self) -> list[ItemRelationTarget]:
        """
        Get the works related to this item.

        :return:
        """
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the works related to this target.

        :param values:
        :return:
        """
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ItemRelationTarget]:
        """
        Get the item-expression relations for this item.

        :return:
        """
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the item-expression relations for this target.

        :param values:
        :return:
        """
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[ItemRelationTarget]:
        """
        Get the manifestation relations for this item.

        :return:
        """
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the manifestation relations for this item.

        :param values:
        :return:
        """
        self.set_related("manifestations", values)

    @property
    def agents(self) -> list[ItemRelationTarget]:
        """
        Get the agent relations for this item.

        :return:
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the agent relations for this item.

        :param values:
        :return:
        """
        self.set_related("agents", values)

    @property
    def digital_assets(self) -> list[ItemRelationTarget]:
        """
        Get the digital assets relations for this item.

        Digital assets are actual files - the lowest level of the program.
        :return:
        """
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the digital assets relations for this item.

        :param values:
        :return:
        """
        self.set_related("digital_assets", values)

    @property
    def composite_digital_assets(self) -> list[ItemRelationTarget]:
        """
        Gets the composite digital assets relations for this item.

        :return:
        """
        return self.get_related("composite_digital_assets")

    @composite_digital_assets.setter
    def composite_digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the composite digital asset relations for this item.

        :param values:
        :return:
        """
        self.set_related("composite_digital_assets", values)

    @property
    def asset_replicas(self) -> list[ItemRelationTarget]:
        """
        Get the asset replica relations for this item.

        :return:
        """
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Write the asset replica relations for this item.

        :param values:
        :return:
        """
        self.set_related("asset_replicas", values)

    @property
    def stores(self) -> list[ItemRelationTarget]:
        """
        Lists the stores this item is related to.

        :return:
        """
        return self.get_related("stores")

    @stores.setter
    def stores(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the stores this item is related to.

        :param values:
        :return:
        """
        self.set_related("stores", values)

    @property
    def folders(self) -> list[ItemRelationTarget]:
        """
        Lists the "folder" structure which this item might be in.

        :return:
        """
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the folders this item is connected to.

        :param values:
        :return:
        """
        self.set_related("folders", values)

    @property
    def files(self) -> list[ItemRelationTarget]:
        """
        All the "files" linked to the item.

        :return:
        """
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the files property for this item.

        :param values:
        :return:
        """
        self.set_related("files", values)

    @property
    def images(self) -> list[ItemRelationTarget]:
        """
        Get the image related "images" linked to the item.

        :return:
        """
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the image-item relations.

        :param values:
        :return:
        """
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[ItemRelationTarget]:
        """
        Return the identifiers relations for this item.

        :return:
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the identifiers relations for this item.

        :param values:
        :return:
        """
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ItemRelationTarget]:
        """
        Get the title relations for this item.

        :return:
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the title relations for this item.

        :param values:
        :return:
        """
        self.set_related("titles", values)

    @property
    def annotations(self) -> list[ItemRelationTarget]:
        """
        Get the annotations relations for this item.

        :return:
        """
        return self.get_related("annotations")

    @annotations.setter
    def annotations(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the annotations relations for this item.

        :param values:
        :return:
        """
        self.set_related("annotations", values)

    @property
    def genres(self) -> list[ItemRelationTarget]:
        """
        Get the genres relations for this item.

        :return:
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the genres relations for this item.

        :param values:
        :return:
        """
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[ItemRelationTarget]:
        """
        Get the subjects relations for this item.

        :return:
        """
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the subject relations for this item.

        :param values:
        :return:
        """
        self.set_related("subjects", values)

    @property
    def series(self) -> list[ItemRelationTarget]:
        """
        Get the series relations for this item.

        :return:
        """
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the series relations for this item.

        :param values:
        :return:
        """
        self.set_related("series", values)

    @property
    def tags(self) -> list[ItemRelationTarget]:
        """
        Get the tag relations for this item.

        :return:
        """
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the tag relations for this item.

        :param values:
        :return:
        """
        self.set_related("tags", values)

    @property
    def labels(self) -> list[ItemRelationTarget]:
        """
        Get the label relations for this item.

        :return:
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the label relations for this item.

        :param values:
        :return:
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ItemRelationTarget]:
        """
        Get the languages relations for this item.

        :return:
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the language relations for this item.

        :param values:
        :return:
        """
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ItemRelationTarget]:
        """
        Get the notes relations for this item.

        :return:
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the notes relations for this item.

        :param values:
        :return:
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ItemRelationTarget]:
        """
        Get the comments relations for this item.

        :return:
        """
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Set the comments relations for this item.

        :param values:
        :return:
        """
        self.set_related("comments", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Serialize container into a mapping representation.

        :param include_related:
        :return:
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Hydrate container from mapping representation.

        :param payload:
        :return:
        """

    def __str__(self) -> str:
        """
        String representation.

        :return:
        """
        return f"{self.__class__.__name__}()"

__all__ = [
    "ItemMetadataAPI",
    "ItemRelationKey",
    "ItemRelationLink",
    "ItemRelationTarget",
]
