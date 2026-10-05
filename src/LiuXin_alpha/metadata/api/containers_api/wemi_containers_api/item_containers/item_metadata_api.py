"""
Define the editable relation-bundle contract for item metadata.

The API combines an optional identity, typed relation links, projections, mapping
conversion and writer delegation without implementing persistence itself.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
    >>> metadata = ItemMetadata()
    >>> metadata.relation_names()[0]
    'works'
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
    validate_relation_link_cardinality,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.metadata_relations_api import (
    WemiMetadataRelationsAPI,
)
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
    Specialize RelationLink for targets accepted by an item metadata bundle.

    The slotted dataclass retains target and metadata values by reference and inherits
    cardinality normalization from RelationLink.

    Example:
        >>> link = ItemRelationLink(target={'item_id': 2})
        >>> link.target['item_id']
        2
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


class ItemMetadataAPI(WemiMetadataRelationsAPI[ItemRelationKey, ItemRelationTarget, ItemRelationLink], abc.ABC):
    """
    Define relation names, aliases, cardinalities and editable projections for one item.

        Concrete bundles supply identity storage, live or copied relation buckets,
        serialization and database writing. A ``relation_key`` selects an entry from
        ``RELATION_KEYS``. Logical buckets are API contracts; they do not describe a
        physical database table.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
        >>> metadata = ItemMetadata()
        >>> 'agents' in metadata.relation_names()
        True
    """

    RELATION_LINK_CLASS: ClassVar[type[ItemRelationLink]] = ItemRelationLink

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
        Return the canonical item relation keys in declared order.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.relation_names()[0]
            'works'


        :return: Tuple of canonical relation keys.
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ItemRelationKey:
        """
        Normalize a relation key with stripping, lowercase conversion and the alias table.

        Unknown keys raise KeyError and the original input is included in its message.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.validate_relation_name(' TAG ')
            'tags'


        :param relation_key: Value supplied for relation key.
        :return: Canonical relation key.
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
        Return the local target-count policy for a normalized relation key.

        Explicit graph and list-like policies come from RELATION_CARDINALITIES; unspecified
        buckets default to MANY_TO_MANY.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.relation_cardinality('tags') is RelationCardinality.MANY_TO_MANY
            True


        :param relation_key: Value supplied for relation key.
        :return: Canonical RelationCardinality member.
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
        Normalize the key, materialize the iterable and enforce its local cardinality policy.

        The returned list retains link objects; no target validation or cross-bucket
        uniqueness check occurs here.

        Example:
            >>> link = ItemRelationLink(target='History')
            >>> ItemMetadataAPI.validate_relation_links('tags', [link])[0] is link
            True


        :param relation_key: Value supplied for relation key.
        :param links: Value supplied for links.
        :return: New validated list of relation links.
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
        Require access to the optional item identity retained by the bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> identity_value = ItemIdentity(**{'item_id': 2})
            >>> metadata = ItemMetadata(**{'item': identity_value})
            >>> metadata.item is identity_value
            True


        :return: Shared ItemIdentity object, or None.
        """

    @item.setter
    @abc.abstractmethod
    def item(self, value: Optional[ItemIdentityAPI]) -> None:
        """
        Require replacement of the optional item identity without prescribing relation changes.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.item = ItemIdentity(**{'item_id': 2})
            >>> metadata.item.item_id
            2


        :param value: New ItemIdentity object, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def values(self) -> MetadataValuesViewAPI:
        """
        Require a structured read-only projection backed by this bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.values.tags
            ()


        :return: MetadataValuesViewAPI implementation.
        """

    @property
    @abc.abstractmethod
    def text(self) -> MetadataTextViewAPI:
        """
        Require a display/export text projection backed by this bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.text.tags
            ''


        :return: MetadataTextViewAPI implementation.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: ItemRelationKey) -> list[ItemRelationLink]:
        """
        Require access to relation links for a canonical key.

        Concrete implementations define whether the returned list is live; callers should
        use editing helpers when validation matters.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.get_relation_links('tags')
            []


        :param relation_key: Value supplied for relation key.
        :return: List of relation links in stored order.
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ItemRelationKey, links: Iterable[ItemRelationLink]) -> None:
        """
        Require complete replacement of a relation bucket after concrete validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.set_relation_links('tags', [])
            >>> metadata.get_relation_links('tags')
            []


        :param relation_key: Supported relation name or alias.
        :param links: Iterable of replacement links.
        :return: None.
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
        Require persistence of supported item relation changes through a caller-owned write database.

        Field selection, target resolution, replacement, dirty marking, transactions and
        report contents belong to the implementation.

        Example:
            Exercise writer delegation with pytest::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param database: Caller-owned metadata write database.
        :param fields: Optional fields to write; None selects implementation defaults.
        :param item_id: Optional item id for target resolution.
        :param target_row: Optional target Row or mapping.
        :param replace: Request replacement rather than append semantics when True.
        :param mark_dirty: Request dirty marking when True.
        :return: MetadataWriteReportAPI describing the operation.
        """

    @property
    def primary_work(self) -> ItemRelationTarget | None:
        """
        Return the preferred work target using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work
            {'work_id': '3'}


        :return: Preferred work target, or None.
        """

        return self.primary_related("works")

    @property
    def primary_work_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred work using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work_id
            3


        :return: Integer id of the preferred work, or None.
        """
        return relation_target_id(self.primary_work, "work_id")

    @property
    def primary_expression(self) -> ItemRelationTarget | None:
        """
        Return the preferred expression target using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression
            {'expression_id': '4'}


        :return: Preferred expression target, or None.
        """

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred expression using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression_id
            4


        :return: Integer id of the preferred expression, or None.
        """
        return relation_target_id(self.primary_expression, "expression_id")

    @property
    def primary_manifestation(self) -> ItemRelationTarget | None:
        """
        Return the preferred manifestation target using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '6'}]
            >>> metadata.primary_manifestation
            {'manifestation_id': '6'}


        :return: Preferred manifestation target, or None.
        """

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        """
        Return the preferred manifestation id, falling back to item_manifestation_id using the shared primary-link ordering.

        Id properties use relation_target_id; primary_manifestation_id falls back to the
        attached item's legacy manifestation hint when the relation has no usable id.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '6'}]
            >>> metadata.primary_manifestation_id
            6


        :return: Preferred manifestation id, falling back to item_manifestation_id, or None.
        """
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
        Return targets from the works relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the works bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ItemRelationTarget]:
        """
        Return targets from the expressions relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the expressions bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[ItemRelationTarget]:
        """
        Return targets from the manifestations relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the manifestations bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("manifestations", values)

    @property
    def agents(self) -> list[ItemRelationTarget]:
        """
        Return targets from the agents relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the agents bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("agents", values)

    @property
    def digital_assets(self) -> list[ItemRelationTarget]:
        """
        Return targets from the digital_assets relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.digital_assets = [{'value': 'Example'}]
            >>> metadata.digital_assets
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the digital_assets bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.digital_assets = [{'value': 'Example'}]
            >>> metadata.digital_assets
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("digital_assets", values)

    @property
    def composite_digital_assets(self) -> list[ItemRelationTarget]:
        """
        Return targets from the composite_digital_assets relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.composite_digital_assets = [{'value': 'Example'}]
            >>> metadata.composite_digital_assets
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("composite_digital_assets")

    @composite_digital_assets.setter
    def composite_digital_assets(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the composite_digital_assets bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.composite_digital_assets = [{'value': 'Example'}]
            >>> metadata.composite_digital_assets
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("composite_digital_assets", values)

    @property
    def asset_replicas(self) -> list[ItemRelationTarget]:
        """
        Return targets from the asset_replicas relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.asset_replicas = [{'value': 'Example'}]
            >>> metadata.asset_replicas
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the asset_replicas bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.asset_replicas = [{'value': 'Example'}]
            >>> metadata.asset_replicas
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("asset_replicas", values)

    @property
    def stores(self) -> list[ItemRelationTarget]:
        """
        Return targets from the stores relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.stores = [{'value': 'Example'}]
            >>> metadata.stores
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("stores")

    @stores.setter
    def stores(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the stores bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.stores = [{'value': 'Example'}]
            >>> metadata.stores
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("stores", values)

    @property
    def folders(self) -> list[ItemRelationTarget]:
        """
        Return targets from the folders relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.folders = [{'value': 'Example'}]
            >>> metadata.folders
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the folders bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.folders = [{'value': 'Example'}]
            >>> metadata.folders
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("folders", values)

    @property
    def files(self) -> list[ItemRelationTarget]:
        """
        Return targets from the files relation bucket in stored order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the files bucket with new relation links around supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("files", values)

    @property
    def images(self) -> list[ItemRelationTarget]:
        """
        Return targets from the images relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the images bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[ItemRelationTarget]:
        """
        Return targets from the identifiers relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the identifiers bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ItemRelationTarget]:
        """
        Return targets from the titles relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the titles bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("titles", values)

    @property
    def annotations(self) -> list[ItemRelationTarget]:
        """
        Return targets from the annotations relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.annotations = [{'value': 'Example'}]
            >>> metadata.annotations
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("annotations")

    @annotations.setter
    def annotations(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the annotations bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.annotations = [{'value': 'Example'}]
            >>> metadata.annotations
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("annotations", values)

    @property
    def genres(self) -> list[ItemRelationTarget]:
        """
        Return targets from the genres relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the genres bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[ItemRelationTarget]:
        """
        Return targets from the subjects relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.subjects = [{'value': 'Example'}]
            >>> metadata.subjects
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the subjects bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.subjects = [{'value': 'Example'}]
            >>> metadata.subjects
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("subjects", values)

    @property
    def series(self) -> list[ItemRelationTarget]:
        """
        Return targets from the series relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.series = [{'value': 'Example'}]
            >>> metadata.series
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the series bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.series = [{'value': 'Example'}]
            >>> metadata.series
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("series", values)

    @property
    def tags(self) -> list[ItemRelationTarget]:
        """
        Return targets from the tags relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the tags bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("tags", values)

    @property
    def labels(self) -> list[ItemRelationTarget]:
        """
        Return targets from the labels relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the labels bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ItemRelationTarget]:
        """
        Return targets from the languages relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the languages bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ItemRelationTarget]:
        """
        Return targets from the notes relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the notes bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ItemRelationTarget]:
        """
        Return targets from the comments relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ItemRelationTarget]) -> None:
        """
        Replace the comments bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("comments", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Require serialization of the item identity and, optionally, relation links.

        Concrete implementations define shallow-copy details and target conversion;
        serialization performs no persistence.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> 'item' in metadata.to_mapping(include_related=False)
            True


        :param include_related: Include relation-link payloads when true.
        :return: Mutable metadata record.
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Require construction of an item bundle from identity and relation payloads.

        Concrete implementations define recognized targets, ignored entries and copy depth.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> restored = ItemMetadata.from_mapping({'item': {'item_id': 5}})
            >>> restored.item.item_id
            5


        :param payload: Metadata record containing optional item and relations entries.
        :return: New bundle of the requested class.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete bundle does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import ItemMetadata
            >>> metadata = ItemMetadata()
            >>> ItemMetadataAPI.__str__(metadata)
            'ItemMetadata()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"

__all__ = [
    "ItemMetadataAPI",
    "ItemRelationKey",
    "ItemRelationLink",
    "ItemRelationTarget",
]
