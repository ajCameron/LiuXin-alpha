"""
Define the editable relation-bundle contract for manifestation metadata.

The API combines an optional identity, typed relation links, projections, mapping
conversion and writer delegation without implementing persistence.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
    >>> metadata = ManifestationMetadata()
    >>> bool(metadata.relation_names())
    True
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
    """
    Specialize RelationLink for targets accepted by a manifestation metadata bundle.

    The slotted dataclass retains target and metadata values by reference and inherits
    cardinality normalization from RelationLink.

    Example:
        >>> link = ManifestationRelationLink(target={'manifestation_id': 2})
        >>> link.target['manifestation_id']
        2
    """

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


class ManifestationMetadataAPI(WemiMetadataRelationsAPI[ManifestationRelationKey, ManifestationRelationTarget, ManifestationRelationLink], abc.ABC):
    """
    Define relation_key names, aliases, cardinalities and editable projections for one manifestation.

    RELATION_KEYS identifies logical buckets rather than a physical database table.
    Concrete bundles supply identity storage, relation buckets, serialization and
    database writing.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
        >>> metadata = ManifestationMetadata()
        >>> 'agents' in metadata.relation_names()
        True
    """

    RELATION_LINK_CLASS: ClassVar[type[ManifestationRelationLink]] = ManifestationRelationLink

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
        Return the canonical manifestation relation keys in declared order.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> 'agents' in metadata.relation_names()
            True


        :return: Tuple of canonical relation keys.
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ManifestationRelationKey:
        """
        Normalize a relation key with whitespace removal, lowercase conversion and the alias table.

        Unknown keys raise KeyError with the original input represented in the message.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.validate_relation_name(' LABELS ')
            'labels'


        :param relation_key: Value supplied for relation key.
        :return: Canonical relation key.
        """
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(f"Unknown manifestation-metadata relation key {relation_key!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
        return cast(ManifestationRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: ManifestationRelationKey) -> RelationCardinality:
        """
        Return the local target-count policy for a normalized relation key.

        Explicit policies come from RELATION_CARDINALITIES; unspecified buckets use the
        shared many-to-many default.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.relation_cardinality('labels').name
            'MANY_TO_MANY'


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
        relation_key: ManifestationRelationKey,
        links: Iterable[ManifestationRelationLink],
    ) -> list[ManifestationRelationLink]:
        """
        Normalize the relation key, materialize the iterable and enforce its local cardinality.

        The returned list retains its link objects and performs no target-shape validation.

        Example:
            >>> link = ManifestationRelationLink(target='History')
            >>> ManifestationMetadataAPI.validate_relation_links('labels', [link])[0] is link
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
    def manifestation(self) -> Optional[ManifestationIdentityAPI]:
        """
        Require access to the optional manifestation identity retained by the bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> value = ManifestationIdentity(**{'manifestation_id': 2})
            >>> metadata = ManifestationMetadata(**{'manifestation': value})
            >>> metadata.manifestation is value
            True


        :return: Shared ManifestationIdentity object, or None.
        """

    @manifestation.setter
    @abc.abstractmethod
    def manifestation(self, value: Optional[ManifestationIdentityAPI]) -> None:
        """
        Require replacement of the optional manifestation identity without prescribing relation changes.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity
            >>> metadata = ManifestationMetadata()
            >>> metadata.manifestation = ManifestationIdentity(**{'manifestation_id': 2})
            >>> metadata.manifestation.manifestation_id
            2


        :param value: New ManifestationIdentity object, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def values(self) -> MetadataValuesViewAPI:
        """
        Require a structured read-only projection backed by this metadata bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.values.tags
            ()


        :return: MetadataValuesViewAPI implementation.
        """

    @property
    @abc.abstractmethod
    def text(self) -> MetadataTextViewAPI:
        """
        Require a display and export text projection backed by this metadata bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.text.tags
            ''


        :return: MetadataTextViewAPI implementation.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: ManifestationRelationKey) -> list[ManifestationRelationLink]:
        """
        Require access to links in a relation bucket.

        Concrete implementations define whether the returned list is live; callers should
        use editing helpers when validation matters.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.get_relation_links('labels')
            []


        :param relation_key: Value supplied for relation key.
        :return: List of relation links in stored order.
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ManifestationRelationKey, links: Iterable[ManifestationRelationLink]) -> None:
        """
        Require complete replacement of a relation bucket after concrete validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.set_relation_links('labels', [])
            >>> metadata.get_relation_links('labels')
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
        Require persistence of supported manifestation relation changes through a caller-owned write database.

        Target resolution, replacement, dirty marking, transactions and report contents
        belong to the implementation.

        Example:
            Exercise writer delegation with pytest::

                python -m pytest -q tests/metadata/api/test_wemi_metadata_relation_properties_api.py


        :param database: Caller-owned metadata write database.
        :param fields: Optional fields to write; None selects implementation defaults.
        :param item_id: Optional item id for target resolution.
        :param target_row: Optional target Row or mapping.
        :param replace: Request replacement semantics when true.
        :param mark_dirty: Request dirty marking when true.
        :return: MetadataWriteReportAPI describing the operation.
        """

    @property
    def primary_work(self) -> ManifestationRelationTarget | None:
        """
        Return the preferred work target using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work
            {'work_id': '3'}


        :return: Preferred work target, or None.
        """

        return self.primary_related("works")

    @property
    def primary_work_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred work using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work_id
            3


        :return: Integer id of the preferred work, or None.
        """
        return relation_target_id(self.primary_work, "work_id")

    @property
    def primary_expression(self) -> ManifestationRelationTarget | None:
        """
        Return the preferred expression target using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression
            {'expression_id': '4'}


        :return: Preferred expression target, or None.
        """

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        """
        Return the preferred expression id, with identity fallback using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression_id
            4


        :return: Preferred expression id, with identity fallback, or None.
        """
        primary_id = relation_target_id(self.primary_expression, "expression_id")
        if primary_id is not None:
            return primary_id
        manifestation = self.manifestation
        if manifestation is None:
            return None
        return manifestation.manifestation_expression_id

    @property
    def primary_item(self) -> ManifestationRelationTarget | None:
        """
        Return the preferred item target using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.items = [{'item_id': '5'}]
            >>> metadata.primary_item
            {'item_id': '5'}


        :return: Preferred item target, or None.
        """

        return self.primary_related("items")

    @property
    def primary_item_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred item using shared primary-link ordering.

        Id properties extract supported target ids; primary_expression_id may use the
        attached identity's source-row hint when no link supplies one.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.items = [{'item_id': '5'}]
            >>> metadata.primary_item_id
            5


        :return: Integer id of the preferred item, or None.
        """
        return relation_target_id(self.primary_item, "item_id")

    @property
    def works(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the works relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the works bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("works", values)

    @property
    def expressions(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the expressions relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the expressions bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("expressions", values)

    @property
    def items(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the items relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the items bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("items", values)

    @property
    def agents(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the agents relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the agents bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the identifiers relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the identifiers bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the titles relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the titles bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the genres relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the genres bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("genres", values)

    @property
    def labels(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the labels relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the labels bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the languages relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the languages bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the notes relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the notes bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the comments relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the comments bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("comments", values)

    @property
    def files(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the files relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the files bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("files", values)

    @property
    def images(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the images relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the images bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("images", values)

    @property
    def digital_assets(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the digital_assets relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.digital_assets = [{'value': 'Example'}]
            >>> metadata.digital_assets
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("digital_assets")

    @digital_assets.setter
    def digital_assets(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the digital_assets bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.digital_assets = [{'value': 'Example'}]
            >>> metadata.digital_assets
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("digital_assets", values)

    @property
    def asset_replicas(self) -> list[ManifestationRelationTarget]:
        """
        Return targets from the asset_replicas relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.asset_replicas = [{'value': 'Example'}]
            >>> metadata.asset_replicas
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("asset_replicas")

    @asset_replicas.setter
    def asset_replicas(self, values: Iterable[ManifestationRelationTarget]) -> None:
        """
        Replace the asset_replicas bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> metadata.asset_replicas = [{'value': 'Example'}]
            >>> metadata.asset_replicas
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("asset_replicas", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Require serialization of the manifestation identity and, optionally, relation links.

        Concrete implementations define target conversion and copy depth; serialization
        performs no persistence.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> 'manifestation' in metadata.to_mapping(include_related=False)
            True


        :param include_related: Include relation-link payloads when true.
        :return: Mutable metadata record.
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Require construction of a manifestation bundle from identity and relation payloads.

        Concrete implementations define recognized targets, ignored entries and copy depth.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> restored = ManifestationMetadata.from_mapping({'manifestation': {'manifestation_id': 3}})
            >>> restored.manifestation.manifestation_id
            3


        :param payload: Metadata record containing optional manifestation and relations
            entries.
        :return: New bundle of the requested class.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete bundle does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import ManifestationMetadata
            >>> metadata = ManifestationMetadata()
            >>> ManifestationMetadataAPI.__str__(metadata)
            'ManifestationMetadata()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"

__all__ = [
    "ManifestationRelationKey",
    "ManifestationRelationLink",
    "ManifestationRelationTarget",
    "ManifestationMetadataAPI",
]
