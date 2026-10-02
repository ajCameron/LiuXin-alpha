"""
Define the editable relation-bundle contract for expression metadata.

The API combines an optional identity, typed relation links, projections, mapping
conversion and writer delegation without implementing persistence itself.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
    >>> metadata = ExpressionMetadata()
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.metadata_relations_api import (
    WemiMetadataRelationsAPI,
)
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
    """
    Specialize RelationLink for targets accepted by an expression metadata bundle.

    The slotted dataclass retains target and metadata values by reference and inherits
    cardinality normalization from RelationLink.

    Example:
        >>> link = ExpressionRelationLink(target={'expression_id': 2})
        >>> link.target['expression_id']
        2
    """

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


class ExpressionMetadataAPI(WemiMetadataRelationsAPI[ExpressionRelationKey, ExpressionRelationTarget, ExpressionRelationLink], abc.ABC):
    """
    Define relation names, aliases, cardinalities and editable projections for one expression.

        Concrete bundles supply identity storage, live or copied relation buckets,
        serialization and database writing. A ``relation_key`` selects an entry from
        ``RELATION_KEYS``. Logical buckets are API contracts; they do not describe a
        physical database table.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
        >>> metadata = ExpressionMetadata()
        >>> 'agents' in metadata.relation_names()
        True
    """

    RELATION_LINK_CLASS: ClassVar[type[ExpressionRelationLink]] = ExpressionRelationLink

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
        Return the canonical expression relation keys in declared order.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.relation_names()[0]
            'works'


        :return: Tuple of canonical relation keys.
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> ExpressionRelationKey:
        """
        Normalize a relation key with stripping, lowercase conversion and the alias table.

        Unknown keys raise KeyError and the original input is included in its message.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.validate_relation_name(' TAG ')
            'tags'


        :param relation_key: Value supplied for relation key.
        :return: Canonical relation key.
        """
        normalized = str(relation_key).strip().lower()
        normalized = cls.RELATION_ALIASES.get(normalized, normalized)
        if normalized not in cls.RELATION_KEYS:
            raise KeyError(f"Unknown expression-metadata relation key {relation_key!r}. Expected one of {', '.join(cls.RELATION_KEYS)}.")
        return cast(ExpressionRelationKey, normalized)

    @classmethod
    def relation_cardinality(cls, relation_key: ExpressionRelationKey) -> RelationCardinality:
        """
        Return the local target-count policy for a normalized relation key.

        Explicit graph and list-like policies come from RELATION_CARDINALITIES; unspecified
        buckets default to MANY_TO_MANY.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
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
        relation_key: ExpressionRelationKey,
        links: Iterable[ExpressionRelationLink],
    ) -> list[ExpressionRelationLink]:
        """
        Normalize the key, materialize the iterable and enforce its local cardinality policy.

        The returned list retains link objects; no target validation or cross-bucket
        uniqueness check occurs here.

        Example:
            >>> link = ExpressionRelationLink(target='History')
            >>> ExpressionMetadataAPI.validate_relation_links('tags', [link])[0] is link
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
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        """
        Require access to the optional expression identity retained by the bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> identity_value = ExpressionIdentity(**{'expression_id': 2})
            >>> metadata = ExpressionMetadata(**{'expression': identity_value})
            >>> metadata.expression is identity_value
            True


        :return: Shared ExpressionIdentity object, or None.
        """

    @expression.setter
    @abc.abstractmethod
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        """
        Require replacement of the optional expression identity without prescribing relation changes.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.expression = ExpressionIdentity(**{'expression_id': 2})
            >>> metadata.expression.expression_id
            2


        :param value: New ExpressionIdentity object, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def values(self) -> MetadataValuesViewAPI:
        """
        Require a structured read-only projection backed by this bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
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
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.text.tags
            ''


        :return: MetadataTextViewAPI implementation.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: ExpressionRelationKey) -> list[ExpressionRelationLink]:
        """
        Require access to relation links for a canonical key.

        Concrete implementations define whether the returned list is live; callers should
        use editing helpers when validation matters.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.get_relation_links('tags')
            []


        :param relation_key: Value supplied for relation key.
        :return: List of relation links in stored order.
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: ExpressionRelationKey, links: Iterable[ExpressionRelationLink]) -> None:
        """
        Require complete replacement of a relation bucket after concrete validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
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
        Require persistence of supported expression relation changes through a caller-owned write database.

        Field selection, target resolution, replacement, dirty marking, transactions and
        report contents belong to the implementation.

        Example:
            Exercise writer delegation with pytest::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Caller-owned metadata write database.
        :param fields: Optional fields to write; None selects implementation defaults.
        :param item_id: Optional item id for target resolution.
        :param target_row: Optional target Row or mapping.
        :param replace: Request replacement rather than append semantics when True.
        :param mark_dirty: Request dirty marking when True.
        :return: MetadataWriteReportAPI describing the operation.
        """

    @property
    def primary_work(self) -> ExpressionRelationTarget | None:
        """
        Return the preferred work target using the shared primary-link ordering.

        This reads current links without modifying their primary flags.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work
            {'work_id': '3'}


        :return: Preferred work target, or None.
        """

        return self.primary_related("works")

    @property
    def primary_manifestation(self) -> ExpressionRelationTarget | None:
        """
        Return the preferred manifestation target using the shared primary-link ordering.

        This reads current links without modifying their primary flags.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '4'}]
            >>> metadata.primary_manifestation
            {'manifestation_id': '4'}


        :return: Preferred manifestation target, or None.
        """

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred manifestation using the shared primary-link ordering.

        This reads current links without modifying their primary flags.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '4'}]
            >>> metadata.primary_manifestation_id
            4


        :return: Integer id of the preferred manifestation, or None.
        """
        return relation_target_id(self.primary_manifestation, "manifestation_id")

    @property
    def primary_item(self) -> ExpressionRelationTarget | None:
        """
        Return the preferred item target using the shared primary-link ordering.

        This reads current links without modifying their primary flags.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.items = [{'item_id': '5'}]
            >>> metadata.primary_item
            {'item_id': '5'}


        :return: Preferred item target, or None.
        """

        return self.primary_related("items")

    @property
    def primary_item_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred item using the shared primary-link ordering.

        This reads current links without modifying their primary flags.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.items = [{'item_id': '5'}]
            >>> metadata.primary_item_id
            5


        :return: Integer id of the preferred item, or None.
        """
        return relation_target_id(self.primary_item, "item_id")

    @property
    def work_id(self) -> Optional[int]:
        """
        Require the legacy singular work-id hint for compatibility.

        Graph traversal and primary preference use the works relation; concrete
        implementations decide how this hint is stored.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.work_id is None
            True


        :return: Legacy work id hint, or None.
        """
        return self.expression_work_id

    @work_id.setter
    def work_id(self, value: Optional[int]) -> None:
        """
        Require assignment of the legacy singular work-id hint.

        This does not by contract replace or select a works relation link.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.work_id = 3
            >>> metadata.work_id
            3


        :param value: New legacy work id hint, or None.
        :return: None.
        """
        self.expression_work_id = value

    @property
    @abc.abstractmethod
    def work_ids(self) -> Optional[Iterable[int]]:
        """
        Require access to all linked work ids under concrete extraction policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.work_ids = [3, 4]
            >>> list(metadata.work_ids)
            [3, 4]


        :return: Iterable of work ids, or None.
        """

    @work_ids.setter
    @abc.abstractmethod
    def work_ids(self, work_ids: Optional[Iterable[int]]) -> None:
        """
        Require replacement of all work-id associations under concrete bundle policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.work_ids = [3, 4]
            >>> list(metadata.work_ids)
            [3, 4]


        :param work_ids: Replacement iterable of work ids, or None.
        :return: None.
        """

    @property
    def primary_work_id(self) -> Optional[int]:
        """
        Return the preferred works-link id, falling back to expression_work_id.

        A usable id from primary_work wins; invalid or absent relation ids use the
        legacy/source-row hint.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.expression_work_id = 2
            >>> metadata.works = [{'work_id': '3'}]
            >>> metadata.primary_work_id
            3


        :return: Preferred or fallback integer work id, or None.
        """
        primary_id = relation_target_id(self.primary_work, "work_id")
        if primary_id is not None:
            return primary_id
        return self.expression_work_id

    @primary_work_id.setter
    def primary_work_id(self, value: Optional[int]) -> None:
        """
        Set the legacy/source-row work id hint through expression_work_id.

        Use set_primary_relation_link to change graph-link preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.primary_work_id = 3
            >>> metadata.expression_work_id
            3


        :param value: New legacy work id hint, or None.
        :return: None.
        """
        self.expression_work_id = value


    @property
    @abc.abstractmethod
    def expression_work_id(self) -> Optional[int]:
        """
        Require access to the legacy/source-row work id stored with the expression identity or bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.expression_work_id is None
            True


        :return: Legacy work id hint, or None.
        """

    @expression_work_id.setter
    @abc.abstractmethod
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        """
        Require replacement of the legacy/source-row work id hint without prescribing graph-link changes.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.expression_work_id = 3
            >>> metadata.expression_work_id
            3


        :param expression_work_id: New legacy/source-row work id, or None.
        :return: None.
        """

    @property
    def works(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the works relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("works")

    @works.setter
    def works(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the works bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.works = [{'value': 'Example'}]
            >>> metadata.works
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("works", values)

    @property
    def manifestations(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the manifestations relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the manifestations bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the items relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the items bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("items", values)

    @property
    def agents(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the agents relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the agents bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("agents", values)

    @property
    def identifiers(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the identifiers relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the identifiers bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("identifiers", values)

    @property
    def titles(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the titles relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the titles bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("titles", values)

    @property
    def genres(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the genres relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the genres bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("genres", values)

    @property
    def tags(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the tags relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the tags bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("tags", values)

    @property
    def labels(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the labels relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the labels bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the languages relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the languages bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("languages", values)

    @property
    def notes(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the notes relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the notes bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[ExpressionRelationTarget]:
        """
        Return targets from the comments relation bucket in stored link order.

        The result is a new list containing shared targets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[ExpressionRelationTarget]) -> None:
        """
        Replace the comments bucket with new relation links around the supplied targets.

        Targets remain shared; relation cardinality and concrete setter validation apply.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
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
        Require serialization of the expression identity and, optionally, relation links.

        Concrete implementations define shallow-copy details and target conversion; this
        operation does not persist data.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> 'expression' in metadata.to_mapping(include_related=False)
            True


        :param include_related: Include relation-link payloads when True.
        :return: Mutable metadata record.
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Require construction of an expression bundle from identity and relation payloads.

        Concrete implementations define recognized targets, ignored entries and copy depth.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> restored = ExpressionMetadata.from_mapping({'expression': {'expression_id': 2}})
            >>> restored.expression.expression_id
            2


        :param payload: Metadata record containing optional expression and relations
            entries.
        :return: New bundle of the requested class.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete bundle does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import ExpressionMetadata
            >>> metadata = ExpressionMetadata()
            >>> ExpressionMetadataAPI.__str__(metadata)
            'ExpressionMetadata()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"

__all__ = [
    "ExpressionRelationKey",
    "ExpressionRelationLink",
    "ExpressionRelationTarget",
    "ExpressionMetadataAPI",
]
