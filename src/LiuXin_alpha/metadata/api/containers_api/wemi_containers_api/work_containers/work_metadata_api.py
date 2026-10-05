"""
Define the editable relation-bundle contract for work metadata.

The API combines an optional identity, typed relation links, projections, mapping
conversion and writer delegation without implementing persistence.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
    >>> metadata = WorkMetadata()
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.metadata_relations_api import (
    WemiMetadataRelationsAPI,
)
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
    Specialize RelationLink for targets accepted by a work metadata bundle.

    The slotted dataclass retains target and metadata values by reference and inherits
    cardinality normalization from RelationLink.

    Example:
        >>> link = WorkRelationLink(target={'work_id': 2})
        >>> link.target['work_id']
        2
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


class WorkMetadataAPI(WemiMetadataRelationsAPI[WorkRelationKey, WorkRelationTarget, WorkRelationLink], abc.ABC):
    """
    Define relation_key names, aliases, cardinalities and editable projections for one work.

    RELATION_KEYS identifies logical buckets rather than a physical database table.
    Concrete bundles supply identity storage, relation buckets, serialization and
    database writing.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
        >>> metadata = WorkMetadata()
        >>> 'agents' in metadata.relation_names()
        True
    """

    RELATION_LINK_CLASS: ClassVar[type[WorkRelationLink]] = WorkRelationLink

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
        Return the canonical work relation keys in declared order.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> 'agents' in metadata.relation_names()
            True


        :return: Tuple of canonical relation keys.
        """
        return cls.RELATION_KEYS

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> WorkRelationKey:
        """
        Normalize a relation key with whitespace removal, lowercase conversion and the alias table.

        Unknown keys raise KeyError with the original input represented in the message.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.validate_relation_name(' TAGS ')
            'tags'


        :param relation_key: Value supplied for relation key.
        :return: Canonical relation key.
        """
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
        """
        Return the local target-count policy for a normalized relation key.

        Explicit policies come from RELATION_CARDINALITIES; unspecified buckets use the
        shared many-to-many default.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.relation_cardinality('tags').name
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
        relation_key: WorkRelationKey,
        links: Iterable[WorkRelationLink],
    ) -> list[WorkRelationLink]:
        """
        Normalize the relation key, materialize the iterable and enforce its local cardinality.

        The returned list retains its link objects and performs no target-shape validation.

        Example:
            >>> link = WorkRelationLink(target='History')
            >>> WorkMetadataAPI.validate_relation_links('tags', [link])[0] is link
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
    def work(self) -> Optional[WorkIdentityAPI]:
        """
        Require access to the optional work identity retained by the bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> value = WorkIdentity(**{'work_id': 2})
            >>> metadata = WorkMetadata(**{'work': value})
            >>> metadata.work is value
            True


        :return: Shared WorkIdentity object, or None.
        """

    @work.setter
    @abc.abstractmethod
    def work(self, value: Optional[WorkIdentityAPI]) -> None:
        """
        Require replacement of the optional work identity without prescribing relation changes.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> metadata = WorkMetadata()
            >>> metadata.work = WorkIdentity(**{'work_id': 2})
            >>> metadata.work.work_id
            2


        :param value: New WorkIdentity object, or None.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def values(self) -> MetadataValuesViewAPI:
        """
        Require a structured read-only projection backed by this metadata bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
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
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.tags
            ''


        :return: MetadataTextViewAPI implementation.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: WorkRelationKey) -> list[WorkRelationLink]:
        """
        Require access to links in a relation bucket.

        Concrete implementations define whether the returned list is live; callers should
        use editing helpers when validation matters.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.get_relation_links('tags')
            []


        :param relation_key: Value supplied for relation key.
        :return: List of relation links in stored order.
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: WorkRelationKey, links: Iterable[WorkRelationLink]) -> None:
        """
        Require complete replacement of a relation bucket after concrete validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
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
        Require persistence of supported work relation changes through a caller-owned write database.

        Target resolution, replacement, dirty marking, transactions and report contents
        belong to the implementation.

        Example:
            Exercise writer delegation with pytest::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param database: Caller-owned metadata write database.
        :param fields: Optional fields to write; None selects implementation defaults.
        :param item_id: Optional item id for target resolution.
        :param target_row: Optional target Row or mapping.
        :param replace: Request replacement semantics when true.
        :param mark_dirty: Request dirty marking when true.
        :return: MetadataWriteReportAPI describing the operation.
        """

    @property
    def primary_expression(self) -> WorkRelationTarget | None:
        """
        Return the preferred expression target using shared primary-link ordering.

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression
            {'expression_id': '4'}


        :return: Preferred expression target, or None.
        """

        return self.primary_related("expressions")

    @property
    def primary_expression_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred expression using shared primary-link ordering.

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.expressions = [{'expression_id': '4'}]
            >>> metadata.primary_expression_id
            4


        :return: Integer id of the preferred expression, or None.
        """
        return relation_target_id(self.primary_expression, "expression_id")

    @property
    def primary_manifestation(self) -> WorkRelationTarget | None:
        """
        Return the preferred manifestation target using shared primary-link ordering.

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '6'}]
            >>> metadata.primary_manifestation
            {'manifestation_id': '6'}


        :return: Preferred manifestation target, or None.
        """

        return self.primary_related("manifestations")

    @property
    def primary_manifestation_id(self) -> Optional[int]:
        """
        Return the integer id of the preferred manifestation using shared primary-link ordering.

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.manifestations = [{'manifestation_id': '6'}]
            >>> metadata.primary_manifestation_id
            6


        :return: Integer id of the preferred manifestation, or None.
        """
        return relation_target_id(self.primary_manifestation, "manifestation_id")

    @property
    def primary_item(self) -> WorkRelationTarget | None:
        """
        Return the preferred item target using shared primary-link ordering.

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
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

        Id properties extract supported integer ids without modifying relation preference.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.items = [{'item_id': '5'}]
            >>> metadata.primary_item_id
            5


        :return: Integer id of the preferred item, or None.
        """
        return relation_target_id(self.primary_item, "item_id")

    @property
    def agents(self) -> list[WorkRelationTarget]:
        """
        Return targets from the agents relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("agents")

    @agents.setter
    def agents(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the agents bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.agents = [{'value': 'Example'}]
            >>> metadata.agents
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("agents", values)

    @property
    def expressions(self) -> list[WorkRelationTarget]:
        """
        Return targets from the expressions relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("expressions")

    @expressions.setter
    def expressions(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the expressions bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.expressions = [{'value': 'Example'}]
            >>> metadata.expressions
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("expressions", values)

    @property
    def manifestations(self) -> list[WorkRelationTarget]:
        """
        Return targets from the manifestations relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("manifestations")

    @manifestations.setter
    def manifestations(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the manifestations bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.manifestations = [{'value': 'Example'}]
            >>> metadata.manifestations
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("manifestations", values)

    @property
    def items(self) -> list[WorkRelationTarget]:
        """
        Return targets from the items relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("items")

    @items.setter
    def items(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the items bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.items = [{'value': 'Example'}]
            >>> metadata.items
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("items", values)

    @property
    def files(self) -> list[WorkRelationTarget]:
        """
        Return targets from the files relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("files")

    @files.setter
    def files(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the files bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.files = [{'value': 'Example'}]
            >>> metadata.files
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("files", values)

    @property
    def titles(self) -> list[WorkRelationTarget]:
        """
        Return targets from the titles relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("titles")

    @titles.setter
    def titles(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the titles bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.titles = [{'value': 'Example'}]
            >>> metadata.titles
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("titles", values)

    @property
    def genres(self) -> list[WorkRelationTarget]:
        """
        Return targets from the genres relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("genres")

    @genres.setter
    def genres(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the genres bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.genres = [{'value': 'Example'}]
            >>> metadata.genres
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("genres", values)

    @property
    def subjects(self) -> list[WorkRelationTarget]:
        """
        Return targets from the subjects relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.subjects = [{'value': 'Example'}]
            >>> metadata.subjects
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("subjects")

    @subjects.setter
    def subjects(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the subjects bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.subjects = [{'value': 'Example'}]
            >>> metadata.subjects
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("subjects", values)

    @property
    def series(self) -> list[WorkRelationTarget]:
        """
        Return targets from the series relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.series = [{'value': 'Example'}]
            >>> metadata.series
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("series")

    @series.setter
    def series(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the series bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.series = [{'value': 'Example'}]
            >>> metadata.series
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("series", values)

    @property
    def tags(self) -> list[WorkRelationTarget]:
        """
        Return targets from the tags relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("tags")

    @tags.setter
    def tags(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the tags bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.tags = [{'value': 'Example'}]
            >>> metadata.tags
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("tags", values)

    @property
    def labels(self) -> list[WorkRelationTarget]:
        """
        Return targets from the labels relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("labels")

    @labels.setter
    def labels(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the labels bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.labels = [{'value': 'Example'}]
            >>> metadata.labels
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("labels", values)

    @property
    def languages(self) -> list[WorkRelationTarget]:
        """
        Return targets from the languages relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("languages")

    @languages.setter
    def languages(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the languages bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.languages = [{'value': 'Example'}]
            >>> metadata.languages
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("languages", values)

    @property
    def images(self) -> list[WorkRelationTarget]:
        """
        Return targets from the images relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("images")

    @images.setter
    def images(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the images bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.images = [{'value': 'Example'}]
            >>> metadata.images
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("images", values)

    @property
    def identifiers(self) -> list[WorkRelationTarget]:
        """
        Return targets from the identifiers relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("identifiers")

    @identifiers.setter
    def identifiers(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the identifiers bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.identifiers = [{'value': 'Example'}]
            >>> metadata.identifiers
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("identifiers", values)

    @property
    def ratings(self) -> list[WorkRelationTarget]:
        """
        Return targets from the ratings relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.ratings = [{'value': 'Example'}]
            >>> metadata.ratings
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("ratings")

    @ratings.setter
    def ratings(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the ratings bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.ratings = [{'value': 'Example'}]
            >>> metadata.ratings
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("ratings", values)

    @property
    def notes(self) -> list[WorkRelationTarget]:
        """
        Return targets from the notes relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("notes")

    @notes.setter
    def notes(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the notes bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.notes = [{'value': 'Example'}]
            >>> metadata.notes
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("notes", values)

    @property
    def comments(self) -> list[WorkRelationTarget]:
        """
        Return targets from the comments relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("comments")

    @comments.setter
    def comments(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the comments bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.comments = [{'value': 'Example'}]
            >>> metadata.comments
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("comments", values)

    @property
    def synopses(self) -> list[WorkRelationTarget]:
        """
        Return targets from the synopses relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.synopses = [{'value': 'Example'}]
            >>> metadata.synopses
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("synopses")

    @synopses.setter
    def synopses(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the synopses bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.synopses = [{'value': 'Example'}]
            >>> metadata.synopses
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("synopses", values)

    @property
    def folders(self) -> list[WorkRelationTarget]:
        """
        Return targets from the folders relation bucket in stored link order.

        The result is a new list containing shared targets; mutating the list does not
        replace the stored bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.folders = [{'value': 'Example'}]
            >>> metadata.folders
            [{'value': 'Example'}]


        :return: New list of related targets.
        """
        return self.get_related("folders")

    @folders.setter
    def folders(self, values: Iterable[WorkRelationTarget]) -> None:
        """
        Replace the folders bucket with relation links around the supplied targets.

        Targets remain shared; the concrete bundle applies key and cardinality validation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.folders = [{'value': 'Example'}]
            >>> metadata.folders
            [{'value': 'Example'}]


        :param values: Iterable of replacement relation targets.
        :return: None.
        """
        self.set_related("folders", values)

    @abc.abstractmethod
    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Require serialization of the work identity and, optionally, relation links.

        Concrete implementations define target conversion and copy depth; serialization
        performs no persistence.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> 'work' in metadata.to_mapping(include_related=False)
            True


        :param include_related: Include relation-link payloads when true.
        :return: Mutable metadata record.
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Require construction of a work bundle from identity and relation payloads.

        Concrete implementations define recognized targets, ignored entries and copy depth.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> restored = WorkMetadata.from_mapping({'work': {'work_id': 3}})
            >>> restored.work.work_id
            3


        :param payload: Metadata record containing optional work and relations entries.
        :return: New bundle of the requested class.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete bundle does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> WorkMetadataAPI.__str__(metadata)
            'WorkMetadata()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"

__all__ = [
    "WorkMetadataAPI",
    "WorkRelationKey",
    "WorkRelationLink",
    "WorkRelationTarget",
]
