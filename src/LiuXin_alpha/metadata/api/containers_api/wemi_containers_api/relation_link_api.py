"""
Define relation-link values, structural contracts and local selection policies.

Links retain targets and metadata without persistence. Helpers normalize
cardinality, limit local target counts and choose a preferred link without changing
it.

Example:
    >>> link = RelationLink(target='History', cardinality='many-to-many')
    >>> link.cardinality is RelationCardinality.MANY_TO_MANY
    True
"""

from __future__ import annotations

import dataclasses

from collections.abc import Iterable
from enum import StrEnum
from typing import Generic, Literal, Optional, Protocol, TypeAlias, TypeVar, runtime_checkable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
    RelationLinkType,
    RelationTarget,
)

RelationLinkID: TypeAlias = int | str
RelationLinkSource: TypeAlias = str


class RelationCardinality(StrEnum):
    """
    Name the four source-to-target multiplicity policies.

    ONE_TO_ONE and MANY_TO_ONE allow at most one target in a local bucket; ONE_TO_MANY
    and MANY_TO_MANY permit several. This enum does not enforce cross-container
    uniqueness.

    Example:
        >>> RelationCardinality.MANY_TO_ONE.allows_many_targets
        False
    """

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"

    @property
    def allows_many_targets(self) -> bool:
        """
        Identify whether this policy permits multiple targets in the current container.

        Example:
            >>> RelationCardinality.ONE_TO_MANY.allows_many_targets
            True


        :return: True for ONE_TO_MANY or MANY_TO_MANY, otherwise False.
        """

        return self in {self.ONE_TO_MANY, self.MANY_TO_MANY}


RelationCardinalityValue: TypeAlias = RelationCardinality | str
RelationLinkTargetT = TypeVar("RelationLinkTargetT", bound=RelationTarget)
RelationLinkT = TypeVar("RelationLinkT")


def normalize_relation_cardinality(
    cardinality: RelationCardinalityValue,
) -> RelationCardinality:
    """
    Convert a cardinality string to its canonical enum member.

    Existing members are returned unchanged. Strings are stripped, lowercased and have
    hyphens replaced with underscores; unknown values raise ValueError. Other types are
    not coerced to strings.

    Example:
        >>> normalize_relation_cardinality(' ONE-TO-MANY ') is RelationCardinality.ONE_TO_MANY
        True


    :param cardinality: Existing enum or recognized string spelling.
    :return: Canonical RelationCardinality member.
    """

    if isinstance(cardinality, RelationCardinality):
        return cardinality
    normalized = cardinality.strip().lower().replace("-", "_")
    return RelationCardinality(normalized)


def validate_relation_link_cardinality(
    relation_key: str,
    links: Iterable[RelationLinkT],
    cardinality: RelationCardinality,
) -> list[RelationLinkT]:
    """
    Materialize links and enforce only the local maximum target count.

    Singleton policies reject more than one entry with ValueError; empty buckets are
    valid. Duplicate entries count separately. Link types, target validity and
    uniqueness across containers are not checked.

    Example:
        >>> link = RelationLink(target='History')
        >>> result = validate_relation_link_cardinality('tags', [link], RelationCardinality.ONE_TO_ONE)
        >>> result[0] is link
        True
        >>> validate_relation_link_cardinality('tags', [], RelationCardinality.ONE_TO_ONE)
        []


    :param relation_key: Relation name included in an error message.
    :param links: Iterable consumed once into a list.
    :param cardinality: Canonical enum governing whether several targets are allowed.
    :return: New list retaining the supplied link objects in order.
    """

    link_list = list(links)
    if not cardinality.allows_many_targets and len(link_list) > 1:
        raise ValueError(
            "Relation key {!r} has cardinality {!s} and accepts at most one target.".format(
                relation_key,
                cardinality.value,
            )
        )
    return link_list


def select_primary_relation_link(
    links: Iterable[RelationLinkT],
) -> RelationLinkT | None:
    """
    Choose a preferred shared link without changing flags or validating cardinality.

    Materialize the iterable, then prefer truthy primary flags, lower optional priority,
    lower optional index and original position. Missing order values sort last; other
    values follow _optional_order_value.

    Example:
        >>> first = RelationLink(target='First', priority=0)
        >>> primary = RelationLink(target='Preferred', primary=True, priority=9)
        >>> select_primary_relation_link([first, primary]) is primary
        True
        >>> select_primary_relation_link([]) is None
        True


    :param links: Iterable of objects with optional primary, priority and index
        attributes.
    :return: Selected link, or None for an empty iterable.
    """

    link_list = list(links)
    if not link_list:
        return None
    return min(
        enumerate(link_list),
        key=lambda item: _primary_relation_sort_key(item[0], item[1]),
    )[1]


def _primary_relation_sort_key(index: int, link) -> tuple:
    """
    Build the primary, priority, index and input-position selection key.

    Absent attributes count as None. Primary uses bool truthiness, so nonempty text such
    as false is treated as primary.

    Example:
        >>> a = RelationLink(target='A', primary=True)
        >>> b = RelationLink(target='B', primary=False)
        >>> _primary_relation_sort_key(1, a) < _primary_relation_sort_key(0, b)
        True


    :param index: Original iterable position used as the final tie breaker.
    :param link: Object whose optional selection attributes are read.
    :return: Comparable tuple used for minimum selection.
    """
    return (
        0 if bool(getattr(link, "primary", None)) else 1,
        _optional_order_value(getattr(link, "priority", None)),
        _optional_order_value(getattr(link, "index", None)),
        index,
    )


def _optional_order_value(value) -> tuple[int, int, str]:
    """
    Rank missing values last and integer-convertible values numerically.

    None and empty text are missing. int conversion may truncate numbers; TypeError,
    ValueError or OverflowError falls back to a string at numeric rank zero. Other
    errors propagate.

    Example:
        >>> _optional_order_value(None), _optional_order_value('2'), _optional_order_value('later')
        ((1, 0, ''), (0, 2, ''), (0, 0, 'later'))


    :param value: Optional priority or index value.
    :return: Three-part ordering tuple: missing flag, integer rank and fallback text.
    """
    if value in (None, ""):
        return (1, 0, "")
    try:
        return (0, int(value), "")
    except (TypeError, ValueError, OverflowError):
        return (0, 0, str(value))


@runtime_checkable
class RelationLinkAPI(Protocol[RelationLinkTargetT]):
    """
    Describe a target and its durable relation metadata structurally.

    Implementations expose ordering, primary, provenance, policy, id, cardinality and
    extra fields. Runtime protocol checks test member presence, without validating
    annotations or field values.

    Example:
        >>> isinstance(RelationLink(target='History'), RelationLinkAPI)
        True
    """

    target: RelationLinkTargetT
    priority: Optional[int]
    primary: Optional[bool]
    type: Optional[RelationLinkType]
    origin: Optional[str]
    source: Optional[RelationLinkSource]
    policy: Optional[str]
    data: Optional[str]
    index: Optional[int | str]
    link_id: Optional[RelationLinkID]
    cardinality: Optional[RelationCardinality]
    extra: MutableMetadataRecord

    def __str__(self) -> str:
        """
        Require a human-readable relation-link representation.

        The concrete implementation controls formatting and which metadata is shown.

        Example:
            >>> str(RelationLink(target='History'))
            'RelationLink(target=History)'


        :return: Diagnostic link string.
        """
        ...


@runtime_checkable
class OneOneRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """
    Specify the one-to-one cardinality in the relation-link type contract.

    The Literal annotation describes the expected member; runtime structural checks do
    not validate that cardinality value or enforce multiplicity.

    Example:
        >>> link = RelationLink(target="History", cardinality=RelationCardinality.ONE_TO_ONE)
        >>> isinstance(link, OneOneRelationLinkAPI)
        True
    """

    cardinality: Literal[RelationCardinality.ONE_TO_ONE]


@runtime_checkable
class OneManyRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """
    Specify the one-to-many cardinality in the relation-link type contract.

    The Literal annotation describes the expected member; runtime structural checks do
    not validate that cardinality value or enforce multiplicity.

    Example:
        >>> link = RelationLink(target="History", cardinality=RelationCardinality.ONE_TO_MANY)
        >>> isinstance(link, OneManyRelationLinkAPI)
        True
    """

    cardinality: Literal[RelationCardinality.ONE_TO_MANY]


@runtime_checkable
class ManyOneRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """
    Specify the many-to-one cardinality in the relation-link type contract.

    The Literal annotation describes the expected member; runtime structural checks do
    not validate that cardinality value or enforce multiplicity.

    Example:
        >>> link = RelationLink(target="History", cardinality=RelationCardinality.MANY_TO_ONE)
        >>> isinstance(link, ManyOneRelationLinkAPI)
        True
    """

    cardinality: Literal[RelationCardinality.MANY_TO_ONE]


@runtime_checkable
class ManyManyRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """
    Specify the many-to-many cardinality in the relation-link type contract.

    The Literal annotation describes the expected member; runtime structural checks do
    not validate that cardinality value or enforce multiplicity.

    Example:
        >>> link = RelationLink(target="History", cardinality=RelationCardinality.MANY_TO_MANY)
        >>> isinstance(link, ManyManyRelationLinkAPI)
        True
    """

    cardinality: Literal[RelationCardinality.MANY_TO_MANY]


@dataclasses.dataclass(slots=True)
class RelationLink(Generic[RelationLinkTargetT]):
    """
    Store a backend-independent target and editable relation metadata.

    This slotted dataclass retains supplied targets and extra mappings by reference.
    Each omitted extra gets a fresh dictionary. Construction normalizes non-None
    cardinality but otherwise leaves values unvalidated.

    Example:
        >>> extra = {'source_entity_type': 'work'}
        >>> link = RelationLink(target='History', extra=extra)
        >>> link.extra is extra
        True
    """

    target: RelationLinkTargetT
    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[RelationLinkType] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int | str] = None
    extra: MutableMetadataRecord = dataclasses.field(default_factory=dict)
    link_id: Optional[RelationLinkID] = None
    cardinality: Optional[RelationCardinalityValue] = None
    source: Optional[RelationLinkSource] = None

    def __post_init__(self) -> None:
        """
        Normalize a supplied cardinality after dataclass initialization.

        None is retained; invalid cardinality strings raise ValueError. Other fields are not
        checked.

        Example:
            >>> link = RelationLink(target='History', cardinality='many-to-many')
            >>> link.cardinality is RelationCardinality.MANY_TO_MANY
            True


        :return: None.
        """
        if self.cardinality is not None:
            self.cardinality = normalize_relation_cardinality(self.cardinality)

    def __str__(self) -> str:
        """
        Render the target and populated id, type, source and priority fields.

        Target uses str; the optional fields use repr and are included whenever non-None.
        Primary, cardinality and other metadata are omitted.

        Example:
            >>> str(RelationLink(target='History', link_id=1, priority=0))
            'RelationLink(target=History, link_id=1, priority=0)'


        :return: Class-named diagnostic string.
        """
        pieces = [f"target={self.target}"]
        if self.link_id is not None:
            pieces.append(f"link_id={self.link_id!r}")
        if self.type is not None:
            pieces.append(f"type={self.type!r}")
        if self.source is not None:
            pieces.append(f"source={self.source!r}")
        if self.priority is not None:
            pieces.append(f"priority={self.priority!r}")
        return f"{self.__class__.__name__}({', '.join(pieces)})"


__all__ = [
    "ManyManyRelationLinkAPI",
    "ManyOneRelationLinkAPI",
    "OneManyRelationLinkAPI",
    "OneOneRelationLinkAPI",
    "RelationCardinality",
    "RelationCardinalityValue",
    "RelationLink",
    "RelationLinkAPI",
    "RelationLinkID",
    "RelationLinkSource",
    "normalize_relation_cardinality",
    "select_primary_relation_link",
    "validate_relation_link_cardinality",
]
