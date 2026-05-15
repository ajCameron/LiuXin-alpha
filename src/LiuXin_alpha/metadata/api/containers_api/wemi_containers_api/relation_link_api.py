"""Shared metadata relation-link API types."""

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
    """Cardinality for a relation link from the current container to targets."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"

    @property
    def allows_many_targets(self) -> bool:
        """Return whether this container may expose multiple target links."""

        return self in {self.ONE_TO_MANY, self.MANY_TO_MANY}


RelationCardinalityValue: TypeAlias = RelationCardinality | str
RelationLinkTargetT = TypeVar("RelationLinkTargetT", bound=RelationTarget)
RelationLinkT = TypeVar("RelationLinkT")


def normalize_relation_cardinality(
    cardinality: RelationCardinalityValue,
) -> RelationCardinality:
    """Normalize a string/cardinality value into the canonical enum."""

    if isinstance(cardinality, RelationCardinality):
        return cardinality
    normalized = cardinality.strip().lower().replace("-", "_")
    return RelationCardinality(normalized)


def validate_relation_link_cardinality(
    relation_key: str,
    links: Iterable[RelationLinkT],
    cardinality: RelationCardinality,
) -> list[RelationLinkT]:
    """Validate local target multiplicity for one relation key."""

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
    Select the preferred link without enforcing singleton cardinality.

    Selection is deterministic: explicit ``primary`` links win first, then
    lower ``priority``, lower ``index``, then original order.
    """

    link_list = list(links)
    if not link_list:
        return None
    return min(
        enumerate(link_list),
        key=lambda item: _primary_relation_sort_key(item[0], item[1]),
    )[1]


def _primary_relation_sort_key(index: int, link) -> tuple:
    return (
        0 if bool(getattr(link, "primary", None)) else 1,
        _optional_order_value(getattr(link, "priority", None)),
        _optional_order_value(getattr(link, "index", None)),
        index,
    )


def _optional_order_value(value) -> tuple[int, int, str]:
    if value in (None, ""):
        return (1, 0, "")
    try:
        return (0, int(value), "")
    except (TypeError, ValueError, OverflowError):
        return (0, 0, str(value))


@runtime_checkable
class RelationLinkAPI(Protocol[RelationLinkTargetT]):
    """Structural API for one durable relation link."""

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

    def __str__(self) -> str: ...


@runtime_checkable
class OneOneRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """Structural API for a one-to-one relation link."""

    cardinality: Literal[RelationCardinality.ONE_TO_ONE]


@runtime_checkable
class OneManyRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """Structural API for a one-to-many relation link."""

    cardinality: Literal[RelationCardinality.ONE_TO_MANY]


@runtime_checkable
class ManyOneRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """Structural API for a many-to-one relation link."""

    cardinality: Literal[RelationCardinality.MANY_TO_ONE]


@runtime_checkable
class ManyManyRelationLinkAPI(RelationLinkAPI[RelationLinkTargetT], Protocol[RelationLinkTargetT]):
    """Structural API for a many-to-many relation link."""

    cardinality: Literal[RelationCardinality.MANY_TO_MANY]


@dataclasses.dataclass(slots=True)
class RelationLink(Generic[RelationLinkTargetT]):
    """Backend-agnostic value object for one metadata relation link."""

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
        if self.cardinality is not None:
            self.cardinality = normalize_relation_cardinality(self.cardinality)

    def __str__(self) -> str:
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
