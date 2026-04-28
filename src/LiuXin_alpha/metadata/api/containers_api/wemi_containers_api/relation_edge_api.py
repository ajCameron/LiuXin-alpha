"""Shared metadata relation-edge API types."""

from __future__ import annotations

import dataclasses

from collections.abc import Iterable
from enum import StrEnum
from typing import Generic, Literal, Optional, Protocol, TypeAlias, TypeVar, runtime_checkable

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
    RelationEdgeType,
    RelationTarget,
)

RelationEdgeID: TypeAlias = int | str
RelationEdgeSource: TypeAlias = str


class RelationCardinality(StrEnum):
    """Cardinality for a relation edge from the current container to targets."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"

    @property
    def allows_many_targets(self) -> bool:
        """Return whether this container may expose multiple target edges."""

        return self in {self.ONE_TO_MANY, self.MANY_TO_MANY}


RelationCardinalityValue: TypeAlias = RelationCardinality | str
RelationEdgeTargetT = TypeVar("RelationEdgeTargetT", bound=RelationTarget)
RelationEdgeT = TypeVar("RelationEdgeT")


def normalize_relation_cardinality(
    cardinality: RelationCardinalityValue,
) -> RelationCardinality:
    """Normalize a string/cardinality value into the canonical enum."""

    if isinstance(cardinality, RelationCardinality):
        return cardinality
    normalized = cardinality.strip().lower().replace("-", "_")
    return RelationCardinality(normalized)


def validate_relation_edge_cardinality(
    relation: str,
    edges: Iterable[RelationEdgeT],
    cardinality: RelationCardinality,
) -> list[RelationEdgeT]:
    """Validate local target multiplicity for one relation collection."""

    edge_list = list(edges)
    if not cardinality.allows_many_targets and len(edge_list) > 1:
        raise ValueError(
            "Relation {!r} has cardinality {!s} and accepts at most one target.".format(
                relation,
                cardinality.value,
            )
        )
    return edge_list


@runtime_checkable
class RelationEdgeAPI(Protocol[RelationEdgeTargetT]):
    """Structural API for one durable link-table edge."""

    target: RelationEdgeTargetT
    priority: Optional[int]
    primary: Optional[bool]
    type: Optional[RelationEdgeType]
    origin: Optional[str]
    source: Optional[RelationEdgeSource]
    policy: Optional[str]
    data: Optional[str]
    index: Optional[int | str]
    edge_id: Optional[RelationEdgeID]
    cardinality: Optional[RelationCardinality]
    extra: MutableMetadataRecord


@runtime_checkable
class OneOneRelationEdgeAPI(RelationEdgeAPI[RelationEdgeTargetT], Protocol[RelationEdgeTargetT]):
    """Structural API for a one-to-one relation edge."""

    cardinality: Literal[RelationCardinality.ONE_TO_ONE]


@runtime_checkable
class OneManyRelationEdgeAPI(RelationEdgeAPI[RelationEdgeTargetT], Protocol[RelationEdgeTargetT]):
    """Structural API for a one-to-many relation edge."""

    cardinality: Literal[RelationCardinality.ONE_TO_MANY]


@runtime_checkable
class ManyOneRelationEdgeAPI(RelationEdgeAPI[RelationEdgeTargetT], Protocol[RelationEdgeTargetT]):
    """Structural API for a many-to-one relation edge."""

    cardinality: Literal[RelationCardinality.MANY_TO_ONE]


@runtime_checkable
class ManyManyRelationEdgeAPI(RelationEdgeAPI[RelationEdgeTargetT], Protocol[RelationEdgeTargetT]):
    """Structural API for a many-to-many relation edge."""

    cardinality: Literal[RelationCardinality.MANY_TO_MANY]


@dataclasses.dataclass(slots=True)
class RelationEdge(Generic[RelationEdgeTargetT]):
    """Backend-agnostic value object for one metadata relation edge."""

    target: RelationEdgeTargetT
    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[RelationEdgeType] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int | str] = None
    extra: MutableMetadataRecord = dataclasses.field(default_factory=dict)
    edge_id: Optional[RelationEdgeID] = None
    cardinality: Optional[RelationCardinalityValue] = None
    source: Optional[RelationEdgeSource] = None

    def __post_init__(self) -> None:
        if self.cardinality is not None:
            self.cardinality = normalize_relation_cardinality(self.cardinality)


__all__ = [
    "ManyManyRelationEdgeAPI",
    "ManyOneRelationEdgeAPI",
    "OneManyRelationEdgeAPI",
    "OneOneRelationEdgeAPI",
    "RelationCardinality",
    "RelationCardinalityValue",
    "RelationEdge",
    "RelationEdgeAPI",
    "RelationEdgeID",
    "RelationEdgeSource",
    "normalize_relation_cardinality",
    "validate_relation_edge_cardinality",
]
