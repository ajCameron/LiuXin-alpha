"""Pure API contracts for non-WEMI same-table relation containers.

Category: metadata main-table relation API.
This module defines tree/parent-child relation contracts for metadata-owned
lookup tables such as genres, subjects, and series.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import ClassVar, Generic, Protocol, TypeVar, runtime_checkable

from LiuXin_alpha.metadata.api.containers_api.main_table_containers_api.row_api import (
    GenreRowAPI,
    MetadataRowValue,
    MetadataTableRowAPI,
    SeriesRowAPI,
    SubjectRowAPI,
)


RowAPIT = TypeVar("RowAPIT", bound=MetadataTableRowAPI)
RelationAPIT = TypeVar("RelationAPIT", bound="InlineSelfRelationAPI[MetadataTableRowAPI]")


@runtime_checkable
class InlineSelfRelationAPI(Protocol[RowAPIT]):
    """Structural API for an inline parent/child link in one table."""

    ROW_TYPE: ClassVar[type[MetadataTableRowAPI]]
    TABLE_NAME: ClassVar[str]
    RELATION_NAME: ClassVar[str]
    CHILD_ID_COLUMN: ClassVar[str]
    PARENT_ID_COLUMN: ClassVar[str]
    POSITION_COLUMN: ClassVar[str | None]
    TREE_ID_COLUMN: ClassVar[str | None]

    child: RowAPIT
    parent: RowAPIT | None
    parent_id: int | None
    position: int | None
    tree_id: str | int | None
    source: str | None

    @property
    def child_id(self) -> int | None:
        """
        Primary id of the child row in this relation.

        :return:
        """

    @property
    def resolved_parent_id(self) -> int | None:
        """
        Parent id resolved from the parent row or direct parent-id field.

        :return:
        """

    def validate(self) -> None:
        """
        Validate that this same-table relation is internally consistent.

        :return:
        """

    def as_child_update_payload(self) -> dict[str, MetadataRowValue]:
        """
        Serialize fields needed to update the child row inline.

        :return:
        """

    def as_relation_payload(self) -> dict[str, MetadataRowValue]:
        """
        Serialize this relation as a relation-link payload.

        :return:
        """

    def __str__(self) -> str:
        """
        Return a compact human-readable relation summary.

        :return:
        """


@runtime_checkable
class GenreTreeRelationAPI(InlineSelfRelationAPI[GenreRowAPI], Protocol):
    child: GenreRowAPI
    parent: GenreRowAPI | None


@runtime_checkable
class SubjectTreeRelationAPI(InlineSelfRelationAPI[SubjectRowAPI], Protocol):
    child: SubjectRowAPI
    parent: SubjectRowAPI | None


@runtime_checkable
class SeriesTreeRelationAPI(InlineSelfRelationAPI[SeriesRowAPI], Protocol):
    child: SeriesRowAPI
    parent: SeriesRowAPI | None


@runtime_checkable
class SelfRelationsContainerAPI(Protocol[RelationAPIT]):
    """Structural API for a collection of same-table relation links."""

    def __iter__(self) -> Iterator[RelationAPIT]:
        """
        Iterate over contained relation links.

        :return:
        """

    def __len__(self) -> int:
        """
        Number of contained relation links.

        :return:
        """

    def relations(self) -> tuple[RelationAPIT, ...]:
        """
        Return contained relation links as an immutable tuple.

        :return:
        """

    def add_relation(self, relation: RelationAPIT) -> None:
        """
        Add one relation link to this container.

        :param relation:
        :return:
        """

    def roots(self) -> tuple[RelationAPIT, ...]:
        """
        Return root-level relations with no parent.

        :return:
        """

    def children_of(self, parent_id: int) -> tuple[RelationAPIT, ...]:
        """
        Return relation links whose parent id matches ``parent_id``.

        :param parent_id:
        :return:
        """

    def validate(self) -> None:
        """
        Validate every relation link in this container.

        :return:
        """

    def __str__(self) -> str:
        """
        Return a compact human-readable container summary.

        :return:
        """


@runtime_checkable
class GenreTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[GenreTreeRelationAPI],
    Protocol,
):
    """
    Represents a tree container in the genre metadata.
    """


@runtime_checkable
class SubjectTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SubjectTreeRelationAPI],
    Protocol,
):
    """
    Represents a tree container in the subject metadata.
    """


@runtime_checkable
class SeriesTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SeriesTreeRelationAPI],
    Protocol,
):
    """
    Represents a tree container in the series metadata.
    """
    pass


__all__ = [
    "GenreTreeRelationAPI",
    "GenreTreeRelationsContainerAPI",
    "InlineSelfRelationAPI",
    "SelfRelationsContainerAPI",
    "SeriesTreeRelationAPI",
    "SeriesTreeRelationsContainerAPI",
    "SubjectTreeRelationAPI",
    "SubjectTreeRelationsContainerAPI",
]
