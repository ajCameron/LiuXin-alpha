"""Pure API contracts for non-WEMI same-table relation containers."""

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
    """Structural API for an inline parent/child edge in one table."""

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
    def child_id(self) -> int | None: ...

    @property
    def resolved_parent_id(self) -> int | None: ...

    def validate(self) -> None: ...

    def as_child_update_payload(self) -> dict[str, MetadataRowValue]: ...

    def as_relation_payload(self) -> dict[str, MetadataRowValue]: ...


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
    """Structural API for a collection of same-table relation edges."""

    def __iter__(self) -> Iterator[RelationAPIT]: ...

    def __len__(self) -> int: ...

    def relations(self) -> tuple[RelationAPIT, ...]: ...

    def add_relation(self, relation: RelationAPIT) -> None: ...

    def roots(self) -> tuple[RelationAPIT, ...]: ...

    def children_of(self, parent_id: int) -> tuple[RelationAPIT, ...]: ...

    def validate(self) -> None: ...


@runtime_checkable
class GenreTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[GenreTreeRelationAPI],
    Protocol,
):
    pass


@runtime_checkable
class SubjectTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SubjectTreeRelationAPI],
    Protocol,
):
    pass


@runtime_checkable
class SeriesTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SeriesTreeRelationAPI],
    Protocol,
):
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
