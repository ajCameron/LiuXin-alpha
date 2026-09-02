"""Concrete self-relation containers for non-WEMI metadata rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, TypeVar

from ._row_base import MetadataTableRow, MetadataRowValue
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_container_string,
    compact_mapping_string,
)
from .genre_row import GenreRow
from .series_row import SeriesRow
from .subject_row import SubjectRow


RowT = TypeVar("RowT", bound=MetadataTableRow)
RelationT = TypeVar("RelationT", bound="InlineSelfRelation[MetadataTableRow]")


@dataclass(slots=True, kw_only=True)
class InlineSelfRelation(Generic[RowT]):
    """Relation between two rows in the same table, stored on the child row."""

    child: RowT
    parent: RowT | None = None
    parent_id: int | None = None
    position: int | None = None
    tree_id: str | int | None = None
    source: str | None = None

    ROW_TYPE: ClassVar[type[MetadataTableRow]]
    TABLE_NAME: ClassVar[str]
    RELATION_NAME: ClassVar[str]
    CHILD_ID_COLUMN: ClassVar[str]
    PARENT_ID_COLUMN: ClassVar[str]
    POSITION_COLUMN: ClassVar[str | None] = None
    TREE_ID_COLUMN: ClassVar[str | None] = None

    @classmethod
    def from_child_row(
        cls,
        child: RowT,
        *,
        parent: RowT | None = None,
        source: str | None = None,
    ) -> "InlineSelfRelation[RowT]":
        parent_id = cls._int_or_none(getattr(child, cls.PARENT_ID_COLUMN))
        position = (
            cls._int_or_none(getattr(child, cls.POSITION_COLUMN))
            if cls.POSITION_COLUMN is not None
            else None
        )
        tree_id = (
            cls._tree_id_or_none(getattr(child, cls.TREE_ID_COLUMN))
            if cls.TREE_ID_COLUMN is not None
            else None
        )
        return cls(
            child=child,
            parent=parent,
            parent_id=parent_id,
            position=position,
            tree_id=tree_id,
            source=source,
        )

    @property
    def child_id(self) -> int | None:
        return self.child.primary_id

    @property
    def resolved_parent_id(self) -> int | None:
        if self.parent is not None and self.parent.primary_id is not None:
            return self.parent.primary_id
        return self.parent_id

    def validate(self) -> None:
        if not isinstance(self.child, self.ROW_TYPE):
            raise TypeError(f"{self.RELATION_NAME} child must be {self.ROW_TYPE.__name__}")
        if self.parent is not None and not isinstance(self.parent, self.ROW_TYPE):
            raise TypeError(f"{self.RELATION_NAME} parent must be {self.ROW_TYPE.__name__}")
        if self.position is not None and self.position < 0:
            raise ValueError(f"{self.RELATION_NAME} position cannot be negative")

        child_parent_id = self._int_or_none(getattr(self.child, self.PARENT_ID_COLUMN))
        resolved_parent_id = self.resolved_parent_id
        if (
            child_parent_id is not None
            and resolved_parent_id is not None
            and child_parent_id != resolved_parent_id
        ):
            raise ValueError(
                f"{self.RELATION_NAME} parent id {resolved_parent_id} does not match child row parent id {child_parent_id}"
            )
        if self.child_id is not None and resolved_parent_id is not None:
            if self.child_id == resolved_parent_id:
                raise ValueError(f"{self.RELATION_NAME} cannot relate a row to itself")

    def as_child_update_payload(self) -> dict[str, MetadataRowValue]:
        payload: dict[str, MetadataRowValue] = {
            self.PARENT_ID_COLUMN: self.resolved_parent_id,
        }
        if self.POSITION_COLUMN is not None:
            payload[self.POSITION_COLUMN] = self.position
        if self.TREE_ID_COLUMN is not None:
            payload[self.TREE_ID_COLUMN] = self.tree_id
        return payload

    def as_relation_payload(self) -> dict[str, MetadataRowValue]:
        return {
            "relation_name": self.RELATION_NAME,
            "table_name": self.TABLE_NAME,
            "child_id": self.child_id,
            "parent_id": self.resolved_parent_id,
            "position": self.position,
            "tree_id": self.tree_id,
            "source": self.source,
        }

    def __str__(self) -> str:
        return compact_mapping_string(
            self,
            self.as_relation_payload(),
            id_keys=("child_id", "parent_id"),
            display_keys=("relation_name", "table_name"),
        )

    @staticmethod
    def _int_or_none(value: MetadataRowValue) -> int | None:
        return value if type(value) is int else None

    @staticmethod
    def _tree_id_or_none(value: MetadataRowValue) -> str | int | None:
        return value if type(value) in {str, int} else None


@dataclass(slots=True, kw_only=True)
class GenreTreeRelation(InlineSelfRelation[GenreRow]):
    """
    Represent the parent, position, and tree identity of one Genre row.
    """
    ROW_TYPE: ClassVar[type[MetadataTableRow]] = GenreRow
    TABLE_NAME: ClassVar[str] = "genres"
    RELATION_NAME: ClassVar[str] = "genre_tree_parent"
    CHILD_ID_COLUMN: ClassVar[str] = "genre_id"
    PARENT_ID_COLUMN: ClassVar[str] = "genre_parent_id"
    POSITION_COLUMN: ClassVar[str | None] = "genre_position"
    TREE_ID_COLUMN: ClassVar[str | None] = "genre_tree_id"


@dataclass(slots=True, kw_only=True)
class SubjectTreeRelation(InlineSelfRelation[SubjectRow]):
    """
    Represent the parent, position, and tree identity of one Subject row.
    """
    ROW_TYPE: ClassVar[type[MetadataTableRow]] = SubjectRow
    TABLE_NAME: ClassVar[str] = "subjects"
    RELATION_NAME: ClassVar[str] = "subject_tree_parent"
    CHILD_ID_COLUMN: ClassVar[str] = "subject_id"
    PARENT_ID_COLUMN: ClassVar[str] = "subject_parent_id"
    POSITION_COLUMN: ClassVar[str | None] = "subject_parent_position"
    TREE_ID_COLUMN: ClassVar[str | None] = "subject_tree_id"


@dataclass(slots=True, kw_only=True)
class SeriesTreeRelation(InlineSelfRelation[SeriesRow]):
    """
    Represent the parent, position, and tree identity of one Series row.
    """
    ROW_TYPE: ClassVar[type[MetadataTableRow]] = SeriesRow
    TABLE_NAME: ClassVar[str] = "series"
    RELATION_NAME: ClassVar[str] = "series_tree_parent"
    CHILD_ID_COLUMN: ClassVar[str] = "series_id"
    PARENT_ID_COLUMN: ClassVar[str] = "series_parent_id"
    POSITION_COLUMN: ClassVar[str | None] = "series_parent_position"
    TREE_ID_COLUMN: ClassVar[str | None] = "series_tree_id"


@dataclass(slots=True, kw_only=True)
class SelfRelationsContainer(Generic[RelationT]):
    """Small editable collection for same-table relation links."""

    _relations: list[RelationT] = field(default_factory=list)

    def __iter__(self) -> Iterator[RelationT]:
        return iter(self._relations)

    def __len__(self) -> int:
        return len(self._relations)

    def relations(self) -> tuple[RelationT, ...]:
        return tuple(self._relations)

    def add_relation(self, relation: RelationT) -> None:
        relation.validate()
        child_id = relation.child_id
        if child_id is not None:
            for existing in self._relations:
                if existing.child_id == child_id:
                    raise ValueError(f"Duplicate self-relation for child id {child_id}")
        self._relations.append(relation)

    def roots(self) -> tuple[RelationT, ...]:
        return tuple(
            relation
            for relation in self._relations
            if relation.resolved_parent_id is None
        )

    def children_of(self, parent_id: int) -> tuple[RelationT, ...]:
        return tuple(
            relation
            for relation in self._relations
            if relation.resolved_parent_id == parent_id
        )

    def validate(self) -> None:
        seen_child_ids: set[int] = set()
        for relation in self._relations:
            relation.validate()
            child_id = relation.child_id
            if child_id is None:
                continue
            if child_id in seen_child_ids:
                raise ValueError(f"Duplicate self-relation for child id {child_id}")
            seen_child_ids.add(child_id)

    def __str__(self) -> str:
        return compact_container_string(self, count_label="relations")


@dataclass(slots=True, kw_only=True)
class GenreTreeRelationsContainer(SelfRelationsContainer[GenreTreeRelation]):
    """
    Collect and validate Genre parent links as an editable forest.
    """
    pass


@dataclass(slots=True, kw_only=True)
class SubjectTreeRelationsContainer(SelfRelationsContainer[SubjectTreeRelation]):
    """
    Collect and validate Subject parent links as an editable forest.
    """
    pass


@dataclass(slots=True, kw_only=True)
class SeriesTreeRelationsContainer(SelfRelationsContainer[SeriesTreeRelation]):
    """
    Collect and validate Series parent links as an editable forest.
    """
    pass


NON_WEMI_SELF_RELATION_CONTAINERS = (
    GenreTreeRelation,
    SubjectTreeRelation,
    SeriesTreeRelation,
)


__all__ = [
    "GenreTreeRelation",
    "GenreTreeRelationsContainer",
    "InlineSelfRelation",
    "NON_WEMI_SELF_RELATION_CONTAINERS",
    "SelfRelationsContainer",
    "SeriesTreeRelation",
    "SeriesTreeRelationsContainer",
    "SubjectTreeRelation",
    "SubjectTreeRelationsContainer",
]
