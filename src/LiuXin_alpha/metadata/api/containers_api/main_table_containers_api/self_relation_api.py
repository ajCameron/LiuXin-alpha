"""
Define contracts for parent-child relations stored on non-WEMI child rows.

Genre, subject and series trees expose inline update payloads and editable relation
collections. Validation checks local link consistency and duplicate child ids; it
does not promise graph-wide cycle detection.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
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
    """
    Describe a same-table link with child, optional parent and inline tree metadata.

    The resolved parent id prefers a present parent row id over the direct parent_id
    hint. Payload construction alone does not validate or write the relation.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """

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
        Read the child row's primary database id.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Child id, or None for an unpersisted or invalid id.
        """

    @property
    def resolved_parent_id(self) -> int | None:
        """
        Prefer the parent row's id, falling back to the direct parent_id hint.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Resolved parent id, or None for a root link.
        """

    def validate(self) -> None:
        """
        Check row types, nonnegative position, parent-id consistency and self-parenting.

        Concrete links raise TypeError for the wrong row family and ValueError for invalid
        relation values. Longer graph cycles are not checked here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: None when the link is valid.
        """

    def as_child_update_payload(self) -> dict[str, MetadataRowValue]:
        """
        Build inline parent, position and tree-id column values for the child.

        Only supported optional columns are emitted; this neither validates nor writes the
        child row.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: New column-to-value update dictionary.
        """

    def as_relation_payload(self) -> dict[str, MetadataRowValue]:
        """
        Describe the link using relation/table names, ids, position, tree id and source.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: New relation payload dictionary.
        """

    def __str__(self) -> str:
        """
        Render a compact description of the child and resolved parent link.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Human-readable relation summary.
        """


@runtime_checkable
class GenreTreeRelationAPI(InlineSelfRelationAPI[GenreRowAPI], Protocol):
    """
    Structural contract for an inline Genre parent relation.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """
    child: GenreRowAPI
    parent: GenreRowAPI | None


@runtime_checkable
class SubjectTreeRelationAPI(InlineSelfRelationAPI[SubjectRowAPI], Protocol):
    """
    Structural contract for an inline Subject parent relation.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """
    child: SubjectRowAPI
    parent: SubjectRowAPI | None


@runtime_checkable
class SeriesTreeRelationAPI(InlineSelfRelationAPI[SeriesRowAPI], Protocol):
    """
    Structural contract for an inline Series parent relation.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """
    child: SeriesRowAPI
    parent: SeriesRowAPI | None


@runtime_checkable
class SelfRelationsContainerAPI(Protocol[RelationAPIT]):
    """
    Describe an ordered, editable collection of same-table parent links.

    Tuple access protects collection structure, while contained relation objects remain
    mutable.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """

    def __iter__(self) -> Iterator[RelationAPIT]:
        """
        Iterate over stored relation objects in collection order.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Iterator over the current relation objects.
        """

    def __len__(self) -> int:
        """
        Count stored relation links.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Number of links in the collection.
        """

    def relations(self) -> tuple[RelationAPIT, ...]:
        """
        Snapshot the collection order without copying relation objects.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Tuple containing the current links.
        """

    def add_relation(self, relation: RelationAPIT) -> None:
        """
        Validate and append one relation link.

        Concrete collections reject duplicate non-None child ids with ValueError; multiple
        id-less children are permitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :param relation: Link to validate and append; the existing object is retained.
        :return: None.
        """

    def roots(self) -> tuple[RelationAPIT, ...]:
        """
        Select links whose resolved parent id is None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Tuple of root links in collection order.
        """

    def children_of(self, parent_id: int) -> tuple[RelationAPIT, ...]:
        """
        Select links whose resolved parent id equals the requested id.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :param parent_id: Parent database id to match against each resolved_parent_id.
        :return: Tuple of matching child links in collection order.
        """

    def validate(self) -> None:
        """
        Validate each link and reject duplicate non-None child ids.

        This checks local consistency rather than performing graph-wide cycle detection.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: None when every link and child id is valid.
        """

    def __str__(self) -> str:
        """
        Render a compact description of the relation collection.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py


        :return: Human-readable summary including the relation count.
        """


@runtime_checkable
class GenreTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[GenreTreeRelationAPI],
    Protocol,
):
    """
    Structural contract for an editable forest of Genre parent relations.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """


@runtime_checkable
class SubjectTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SubjectTreeRelationAPI],
    Protocol,
):
    """
    Structural contract for an editable forest of Subject parent relations.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
    """


@runtime_checkable
class SeriesTreeRelationsContainerAPI(
    SelfRelationsContainerAPI[SeriesTreeRelationAPI],
    Protocol,
):
    """
    Structural contract for an editable forest of Series parent relations.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_non_wemi_container_api.py
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
