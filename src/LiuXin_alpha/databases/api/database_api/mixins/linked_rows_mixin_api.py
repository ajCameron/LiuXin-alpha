"""
Uncached direct linked-row queries and fingerprints.

Abstract declarations describe the concrete facade conventions; their bodies do not execute database operations. Same-table lookup returns the seed; it does not traverse self-link neighbors. Concrete seed coercion accepts Row objects or dictionaries rather than arbitrary RowAPI implementations.
"""

from __future__ import annotations

import abc
from typing import Any, Iterable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseLinkedRowsMixinAPI(abc.ABC):
    """
    Declare uncached direct linked-row queries and fingerprints.

    Implement all abstract members before instantiation. Same-table lookup returns the seed; it does not traverse self-link neighbors. Concrete seed coercion accepts Row objects or dictionaries rather than arbitrary RowAPI implementations.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseLinkedRowsMixinAPI)
        True
    """

    @abc.abstractmethod
    def get_linked_rows(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Return the seed for its own table or follow cross-table interlinks.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Same-table lookup ignores type_filter and does not query self-links. Cross-table results retain the interlink reader descending-priority order and duplicates. No cache or ownership check is introduced.

        Example:
            For an agent Row, db.get_linked_rows(agent, "agents") returns [agent]; db.get_linked_rows(agent, "works") follows its cross-table links.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper table name converted to text and validated first.
        :param type_filter: Optional exact relationship-type filter for cross-table queries.
        :return: Single-element seed list for the same table, otherwise the interlink reader endpoint list.
        """

    @abc.abstractmethod
    def get_first_linked_row(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Return the first linked result using the normal linked-row ordering.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The full linked list is retrieved before selecting its first element.

        Example:
            For an agent with linked works, first = db.get_first_linked_row(agent, "works") chooses the highest-priority result when priorities exist.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper target table.
        :param type_filter: Optional cross-table relationship-type filter.
        :return: First Row, or None for an empty result.
        """

    @abc.abstractmethod
    def get_linked_ids_set(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> set[Any]:
        """
        Collect distinct target ID-column values from linked Rows.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Resolve the target ID column even when the linked result list is empty. Ordering is discarded.

        Example:
            For an agent Row, db.get_linked_ids_set(agent, "works") returns the distinct IDs of its linked works.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper target table converted to text.
        :param type_filter: Optional cross-table type filter.
        :return: Set of stored target ID values, with ordering and duplicate values discarded.
        """

    @abc.abstractmethod
    def get_linked_fingerprint(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        *,
        target_tables: Optional[Iterable[str]] = None,
        type_filter: Optional[str] = None,
    ) -> set[str]:
        """
        Collect table-and-ID strings for requested direct linked targets.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The seed itself is included when its table is selected. This is a simple membership representation, not a cryptographic digest or transitive graph fingerprint; queries are not atomic as a group.

        Example:
            For an agent Row, db.get_linked_fingerprint(agent, target_tables=["agents", "works"]) includes its own agent identifier and each directly linked work identifier.


        :param seed_row: Concrete seed Row or row dictionary reused for each target query.
        :param target_tables: Target table iterable, or all main tables when None.
        :param type_filter: Optional cross-table type filter applied by each query.
        :return: Set of strings formatted as table_id; duplicates and ordering are discarded.
        """
