"""
Portable link lookup macros attached to a database facade.

``MacrosBase`` provides link retrieval in set, ordered-list and typed forms.
It is not yet the common ancestor of all backend macro implementations.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins._macro_state import MacroHostState
from typing import TYPE_CHECKING, Any
from LiuXin_alpha.databases.db_types.macro_types import LegacyLinkData
from LiuXin_alpha.errors import DatabaseIntegrityError

from collections import defaultdict

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


# Todo: Actually make this the base.


class MacrosBase(MacroHostState):
    """
    Hold a database facade for portable link retrieval.

    ``db`` supplies row lookup, link traversal and driver-wrapper column naming.

    Example:
        ``MacrosBase(db).get_link_data("works", "agents", work_id)`` returns
        the linked agent IDs without retaining duplicates.
    """

    def __init__(self, db: DatabaseAPI | None) -> None:
        """
        Retain the database facade without opening or closing connections.

        Example:
            ``macros = MacrosBase(db)`` keeps ``db`` as ``macros.db``.


        :param db: Database facade providing row/link access and a driver wrapper.
        :return: None; stores the facade.
        """
        self.db = db

    # Todo: This should be a dataclass return
    def get_link_data(
        self,
        table1: str,
        table2: str,
        table1_id: int,
        typed: bool = False,
        priority: bool = False,
    ) -> LegacyLinkData:
        """
        Collect linked IDs, optionally grouped by link type and preserving traversal
        order.

        Unordered results use sets; priority results use lists in the order returned by
        ``get_interlinked_rows`` without an additional sort. Typed results are
        defaultdicts whose missing groups create an empty set or list.

        Example:
            ``get_link_data("works", "agents", work_id, typed=True)`` groups agent IDs
            by the type column on each work/agent interlink row.


        :param table1: Source table containing the selected row.
        :param table2: Target table whose linked row IDs are collected.
        :param table1_id: ID used to look up the source row.
        :param typed: Whether to look up each interlink row and group by its type.
        :param priority: Whether to retain traversal order and duplicates in lists.
        :return: A set or list of IDs, or a defaultdict mapping link types to those
            containers.
        """
        table2_id_col = self._macro_database.driver_wrapper.get_id_column(table2)

        if not typed and not priority:
            table1_row = self._macro_database.get_row_from_id(table1, table1_id)
            if table1_row is None:
                raise DatabaseIntegrityError(
                    f"Cannot read links for missing row {table1}:{table1_id}"
                )
            linked_rows = self._macro_database.get_interlinked_rows(table1_row, table2)
            if any(row is None for row in linked_rows):
                raise DatabaseIntegrityError("Link refers to a missing target row")
            return set([lr[table2_id_col] for lr in linked_rows if lr is not None])

        elif not typed and priority:
            table1_row = self._macro_database.get_row_from_id(table1, table1_id)
            if table1_row is None:
                raise DatabaseIntegrityError(
                    f"Cannot read links for missing row {table1}:{table1_id}"
                )
            linked_rows = self._macro_database.get_interlinked_rows(table1_row, table2)
            if any(row is None for row in linked_rows):
                raise DatabaseIntegrityError("Link refers to a missing target row")
            return [lr[table2_id_col] for lr in linked_rows if lr is not None]

        elif typed and not priority:
            table1_row = self._macro_database.get_row_from_id(table1, table1_id)
            if table1_row is None:
                raise DatabaseIntegrityError(
                    f"Cannot read links for missing row {table1}:{table1_id}"
                )
            linked_rows = self._macro_database.get_interlinked_rows(table1_row, table2)
            if any(row is None for row in linked_rows):
                raise DatabaseIntegrityError("Link refers to a missing target row")

            link_table_type_col = self._macro_database.driver_wrapper.get_link_column(
                table1, table2, "type"
            )

            link_container: defaultdict[Any, set[int]] = defaultdict(set)
            for lr in linked_rows:
                if lr is None:
                    raise DatabaseIntegrityError("Link refers to a missing target row")
                tlr = self._macro_database.get_interlink_row(
                    primary_row=table1_row, secondary_row=lr
                )
                if tlr is None:
                    raise DatabaseIntegrityError(
                        "Cannot read the missing interlink row"
                    )
                link_container[tlr[link_table_type_col]].add(lr[table2_id_col])
            return link_container

        elif typed and priority:
            table1_row = self._macro_database.get_row_from_id(table1, table1_id)
            if table1_row is None:
                raise DatabaseIntegrityError(
                    f"Cannot read links for missing row {table1}:{table1_id}"
                )
            linked_rows = self._macro_database.get_interlinked_rows(table1_row, table2)
            if any(row is None for row in linked_rows):
                raise DatabaseIntegrityError("Link refers to a missing target row")

            link_table_type_col = self._macro_database.driver_wrapper.get_link_column(
                table1, table2, "type"
            )

            ordered_links: defaultdict[Any, list[int]] = defaultdict(list)
            for lr in linked_rows:
                if lr is None:
                    raise DatabaseIntegrityError("Link refers to a missing target row")
                tlr = self._macro_database.get_interlink_row(
                    primary_row=table1_row, secondary_row=lr
                )
                if tlr is None:
                    raise DatabaseIntegrityError(
                        "Cannot read the missing interlink row"
                    )
                ordered_links[tlr[link_table_type_col]].append(lr[table2_id_col])
            return ordered_links

        else:
            raise NotImplementedError
