from __future__ import annotations

import abc
from typing import Optional, Union, Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseInterlinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseInterlinkRowsMixin``."""

    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Return the interlink row - if there is one - linking the two given rows.

        :param primary_row:
        :param secondary_row:
        :param onelink:
        :return:
        """

    # Todo: get_interlink_rows and get_interlinked_rows seem similar
    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """
        Return all the interlink rows between the primary row and an entire secondary table.

        :param primary_row:
        :param secondary_table:
        :return:
        """

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        primary_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """
        Get the interlinked rows between the primary row and an entire secondary table.

        :param primary_row:
        :param secondary_table:
        :param type_filter:
        :param kwargs:
        :return:
        """

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """
        Get the interlink values from the secondary table for the target_row.

        :param target_row:
        :param secondary_column:
        :return:
        """

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        ...

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """
        Dupe the interlinks from one row onto another.

        :param src_row:
        :param dst_row:
        :param swap_priorities:
        :param restrict_to_tables:
        :param force_priority:
        :return:
        """

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """
        Swap the priorities of two rows linked to the src_row.

        :param src_row:
        :param dst_row_1:
        :param dst_row_2:
        :return:
        """

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Update the interlink values of the link between two rows.

        :param primary_row:
        :param secondary_row:
        :param priority:
        :param col_value_pairs:
        :return:
        """

    @abc.abstractmethod
    def update_interlink_priority(
            self,
            primary_row: "RowAPI",
            secondary_table: str,
            ordered_ids: Iterable[int]) -> None:
        """
        Update the priority of an interlink connecting two rows.

        :param primary_row:
        :param secondary_table:
        :param ordered_ids:
        :return:
        """

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Unlink two interlinked rows.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """
        Unlink all rows connecting the primary row and the secondary table.

        :param primary_row:
        :param secondary_table:
        :param type_filter:
        :return:
        """


    # ---------------------------------------------------------------------------------------------
    # Interlink tables (many-to-many between two *different* tables)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """Return the interlink row(s) connecting two rows from different tables."""

    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """Return interlink rows connecting primary_row to any row in secondary_table."""

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        primary_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """Return the rows in secondary_table linked to target_row (optionally filtering by type)."""

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """Return the values from a secondary column across all interlinks from target_row."""

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """Create an interlink between two rows and return the created interlink row."""

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """Duplicate interlinks from src_row to dst_row."""

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """Swap interlink priorities between dst_row_1 and dst_row_2 for the link anchored at src_row."""

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """Update (or create) an interlink between two rows (priority-aware)."""

    @abc.abstractmethod
    def update_interlink_priority(
        self,
        primary_row: "RowAPI",
        secondary_table: str,
        ordered_ids: Iterable[int],
    ) -> None:
        """Rewrite interlink priorities for primary_row -> secondary_table according to ordered secondary ids."""

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """Remove an interlink between two rows."""

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """Remove all interlinks from primary_row to rows in secondary_table (optionally filtering by type)."""


