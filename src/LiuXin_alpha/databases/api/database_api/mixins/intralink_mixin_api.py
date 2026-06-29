from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseIntralinkRowsMixin``.

    This is responsible for dealing with intalink rows - rows which link a table back to itself.
    """

    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """
        Make a link between the primary row and the secondary rows with given type.

        :param primary_row:
        :param secondary_row:
        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """
        Get the interlink row - if any - linking the primary row and the secondary row.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    # Todo: Merge with the method below
    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Get all rows intralinked to the primary row.

        :param row:
        :param primary:
        :param secondary:
        :param link_type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """
        Get the intralink rows with a type filter.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """
        Unlink an interlink between the primary row and the secondary row - if there is one.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    # Todo: unlink_all_intralinks

    # ---------------------------------------------------------------------------------------------
    # Intralink tables (many-to-many within the *same* table)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """Create an intralink between two rows from the same table."""

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """Return the intralink row connecting two rows (or None if not present)."""

    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """Return intralink rows linked to the given row (primary/secondary direction flags)."""

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """Return rows intralinked to primary_row/secondary_row (direction depends on driver_wrapper policy)."""

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """Remove an intralink between two rows."""

