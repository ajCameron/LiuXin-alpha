
"""
Search mixin allows for searching for assets on the database.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional


class DriverWrapperSearchMixin:
    """
    Adds search methods to the driver wrapper.
    """

    def get_all_hashes(self) -> set[str]:
        """
        Returns a set of all the hashes on the database.

        :return:
        """
        return self.driver.direct_get_all_hashes()

    def get_random_row(self, table: str, direct_access: bool = False) -> dict[str, Any]:
        """
        Returns a random row from the given table.

        Note DO NOT USE THIS WHEN CONSTRUCTING TEST DATABASES/FSM!
        The results of this method is platform dependant - so the test object will not be reproducible. This is rarely
        helpful.
        :param table:
        :param row_dict:
        :param direct_access:
        :return:
        """
        return self.driver.direct_get_random_row_dict(target_table=table, direct=direct_access)


    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO SEARCH THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_row_from_id(self, table: str, row_id: int) -> dict[str, Any]:
        """
        Gets a row_dict directly from the DatabasePing.

        :param table:
        :param row_id:
        :return:
        """
        return self.driver.direct_get_row_dict_from_id(table, row_id)

    def get_all_rows(self, table: str, sort_column: str = None, reverse: bool = False) -> Iterable[dict[str, Any]]:
        """
        Gets a list of all the rows dicts in the given table from the database.

        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """
        return self.driver.direct_get_all_rows(table, sort_column, reverse)

    def read(self, table: str, sort_column: Optional[str] = None, reverse: bool = False) -> Iterable[dict[str, Any]]:
        """
        Compatibility convenience alias for fetching all rows from a table.

        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """
        return self.get_all_rows(table, sort_column=sort_column, reverse=reverse)

    def search(self, table: str, column: str, search_term: str) -> Iterable[dict[str, Any]]:
        """
        Searches a specified column in a table by the given search term. Returns all rows which match that term.

        :param table:
        :param column:
        :param search_term:
        :return:
        """
        return self.driver.direct_search_table(table, column, search_term)

