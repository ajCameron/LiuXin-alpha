
"""
Search mixins support searching the database.

This can be in basic ways "get this row" or sophisticated ways "get this random row".
"""

from __future__ import annotations

from copy import deepcopy

from typing import TYPE_CHECKING, Optional, Any, Union, Iterable

from LiuXin_alpha.databases.row import Row

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


class DatabaseSearchMixin:
    """
    Mixes in search methods for the database.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO SEARCH THE DATABASE START HERE

    def search(self: "DatabaseAPI", table: str, column: str, search_term: Optional[str]) -> list["RowAPI"]:
        """
        Search the database for specific values.

        :param table: Table to search in
        :param column: Column within that table
        :param search_term: The thing to search with (will be coerced to Unicode)
        :return:
        """
        return [Row(row_dict=r, database=self) for r in self.driver_wrapper.search(table, column, search_term)]

    # Todo: This does not work
    def multi_column_search(self: "DatabaseAPI", search_index: Any, iterator_return: bool = False) -> list["RowAPI"]:
        """
        Takes an index of tuples and uses it to search the database.

        (or indexes - the method is not fussy provided it contains the required terms). Which
        can then be used to search the database.
        Tuples should take the form (column_name, binary_comparison_operator, target_value).
        Binary comparison operators can include the LIKE operator.
        Every tuple is joined together by an AND statement.
        Thus [(u'creator', u'=', u'David Weber'),(u'series',u'=',u'Honor Harrington')] becomes
        SELECT * FROM `creators` * WHERE creator = 'David Weber' AND series = 'Honor Harrington';
        # Todo: Which WILL NOT work
        :param search_index:
        :param iterator_return: Should the return be in the form of an iterator, on an index of row_dicts
        :return found_rows:
        """
        row_dicts = self.driver.direct_multi_column_search(search_index=search_index, iterator_return=iterator_return)
        return [Row(row_dict=r, database=self) for r in row_dicts]

    # Todo: We can probably do better with some abuse of typing
    def get_unique(self, target_column: str) -> set[Any]:
        """
        Returns all the values, in a set, for the target column.

        :param target_column:
        :return:
        """
        return self.get_values_set(target_column=target_column)

    def get_values_set(self: "DatabaseAPI", target_column, iterator_return=False):
        """
        Gets a set of the unique values that a particular column has.

        :param target_column: Which column should the unique values be extracted from?
        :param iterator_return: Should the function return an iterator or not?
        :return:
        """
        if iterator_return:
            return self.driver.direct_get_unique_values_iterator(target_column=target_column)
        else:
            return self.driver.direct_get_unique_values_set(target_column=target_column)

    def get_row_from_id(self: "DatabaseAPI", table: str, row_id: int) -> Optional["RowAPI"]:
        """
        Gets a row from its particular id.

        :param table: The table to search in
        :param row_id: The id of the row to search for
        :return row: A row with the relevant id - or None if the row can't be found
        """
        row_dict = self.driver_wrapper.get_row_from_id(table, row_id)
        if not row_dict:
            return None
        else:
            return Row(row_dict=row_dict, database=self)

    def get_random_row(self: "DatabaseAPI", table: str) -> "RowAPI":
        """
        Return a randomly chosen row from the given table

        :param table:
        :return:
        """
        row_dict = self.driver_wrapper.get_random_row(table=table)
        return Row(row_dict=row_dict, database=self)

    def get_all_rows(
            self: "DatabaseAPI",
            table: str,
            iterator_return: bool = True,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> Iterable["RowAPI"]:
        """
        Returns all rows from a given table in the database in the form of a list of Rows, or an iterator.

        Iterator_return is on by default, as otherwise the return could be very large.
        :param table:
        :param iterator_return:
        :param sort_column:
        :param reverse:
        :return:
        """
        if iterator_return:
            if reverse or sort_column is not None:
                raise NotImplementedError("Need to go back and work on the driver.")
            else:
                return self.__get_all_rows_iterator_return(table)
        else:
            row_dicts = self.driver_wrapper.get_all_rows(table, sort_column, reverse)
            return [Row(row_dict=r, database=self) for r in row_dicts]

    # Todo: Merge with the above - if we can
    def __get_all_rows_iterator_return(self: "DatabaseAPI", table: str) -> Iterable["RowAPI"]:
        """
        Helper function to get round one of the limitations of Python 2.7

        (that you can't have both a return and a yield statement in the same function. Can be merged into get_all_rows after upgrading.
        :param table:
        :return:
        """
        row_dict_iterator = self.driver.direct_get_row_dict_iterator(table)
        for row_dict in row_dict_iterator:
            yield Row(row_dict=row_dict, database=self)

    # Todo: Test
    def chunk_iterator(
            self: "DatabaseAPI",
            column: str,
            target_table: Optional[str] = None) -> Iterable[list["RowAPI"]]:
        """
        Iterates through a table retuning rows from it grouped by the grouping_column.

        :param column: Return will be grouped using this column
        :param target_table: The table to be grouped - if None will assume that the grouping column is in the
        target_table
        :return:
        """
        column = six_unicode(deepcopy(column))
        column_table = self.driver_wrapper.direct_identify_table_from_column(column)

        # Iterate over the table - yield rows from the table in chunks
        if target_table is None or (target_table == column_table):

            for unique_val in self.get_values_set(target_column=column, iterator_return=True):
                yield self.search(table=column_table, column=column, search_term=unique_val)

        elif target_table != column_table:

            # Iterate over the column. For each unique value in that column get the rows that correspond to it. Then
            # get all the rows in the other table linked to it - return them as a chunk
            for unique_val in self.get_values_set(target_column=column, iterator_return=True):
                return_rows = []
                for ct_row in self.search(table=column_table, column=column, search_term=unique_val):
                    return_rows += [
                        r for r in self.get_interlinked_rows(target_row=ct_row, secondary_table=target_table)
                    ]
                yield return_rows
