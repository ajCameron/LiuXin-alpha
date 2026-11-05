"""
Wrap backend search results in Rows and provide set, iterator and grouped retrieval.

The facade passes query semantics to its wrapper/driver. List-returning methods eagerly
wrap each result; iterator helpers keep the database live while consumed. Grouping is by
distinct column value, not a bounded batch size.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database._state import DatabaseMixinState

from copy import deepcopy

from typing import TYPE_CHECKING, Optional, Any, Union, Iterator, Literal, overload

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.row_api import RowAPI
    from LiuXin_alpha.databases.db_types.query_types import SearchIndex


class DatabaseSearchMixin(DatabaseMixinState):
    """
    Compose Row-oriented retrieval on top of backend search and scan primitives.

    Example:
        For an open db, matches = db.search("works", "work_title", title) wraps each
        backend match as a Row bound to db.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO SEARCH THE DATABASE START HERE

    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """
        Wrap every result of a single-column wrapper search in a facade Row.

        Backend validation and coercion determine comparison behavior; the facade itself
        does not convert the value. Current SQLite equality search does not make None
        match SQL NULL.

        Example:
            For an open db, db.search("works", "work_title", "Example") performs the
            wrapper exact-match search and returns Rows.


        :param table: Target table name.
        :param column: Column to compare.
        :param search_term: Search value passed unchanged to the wrapper.
        :return: List of Rows, or [] when the backend finds no matches.
        """
        return [
            Row(row_dict=r, database=self._database)
            for r in self._database.driver_wrapper.search(table, column, search_term)
        ]

    # Todo: This does not work
    def multi_column_search(
        self, search_index: SearchIndex, iterator_return: bool = False
    ) -> list["RowAPI"]:
        """
        Delegate a backend multi-column query and eagerly wrap every returned record.

        No query rewriting or validation is added here. Available comparison operators
        and cross-table support depend on the driver; the legacy surface does not
        promise arbitrary joins.

        Example:
            On a driver supporting multi-column predicates,
            db.multi_column_search(predicates, iterator_return=True) still consumes the
            backend result into a Row list.


        :param search_index: Backend search specification, conventionally
            column/operator/value triples.
        :param iterator_return: Forward the backend iterator preference; this facade
            still returns a list.
        :return: Materialized list of Rows regardless of iterator_return.
        """
        row_dicts = self._database.driver.direct_multi_column_search(
            search_index=search_index, iterator_return=iterator_return
        )
        if row_dicts is None:
            return []
        return [Row(row_dict=r, database=self._database) for r in row_dicts]

    # Todo: We can probably do better with some abuse of typing
    def get_unique(self, target_column: str) -> set[Any]:
        """
        Return the non-iterator unique-value set for a column.

        Example:
            For an open db, db.get_unique("work_title") requests the same set as
            db.get_values_set("work_title").


        :param target_column: Column identifier understood by the backend.
        :return: Set returned by get_values_set.
        """
        return self.get_values_set(target_column=target_column)

    @overload
    def get_values_set(
        self, target_column: str, iterator_return: Literal[False] = False
    ) -> set[Any]: ...

    @overload
    def get_values_set(
        self, target_column: str, iterator_return: Literal[True]
    ) -> Iterator[Any]: ...

    @overload
    def get_values_set(
        self, target_column: str, iterator_return: bool
    ) -> set[Any] | Iterator[Any]: ...

    def get_values_set(
        self, target_column: str, iterator_return: bool = False
    ) -> set[Any] | Iterator[Any]:
        """
        Choose the driver unique-value set or iterator for a column.

        Values are returned directly without Row wrapping or facade conversion.

        Example:
            For an open db, values = db.get_values_set("work_title",
            iterator_return=False) collects distinct title values.


        :param target_column: Column identifier used to locate its table.
        :param iterator_return: True selects the driver iterator; False selects its set
            result.
        :return: Backend iterator or set of distinct values, according to
            iterator_return.
        """
        if iterator_return:
            return self._database.driver.direct_get_unique_values_iterator(
                target_column=target_column
            )
        else:
            return self._database.driver.direct_get_unique_values_set(
                target_column=target_column
            )

    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """
        Fetch one stored record and wrap it unless the wrapper returns a false value.

        Example:
            For an open db, row = db.get_row_from_id("works", work_id) retrieves that
            identity or None.


        :param table: Table to query.
        :param row_id: Row identity passed unchanged to the wrapper.
        :return: Row bound to this facade, or None when no record is returned.
        """
        row_dict = self._database.driver_wrapper.get_row_from_id(table, row_id)
        if not row_dict:
            return None
        else:
            return Row(row_dict=row_dict, database=self._database)

    def get_random_row(self, table: str) -> "RowAPI":
        """
        Wrap the record selected by the backend random-row helper.

        There is no missing-result guard. Under the current SQLite backend, an empty
        table produces a Row with table=None whose row_id access fails.

        Example:
            For a nonempty works table, db.get_random_row("works") returns one existing
            work Row.


        :param table: Table from which to choose a record.
        :return: Row bound to this facade; an empty backend result can produce an
            untyped Row.
        """
        row_dict = self._database.driver_wrapper.get_random_row(table=table)
        return Row(row_dict=row_dict, database=self._database)

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """
        Return an unsorted Row iterator or an optionally sorted materialized list.

        List mode delegates sorting to the wrapper. Iterator mode rejects reverse=True
        or any non-None sort column before returning the generator.

        Example:
            For an open db, db.get_all_rows("works", iterator_return=False,
            sort_column="work_id", reverse=True) requests a descending list.


        :param table: Table to scan.
        :param iterator_return: True returns a lazy iterator; False materializes all
            records.
        :param sort_column: Optional sort column, supported only in list mode.
        :param reverse: Request descending order, supported only in list mode.
        :return: Row iterator or list according to iterator_return.
        :raises NotImplementedError: Sorting or reverse order is requested in iterator
            mode.
        """
        if iterator_return:
            if reverse or sort_column is not None:
                raise NotImplementedError("Need to go back and work on the driver.")
            else:
                return self.__get_all_rows_iterator_return(table)
        else:
            row_dicts = self._database.driver_wrapper.get_all_rows(
                table, sort_column, reverse
            )
            return [Row(row_dict=r, database=self._database) for r in row_dicts]

    # Todo: Merge with the above - if we can
    def __get_all_rows_iterator_return(self, table: str) -> Iterator["RowAPI"]:
        """
        Lazily wrap each record yielded by the driver row-dictionary iterator.

        Backend access occurs during iteration; keep the owning database open while
        consuming it.

        Example:
            The public db.get_all_rows("works") returns this lazy wrapping path without
            materializing the whole table.


        :param table: Table to scan.
        :return: Generator of Rows bound to this facade.
        """
        row_dict_iterator = self._database.driver.direct_get_row_dict_iterator(table)
        for row_dict in row_dict_iterator:
            yield Row(row_dict=row_dict, database=self._database)

    # Todo: Test
    def chunk_iterator(
        self, column: str, target_table: Optional[str] = None
    ) -> Iterator[list["RowAPI | None"]]:
        """
        Yield one Row list per distinct grouping value, optionally following cross-table
        links.

        For another target table, concatenate endpoints linked from each matching source
        Row without deduplication. Distinct-value order is backend-defined. Current
        SQLite equality search yields empty groups for NULL values.

        Example:
            For an open db, list(db.chunk_iterator("work_title")) groups works by
            distinct title values, with no promise of a fixed number of Rows per group.


        :param column: Grouping column converted to text and resolved to its owning
            table.
        :param target_table: Target table for returned Rows; None or the column table
            returns direct matches.
        :return: Generator of Row lists; groups can be empty and have no fixed size
            bound.
        """
        column = six_unicode(deepcopy(column))
        column_table = self._database.driver_wrapper.identify_table_from_column(column)
        if column_table is None:
            raise InputIntegrityError(
                f"Cannot identify a table for grouping column {column!r}"
            )

        # Iterate over the table - yield rows from the table in chunks
        if target_table is None or (target_table == column_table):
            for unique_val in self.get_values_set(
                target_column=column, iterator_return=True
            ):
                chunk: list[RowAPI | None] = list(
                    self.search(
                        table=column_table, column=column, search_term=unique_val
                    )
                )
                yield chunk

        elif target_table != column_table:
            # Iterate over the column. For each unique value in that column get the rows that correspond to it. Then
            # get all the rows in the other table linked to it - return them as a chunk
            for unique_val in self.get_values_set(
                target_column=column, iterator_return=True
            ):
                return_rows = []
                for ct_row in self.search(
                    table=column_table, column=column, search_term=unique_val
                ):
                    return_rows += [
                        r
                        for r in self._database.get_interlinked_rows(
                            target_row=ct_row, secondary_table=target_table
                        )
                    ]
                yield return_rows
