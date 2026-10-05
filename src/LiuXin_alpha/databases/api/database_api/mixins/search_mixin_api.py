"""
Declare DatabaseSearchMixinAPI operations for database facade implementations.

Repeated declarations are retained; the later definition supplies the runtime member. Abstract bodies perform no backend work. Concrete behavior and its limitations are described for callers without changing that implementation.
"""

from __future__ import annotations

import abc
from typing import Any, Optional, Union, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseSearchMixinAPI(abc.ABC):
    """
    Specify Row searches, scans, value sets and grouped iteration.

    Implement every abstract member before instantiating this interface. Backend resource and transaction policies remain the concrete implementation responsibility.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseSearchMixinAPI)
        True
    """

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """
        Wrap every result of a single-column wrapper search in a facade Row.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Backend validation and coercion determine comparison behavior; the facade itself does not convert the value. Current SQLite equality search does not make None match SQL NULL.

        Example:
            For an open db, db.search("works", "work_title", "Example") performs the wrapper exact-match search and returns Rows.


        :param table: Target table name.
        :param column: Column to compare.
        :param search_term: Search value passed unchanged to the wrapper.
        :return: List of Rows, or [] when the backend finds no matches.
        """

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """
        Delegate a backend multi-column query and eagerly wrap every returned record.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No query rewriting or validation is added here. Available comparison operators and cross-table support depend on the driver; the legacy surface does not promise arbitrary joins.

        Example:
            On a driver supporting multi-column predicates, db.multi_column_search(predicates, iterator_return=True) still consumes the backend result into a Row list.


        :param search_index: Backend search specification, conventionally column/operator/value triples.
        :param iterator_return: Forward the backend iterator preference; this facade still returns a list.
        :return: Materialized list of Rows regardless of iterator_return.
        """

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """
        Return the non-iterator unique-value set for a column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Return the non-iterator unique-value set for a column.

        Example:
            For an open db, db.get_unique("work_title") requests the same set as db.get_values_set("work_title").


        :param target_column: Column identifier understood by the backend.
        :return: Set returned by get_values_set.
        """

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """
        Choose the driver unique-value set or iterator for a column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Values are returned directly without Row wrapping or facade conversion.

        Example:
            For an open db, values = db.get_values_set("work_title", iterator_return=False) collects distinct title values.


        :param target_column: Column identifier used to locate its table.
        :param iterator_return: True selects the driver iterator; False selects its set result.
        :return: Backend iterator or set of distinct values, according to iterator_return.
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: Union[int, str]) -> Optional["RowAPI"]:
        """
        Fetch one stored record and wrap it unless the wrapper returns a false value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Fetch one stored record and wrap it unless the wrapper returns a false value.

        Example:
            For an open db, row = db.get_row_from_id("works", work_id) retrieves that identity or None.


        :param table: Table to query.
        :param row_id: Row identity passed unchanged to the wrapper.
        :return: Row bound to this facade, or None when no record is returned.
        """

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """
        Wrap the record selected by the backend random-row helper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: There is no missing-result guard. Under the current SQLite backend, an empty table produces a Row with table=None whose row_id access fails.

        Example:
            For a nonempty works table, db.get_random_row("works") returns one existing work Row.


        :param table: Table from which to choose a record.
        :return: Row bound to this facade; an empty backend result can produce an untyped Row.
        """

    # Todo: Split this down into iterator and list

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """
        Return an unsorted Row iterator or an optionally sorted materialized list.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: List mode delegates sorting to the wrapper. Iterator mode rejects reverse=True or any non-None sort column before returning the generator.

        Example:
            For an open db, db.get_all_rows("works", iterator_return=False, sort_column="work_id", reverse=True) requests a descending list.


        :param table: Table to scan.
        :param iterator_return: True returns a lazy iterator; False materializes all records.
        :param sort_column: Optional sort column, supported only in list mode.
        :param reverse: Request descending order, supported only in list mode.
        :return: Row iterator or list according to iterator_return.
        :raises NotImplementedError: Sorting or reverse order is requested in iterator mode.
        """

    # Todo: Add chunk size
    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """
        Yield one Row list per distinct grouping value, optionally following cross-table links.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: For another target table, concatenate endpoints linked from each matching source Row without deduplication. Distinct-value order is backend-defined. Current SQLite equality search yields empty groups for NULL values.

        Example:
            For an open db, list(db.chunk_iterator("work_title")) groups works by distinct title values, with no promise of a fixed number of Rows per group.


        :param column: Grouping column converted to text and resolved to its owning table.
        :param target_table: Target table for returned Rows; None or the column table returns direct matches.
        :return: Generator of Row lists; groups can be empty and have no fixed size bound.
        """


    # ---------------------------------------------------------------------------------------------
    # Search / retrieval
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """
        Wrap every result of a single-column wrapper search in a facade Row.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Backend validation and coercion determine comparison behavior; the facade itself does not convert the value. Current SQLite equality search does not make None match SQL NULL.

        Example:
            For an open db, db.search("works", "work_title", "Example") performs the wrapper exact-match search and returns Rows.


        :param table: Target table name.
        :param column: Column to compare.
        :param search_term: Search value passed unchanged to the wrapper.
        :return: List of Rows, or [] when the backend finds no matches.
        """

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """
        Delegate a backend multi-column query and eagerly wrap every returned record.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No query rewriting or validation is added here. Available comparison operators and cross-table support depend on the driver; the legacy surface does not promise arbitrary joins.

        Example:
            On a driver supporting multi-column predicates, db.multi_column_search(predicates, iterator_return=True) still consumes the backend result into a Row list.


        :param search_index: Backend search specification, conventionally column/operator/value triples.
        :param iterator_return: Forward the backend iterator preference; this facade still returns a list.
        :return: Materialized list of Rows regardless of iterator_return.
        """

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """
        Return the non-iterator unique-value set for a column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Return the non-iterator unique-value set for a column.

        Example:
            For an open db, db.get_unique("work_title") requests the same set as db.get_values_set("work_title").


        :param target_column: Column identifier understood by the backend.
        :return: Set returned by get_values_set.
        """

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """
        Choose the driver unique-value set or iterator for a column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Values are returned directly without Row wrapping or facade conversion.

        Example:
            For an open db, values = db.get_values_set("work_title", iterator_return=False) collects distinct title values.


        :param target_column: Column identifier used to locate its table.
        :param iterator_return: True selects the driver iterator; False selects its set result.
        :return: Backend iterator or set of distinct values, according to iterator_return.
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """
        Fetch one stored record and wrap it unless the wrapper returns a false value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Fetch one stored record and wrap it unless the wrapper returns a false value.

        Example:
            For an open db, row = db.get_row_from_id("works", work_id) retrieves that identity or None.


        :param table: Table to query.
        :param row_id: Row identity passed unchanged to the wrapper.
        :return: Row bound to this facade, or None when no record is returned.
        """

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """
        Wrap the record selected by the backend random-row helper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: There is no missing-result guard. Under the current SQLite backend, an empty table produces a Row with table=None whose row_id access fails.

        Example:
            For a nonempty works table, db.get_random_row("works") returns one existing work Row.


        :param table: Table from which to choose a record.
        :return: Row bound to this facade; an empty backend result can produce an untyped Row.
        """

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """
        Return an unsorted Row iterator or an optionally sorted materialized list.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: List mode delegates sorting to the wrapper. Iterator mode rejects reverse=True or any non-None sort column before returning the generator.

        Example:
            For an open db, db.get_all_rows("works", iterator_return=False, sort_column="work_id", reverse=True) requests a descending list.


        :param table: Table to scan.
        :param iterator_return: True returns a lazy iterator; False materializes all records.
        :param sort_column: Optional sort column, supported only in list mode.
        :param reverse: Request descending order, supported only in list mode.
        :return: Row iterator or list according to iterator_return.
        :raises NotImplementedError: Sorting or reverse order is requested in iterator mode.
        """

    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """
        Yield one Row list per distinct grouping value, optionally following cross-table links.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: For another target table, concatenate endpoints linked from each matching source Row without deduplication. Distinct-value order is backend-defined. Current SQLite equality search yields empty groups for NULL values.

        Example:
            For an open db, list(db.chunk_iterator("work_title")) groups works by distinct title values, with no promise of a fixed number of Rows per group.


        :param column: Grouping column converted to text and resolved to its owning table.
        :param target_table: Target table for returned Rows; None or the column table returns direct matches.
        :return: Generator of Row lists; groups can be empty and have no fixed size bound.
        """
