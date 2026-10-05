

"""
Specify row, value-batch and whole-table deletion operations.

Boolean success results in the shared SQL backend describe execution, not
necessarily matched rows. Transaction and cleanup behavior varies by operation; the
abstract declarations add no rollback or cascade guarantees.
"""

import abc

from typing import Iterable, Any


class DriverDeleteMixinAPI(abc.ABC):
    """
    Specify row, value-batch and whole-table deletion operations.

    Boolean success results in the shared SQL backend describe execution, not
    necessarily matched rows. Transaction and cleanup behavior varies by operation; the
    abstract declarations add no rollback or cascade guarantees. Abstract members must
    be implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverDeleteMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_clear_table(self, target_table: str) -> bool:
        """
        Delete every row, commit, and count remaining rows before closing the connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Translate SQLite operational/integrity errors. The count is taken after an explicit
        commit and is not an atomic guarantee against concurrent inserts.

        Example:
            >>> driver.direct_clear_table("works")  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :return: Whether the follow-up row count is zero.
        """

    @abc.abstractmethod
    def direct_delete(
            self,
            target_table: str,
            column: str,
            value: Any,
            many: bool = False) -> bool:
        """
        Delete matching column values, optionally as repeated bound-value statements.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Only the table is validated; column identifiers must be trusted. The batch form
        stringifies values. Commit/close run even on errors, and duplicate error-path
        cleanup can mask a translated SQLite exception.

        Example:
            >>> driver.direct_delete("works", "work_title", "Example")  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param value: One bound value, or an iterable when ``many`` is true.
        :param many: Execute one deletion per supplied value instead of one scalar deletion.
        :return: ``True`` on successful execution, even if no row matched.
        """


    # Todo: Switch over to returning the affected ids?
    @abc.abstractmethod
    def direct_delete_many(
            self,
            target_table: str,
            column: str,
            values: Iterable[Any]) -> None:
        """
        Delegate repeated column-value deletion to ``direct_delete``.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_delete_many("works", "work_title", [1, 2])  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param values: Iterable of values stringified and bound by the batch deletion
            helper.
        :return: ``None``; the delegated boolean result is discarded.
        """

    @abc.abstractmethod
    def direct_delete_many_by_ids(self, target_table: str, row_ids: Iterable[int]) -> bool:
        """
        Delete rows by string-coerced IDs using executemany.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Commit/close run in finally. Handled SQLite errors also close first, so the second
        commit on a closed connection can mask the translated error. Successful execution
        does not verify how many rows matched.

        Example:
            >>> driver.direct_delete_many_by_ids("works", [1, 2])  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :param row_ids: Iterable of row IDs, each converted to a string binding.
        :return: ``True`` after successful execution, including no matches.
        """

    @abc.abstractmethod
    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """
        Delete one ID match and commit without closing the acquired connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Missing IDs still count as success. SQLite operational/integrity errors are
        translated, with commit attempted on error as well.

        Example:
            >>> driver.direct_delete_row_by_id("works", 1)  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :param row_id: ID value bound against the table's ID column.
        :return: ``True`` after successful execution.
        """
