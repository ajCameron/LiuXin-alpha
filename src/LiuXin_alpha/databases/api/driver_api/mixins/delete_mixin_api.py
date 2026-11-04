

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
        Delete every row, commit, and count remaining rows before closing the
        connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Translate SQLite operational/integrity errors. The count is taken after an
        explicit commit and is not an atomic guarantee against concurrent inserts.

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
        Delete matching rows in an owned transaction and always close the connection.

        A failed batch rolls back earlier deletions. Exception translation retains the
        original SQLite failure as its cause; cleanup cannot commit partial work.

        Example:
            A foreign-key failure on the second ID leaves both target rows intact.

        :param target_table: Existing table whose rows are deleted.
        :param column: Trusted match column in that table.
        :param value: Scalar match value, or iterable of values in batch mode.
        :param many: Execute one equality predicate per supplied value when true.
        :return: True after successful completion, including absent matches.
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
            >>> driver.direct_delete_many(
            ...     "works", "work_title", [1, 2]
            ... )  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param values: Iterable of values bound without string coercion by the batch
            deletion
            helper.
        :return: ``None``; the delegated boolean result is discarded.
        """

    @abc.abstractmethod
    def direct_delete_many_by_ids(self, target_table: str, row_ids: Iterable[int]) -> bool:
        """
        Delete IDs atomically, rolling back every deletion if any row is protected.

        :param target_table: Validated existing table name.
        :param row_ids: Iterable of IDs; missing rows are harmless no-ops.
        :return: True after the complete batch commits.
        """

    @abc.abstractmethod
    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """
        Delete an ID and promptly release the operation's owned connection.

        :param target_table: Existing table containing the row.
        :param row_id: ID to delete; missing rows count as successful no-ops.
        :return: True after commit; errors roll back and preserve their cause.
        """
