"""
Delete rows or link tables through SQL driver primitives.

Row and value-batch deletes commit only on success and close their connections.
Whole-table and link-DDL helpers retain their documented operation boundaries.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from contextlib import closing
from typing import Any

from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    InputIntegrityError,
)
from LiuXin_alpha.utils.logging import default_log


class DeleteMixin:
    """
    Provide validated-table deletion and link-table removal helpers.

    Example:
        ``driver.direct_clear_table("books")`` removes all rows from an existing table.
    """

    def direct_delete_many_by_ids(
        self, target_table: str, row_ids: Iterable[int]
    ) -> bool:
        """Delete IDs atomically, rolling back every deletion if any row is protected.

        :param target_table: Validated existing table name.
        :param row_ids: Iterable of IDs; missing rows are harmless no-ops.
        :return: True after the complete batch commits.
        """
        id_column = self.direct_get_id_column(target_table)
        return self.direct_delete(target_table, id_column, row_ids, many=True)

    # Todo: Merge
    def direct_delete(
        self, target_table: str, column: str, value: Any, many: bool = False
    ) -> bool:
        """Delete matching rows in an owned transaction and always close the connection.

        A failed batch rolls back earlier deletions. Exception translation retains
        the original SQLite failure as its cause; cleanup cannot commit partial work.

        Example:
            A foreign-key failure on the second ID leaves both target rows intact.

        :param target_table: Existing table whose rows are deleted.
        :param column: Trusted match column in that table.
        :param value: Scalar match value, or iterable of values in batch mode.
        :param many: Execute one equality predicate per supplied value when true.
        :return: True after successful completion, including absent matches.
        """
        if not self.direct_validate_existing_table_name(target_table):
            raise InputIntegrityError(f"Table {target_table!r} not found in database")
        stmt = f'DELETE FROM "{target_table}" WHERE "{column}" = ?'
        try:
            with closing(self.get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    if many:
                        conn.executemany(stmt, ((item,) for item in value))
                    else:
                        conn.execute(stmt, (value,))
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Delete from {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Delete from {target_table!r} failed: {exc}"
            ) from exc
        return True

    # Todo: Standardize on "table" not "target_table"
    def direct_delete_many(self, target_table: str, column: str, values: Any) -> None:
        """
        Delegate repeated column-value deletion to ``direct_delete``.

        Example:
            ``driver.direct_delete_many("books", "book_id", [2, 4])`` deletes both IDs.


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param values: Iterable of values bound without string coercion by the batch
            deletion
            helper.
        :return: ``None``; the delegated boolean result is discarded.
        """
        self.direct_delete(
            target_table=target_table, column=column, value=values, many=True
        )

    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """Delete an ID and promptly release the operation's owned connection.

        :param target_table: Existing table containing the row.
        :param row_id: ID to delete; missing rows count as successful no-ops.
        :return: True after commit; errors roll back and preserve their cause.
        """
        id_column = self.direct_get_id_column(target_table)
        return self.direct_delete(target_table, id_column, row_id)

    def direct_clear_table(self, target_table: str) -> bool:
        """
        Delete every row, commit, and count remaining rows before closing the
        connection.

        Translate SQLite operational/integrity errors. The count is taken after an
        explicit commit and is not an atomic guarantee against concurrent inserts.

        Example:
            ``driver.direct_clear_table("books")`` returns whether its follow-up count
            is zero.


        :param target_table: Existing table name, validated by the driver.
        :return: Whether the follow-up row count is zero.
        """
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(
                err_str, "ERROR", ("target_table", target_table)
            )
            raise InputIntegrityError(err_str)

        # Lock the database (to stop anything being assigned into the space that has just been freed by the delete
        # between the delete and the check) - clear the table - check that there are actually no rows in the table
        conn = self.get_connection()

        row_count = None
        try:
            with conn:
                # Delete the row
                stmt = f"DELETE FROM {target_table};"
                conn.execute(stmt)
                conn.commit()

                # Check to see if there are actually any rows left in the table
                stmt = f"SELECT COUNT(*) FROM {target_table};"
                c = conn.cursor()
                for row in c.execute(stmt):
                    row_count = row[0]
                if row_count is None:
                    row_count = 0

        except sqlite3.OperationalError as e:
            err_str = "Unable to delete target row - OperationalError.\n"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("target_table", target_table)
            )
            raise DatabaseDriverError(err_str) from e

        except sqlite3.IntegrityError as e:
            err_str = "Unable to delete target row - IntegrityError.\n"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("target_table", target_table)
            )
            raise DatabaseIntegrityError(err_str) from e

        finally:
            conn.close()

        if row_count == 0:
            return True
        else:
            return False

    def direct_unlink_main_tables(
        self, primary_table: str, secondary_table: str
    ) -> None:
        """
        Drop the whole interlink table between two main tables and invalidate caches.

        This removes all link rows and types, not an individual row relationship.

        Example:
            ``driver.direct_unlink_main_tables("books", "tags")`` drops their interlink
            table.


        :param primary_table: First main-table name used to resolve the link table.
        :param secondary_table: Second main-table name used to resolve the link table.
        :return: ``None``.
        """
        table_name, column_name = self._get_link_table_name_col_name(
            primary_table, secondary_table
        )

        unlink_sqlite = f"""
        DROP TABLE {table_name};
        """

        self.direct_execute_sql(unlink_sqlite)
        self._zero_prop_cache()

    #
    # ----------------------------------------------------------------------------------------------------------------------
