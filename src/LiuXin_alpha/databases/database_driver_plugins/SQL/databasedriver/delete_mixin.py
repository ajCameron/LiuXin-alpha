
"""
Delete rows or link tables through SQL driver primitives.

These helpers have distinct commit/close behavior; callers should not assume batch failures roll back.
"""

from __future__ import annotations

import sqlite3

from typing import Iterable, Optional, Any

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, DatabaseDriverError


class DeleteMixin:
    """
    Provide validated-table deletion and link-table removal helpers.

    Example:
        ``driver.direct_clear_table("books")`` removes all rows from an existing table.
    """

    def direct_delete_many_by_ids(self, target_table: str, row_ids: Iterable[int]) -> bool:
        """
        Delete rows by string-coerced IDs using executemany.

        Commit/close run in finally. Handled SQLite errors also close first, so the second commit on a closed connection can mask the translated error. Successful execution does not verify how many rows matched.

        Example:
            ``driver.direct_delete_many_by_ids("books", [2, 4])`` deletes matching IDs.


        :param target_table: Existing table name, validated by the driver.
        :param row_ids: Iterable of row IDs, each converted to a string binding.
        :return: ``True`` after successful execution, including no matches.
        """
        row_ids = ((str(rid),) for rid in row_ids)

        # Todo: Check that this is used everywhere it should be
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_ids", row_ids))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self.direct_get_id_column(target_table)
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, target_table_id_column)

        try:
            conn.executemany(stmt, row_ids)

        except sqlite3.OperationalError as e:
            err_str = "Operational error on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_ids", row_ids),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseDriverError(err_str)

        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_ids", row_ids),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseIntegrityError(err_str)

        finally:
            conn.commit()
            conn.close()

        # Todo: Add checking that the delete has gone through
        return True

    # Todo: Merge
    def direct_delete(self, target_table: str, column: str, value: str, many: bool = False) -> bool:
        """
        Delete matching column values, optionally as repeated bound-value statements.

        Only the table is validated; column identifiers must be trusted. The batch form stringifies values. Commit/close run even on errors, and duplicate error-path cleanup can mask a translated SQLite exception.

        Example:
            ``driver.direct_delete("books", "book_id", 3)`` deletes a matching row.


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param value: One bound value, or an iterable when ``many`` is true.
        :param many: Execute one deletion per supplied value instead of one scalar deletion.
        :return: ``True`` on successful execution, even if no row matched.
        """
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
            )
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, column)
        try:
            if not many:
                conn.execute(stmt, (value,))
            else:
                value = tuple([(str(v),) for v in value])
                conn.executemany(stmt, value)
        except sqlite3.OperationalError as e:
            err_str = "Operational error on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()

        # Todo: Add checking that the delete has gone through
        return True

    # Todo: Standardize on "table" not "target_table"
    def direct_delete_many(self, target_table: str, column: str, values: Any) -> None:
        """
        Delegate repeated column-value deletion to ``direct_delete``.

        Example:
            ``driver.direct_delete_many("books", "book_id", [2, 4])`` deletes both IDs.


        :param target_table: Existing table name, validated by the driver.
        :param column: Trusted column identifier interpolated into SQL.
        :param values: Iterable of values stringified and bound by the batch deletion helper.
        :return: ``None``; the delegated boolean result is discarded.
        """
        self.direct_delete(target_table=target_table, column=column, value=values, many=True)

    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """
        Delete one ID match and commit without closing the acquired connection.

        Missing IDs still count as success. SQLite operational/integrity errors are translated, with commit attempted on error as well.

        Example:
            ``driver.direct_delete_row_by_id("books", 3)`` deletes the ID if present.


        :param target_table: Existing table name, validated by the driver.
        :param row_id: ID value bound against the table's ID column.
        :return: ``True`` after successful execution.
        """
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database."
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_id", row_id))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self.direct_get_id_column(target_table)
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, target_table_id_column)

        try:
            conn.execute(stmt, (row_id,))

        except sqlite3.OperationalError as e:
            err_str = "Operational error on table."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_id", row_id),
                ("stmt", stmt),
            )
            conn.commit()
            raise DatabaseDriverError(err_str)

        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_id", row_id),
                ("stmt", stmt),
            )
            default_log.log_exception(message=err_str, exception=e, level="ERROR")
            conn.commit()
            raise DatabaseIntegrityError(err_str)

        finally:
            conn.commit()

        # Todo: Add checking that the delete has gone through
        return True

    def direct_clear_table(self, target_table: str) -> bool:
        """
        Delete every row, commit, and count remaining rows before closing the connection.

        Translate SQLite operational/integrity errors. The count is taken after an explicit commit and is not an atomic guarantee against concurrent inserts.

        Example:
            ``driver.direct_clear_table("books")`` returns whether its follow-up count is zero.


        :param target_table: Existing table name, validated by the driver.
        :return: Whether the follow-up row count is zero.
        """
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        # Lock the database (to stop anything being assigned into the space that has just been freed by the delete
        # between the delete and the check) - clear the table - check that there are actually no rows in the table
        conn = self.get_connection()

        row_count = None
        try:
            with conn:
                # Delete the row
                stmt = "DELETE FROM {};".format(target_table)
                conn.execute(stmt)
                conn.commit()

                # Check to see if there are actually any rows left in the table
                stmt = "SELECT COUNT(*) FROM {};".format(target_table)
                c = conn.cursor()
                for row in c.execute(stmt):
                    row_count = row[0]
                if row_count is None:
                    row_count = 0

        except sqlite3.OperationalError as e:
            err_str = "Unable to delete target row - OperationalError.\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("target_table", target_table))
            raise DatabaseDriverError(err_str)

        except sqlite3.IntegrityError as e:
            err_str = "Unable to delete target row - IntegrityError.\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("target_table", target_table))
            raise DatabaseIntegrityError(err_str)

        finally:
            conn.close()

        if row_count == 0:
            return True
        else:
            return False

    def direct_unlink_main_tables(self, primary_table: str, secondary_table: str) -> None:
        """
        Drop the whole interlink table between two main tables and invalidate caches.

        This removes all link rows and types, not an individual row relationship.

        Example:
            ``driver.direct_unlink_main_tables("books", "tags")`` drops their interlink table.


        :param primary_table: First main-table name used to resolve the link table.
        :param secondary_table: Second main-table name used to resolve the link table.
        :return: ``None``.
        """
        table_name, column_name = self._get_link_table_name_col_name(primary_table, secondary_table)

        unlink_sqlite = """
        DROP TABLE {};
        """.format(
            table_name
        )

        self.direct_execute_sql(unlink_sqlite)
        self._zero_prop_cache()

    #
    # ----------------------------------------------------------------------------------------------------------------------
