
"""
Mixin for deleting entries from the table.
"""

import sqlite3

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, DatabaseDriverError


class DeleteMixin:
    """
    Methods to delete entries from the database.
    """


    def direct_delete_many_by_ids(self, target_table, row_ids):
        """
        Delete many entries from the given table.
        :param target_table:
        :param values:
        :return:
        """
        row_ids = ((str(rid),) for rid in row_ids)

        # Todo: Check that this is used everywhere it should be
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_ids", row_ids))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
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
    def direct_delete(self, target_table, column, value, many=False):
        """
        Delete all the entries in the target_table whose column matches that value.
        :param target_table:
        :param column:
        :param value:
        :param many: Is it a single value or many
        :return:
        """
        if not self.validate_existing_table_name(target_table):
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
    def direct_delete_many(self, target_table, column, values):
        """
        Delete all the entries in the target_table whose column matches that value.
        :param target_table:
        :param column:
        :param values:
        :return:
        """
        self.direct_delete(target_table=target_table, column=column, value=values, many=True)


    def direct_delete_row_by_id(self, target_table, row_id):
        """
        Takes a table and a row_id - deletes the row with that id.
        :param target_table:
        :param row_id:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database."
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_id", row_id))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
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


    def direct_clear_table(self, target_table):
        """
        Deletes every record from a table.

        :param target_table:
        :param prompt:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
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
