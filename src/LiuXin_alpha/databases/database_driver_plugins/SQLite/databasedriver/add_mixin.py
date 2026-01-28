
import sqlite3

from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log, LiuXin_debug_print

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode


class AddingMixin:
    """
    Mathods to add to the database.
    """


    @staticmethod
    def _reject_embedded_nul_text(*, target_table: str, row_dict: dict) -> None:
        """Reject embedded NUL ("\x00") in str payloads.

        SQLite can store NULs inside TEXT values, but a lot of tooling (and some
        driver paths) treat NUL as a string terminator or otherwise mis-handle it.
        Calibre-style DB layers typically reject such payloads at the API boundary.

        We raise ValueError to make this a clear caller-input issue.

        Not currently used - to many unicode issues.
        """

        for col, val in row_dict.items():
            if isinstance(val, str) and "\x00" in val:
                raise ValueError(f"Embedded NUL byte rejected for {target_table}.{col}")


    def direct_add_simple_row_dict(self, row_dict):
        """
        Takes a single row in the form of a dictionary and adds the values to the database.
        :param row_dict:
        :return :
        """
        target_table = self.identify_table_from_row(row_dict)

        # Assembling a list of placeholders of the form ?,?,?
        values_placeholders = ""
        for i in range(len(row_dict)):
            values_placeholders += "?,"
        values_placeholders = values_placeholders[:-1]

        # These are the column headings values will be inserted into
        column_headings = list(row_dict.keys())
        column_placeholders = ""
        for i in range(len(row_dict)):
            column_placeholders += force_unicode(column_headings[i]) + ","
        column_placeholders = column_placeholders[:-1]

        # these are the values that will be inserted
        values = [row_dict[col_name] for col_name in column_headings]

        stmt = "INSERT into `{}` ({}) VALUES ({})".format(target_table, column_placeholders, values_placeholders)

        conn = self.get_connection()
        c = conn.cursor()

        if VERBOSE_DEBUG:
            LiuXin_debug_print("add_simple_row about to execute SQL code.")
            LiuXin_debug_print(stmt, " on ", target_table, " with values ", values)

        try:
            c.execute(stmt, values)
        except sqlite3.OperationalError as e:
            err_str = "sqlite3.OperationalError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict", row_dict),
                ("target_table", target_table),
                ("stmt", stmt),
            )
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "sqlite3.IntegrityError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict", row_dict),
                ("target_table", target_table),
                ("table_sqlite", self.get_table_sqlite(table=target_table, conn=conn)),
            )
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()


    def direct_add_multiple_simple_row_dicts(self, row_dict_list):
        """
        Takes an index of new rows in the form of dictionaries. Adds them to the database.
        :param row_dict_list: Takes a list of simple rows
        """
        if len(row_dict_list) == 0:
            return True

        # Gets a reference element. Errors will be thrown if every row doesn;t match this one.
        reference_row_dict = row_dict_list[0]
        target_table = self.identify_table_from_row(reference_row_dict)

        # TODO: re-write add_multiple_simple_rows to handle multiple different types of row
        for row in row_dict_list:
            if target_table != self.identify_table_from_row(row):
                raise InputIntegrityError("Rows from different tables.")

        # TODO: extend the method to deal with this
        for statement in row_dict_list:
            # Check that we're dealing with rows of the same type
            if set([rk for rk in reference_row_dict.keys()]) != set([rk for rk in statement.keys()]):
                raise InputIntegrityError("Rows with different column names.")

            table_id_col = self.direct_get_id_column(target_table)

            if table_id_col in statement and statement[table_id_col] is not None:
                raise InputIntegrityError("Cannot update a row using this method!")

        for i in range(len(row_dict_list)):
            if "table" in row_dict_list[i].keys():
                del row_dict_list[i]["table"]

        # With all those checks run we should have a nice, consistent set of dictionaries to insert into target_table
        reference_row_dict = row_dict_list[0]

        values_placeholders = ""
        for i in range(len(reference_row_dict)):
            values_placeholders += "?,"

        values_placeholders = values_placeholders[:-1]

        # building the list of values
        column_list_string = ""

        column_headings = [_ for _ in reference_row_dict.keys()]

        for i in range(len(reference_row_dict)):
            if column_headings[i] != "table":
                column_list_string += force_unicode(column_headings[i]) + ","

        column_list_string = column_list_string[:-1]

        stmt = "INSERT into `{}` ({}) VALUES ({})".format(target_table, column_list_string, values_placeholders)

        values = []

        for statement in row_dict_list:
            values.append(tuple([_ for _ in statement.values()]))

        conn = self.get_connection()
        c = conn.cursor()

        info_str = "add_multiple_simple_rows about to execute SQL code."
        default_log.log_variables(info_str, "INFO", ("target_table", target_table), ("values", values))

        try:
            c.executemany(stmt, values)
        except sqlite3.OperationalError as e:
            err_str = "sqlite3.OperationalError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict_list", row_dict_list),
                ("target_table", target_table),
                ("stmt", stmt),
                ("values", values),
            )
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "sqlite3.IntegrityError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict_list", row_dict_list),
                ("target_table", target_table),
                ("table_sqlite", self.get_table_sqlite(table=target_table, conn=conn)),
                ("stmt", stmt),
                ("values", values),
            )
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()
