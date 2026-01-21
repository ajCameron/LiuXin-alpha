
from copy import deepcopy
import sqlite3

import pprint

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, force_unicode

from LiuXin_alpha.errors import RowIntegrityError, DatabaseIntegrityError, DatabaseDriverError

from LiuXin_alpha.utils.logging import default_log


class UpdateMixin:
    """
    Methods to update the database.
    """


    # Todo: Check for field degeneracy
    def direct_update_columns(self, id_values_map, field=None, table=None):
        """
        For when you only want to update specific columns in rows.
        Detects the kind of map entered - preforms different actions depending on what it is.
        :return:
        """
        # Check to see if the map is one-one (a id_values_map keyed with an id and values with a single entry - with a
        # field and table for targeting - one value is changed in each row)
        # or one-to-many (and id_values_map keyed with an id and values with a dictionary keyed with the column name and
        # valued with the new column value)

        def detect_mode(int_id_values_map):
            if len(int_id_values_map) == 0:
                return None

            sample_key = iter(int_id_values_map).next()
            sample_values = int_id_values_map[sample_key]
            if isinstance(sample_values, dict):
                return "many"
            else:
                return "one"

        mode = detect_mode(id_values_map)

        if mode == "one":

            # Checking that the field and table make sense
            field_table = self.__identify_table_from_column(field)
            if table is not None:
                if field_table != table:
                    wrn_str = "LiuXin.databases.SQLITE.databasedriver:direct_update_columns was fed inconsistent data."
                    wrn_str += "the given column doesn't belong to the given table.\n"
                    default_log.log_variables(
                        wrn_str,
                        "WARNING",
                        ("field", field),
                        ("table", table),
                        ("id_values_map", id_values_map),
                    )
                    target_table = field_table
                else:
                    target_table = table
            else:
                target_table = table

            # Building the sequence - need it in the form of a tuple of tuples - value, id
            sequence = ((v, k) for k, v in iteritems(id_values_map))

            # Building the statement
            table_id_col = self._get_id_column(target_table)
            stmt = "UPDATE {} SET {}=? WHERE {}=?".format(target_table, field, table_id_col)

            # Executing the statement and the sequence together
            conn = self.get_connection()
            conn.executemany(stmt, sequence)
            conn.commit()

        elif mode == "many":

            # Todo: Fix
            raise NotImplementedError



    def direct_update_row_dict(self, row_dict):
        """
        Takes a row in the form of a row_dict. Updates that row_dict into the database.
        This is the method Row ultimately calls to update itself - THUS DO NOT CALL WITH ROW. IT WAS CAUSE RECURSION.
        :param row_dict:
        :return:
        """
        target_table = self.__identify_table_from_row(row_dict)
        row_dict = deepcopy(row_dict)

        # Trying to write a u'None' to a column with a foreign key constraint causes problems. Replacing all of these
        # with actual None
        new_row_dict = dict()
        for column in row_dict:
            if row_dict[column] == "None":
                new_row_dict[column] = None
            else:
                new_row_dict[column] = row_dict[column]
        row_dict = new_row_dict

        # working out what the id column for the table is called
        row_id = self._get_id_column(target_table)
        if row_id in row_dict:
            target_row_id = row_dict[row_id]
            del row_dict[row_id]
        else:
            err_str = "update_row_in_table method has failed.\n"
            err_str += " It was unable to find a valid row_id.\n"
            err_str += "row_dict: " + pprint.pformat(row_dict) + "\n"
            default_log.error(err_str)
            raise RowIntegrityError(err_str)

        # If removing the id column has reduced the length of the row to zero, then no further action need be taken.
        # some check should be added here to make sure the column you're trying to update has the row you're
        # trying to update in it
        if len(row_dict) == 0:
            return True

        # Assembling a list of placeholders of the form (?,?,?)
        number_of_values = len(row_dict)
        values_placeholders = "("
        for i in range(number_of_values):
            values_placeholders += "?,"
        values_placeholders = values_placeholders[:-1]
        values_placeholders += ")"

        # These are the column headings values will be inserted into, with corresponding values
        column_headings = [_ for _ in row_dict.keys()]
        values = [_ for _ in row_dict.values()]

        # building the list of value
        column_list = ""
        for i in range(number_of_values):
            column_list += force_unicode(column_headings[i]) + " = ? ,"
        column_list = column_list[:-1]
        values.append(target_row_id)

        stmt = "UPDATE {} SET {} WHERE {} = ?".format(target_table, column_list, row_id)

        conn = self.get_connection()
        c = conn.cursor()

        # info_str = "Command about to be executed on the database.\n"
        # info_str += "stmt: " + stmt + "\n"
        # info_str += "values: " + unicode(values) + "\n"
        # info_str += "target_row_id: " + unicode(target_row_id) + "\n"
        # info_str += "row_dict: " + unicode(row_dict) + "\n"
        # default_log.info(info_str)

        try:
            c.execute(stmt, values)
            conn.commit()
            conn.close()

        except sqlite3.InterfaceError as e:
            err_str = "Unable to update - InterfaceError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseDriverError(err_str)

        except sqlite3.OperationalError as e:
            err_str = "Unable to update - OperationalError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseDriverError(err_str)

        except sqlite3.IntegrityError as e:
            conn.commit()
            conn.close()
            err_str = "Unable to update - IntegrityError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseIntegrityError(err_str)
