
"""
Update bound row values and maintain configured normalized identity columns.
"""

from copy import deepcopy
import sqlite3

import pprint

from typing import Any

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, force_unicode

from LiuXin_alpha.errors import RowIntegrityError, DatabaseIntegrityError, DatabaseDriverError
from LiuXin_alpha.databases.normalized_identities import (
    add_derived_identity_values,
    default_normalized_identity_spec,
    normalize_identity_value,
    normalized_identity_defaults_for_table,
)

from LiuXin_alpha.utils.logging import default_log


class UpdateMixin:
    """
    Provide scalar-column batches and dictionary-based row updates through host schema helpers.

    Example:
        ``driver.direct_update_columns({3: "Example"}, field="book_title")`` updates one inferred column.
    """

    # Todo: Check for field degeneracy
    # Todo: I think this may have been superseded by the writers....
    def direct_update_columns(self, id_values_map, field=None, table=None) -> None:
        """
        Update one field for each ID, also deriving its configured identity column when available.

        Infer the table from the field; a conflicting table argument only warns and the inferred table wins. Commit the batch on success and close in finally. Empty mappings are no-ops; dict-valued mappings select an unimplemented multi-column mode.

        Example:
            ``driver.direct_update_columns({2: "A", 3: "B"}, field="book_title")`` writes two titles.


        :param id_values_map: Mapping from row IDs to scalar field values; dict values are currently unsupported.
        :param field: Trusted column heading required for scalar mode.
        :param table: Optional expected table name; a mismatch warns rather than rejecting the update.
        :return: ``None``.
        """
        # Check to see if the map is one-one (a id_values_map keyed with an id and values with a single entry - with a
        # field and table for targeting - one value is changed in each row)
        # or one-to-many (and id_values_map keyed with an id and values with a dictionary keyed with the column name and
        # valued with the new column value)

        def detect_mode(int_id_values_map):
            """
            Inspect only the first mapping value to choose scalar or mapping update mode.

            Example:
                An empty mapping selects no work; a first value of ``{"title": "A"}`` selects the unsupported many mode.


            :param int_id_values_map: Sized mapping of row IDs to candidate update values.
            :return: ``None`` for empty input, ``many`` for a first dict value, otherwise ``one``.
            """
            if len(int_id_values_map) == 0:
                return None

            sample_key = next(iter(int_id_values_map))
            sample_values = int_id_values_map[sample_key]
            if isinstance(sample_values, dict):
                return "many"
            else:
                return "one"

        mode = detect_mode(id_values_map)

        if mode == "one":

            # Checking that the field and table make sense
            field_table = self.direct_identify_table_from_column(field)

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
                target_table = field_table

            table_id_col = self.direct_get_id_column(target_table)
            identity_spec = default_normalized_identity_spec(target_table, field)
            if (
                identity_spec is not None
                and identity_spec.identity_column
                in set(self.direct_get_column_headings(target_table))
            ):
                sequence = (
                    (
                        value,
                        None
                        if value is None
                        else normalize_identity_value(
                            value,
                            identity_spec.normalization_profile,
                        ),
                        row_id,
                    )
                    for row_id, value in iteritems(id_values_map)
                )
                stmt = "UPDATE {} SET {}=?, {}=? WHERE {}=?".format(
                    target_table,
                    field,
                    identity_spec.identity_column,
                    table_id_col,
                )
            else:
                # Building the sequence - need it in the form of a tuple of tuples - value, id
                sequence = ((v, k) for k, v in iteritems(id_values_map))
                stmt = "UPDATE {} SET {}=? WHERE {}=?".format(
                    target_table,
                    field,
                    table_id_col,
                )

            # Executing the statement and the sequence together
            conn = self.get_connection()
            try:
                conn.executemany(stmt, sequence)
                conn.commit()
            finally:
                conn.close()

        elif mode == "many":

            # Todo: Fix
            raise NotImplementedError

    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> None:
        """
        Update the identified row using its ID and the remaining supplied columns.

        Infer the table before copying the mapping, convert exact text ``None`` values to null and derive configured identity fields. Missing ID raises RowIntegrityError. Use a plain dict, since a live Row object can recurse through its writer. Commit/close on success; handled SQLite errors are translated, with an additional commit on the integrity-error path.

        Example:
            ``driver.direct_update_row_dict({"book_id": 3, "book_title": "Example"})`` updates supplied fields only.


        :param row_dict: Plain column/value dictionary including the ID; inference may remove its ``table`` key before copying.
        :return: ``True`` for an ID-only no-op; otherwise ``None`` after executing the update.
        """
        target_table = self.direct_identify_table_from_row(row_dict)
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
        if normalized_identity_defaults_for_table(target_table):
            row_dict = add_derived_identity_values(
                target_table,
                row_dict,
                available_columns=set(
                    self.direct_get_column_headings(target_table)
                ),
            )

        # working out what the id column for the table is called
        row_id = self.direct_get_id_column(target_table)
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
