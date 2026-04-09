
import re

from copy import deepcopy
from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode
from LiuXin_alpha.utils.logging import default_log


class TableNamesMixin:
    """
    Methods to generate names of columns, tables, e.t.c.
    """

    # Todo: To driver base class
    def direct_get_column_name(self, table_name):
        """
        Return a column name for the given table name - just takes the singular form of the table name,
        :param table_name:
        :return:
        """
        return plural_singular_mapper(table_name)

    def direct_validate_existing_table_name(self, test_name):
        return self.validate_existing_table_name(test_name)

    def validate_existing_table_name(self, test_name):
        """
        Test to see if a candidate table name is valid (contains no SQL control characters).
        Intended to help with SQL injection attack proofing. Should be spread to all columns as well.
        :param test_name:
        :return True/False:
        """
        # If the name matches a pre-existing one it is automatically valid (this function is used to validate input)
        # Not to validate potential new table names.
        tables_and_columns = self.direct_get_tables_and_columns()

        # intended to help with SQL injection attack proofing
        try:
            test_name = force_unicode(test_name)
        except UnicodeDecodeError:
            err_str = "Attempt to validate_existing_table_name has failed. Could not coerce test_name to unicode."
            err_str += "test_name: " + repr(test_name) + "\n"
            raise InputIntegrityError(err_str)

        # Testing for SQL special characters - things which might be used to build an attack
        sql_forbidden_chars = [";", ":", "&"]
        for character in sql_forbidden_chars:
            if character in test_name:
                return False

        # stripping whitespace
        test_name = test_name.strip()
        tables = tables_and_columns.keys()
        possible_tables = []

        # characters to be appended to the beginning and end of a table name (characters that SQL ignores)
        additional_characters = ["`", "\\", "", "%", "_"]
        for table in tables:
            for character in additional_characters:
                current_name = character + table + character
                possible_tables.append(current_name)

        if test_name in possible_tables:
            return True
        else:
            return False



    def direct_get_id_column(self, table, tables_and_columns=None):
        """
        Return the id column for a given table.
        :param table:
        :param tables_and_columns:
        :return:
        """
        table = force_unicode(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            headings = tables_and_columns[table]
        except KeyError as e:
            err_str = "DatabaseDriver.direct_get_id_column failed - table couldn't be found.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("table", table),
                ("tables", sorted(tables_and_columns.keys())),
            )
            raise InputIntegrityError(err_str)

        # Check for the special case where there is just a column called "id"
        if "id" in headings:
            return "id"

        candidate_ids = []
        for heading in headings:
            if heading.endswith("_id"):
                candidate_ids.append(heading)
        if len(candidate_ids) > 1:
            candidate_ids = sorted(candidate_ids, key=len)
            return candidate_ids[0]
        elif len(candidate_ids) == 0:
            err_str = "Error - get_id_column failed - no column with a name ending in id found"
            err_str = default_log.log_variables(err_str, "ERROR", ("headings", headings))
            raise InputIntegrityError(err_str)
        else:
            return candidate_ids[0]


    def direct_get_datestamp_column(self, table, tables_and_columns=None):
        """
        Return the id column for a given table.
        :param table:
        :param tables_and_columns:
        :return:
        """
        table = force_unicode(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            headings = tables_and_columns[table]
        except KeyError as e:
            err_str = "DatabaseDriver.direct_get_id_column failed - table couldn't be found.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("table", table),
                ("tables", sorted(tables_and_columns.keys())),
            )
            raise InputIntegrityError(err_str)

        # Check for the special case where there is just a column called "id"
        if "datestamp" in headings:
            return "datestamp"

        candidate_ids = []
        for heading in headings:
            if (
                heading.endswith("_datestamp")
                or heading.endswith("_datestamp_ep_k")
                or heading.endswith("_timestamp")
                or heading.endswith("_timestamp_ep_k")
            ):
                candidate_ids.append(heading)
        if len(candidate_ids) > 1:
            candidate_ids = sorted(candidate_ids, key=len)
            return candidate_ids[0]
        elif len(candidate_ids) == 0:
            err_str = "Error - direct_get_datestamp_column failed - no column with a name ending in datestamp found"
            err_str = default_log.log_variables(err_str, "ERROR", ("headings", headings))
            raise InputIntegrityError(err_str)
        else:
            return candidate_ids[0]


    # needs testing
    # Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows that
    # the shortest string ending in id is the id string. Should be tested every time a new column is added
    def _get_id_column(self, table, tables_and_columns=None):
        """
        Every table in the database should have an id column.

        Currently, assumes that
         - there is a column with a name ending in "id"
          - if this is true for multiple rows the shortest string ending in "id" is the id column.
        Should be tested every time a new column/table is added
        :param table:
        :param tables_and_columns:
        :return:
        """
        return self.direct_get_id_column(table=table, tables_and_columns=tables_and_columns)

    def identify_table_from_column(self, column_heading, headings_and_columns=None, print_error=True):
        """
        Takes a column heading (and optionally a headings and columns dict). Works out the table it falls into.
        :param column_heading: Each column heading should be unique in the database
        :param headings_and_columns: COMPLETELY SUPERFLUOUS
        :param print_error: Will be replaced with LiuXin debug print
        :return:
        """
        if headings_and_columns is None:
            headings_and_columns_local = self.direct_get_tables_and_columns()
        else:
            headings_and_columns_local = headings_and_columns
        tables = headings_and_columns_local.keys()

        for table in tables:
            column_headings = headings_and_columns_local[table]
            if column_heading in column_headings:
                return table
        else:
            if print_error:
                err_str = "identify_table_from_column failed.\n"
                err_str += repr(column_heading) + " was not recognized.\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError

    def get_display_column(self, table_name):
        """
        Gets the display column for a table (currently based off the shortest column which is not the id column)
        :param table_name:
        :return display_column:
        """
        table_name = deepcopy(table_name)
        table_id_column = self._get_id_column(table_name)
        tables_and_columns = self.direct_get_tables_and_columns()
        # Don't want to accidentally remove the title_id from the tables_and_columns cache
        column_names = deepcopy(tables_and_columns[table_name])

        # a display column should never be the id column. Removing it.
        column_names.remove(table_id_column)
        column_names.sort(key=lambda x: len(x))
        if len(column_names) == 0:
            err_str = "table_name seems to only have an id column. If that.\n"
            err_str += "table_name: " + repr(table_name) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            return column_names[0]

    def get_full_column_name(self, target_table):
        """
        Rows which are part of a tree like structure should have a full column. Use to store a string representation of
        the
        This method finds and returns that
        column.
        :param target_table:
        :return target_table_full_column:
        """
        table_and_columns = self.direct_get_tables_and_columns()
        columns = table_and_columns[target_table]

        full_pat = r"^.*_full$"
        full_re = re.compile(full_pat, re.I)
        for column in columns:
            if full_re.match(column) is not None:
                return column
        else:
            return None

    def get_tree_id_column(self, target_table):
        """
        Each table which is in the form of a tree like structure has a tree_id column. The entry in this column is
        unique for every tree in the table. If none is present then it's assumed that the table isn't organized in a
        tree like structure.
        :param target_table:
        :return:
        """
        table_and_columns = self.direct_get_tables_and_columns()
        columns = table_and_columns[target_table]

        full_pat = r"^.*_tree_id$"
        full_re = re.compile(full_pat, re.I)
        for column in columns:
            if full_re.match(column) is not None:
                return column
        else:
            return None

    @staticmethod
    def _get_table_col_base(table_name):
        """
        Returns the base name for a column in the given table. e.g. "title" for "titles"

        :param table_name: Return the base column for this table
        :return:
        """
        from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

        return plural_singular_mapper(table_name)

    # Tree-like tables may use either a legacy ``*_parent`` column or a foreign-key-shaped
    # ``*_parent_id`` column to point at the row above them.
    def get_parent_column_name(self, table_name):
        """
        Takes a table name. Works out if the table has an element ending in "_parent" and returns the parent column name
        if it exists.
        Returns False otherwise
        :param table_name:
        :return parent_column_name/False:
        """
        table_name = deepcopy(table_name)
        tables_and_columns = self.direct_get_tables_and_columns()
        if table_name not in tables_and_columns:
            if VERBOSE_DEBUG:
                err_str = "Input to get_parent_column_name not recognized.\n"
                err_str += "table_name: " + repr(table_name) + "\n"
                err_str += "is not recognized.\n"
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError
        column_names = tables_and_columns[table_name]

        candidate_index = []
        for name in column_names:
            lowered = name.lower()
            if lowered.endswith("_parent") or lowered.endswith("_parent_id"):
                candidate_index.append(name)

        if len(candidate_index) > 1:
            preferred_candidates = [name for name in candidate_index if name.lower().endswith("_parent_id")]
            if len(preferred_candidates) == 1:
                return preferred_candidates[0]

            err_str = "Multiple candidates found to be the parent row pointer.\n"
            err_str += "All candidates: " + repr(candidate_index) + "\n"
            raise DatabaseIntegrityError(err_str)
        elif len(candidate_index) == 1:
            return candidate_index[0]
        elif len(candidate_index) == 0:
            return False
        else:
            raise LogicalError

    @staticmethod
    def _validate_table_name(table_name: str) -> bool:
        """
        Validate that the given table name is valid.

        :param table_name: The name of the table to preform validation for.
        :return:
        """
        table_name_regex = r"^[a-zA-Z_]+$"
        if re.match(table_name_regex, table_name):
            return True
        return False
