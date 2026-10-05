
"""
Resolve conventional SQL column names and validate names against the driver schema.
"""

import re

from copy import deepcopy
from typing import Optional, Iterable

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode
from LiuXin_alpha.utils.logging import default_log


class TableNamesMixin:
    """
    Provide schema-based ID, display, timestamp and tree-column discovery.

    Several choices are naming heuristics rather than SQLite constraint inspection.

    Example:
        ``driver.direct_get_id_column("books")`` chooses the conventional ID heading.
    """

    # Todo: To driver base class
    @ staticmethod
    def direct_get_column_name(table_name: str) -> str:
        """
        Map a plural table name to its conventional singular column base.

        Example:
            >>> TableNamesMixin.direct_get_column_name("books")
            'book'


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The pluralizer's singular form.
        """
        return plural_singular_mapper(table_name)

    def direct_validate_existing_table_name(self, test_name):
        """
        Check a trimmed name against cached tables and selected symmetric wrappers.

        Reject semicolon, colon and ampersand. Accept bare names or backtick, backslash, percent and underscore wrappers; this tests existing-name membership and does not guarantee accepted wrappers are valid SQL syntax.

        Example:
            ``driver.direct_validate_existing_table_name("`books`")`` succeeds when books is known.


        :param test_name: Candidate coerced to Unicode; decoding failure raises InputIntegrityError.
        :return: Whether the name matches an accepted existing-table spelling.
        """
        # If the name matches a pre-existing one it is automatically valid (this function is used to validate input)
        # Not to validate potential new table names.
        tables_and_columns = self.direct_get_tables_and_columns()

        # intended to help with SQL injection attack proofing
        try:
            test_name = force_unicode(test_name)
        except UnicodeDecodeError:
            err_str = "Attempt to direct_validate_existing_table_name has failed. Could not coerce test_name to unicode."
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

    # needs testing
    # Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows that
    # the shortest string ending in id is the id string. Should be tested every time a new column is added
    def direct_get_id_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Choose literal id, otherwise the shortest heading ending in _id.

        Length ties preserve schema order. Unknown tables or absent candidates raise InputIntegrityError; this does not inspect primary-key constraints.

        Example:
            ``driver.direct_get_id_column("books")`` selects book_id when no literal id column exists.


        :param table: Existing table name used for schema lookup.
        :param tables_and_columns: Accepted but ignored; the current schema mapping is always fetched.
        :return: The selected ID column name.
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

    def direct_get_datestamp_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Choose literal datestamp, otherwise the shortest recognized timestamp heading.

        Recognize _datestamp, _timestamp and their _ep_k variants. Unknown tables or absent candidates raise InputIntegrityError; ties preserve schema order.

        Example:
            ``driver.direct_get_datestamp_column("books")`` locates a conventional timestamp field.


        :param table: Existing table name used for schema lookup.
        :param tables_and_columns: Accepted but ignored; the current schema mapping is always fetched.
        :return: The selected timestamp column name.
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

    def direct_identify_table_from_column(
            self,
            column_heading: str,
            headings_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Return the first table containing a column in the supplied or current mapping.

        Ambiguity is resolved by mapping order; an unknown column raises InputIntegrityError.

        Example:
            >>> TableNamesMixin().direct_identify_table_from_column("book_id", {"books": ["book_id"]})
            'books'


        :param column_heading: Exact column heading to locate.
        :param headings_and_columns: Optional table-to-column-list mapping; ``None`` reads the driver schema.
        :return: The first matching table name.
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
            err_str = "identify_table_from_column failed.\n"
            err_str += repr(column_heading) + " was not recognized.\n"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

    def direct_get_display_column(self, table_name: str) -> str:
        """
        Choose the shortest non-ID column without mutating the schema cache.

        Ties preserve heading order. A table with no remaining columns raises DatabaseIntegrityError; the choice is a naming heuristic, not semantic metadata.

        Example:
            ``driver.direct_get_display_column("books")`` identifies the shortest non-ID heading.


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The selected display column name.
        """
        table_name = deepcopy(table_name)
        table_id_column = self.direct_get_id_column(table_name)
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

    def direct_get_full_column_name(self, target_table: str) -> Optional[str]:
        """
        Find the first heading ending in _full, ignoring suffix case.

        Example:
            ``driver.direct_get_full_column_name("series")`` locates the stored path column if present.


        :param target_table: Existing table whose derived column is required.
        :return: The matching column, or ``None``; unknown table keys raise KeyError.
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

    def direct_get_tree_id_column(self, target_table: str) -> Optional[str]:
        """
        Find the first heading ending in _tree_id, ignoring suffix case.

        Example:
            ``driver.direct_get_tree_id_column("series")`` locates the grouping column if present.


        :param target_table: Existing table whose derived column is required.
        :return: The matching column, or ``None``; unknown table keys raise KeyError.
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
    def direct_get_table_col_base(table_name: str) -> str:
        """
        Return the singular table base using the pluralizers module.

        Example:
            >>> TableNamesMixin.direct_get_table_col_base("books")
            'book'


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The conventional singular column base.
        """
        from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

        return plural_singular_mapper(table_name)

    # Todo: If this is still true, it really shouldn't be (see below)
    # Tree-like tables may use either a legacy ``*_parent`` column or a foreign-key-shaped
    # ``*_parent_id`` column to point at the row above them.
    def direct_get_parent_column_name(self, table_name: str) -> Optional[str] | bool:
        """
        Find a heading ending in _parent or _parent_id, ignoring case.

        When several match, prefer a sole _parent_id candidate; otherwise raise DatabaseIntegrityError. An unknown table raises InputIntegrityError.

        Example:
            ``driver.direct_get_parent_column_name("series")`` accepts legacy and foreign-key-style parent headings.


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The parent column name, or ``False`` when there is none.
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
    def direct_validate_table_name(table_name: str) -> bool:
        """
        Test a name against the ASCII letters-and-underscores regular expression.

        Digits are rejected. The regex uses dollar anchoring, so it also accepts a trailing newline; this is not a complete SQL identifier validation rule.

        Example:
            >>> TableNamesMixin.direct_validate_table_name("book_links")
            True
            >>> TableNamesMixin.direct_validate_table_name("books2")
            False


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: Whether the regex matches the supplied string.
        """
        table_name_regex = r"^[a-zA-Z_]+$"
        if re.match(table_name_regex, table_name):
            return True
        return False
