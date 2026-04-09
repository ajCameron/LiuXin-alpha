

import sqlite3
import random
import re
from copy import deepcopy

from LiuXin_alpha.errors import LogicalError, InputIntegrityError, DatabaseIntegrityError, DatabaseDriverError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode, force_unicode

from LiuXin_alpha.utils.logging import LiuXin_debug_print, default_log

from LiuXin_alpha.constants import VERBOSE_DEBUG


class SearchMixin:
    """
    Mixin for the search system.
    """

    @staticmethod
    def _coerce_search_text(value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                return bytes(value).decode("utf-8")
            except UnicodeDecodeError as e:
                err_str = "search_table was passed bytes that were not valid utf-8.\n"
                err_str += "value: " + repr(value) + "\n"
                raise InputIntegrityError(err_str) from e
        return force_unicode(value)


    def direct_get_random_row_dict(self, target_table, direct=False):
        """
        Returns a random row_dict from the specified table.
        :param target_table:
        :param direct:
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()
        target_table = force_unicode(deepcopy(target_table))

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(target_table):
            err_str = "table name passed into direct_get_random_row_dict failed validation.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        highest_id = self.direct_get_highest_id(target_table)
        try:
            highest_id = int(highest_id)
        except TypeError:
            wrn_str = (
                "Unable to coerce highest_id to integer. "
                "Assuming this means that the table is empty. "
                "Could also mean that non-integer ids are being used - in which case this method cannot be used"
            )
            wrn_str = default_log.log_variables(wrn_str, "WARN", ("highest_id", highest_id))
            conn.close()
            return None

        if direct:
            headings = self.direct_get_column_headings(target_table)
            stmt = "SELECT * FROM {} ORDER BY RANDOM() LIMIT 1".format(target_table)
            for row in c.execute(stmt):
                this_row = self._row_to_dict(table=target_table, headings=headings, row=row)
                conn.close()
                return this_row

        elif not direct:
            random.seed()
            conn.close()

            while True:
                new_row_id = random.randint(1, highest_id)
                candidate_row = self.direct_get_row_dict_from_id(table=target_table, row_id=new_row_id)
                if candidate_row:
                    return candidate_row

        # In the case where there are no rows in the table, returns None
        return None


    def direct_get_all_rows(self, table, sort_column=None, reverse=False):
        """
        Returns all rows from a given table in the database in the form of an index of row_dicts.
        Should only be used with small tables. Otherwise the memory cost is prohibitive.
        :param table: Yield the rows from this table
        :param sort_column: Sort the rows by the values in this column
        :param reverse: Should the order of the rows be reversed?
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()
        table = force_unicode(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(table):
            err_str = "table name passed into direct_get_all_rows failed validation.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table))
            raise InputIntegrityError(err_str)

        # Check that the sort_column is in the requested table
        if sort_column not in headings and sort_column is not None:
            err_str = "table and sort_column are not consistent.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("sort_column", sort_column))
            raise InputIntegrityError(err_str)

        if sort_column is None:
            stmt = "SELECT * FROM {};".format(table)
        else:
            if not reverse:
                stmt = "SELECT * FROM {} ORDER BY {} ASC;".format(table, sort_column)
            else:
                stmt = "SELECT * FROM {} ORDER BY {} DESC".format(table, sort_column)

        results = []
        for row in c.execute(stmt):
            this_row = self._row_to_dict(table=table, headings=headings, row=row)
            results.append(this_row)

        conn.close()
        return results


    def direct_get_row_dict_iterator(self, table, sort_column=None, reverse=False):
        """
        Provides an iterator which returns all the rows in a specified table in the form of row_dicts. Ordered by id

        Note:
            This iterator intentionally skips any required sentinel/null row (commonly id=0) used
            by some calibre-compatible tables. The chunked SELECT uses `WHERE <id_col> > start_id_value`
            and starts at start_id_value=0, so the first chunk is `> 0`.
            If you need the sentinel row, fetch it explicitly (e.g. `direct_get_row_dict_from_id(table, 0)`).

        :param table: Get an iterator for all the rows in this table.
        :param sort_column: The column the table should be sorted by
        :param reverse: Should the order of the rows be reversed?
        :return:
        """
        table = force_unicode(table)
        table_id_column = self._get_id_column(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(table):
            err_str = "table name passed into direct_get_all_rows failed validation."
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table))
            raise InputIntegrityError(err_str)

        # Check that the sort_column comes from the table
        if sort_column is not None:
            if sort_column not in headings:
                err_str = "requested sort_column is not in the requested table.\n"
                err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("sort_column", sort_column))
                raise InputIntegrityError(err_str)

        start_id_value = 0
        if sort_column is None:

            # reads data from the database in 10 row chunks - then closing the connection. Should leave the database
            # unlocked for most of the time
            while True:

                conn = self.get_connection()
                c = conn.cursor()

                # Parameterize the moving boundary value to avoid accidental SQL injection
                # and to keep statement parsing consistent across drivers.
                this_stmt = "SELECT * FROM {} WHERE {} > ? ORDER BY {} LIMIT 10;".format(
                    table, table_id_column, table_id_column
                )
                c.execute(this_stmt, (start_id_value,))
                current_rows = deepcopy(c.fetchall())
                conn.close()

                if not current_rows:
                    conn.close()
                    break
                for row in current_rows:
                    this_row = self._row_to_dict(table=table, headings=headings, row=row)
                    yield this_row
                    start_id_value = this_row[table_id_column]

        else:

            # Sort the table by the sort_column and then by the id? Don't have a good solution for this yet (due to
            # concern that the sort order will change while the update is running
            # Do something with timestamps
            raise NotImplementedError("Cannot currently cope with this combination")

    def direct_get_unique_values_set(self, target_column):
        """
        Returns a set of the unique values in a column.

        :param target_column:
        :return values_set: A set of all the unique values in that column
        """
        target_table = self.identify_table_from_column(column_heading=target_column)
        stmt = "SELECT DISTINCT {} FROM {};".format(target_column, target_table)
        values_set = set()
        conn = self.get_connection()
        c = conn.cursor()

        for value in c.execute(stmt):
            values_set.add(value[0])
        conn.close()
        return values_set

    def direct_get_unique_values_iterator(self, target_column):
        """
        Iterates over the unique values in a column.

        Helps to keep memory usage down when dealing with very large tables.
        :param target_column:
        :return:
        """
        # Needs to sort the table after every retrieval - so will be very slow for large databases
        # Todo: Come back and optimize/make this work
        # target_table = self.__identify_table_from_column(column_heading=target_column)
        # stmt = 'SELECT DISTINCT {} FROM {} WHERE {} > {} LIMIT 1;'
        # stmt = stmt.format(target_column, target_table)
        values_set = self.direct_get_unique_values_set(target_column)
        for value in values_set:
            yield value


    def direct_get_row_dict_from_id(self, table, row_id):
        """
        Attempts to get a specific row from the table give. Returns the result as a dictionary kweyed with the column
        name and valued with the values from that row.
        :param table: The table to search in
        :param row_id: The id this function will be looking for
        :return row/False: The requested Row. False if nothing is found.
        """
        table = force_unicode(table)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_column_headings(table)
        table_id_name = self._get_id_column(table)

        stmt = "SELECT * FROM {} WHERE {} = ?".format(table, table_id_name)

        rows = []
        result = dict()
        try:
            for row in c.execute(stmt, (row_id,)):
                result = self._row_to_dict(table=table, headings=headings, row=row)
                rows.append(result)
        except sqlite3.InterfaceError as e:
            err_str = "Interface error while trying to find a row\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("row_id", row_id))
            raise DatabaseDriverError(err_str)

        if len(rows) > 1:
            err_str = "Error - search yielded multiple rows. Aborting.\n"
            err_str += repr(rows)
            default_log.error(err_str)
            conn.close()
            raise DatabaseIntegrityError(err_str)
        elif len(rows) == 0:
            info_str = "Warning - search yielded no results. Consider sources of logical error."
            default_log.log_variables(info_str, "INFO", ("table", table), ("row_id", row_id))
            conn.close()
            return False
        else:
            conn.close()
            return result


    # ------------------------------------------------------------------
    # Sentinel / null-row helpers
    # ------------------------------------------------------------------

    def direct_has_null_row(self, table) -> bool:
        """Return True if the table contains a sentinel/null row at id=0.

        Some calibre-compatible tables reserve an explicit row with primary-key
        value 0 (often with most fields NULL) to represent a missing/unknown
        reference. Contract tests treat this row as *categorically different*
        from "real" rows.
        """
        table = force_unicode(table)
        if not self.validate_existing_table_name(table):
            raise InputIntegrityError(f"Unknown table: {table!r}")

        id_col = self._get_id_column(table)
        stmt = f"SELECT 1 FROM `{table}` WHERE `{id_col}` = 0 LIMIT 1;"
        conn = self.get_connection()
        try:
            c = conn.cursor()
            row = c.execute(stmt).fetchone()
            return row is not None
        finally:
            conn.close()

    def direct_get_null_row(self, table):
        """Fetch the sentinel/null row at id=0, or False if none exists."""
        table = force_unicode(table)
        if not self.direct_has_null_row(table):
            return False
        return self.direct_get_row_dict_from_id(table, 0)

    def direct_update_null_row(self, table, updates=None, **fields) -> bool:
        """Update the sentinel/null row (id=0) with the provided fields.

        :param table: Target table.
        :param updates: Optional mapping of column -> value.
        :param fields: Convenience keyword arguments (merged over ``updates``).
        :raises InputIntegrityError: if the table has no null row.
        """
        table = force_unicode(table)
        if updates is None:
            updates = {}
        if not isinstance(updates, dict):
            updates = dict(updates)
        updates.update(fields)

        if not self.direct_has_null_row(table):
            raise InputIntegrityError(f"Table {table!r} has no sentinel/null row at id=0")

        id_col = self._get_id_column(table)
        row_dict = {id_col: 0}
        row_dict.update(updates)
        self.direct_update_row_dict(row_dict)
        return True


    def direct_get_all_hashes(self):
        """
        Returns a set of all hashes in the database.
        :return:
        """
        candidate_columns_by_table = {
            "files": ("file_hash", "file_hash_sha256", "file_hash_blake3"),
            "compressed_files": ("compressed_file_hash_1", "compressed_file_hash_2"),
            "new_books": ("new_book_hash_1", "new_book_hash_2"),
            "hashes": ("hash",),
        }

        discovered_hashes = set()
        for table, candidate_columns in candidate_columns_by_table.items():
            try:
                headings = set(self.direct_get_column_headings(table))
            except Exception:
                continue

            for column in candidate_columns:
                if column in headings:
                    discovered_hashes.update(self.direct_get_all_values(table=table, column=column))

        return {hash_value for hash_value in discovered_hashes if hash_value is not None}

    # Todo - Merge with direct_get_unique_values - after an upgrade to allow specify a table
    def direct_get_all_values(self, table, column):
        """
        Returns a set of all values in the given column in the given table.

        :param table: The table to be searched
        :param column: The column in that table
        :return:
        """
        if table is not None:
            table = deepcopy(force_unicode(table))
        else:
            table = self.__identify_table_from_column(column)
        column = deepcopy(force_unicode(column))

        current_values = set()
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT {} FROM {}".format(column, table)
        for row in c.execute(stmt):
            current_values.add(row[0])
        return current_values

    def iterator_return(self, stmt, headings, table=None, bindings=None):
        """Yield row dicts for a pre-built SQL statement.

        When `table` is provided, values are coerced using declared column types
        (see :class:`~LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.value_casting_mixin.ValueCastingMixin`).
        """
        conn = self.get_connection()
        c = conn.cursor()
        try:
            if bindings is None:
                row_iter = c.execute(stmt)
            else:
                row_iter = c.execute(stmt, bindings)

            for row in row_iter:
                # Centralize row->dict conversion: typed when table provided, best-effort otherwise.
                this_row = self._row_to_dict(table=table, headings=headings, row=row)
                yield this_row
        finally:
            default_log.info("Connection has closed!")
            conn.close()


    def direct_search_table(self, table=None, column=None, search_term=None):
        """
        Searches a specified column in a table by the given search term.
        Returns an empty index if no results are found.
        :param table: The table to search (can be unspecified - but don't want to break backwards compatibility
        :param column: The column to search in
        :param search_term: The string to search with
        :return results: An index of row_dicts
        """
        if (table is not None) and (column is not None) and (search_term is not None):
            try:
                table = force_unicode(table)
                column = force_unicode(column)
                search_term = self._coerce_search_text(search_term)
            except UnicodeDecodeError:
                err_str = "search_table was passed something it couldn't coerce to unicode?\n"
                err_str += "table: " + repr(table) + "\n"
                err_str += "column: " + repr(column) + "\n"
                err_str += "search_term: " + repr(search_term) + "\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)

        elif (table is None) and (column is not None) and (search_term is not None):
            try:
                column = force_unicode(column)
                search_term = self._coerce_search_text(search_term)
            except UnicodeDecodeError:
                err_str = "search_table was passed something it couldn't coerce to unicode?\n"
                err_str += "table: " + repr(table) + "\n"
                err_str += "column: " + repr(column) + "\n"
                err_str += "search_term: " + repr(search_term) + "\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)

        else:
            err_str = "Request to search table was not properly formatted.\n"
            err_str += "table: " + repr(table) + "\n"
            err_str += "column: " + repr(column) + "\n"
            err_str += "search_term: " + repr(search_term) + "\n"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        c = conn.cursor()

        results = []
        if not self.validate_existing_table_name(table):
            err_str = "table name passed into direct_search_table failed validation.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table))
            conn.close()
            raise InputIntegrityError(err_str)

        headings = self.direct_get_column_headings(table)
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column or "") or column not in headings:
            err_str = "requested column is not in the requested table.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("column", column))
            conn.close()
            raise InputIntegrityError(err_str)

        stmt = "SELECT * FROM {} WHERE {} = ?;".format(table, column)
        try:

            for row in c.execute(stmt, (search_term,)):
                this_row = self._row_to_dict(table=table, headings=headings, row=row)
                results.append(this_row)

        except sqlite3.OperationalError as e:
            err_str = "Unable to update - OperationalError - search term might be malformed\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("stmt", stmt), ("search_term", search_term))
            conn.close()
            raise InputIntegrityError(err_str)

        conn.close()
        return results


    # Todo: Paused while adding a method to import metadata from a csv file - so I can test something fancy with a join
    def direct_multi_column_search(self, search_index, iterator_return=False):
        """
        Takes an index of tuples (or indexes - the method is not fussy provided it contains the required terms). Which
        can then be used to search the database.
        Tuples should take the form (column_name, binary_comparison_operator, target_value).
        Binary comparison operators can include the LIKE operator.
        Every tuple is joined together by an AND statement.
        Will currently fail unless every row is in the same table.
        Thus [(u'creator', u'=', u'David Weber'),(u'series',u'=',u'Honor Harrington')] becomes
        SELECT * FROM `creators` * WHERE creator = 'David Weber' AND series = 'Honor Harrington';
        :param search_index:
        :param iterator_return: Should an iterator leading to the database be returned? Default: False - in which case
        result is returned as an index
        :return found_rows:
        """
        if len(search_index) == 0:
            if VERBOSE_DEBUG:
                debug_str = "multi-column search has been passed an empty index.\n"
                LiuXin_debug_print(debug_str)
            else:
                return None

        # Builds a set of the requested tables - to check that every column comes from the same table
        columns_set = set()
        table_set = set()
        for term in search_index:
            columns_set.add(term[0])
        for column in columns_set:
            table_set.add(self.identify_table_from_column(column))

        if len(table_set) == 0:
            err_str = "Attempt to parse the search_index has failed.\n"
            err_str += "table_set is empty.\n"
            err_str += "search_index: " + repr(search_index) + "\n"
            raise InputIntegrityError(err_str)

        elif len(table_set) > 1:
            err_str = "Columns seem to come from multiple tables.\n"
            err_str += "columns_set: " + repr(columns_set) + "\n"
            err_str += "table_set: " + repr(table_set) + "\n"
            err_str += "search_index: " + repr(search_index) + "\n"
            raise InputIntegrityError(err_str)

        else:
            target_table = table_set.pop()

        # Build a parameterised query.
        #
        # Historically this code interpolated raw values into SQL, which breaks for
        # strings (they must be quoted) and is also a SQL injection footgun.
        stmt = "SELECT * FROM {} WHERE ".format(target_table)
        final_search_terms = []
        bindings = []

        for this_term in search_index:
            # Expected: (column_name, binary_operator, target_value)
            try:
                column_name, operator, search_term = this_term[0], this_term[1], this_term[2]
            except Exception as e:
                raise InputIntegrityError(
                    "Malformed search term (expected a 3-tuple): {}".format(repr(this_term))
                ) from e

            op = force_unicode(operator).strip()
            op_upper = op.upper()
            col = force_unicode(column_name).strip()

            # Contract: proactively reject values that *look* like multi-statement SQL.
            # Even though we use bound parameters (so these payloads are not executable),
            # treating them as invalid input keeps the public API conservative.
            if isinstance(search_term, (bytes, str)):
                try:
                    st = search_term.decode("utf-8", errors="replace") if isinstance(search_term, bytes) else str(search_term)
                except Exception:
                    st = None
                if st is not None and (
                    ";" in st
                    or "--" in st
                    or "/*" in st
                    or "*/" in st
                    or "\x00" in st
                ):
                    raise InputIntegrityError("Unsafe-looking search value rejected")

            # Normalise some common NULL behaviours so callers can use `=` semantics.
            if search_term is None:
                if op_upper in ("=", "==", "IS"):
                    final_search_terms.append("{} IS NULL".format(col))
                    continue
                if op_upper in ("!=", "<>", "IS NOT"):
                    final_search_terms.append("{} IS NOT NULL".format(col))
                    continue

            # Support `IN` for convenience.
            if op_upper == "IN":
                if search_term is None:
                    # IN (NULL) is not meaningful; treat as no matches.
                    final_search_terms.append("1=0")
                    continue
                if isinstance(search_term, (bytes, str)) or not hasattr(search_term, "__iter__"):
                    raise InputIntegrityError("IN operator requires a non-string iterable")
                values = list(search_term)
                if not values:
                    # Empty IN list should match nothing.
                    final_search_terms.append("1=0")
                    continue
                placeholders = ",".join(["?"] * len(values))
                final_search_terms.append("{} IN ({})".format(col, placeholders))
                bindings.extend(values)
                continue

            # Default: binary operator with a single bound value.
            final_search_terms.append("{} {} ?".format(col, op))
            bindings.append(search_term)

        final_stmt = stmt + " AND ".join(final_search_terms)

        conn = self.get_connection()
        c = conn.cursor()
        headings = self.direct_get_column_headings(target_table)
        if not iterator_return:
            all_results = []
            try:
                for row in c.execute(final_stmt, bindings):
                    this_row = self._row_to_dict(table=target_table, headings=headings, row=row)
                    all_results.append(this_row)
            except (sqlite3.OperationalError, sqlite3.InterfaceError) as e:
                raise InputIntegrityError(f"Final statement malformed {final_stmt}. Error: {e}") from e
            conn.close()
            return all_results
        else:
            return self.iterator_return(final_stmt, headings, target_table, bindings=bindings)


    # Algorithm is as follows.
    # The output of the search qiuery parser looks something like ['or', ['and', ['or', ['token', u'titles', u'thing'],
    # ['token', u'creators', u'david']], ['token', 'all', u'simon']], ['or', ['token', u'genres', u'thing'],
    # ['token', u'genres', u'thing']]]
    # which was u'((titles:thing or creators:david) and simon) or genres:thing or genres:thing'
    # 1) Scans down looking for an index which is of the form ['string','string','string']
    # 2) Converts it, in place, into a string.
    # 3) Continues, until the entire tree has been converted
    # 4) Should end up with something which is semantically identical to the initial query, before it was parsed
    def locational_search(self, parsed_query):
        """
        Takes an index parsed from a search query - builds an appropriate search query from that parsed query and
        executes it on the database.
        :param parsed_query: A query parsed by the SearchQueryParser.
        :return:
        """
        parsed_query = deepcopy(parsed_query)
        locations = self.locations
        if self.locations is None:
            wrn_str = "DatabaseDriver doesn't have locations loaded.\n"
            LiuXin_debug_print(wrn_str)

        # The tables which will be needed to include in the inner join can be calculated from the required locations
        required_locations = set()

        # Scans down looking for an instance of an index of the form ['string', 'string', 'string'] to transform them
        while not isinstance(parsed_query, unicode):
            # index_location - used to specify a position within the parsed query tree structure
            index_location = []
            current_level = parsed_query
            while not self.can_index_be_transformed(current_level):
                for i in range(len(current_level)):
                    token = current_level[i]
                    if hasattr(token, "__iter__"):
                        current_level = token
                        index_location.append(i)
                        break
                else:
                    err_str = "Attempt to parse query has failed.\n"
                    err_str += "parsed_query: " + repr(parsed_query) + "\n"
                    raise LogicalError(err_str)

            # Including the location in the list of required locations
            if current_level[0] == "token":
                required_locations.add(current_level[1])

            # Using the index_location as a guide to build some code to actually change the value (because the number of
            # indices is variable and this seems to be the best way to access it)
            transformed_index = self.transform_index(current_level)
            stmt = "parsed_query"
            for value in index_location:
                stmt += force_unicode("[" + force_unicode(value) + "]")
            stmt += " = transformed_index"
            default_log.debug("%s", stmt)
            # exec(stmt)
            raise NotImplementedError(stmt)
        default_log.debug("%r", parsed_query)


    @staticmethod
    def can_index_be_transformed(target_index) -> bool:
        """
        Tests to see if an index can be transformed into pure string form.

        :param target_index:
        :return:
        """
        if not hasattr(target_index, "__iter__"):
            return False

        if len(target_index) != 3:
            err_str = "can_index_be_transformed in locational_search has been passed a poorly formed index.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise InputIntegrityError(err_str)
        if hasattr(target_index[1], "__iter__") or hasattr(target_index[2], "__iter__"):
            return False
        else:
            return True


    @staticmethod
    def transform_index(target_index):
        """
        Takes an index - transforms it into intermediate form.
        :param target_index:
        :return:
        """

        if target_index[0] == "token":
            return target_index[1] + ':"' + target_index[2] + '"'
        elif target_index[0] == "or":
            return "( " + target_index[1] + " OR " + target_index[2] + " )"
        elif target_index[0] == "and":
            return "( " + target_index[1] + " AND " + target_index[2] + " )"
        else:
            err_str = "transform_index in locational_search has failed while trying to parse a query.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise LogicalError(err_str)
