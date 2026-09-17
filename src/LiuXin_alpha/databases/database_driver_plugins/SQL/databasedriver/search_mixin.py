
"""
Query SQL rows by ID, equality or compound conditions and expose sentinel-row helpers.

Locational search remains unfinished. Methods differ in buffering and connection ownership; their individual contracts describe those differences.
"""

from __future__ import annotations

import sqlite3
import random
import re
from copy import deepcopy

from typing import Any, Optional, Iterator, Iterable, Union

from LiuXin_alpha.errors import LogicalError, InputIntegrityError, DatabaseIntegrityError, DatabaseDriverError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode, force_unicode

from LiuXin_alpha.utils.logging import LiuXin_debug_print, default_log

from LiuXin_alpha.constants import VERBOSE_DEBUG


class SearchMixin:
    """
    Provide searches using host schema introspection and typed row conversion.

    Example:
        ``driver.direct_get_row_dict_from_id("books", 3)`` retrieves a row or ``False``.
    """

    @staticmethod
    def _coerce_search_text(value: Any) -> str:
        """
        Decode byte-like inputs as UTF-8 and coerce other values to Unicode.

        Invalid UTF-8 raises InputIntegrityError with the decoding exception as its cause.

        Example:
            >>> SearchMixin._coerce_search_text(memoryview(b"book"))
            'book'


        :param value: Search value; bytes, bytearray and memoryview require valid UTF-8.
        :return: The decoded or coerced string.
        """
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                return bytes(value).decode("utf-8")
            except UnicodeDecodeError as e:
                err_str = "search_table was passed bytes that were not valid utf-8.\n"
                err_str += "value: " + repr(value) + "\n"
                raise InputIntegrityError(err_str) from e
        return force_unicode(value)

    def direct_get_random_row_dict(
            self,
            target_table: str,
            direct: bool = False) -> Optional[dict[str, Any]]:
        """
        Pick a random row using SQLite RANDOM or rejection sampling over positive IDs.

        The default repeatedly samples IDs from 1 to the maximum and reseeds Python's global RNG. Sparse IDs can be slow; tables without positive integer IDs are unsuitable. A non-convertible null maximum returns None.

        Example:
            ``driver.direct_get_random_row_dict("books", direct=True)`` asks SQLite to select a row.


        :param target_table: Existing table name to query.
        :param direct: Use ORDER BY RANDOM rather than retrying random positive IDs.
        :return: A converted row dictionary, or ``None`` for an empty table.
        """
        conn = self.get_connection()
        c = conn.cursor()
        target_table = force_unicode(deepcopy(target_table))

        # checks that you're requesting data from an existing table
        if not self.direct_validate_existing_table_name(target_table):
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
            default_log.warn(wrn_str)
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

    def direct_get_all_rows(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> list[dict[str, Any]]:
        """
        Load every row into memory, optionally ordering by a validated column.

        Close the query connection after successful iteration. Use only when materializing the whole table is acceptable.

        Example:
            ``driver.direct_get_all_rows("books", sort_column="book_id", reverse=True)`` orders by descending ID.


        :param table: Table name resolved by the host driver.
        :param sort_column: Optional column belonging to the table; ``None`` adds no ORDER BY.
        :param reverse: Use descending order when a sort column is supplied; otherwise ignored.
        :return: A list of converted row dictionaries.
        """
        conn = self.get_connection()
        c = conn.cursor()
        table = force_unicode(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.direct_validate_existing_table_name(table):
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

    # Todo: Not sure why reverse is not implemented - fix
    def direct_get_row_dict_iterator(
            self,
            table: str,
            sort_column: str = None,
            reverse: bool = False
    ) -> Iterator[dict[str, Any]]:
        """
        Yield positive-ID rows in ascending ID order using ten-row queries.

        Each chunk is buffered and its connection closed before yielding. IDs at or below zero are excluded, and concurrent changes can affect later chunks. A supplied sort column is validated then raises NotImplementedError; reverse is unused.

        Example:
            ``list(driver.direct_get_row_dict_iterator("books"))`` omits a sentinel row with ID zero.


        :param table: Table name resolved by the host driver.
        :param sort_column: Accepted column name; non-null values currently select an unsupported ordering mode.
        :param reverse: Accepted but ignored; traversal always uses ascending IDs.
        :return: An iterator of converted row dictionaries.
        """
        table = force_unicode(table)
        table_id_column = self.direct_get_id_column(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.direct_validate_existing_table_name(table):
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

    def direct_get_unique_values_set(self, target_column: str) -> set[str]:
        """
        Collect distinct column values into a set and close the connection on success.

        Example:
            ``driver.direct_get_unique_values_set("book_title")`` includes ``None`` if SQL NULL occurs.


        :param target_column: Trusted column name whose table is inferred by the driver.
        :return: The set of raw distinct values, without string coercion.
        """
        target_table = self.direct_identify_table_from_column(column_heading=target_column)
        stmt = "SELECT DISTINCT {} FROM {};".format(target_column, target_table)
        values_set = set()
        conn = self.get_connection()
        c = conn.cursor()

        for value in c.execute(stmt):
            values_set.add(value[0])
        conn.close()
        return values_set

    def direct_get_unique_values_iterator(self, target_column: str) -> Iterator[str]:
        """
        Yield from a fully materialized set of distinct values.

        This interface is an iterator but still loads all unique values before the first yield; ordering is unspecified.

        Example:
            ``iter(driver.direct_get_unique_values_iterator("book_title"))`` iterates deduplicated values.


        :param target_column: Trusted column name whose table is inferred by the driver.
        :return: An iterator over the raw distinct values.
        """
        # Needs to sort the table after every retrieval - so will be very slow for large databases
        # Todo: Come back and optimize/make this work
        # target_table = self.__identify_table_from_column(column_heading=target_column)
        # stmt = 'SELECT DISTINCT {} FROM {} WHERE {} > {} LIMIT 1;'
        # stmt = stmt.format(target_column, target_table)
        values_set = self.direct_get_unique_values_set(target_column)
        for value in values_set:
            yield value

    def direct_get_row_dict_from_id(self, table: str, row_id: int) -> Optional[dict[str, Any]] | bool:
        """
        Bind a text-coerced ID and require at most one matching row.

        Multiple matches raise DatabaseIntegrityError; SQLite InterfaceError becomes DatabaseDriverError. Normal found/missing results close the connection.

        Example:
            ``driver.direct_get_row_dict_from_id("books", 0)`` can retrieve a sentinel row explicitly.


        :param table: Table name resolved by the host driver.
        :param row_id: ID converted to Unicode before binding to the query.
        :return: The converted row dictionary, or ``False`` if absent.
        """
        table = force_unicode(table)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_column_headings(table)
        table_id_name = self.direct_get_id_column(table)

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


    # Todo: Should go sideways and become a null rows mixin
    # ------------------------------------------------------------------
    # Sentinel / null-row helpers
    # ------------------------------------------------------------------

    def direct_has_null_row(self, table: str) -> bool:
        """
        Check for an ID-zero sentinel in a validated table and always close the query connection.

        Example:
            ``driver.direct_has_null_row("books")`` tests for the sentinel independently of other row values.


        :param table: Table name resolved by the host driver.
        :return: Whether a row with ID zero exists.
        """
        table = force_unicode(table)
        if not self.direct_validate_existing_table_name(table):
            raise InputIntegrityError(f"Unknown table: {table!r}")

        id_col = self.direct_get_id_column(table)
        stmt = f"SELECT 1 FROM `{table}` WHERE `{id_col}` = 0 LIMIT 1;"
        conn = self.get_connection()
        try:
            c = conn.cursor()
            row = c.execute(stmt).fetchone()
            return row is not None
        finally:
            conn.close()

    def direct_get_null_row(self, table: str) -> Optional[dict[str, Any]] | bool:
        """
        Check for and fetch the ID-zero row using separate queries.

        A concurrent deletion between the queries can still produce False.

        Example:
            ``driver.direct_get_null_row("books")`` fetches the sentinel if present.


        :param table: Table name resolved by the host driver.
        :return: The sentinel row dictionary, or ``False`` if absent.
        """
        table = force_unicode(table)
        if not self.direct_has_null_row(table):
            return False
        return self.direct_get_row_dict_from_id(table, 0)

    # Todo: Go through and add :raises : whereever we can
    def direct_update_null_row(
            self,
            table: str,
            updates: Optional[dict[str, Any]] = None,
            **fields: Any) -> bool:
        """
        Merge updates and delegate a sentinel-row update to the row writer.

        Missing sentinels raise InputIntegrityError. Keyword fields mutate an input dict in place and win over its values. Supplied ID-column updates also override the initial zero, so callers must omit that column to target the sentinel.

        Example:
            ``driver.direct_update_null_row("books", {"book_title": "Unknown"})`` updates an existing sentinel.


        :param table: Table name resolved by the host driver.
        :param updates: Optional dict or dict-convertible pairs of column/value updates.
        :param fields: Column/value keyword overrides merged into the updates mapping.
        :return: ``True`` after the update helper returns.
        """
        table = force_unicode(table)
        if updates is None:
            updates = {}
        if not isinstance(updates, dict):
            updates = dict(updates)
        updates.update(fields)

        if not self.direct_has_null_row(table):
            raise InputIntegrityError(f"Table {table!r} has no sentinel/null row at id=0")

        id_col = self.direct_get_id_column(table)
        row_dict = {id_col: 0}
        row_dict.update(updates)
        self.direct_update_row_dict(row_dict)
        return True

    def direct_get_all_hashes(self) -> set[str]:
        """
        Union non-null values from recognized hash columns across supported tables.

        Skip tables whose headings cannot be read; value-query failures propagate. Empty strings and other non-null values are retained.

        Example:
            ``driver.direct_get_all_hashes()`` collects file, compressed-file, new-book and legacy hash values.


        :return: A set of discovered non-null hash values.
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

    # Todo: For typing, can we just declare self has the whole driver api?
    # Todo - Merge with direct_get_unique_values - after an upgrade to allow specify a table
    # Todo - heavy abuse of typing can probably get this into a better place type wise
    def direct_get_all_values(self, table: str, column: str) -> set[Any]:
        """
        Collect a column's raw values into a set, inferring the table when omitted.

        Identifiers are interpolated and must be trusted. This helper does not explicitly close its acquired connection.

        Example:
            ``driver.direct_get_all_values("books", "book_title")`` includes SQL NULL as ``None``.


        :param table: Trusted table name, or ``None`` to infer it from the column.
        :param column: Column name to read or match.
        :return: A set of raw values, including ``None`` when present.
        """
        if table is not None:
            table = deepcopy(force_unicode(table))
        else:
            table = self.direct_identify_table_from_column(column)
        column = deepcopy(force_unicode(column))

        current_values = set()
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT {} FROM {}".format(column, table)
        for row in c.execute(stmt):
            current_values.add(row[0])
        return current_values

    def direct_iterator_return(
            self,
            stmt: str,
            headings: Iterable[str],
            table: Optional[str] = None,
            bindings: Optional[tuple[str, ...]] = None) -> None:
        """
        Execute supplied SQL lazily and yield converted row dictionaries.

        A table enables declared-type conversion. The connection closes in finally when exhausted, explicitly closed or interrupted by an exception; abandoning a live generator can retain it.

        Example:
            ``rows = driver.direct_iterator_return("SELECT book_id FROM books", ["book_id"], "books")`` creates a lazy query.


        :param stmt: Trusted SQL statement to execute.
        :param headings: Reusable ordered column-name iterable matching each result row.
        :param table: Optional table context for declared-type conversion.
        :param bindings: Optional parameter sequence passed unchanged to execute.
        :return: A generator of row dictionaries despite the None annotation.
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


    def direct_search_table(
            self,
            table: Optional[str] = None,
            column: Optional[str] = None,
            search_term: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Find exact matches for a bound text search term in a validated column.

        The table must validate and the column must be a known simple identifier. Although table defaults to None, this implementation never infers it; callers should supply it. Malformed requests and SQLite operational errors raise InputIntegrityError.

        Example:
            ``driver.direct_search_table("books", "book_title", "Example")`` performs equality matching.


        :param table: Table name resolved by the host driver.
        :param column: Column name to read or match.
        :param search_term: Non-null value coerced to text; byte-like inputs must be UTF-8.
        :return: Converted matching rows, or an empty list.
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
        if not self.direct_validate_existing_table_name(table):
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
    def direct_multi_column_search(
            self,
            search_index: list[str],
            iterator_return: bool = False
    ) -> Optional[Union[Iterator[dict[str, Any]], list[dict[str, Any]]]]:
        """
        AND together same-table comparison triples using bound values.

        Normalize NULL equality/inequality and support iterable IN values; empty/None IN matches nothing. Columns and operators must be trusted because they become SQL syntax. Some SQL-looking scalar text values are rejected even though bound. Empty input returns None normally but raises in verbose-debug mode. The iterator branch leaves its preparatory connection unclosed and delegates query ownership to a generator.

        Example:
            ``driver.direct_multi_column_search([("book_id", "IN", [1, 2])])`` matches either ID.


        :param search_index: Sized sequence of (column, operator, value) triples belonging to one inferred table.
        :param iterator_return: Return a lazy query generator instead of materializing matching rows.
        :return: A list or lazy row iterator, or ``None`` for empty input outside verbose-debug mode.
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
            table_set.add(self.direct_identify_table_from_column(column))

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
            return self.direct_iterator_return(final_stmt, headings, target_table, bindings=bindings)


    # Algorithm is as follows.
    # The output of the search qiuery parser looks something like ['or', ['and', ['or', ['token', u'titles', u'thing'],
    # ['token', u'creators', u'david']], ['token', 'all', u'simon']], ['or', ['token', u'genres', u'thing'],
    # ['token', u'genres', u'thing']]]
    # which was u'((titles:thing or creators:david) and simon) or genres:thing or genres:thing'
    # 1) Scans down looking for an index which is of the form ['string','string','string']
    # 2) Converts it, in place, into a string.
    # 3) Continues, until the entire tree has been converted
    # 4) Should end up with something which is semantically identical to the initial query, before it was parsed
    def direct_locational_search(self, parsed_query):
        """
        Inspect a copied parsed-query tree through an unfinished transformation path.

        No database search is executed. A string input is only logged; a transformable tree raises NotImplementedError at the disabled replacement step, and malformed trees may fail earlier.

        Example:
            ``driver.direct_locational_search("already transformed")`` logs the string and returns.


        :param parsed_query: Search-parser tree or pretransformed Unicode query, copied before inspection.
        :return: ``None`` for an already-string query; tree processing does not complete.
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
        Check for an iterable triple with non-iterable second and third elements.

        Wrong-length iterables raise InputIntegrityError. Strings count as iterable, so text operands do not satisfy this legacy predicate.

        Example:
            >>> SearchMixin.can_index_be_transformed(["token", 1, 2])
            True
            >>> SearchMixin.can_index_be_transformed(["token", "books", "name"])
            False


        :param target_index: Sized, indexable candidate triple, or a non-iterable value.
        :return: Whether the triple passes this structural test.
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
        Render a token, OR or AND triple as intermediate query text.

        Operands are concatenated without escaping or SQL execution; unknown operators raise LogicalError.

        Example:
            >>> SearchMixin.transform_index(["token", "books", "Example"])
            'books:"Example"'


        :param target_index: Indexable triple of operator and two string operands.
        :return: The formatted intermediate string.
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
