
"""
Inspect SQLite schema caches and read or update the single-row database metadata table.
"""

from __future__ import annotations

import uuid

import pprint
from copy import deepcopy

from typing import Optional, Any

from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


class MetadataMethodMixin:
    """
    Supply schema-version-aware introspection and database metadata operations.

    The host provides primary/fresh connections, schema caches and row CRUD helpers.

    Example:
        ``driver.direct_get_tables_and_columns()`` obtains the cached schema mapping.
    """

    @property
    def user_version(self) -> Optional[str]:
        """
        Read the first PRAGMA user_version value from the primary connection.

        Example:
            ``driver.user_version`` reads SQLite's application-controlled integer version.


        :return: The raw PRAGMA value, normally an integer, or ``None`` if no row is returned.
        """
        for row in self.conn.execute("pragma user_version;"):
            return row[0]

    def direct_get_user_version(self) -> str:
        """
        Expose the user_version property through the driver method interface.

        Example:
            ``driver.direct_get_user_version()`` returns the same value as ``driver.user_version``.


        :return: The raw user-version value, normally an integer.
        """
        return self.user_version

    def direct_get_schema_version(self) -> Optional[int]:
        """
        Read SQLite's schema change counter, retrying on a fresh connection after failure.

        Temporary connections are closed. A failing primary handle may be closed without replacing ``self.conn``; failure of the retry propagates. Failed integer conversion preserves the raw value.

        Example:
            ``driver.direct_get_schema_version()`` supports checking for external schema changes.


        :return: The integer counter, a raw non-convertible value, or ``None`` if no row is produced.
        """
        conn = getattr(self, "conn", None)
        close_after = False
        if conn is None:
            conn = self.get_connection()
            close_after = True

        try:
            for row in conn.execute("pragma schema_version;"):
                try:
                    return int(row[0])
                except Exception:
                    return row[0]
        except Exception:
            # If the existing connection is closed/broken, fall back to a fresh one.
            try:
                if not close_after and conn is not None:
                    conn.close()
            except Exception:
                pass

            conn2 = self.get_connection()
            try:
                for row in conn2.execute("pragma schema_version;"):
                    try:
                        return int(row[0])
                    except Exception:
                        return row[0]
            finally:
                try:
                    conn2.close()
                except Exception:
                    pass
        finally:
            if close_after:
                try:
                    conn.close()
                except Exception:
                    pass

        # Should never happen, but keep callers safe.
        return None

    def _invalidate_schema_caches(self) -> None:
        """
        Clear table/column caches, declared-type entries and the cached schema version.

        Example:
            ``driver._invalidate_schema_caches()`` forces subsequent schema reads to rebuild caches.


        :return: ``None``.
        """
        self.tables = None
        self.tables_and_columns = None
        declared_types_cache = getattr(self, "_declared_types_cache", None)
        if isinstance(declared_types_cache, dict):
            declared_types_cache.clear()
        try:
            delattr(self, "_schema_version_cached")
        except Exception:
            pass

    # Either uses the data from self.tables_and_columns, or gets the data while populating it
    def direct_get_tables(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Return cached table/view names, rebuilding when a known schema version changes.

        Suppress an ``_v`` name when its unsuffixed counterpart exists. The returned list is the cache itself; forcing refresh closes and replaces the primary handle.

        Example:
            ``driver.direct_get_tables(force_refresh=True)`` re-reads sqlite_master.


        :param force_refresh: Invalidate cached schema data and reopen the primary connection before introspection.
        :return: The mutable list of visible table and view names.
        """
        if force_refresh:
            self._invalidate_schema_caches()
            old = getattr(self, 'conn', None)
            if old is not None:
                try:
                    old.close()
                except Exception:
                    pass
            self.conn = self.get_connection()

        # Ensure we have a live connection for introspection.
        if getattr(self, 'conn', None) is None:
            self.conn = self.get_connection()

        # If we have cached data, verify it against schema_version.
        if self.tables is not None and not force_refresh:
            current = self.direct_get_schema_version()
            cached = getattr(self, "_schema_version_cached", None)
            if cached is not None and current is not None and cached != current:
                self._invalidate_schema_caches()

        if self.tables is None:
            # Include both tables and views. The FRBR/WEMI schema uses compatibility
            # surfaces (e.g. `titles`, `books`) implemented as views.
            stmt = "SELECT name FROM sqlite_master WHERE type IN ('table','view');"
            processed_return = []
            for row in self.conn.execute(stmt):
                processed_return.append(row[0])

            # If both a base view/table and its "_v" variant exist, prefer the base.
            # This avoids ambiguous row->table inference for compatibility views
            # like titles/books/identifiers.
            names = set(processed_return)
            suppressed = {n for n in names if n.endswith("_v") and n[:-2] in names}
            if suppressed:
                processed_return = [n for n in processed_return if n not in suppressed]

            self.tables = processed_return
            # Record the schema_version the cache was built against.
            self._schema_version_cached = self.direct_get_schema_version()
            return processed_return
        else:
            return self.tables

    # Todo: Not sure what normalize is intended to do...
    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        """
        Look up cached column names after canonicalizing the table identifier.

        Populate the combined cache only when absent; an existing cache is not independently version-checked here. Unknown tables raise InputIntegrityError.

        Example:
            ``driver.direct_get_column_headings("`books`")`` uses the unquoted cache key.


        :param table: Table or view name used for schema introspection.
        :param normalize: Accepted for callers; currently unused.
        :return: The cached column-name list.
        """
        # Todo: Only try and normalize if first try has failed
        # Normalise the input table identifier to the unquoted cache key.
        table = self._canonicalise_table_name_for_cache(table)

        if self.tables_and_columns is None:
            tables_and_columns = self.direct_get_tables_and_columns()
            try:
                return tables_and_columns[table]
            except KeyError:
                raise InputIntegrityError("table {} not found".format(table))

        else:
            try:
                return self.tables_and_columns[table]
            except KeyError:
                raise InputIntegrityError("table {} not found".format(table))

    @staticmethod
    def _canonicalise_table_name_for_cache(table: str) -> str:
        """
        Coerce and trim a name, discard its schema prefix and remove one wrapper pair.

        Recognize brackets and symmetric quote, backslash, percent or underscore wrappers. This is a cache-key convenience, not an SQL parser or identifier validator; failed text coercion returns the input.

        Example:
            >>> MetadataMethodMixin._canonicalise_table_name_for_cache(" main.[books] ")
            'books'


        :param table: Table or view name used for schema introspection.
        :return: The canonicalized name, or the original input if coercion fails.
        """
        try:
            t = force_unicode(table)
        except Exception:
            # If coercion fails, let the caller raise the usual integrity error.
            return table
        t = t.strip()

        # If a schema prefix is present, keep only the last identifier.
        if "." in t:
            t = t.split(".")[-1].strip()

        wrappers = ["`", '"', "[", "]", "\\", "%", "_"]
        # Bracket form: [name]
        if t.startswith("[") and t.endswith("]") and len(t) >= 2:
            return t[1:-1]

        # Single-char symmetric wrappers.
        if len(t) >= 2 and t[0] == t[-1] and t[0] in {"`", '"', "\\", "%", "_"}:
            return t[1:-1]

        return t

    def direct_get_tables_and_columns(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Build or reuse the table/view-to-column-list cache with schema-version checks.

        Use a fresh connection for PRAGMA table_info and close it on success. Return the cache object directly; force-refresh also replaces the primary connection.

        Example:
            ``driver.direct_get_tables_and_columns()["books"]`` yields the books column names.


        :param force_refresh: Invalidate cached schema data and reopen the primary connection before introspection.
        :return: The cached mapping from visible table/view names to ordered column lists.
        """
        # If the information is already cached, return it unless it is stale.
        if self.tables_and_columns is not None and not force_refresh:
            current = self.direct_get_schema_version()
            cached = getattr(self, "_schema_version_cached", None)
            if cached is None or current is None or cached == current:
                return self.tables_and_columns

        if force_refresh:
            self._invalidate_schema_caches()

        # Ensure we are introspecting a fresh list of tables if required.
        # Note: direct_get_tables may invalidate schema caches if it detects a
        # schema_version change, so do not initialise self.tables_and_columns
        # until *after* we have obtained the final table list.
        tables = self.direct_get_tables(force_refresh=force_refresh)

        self.tables_and_columns = dict()
        conn = self.get_connection()
        c = conn.cursor()
        for table in tables:
            stmt = "PRAGMA table_info({})".format(table)
            headings = []
            for row in c.execute(stmt):
                headings.append(row[1])
            self.tables_and_columns[table] = headings
        conn.close()

        # Record the schema_version the cache was built against.
        self._schema_version_cached = self.direct_get_schema_version()

        return self.tables_and_columns

    def _get_unique_column_groups(self, table: str) -> tuple[tuple[str, ...], ...]:
        """
        Collect column tuples from non-partial unique SQLite indexes.

        Skip expression indexes and empty groups. Validate table/index identifiers, raising integrity errors for unsafe names. Reuse the primary handle or close a temporary one in finally.

        Example:
            ``driver._get_unique_column_groups("books")`` lists uniqueness constraints backed by ordinary indexes.


        :param table: Table or view name used for schema introspection.
        :return: A tuple of column tuples in PRAGMA index-list order.
        """

        table = self._canonicalise_table_name_for_cache(table)
        if (
            not table
            or table[0].isdigit()
            or not all(char.isalnum() or char == "_" for char in table)
        ):
            raise InputIntegrityError(f"Unsafe table name for index introspection: {table!r}")
        owns_connection = self.conn is None
        conn = self.get_connection() if owns_connection else self.conn
        try:
            groups: list[tuple[str, ...]] = []
            for index_row in conn.execute(f'PRAGMA index_list("{table}")'):
                is_unique = bool(index_row[2])
                is_partial = len(index_row) > 4 and bool(index_row[4])
                if not is_unique or is_partial:
                    continue
                index_name = str(index_row[1])
                if not all(char.isalnum() or char == "_" for char in index_name):
                    raise DatabaseIntegrityError(
                        f"SQLite returned an unsafe index name: {index_name!r}"
                    )
                index_columns = tuple(
                    column_row[2]
                    for column_row in conn.execute(f'PRAGMA index_info("{index_name}")')
                )
                if not index_columns or any(column is None for column in index_columns):
                    continue
                columns = tuple(str(column) for column in index_columns)
                if columns:
                    groups.append(columns)
            return tuple(groups)
        finally:
            if owns_connection:
                conn.close()


    def direct_get_highest_id(self, target_table: str) -> Optional[dict[str, Any]]:
        """
        Query the maximum ID value in a table, closing the connection after its result.

        Example:
            ``driver.direct_get_highest_id("books")`` returns ``None`` when the table is empty.


        :param target_table: Existing table name used in the aggregate query.
        :return: The scalar maximum ID, or ``None`` for an empty result/table.
        """
        target_table = force_unicode(target_table)
        target_table_id = self.direct_get_id_column(target_table)
        stmt = "SELECT max({}) FROM {};".format(target_table_id, target_table)

        conn = self.get_connection()
        c = conn.cursor()

        for row in c.execute(stmt):
            conn.close()
            return row[0]

        # In the case where the table has no entries
        return None

    def direct_get_record_count(self, target_table):
        """
        Validate a table name and count its rows using a fresh connection.

        Close the connection after reading the count; an invalid table raises InputIntegrityError.

        Example:
            ``driver.direct_get_record_count("books")`` returns zero for an empty books table.


        :param target_table: Existing table name used in the aggregate query.
        :return: The raw COUNT result, normally an integer.
        """
        if not self.direct_validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        c = conn.cursor()
        stmt = "SELECT COUNT(*) FROM {}".format(target_table)
        for row in c.execute(stmt):
            conn.close()
            return row[0]

        raise NotImplementedError("This position should never be reached")

    def direct_get_row_count(self, table: str) -> int:
        """
        Count rows for a trusted SQL table identifier and convert the count to int.

        This variant does not validate the identifier; it closes the connection after obtaining a result.

        Example:
            ``driver.direct_get_row_count("books")`` counts all rows.


        :param table: Trusted table identifier interpolated directly into SQL.
        :return: The integer row count.
        """
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT COUNT(*) FROM {}".format(table)

        for row in c.execute(stmt):
            conn.close()
            return int(row[0])

        raise NotImplementedError("This position should never be reached")

    def direct_get_db_unique_id(self) -> Optional[str]:
        """
        Read the database identity from its sole metadata row.

        Multiple rows raise DatabaseIntegrityError. Reuse the primary connection when available, otherwise close the temporary handle in finally.

        Example:
            ``driver.direct_get_db_unique_id()`` reads an identity without generating one.


        :return: The stored identity value, or ``None`` when there are no rows or the value is null.
        """
        stmt = "SELECT `database_metadata_unique_id` FROM `database_metadata`"
        conn = getattr(self, "conn", None)
        owns_connection = conn is None
        if conn is None:
            conn = self.get_connection()
        unique_ids = []
        try:
            for row in conn.execute(stmt):
                unique_ids.append(row[0])
        finally:
            if owns_connection:
                conn.close()
        if len(unique_ids) == 0:
            return None
        elif len(unique_ids) == 1:
            return unique_ids[0]
        else:
            err_str = "Unable to return a unique database_metadata_unique_id.\n"
            err_str += "Thus database_metadata has more than one row.\n"
            err_str += "This should never happen.\n"
            err_str += "unique_ids: " + repr(unique_ids) + "\n"
            raise DatabaseIntegrityError(err_str)

    def direct_set_db_unique_id(self, force_value=None):
        """
        Write a supplied identity or new UUID4, commit, and verify it by rereading.

        A non-null current identity updates row ID 1 without prompting. A null/missing identity takes the insert branch, so a pre-existing null-valued row can produce duplicate metadata rows. Verification failures raise DatabaseIntegrityError after the write commits.

        Example:
            ``driver.direct_set_db_unique_id()`` generates a UUID4 string for a suitable metadata table.


        :param force_value: Identity value to write; ``None`` generates a UUID4 string.
        :return: ``True`` when rereading matches the requested value.
        """
        if force_value is None:
            new_force_value = str(uuid.uuid4())
        else:
            new_force_value = force_value

        conn = self.get_connection()
        test_val = self.direct_get_db_unique_id()
        if test_val is not None:

            stmt = (
                "UPDATE `database_metadata` SET `database_metadata_unique_id` = ? " "WHERE `database_metadata_id` = 1"
            )
            conn.execute(stmt, (new_force_value,))
            conn.commit()
            conn.close()
            actual_value = self.direct_get_db_unique_id()
            if actual_value != new_force_value:
                err_str = "Attempt to change database_metadata_unique_id failed.\n"
                err_str += "new_force_value: " + repr(new_force_value) + "\n"
                err_str += "actual_value: " + repr(actual_value) + "\n"
                raise DatabaseIntegrityError(err_str)
            return True

        else:

            stmt = "INSERT into `database_metadata` (`database_metadata_unique_id`) VALUES (?)"
            conn.execute(stmt, (new_force_value,))
            conn.commit()
            conn.close()
            actual_value = self.direct_get_db_unique_id()
            if actual_value != new_force_value:
                err_str = "Attempt to change database_metadata_unique_id failed.\n"
                err_str += "new_force_value: " + repr(new_force_value) + "\n"
                err_str += "actual_value: " + repr(actual_value) + "\n"
                raise DatabaseIntegrityError(err_str)
            return True

    def _initialize_md(self):
        """
        Ensure the metadata table contains one row, inserting a placeholder if empty.

        More than one row raises DatabaseIntegrityError; the inserted scratch value is the literal string ``None``.

        Example:
            ``driver._initialize_md()`` prepares the metadata row before field access.


        :return: ``True`` when exactly one row exists or was inserted.
        """
        md_rows = self.direct_get_all_rows("database_metadata")
        if len(md_rows) == 0:
            md_row_dict = dict()
            md_row_dict["database_metadata_scratch"] = "None"
            self.direct_add_simple_row_dict(md_row_dict)
            return True
        elif len(md_rows) == 1:
            return True
        else:
            err_str = "database_metadata table has more than 1 row.\n"
            err_str += "md_rows: " + pprint.pformat(md_rows) + "\n"
            raise DatabaseIntegrityError(err_str)

    def direct_write_metadata(self, md_field_name: str, md_field_value: Any) -> None:
        """
        Validate a metadata field, initialize the sole row and update its value.

        Unknown fields raise ValueError; multiple metadata rows raise DatabaseIntegrityError. Persistence is delegated to the row update helper.

        Example:
            ``driver.direct_write_metadata("scratch", "reviewed")`` sets the prefixed scratch column.


        :param md_field_name: Metadata column name, with or without the ``database_metadata_`` prefix.
        :param md_field_value: Value assigned to the validated metadata column.
        :return: ``None``.
        """
        md_field_name = force_unicode(deepcopy(md_field_name))

        # Check that the field name exists and can be written to
        if not md_field_name.startswith("database_metadata_"):
            n_md_field_name = "database_metadata_" + md_field_name
        else:
            n_md_field_name = md_field_name
        allowed_values = self.direct_get_column_headings("database_metadata")
        if n_md_field_name not in allowed_values:
            err_str = "Metadata cannot be written to database. - md_field_name is not recognized.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("md_field_name", md_field_name),
                ("n_md_field_name", n_md_field_name),
                ("md_field_value", md_field_value),
            )
            raise ValueError(err_str)

        # After this method has been run there should be one and only one row in the 'database_metadata' table
        self._initialize_md()
        md_rows = self.direct_get_all_rows("database_metadata")
        md_row = md_rows[0]
        md_row[n_md_field_name] = md_field_value
        self.direct_update_row_dict(row_dict=md_row)

    def direct_read_metadata(self, md_field_name: str) -> Any:
        """
        Read a validated metadata field, creating the placeholder row if needed.

        SQL null and values whose lowercase string is ``none`` return None. Other values are deep-copied. Unknown fields raise ValueError.

        Example:
            ``driver.direct_read_metadata("scratch")`` returns ``None`` for the initial placeholder.


        :param md_field_name: Metadata column name, with or without the ``database_metadata_`` prefix.
        :return: The copied field value, or ``None`` for null/none sentinels.
        """
        md_field_name = force_unicode(deepcopy(md_field_name))

        # Check that the field name exists and can be written to
        if not md_field_name.startswith("database_metadata_"):
            n_md_field_name = "database_metadata_" + md_field_name
        else:
            n_md_field_name = md_field_name
        allowed_values = self.direct_get_column_headings("database_metadata")
        if n_md_field_name not in allowed_values:
            err_str = "Metadata cannot be read from the database - md_field_name is not recognized.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("md_field_name", md_field_name),
                ("n_md_field_name", n_md_field_name),
            )
            raise ValueError(err_str)

        # After this method has been run there should be one and only one row in the 'database_metadata' table
        self._initialize_md()
        md_rows = self.direct_get_all_rows("database_metadata")
        md_row = md_rows[0]
        candidate_value = md_row[n_md_field_name]
        if candidate_value is None or str(candidate_value).lower() == "none":
            return None
        else:
            return deepcopy(candidate_value)
