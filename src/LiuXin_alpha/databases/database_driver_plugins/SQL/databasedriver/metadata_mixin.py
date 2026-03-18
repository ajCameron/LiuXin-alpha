
import uuid

import pprint
from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

class MetadataMethodMixin:
    """
    Metadata methods.
    """

    @property
    def user_version(self):
        for row in self.conn.execute("pragma user_version;"):
            return row[0]



    def _get_schema_version(self):
        """Return the current SQLite ``schema_version`` as an ``int``.

        SQLite increments ``schema_version`` whenever the schema changes. We use this
        to detect when our cached table/column metadata has become stale — including
        when another connection modifies the schema (e.g. during concurrency tests).
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


    def _invalidate_schema_caches(self):
        """Invalidate schema-related caches (tables, tables_and_columns)."""
        self.tables = None
        self.tables_and_columns = None
        try:
            delattr(self, "_schema_version_cached")
        except Exception:
            pass



    # Either uses the data from self.tables_and_columns, or gets the data while populating it
    def direct_get_tables(self, force_refresh=False):
        """
        Returns a index of the names of all tables in the database.
        :param force_refresh: Force the driver to introspect the database again
        :return:
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
            current = self._get_schema_version()
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
            self._schema_version_cached = self._get_schema_version()
            return processed_return
        else:
            return self.tables

    def direct_get_column_headings(self, table, normalize: bool = False):
        """
        Gets an index of column headings for the given table. Tries to use the cached version - falls back on direct
        access if that fails.
        :param table:
        :return column_headings:
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


    def _canonicalise_table_name_for_cache(self, table):
        """Return the unquoted table name used as the key in ``tables_and_columns``.

        Various call sites (especially legacy code) may pass table identifiers that include
        harmless wrapper characters (e.g. backticks) that SQLite accepts in SQL. Our internal
        caches, however, use *unquoted* names as keys.

        This function keeps behaviour conservative: it only strips a single matching wrapper
        pair at the ends, and does not attempt to parse/transform arbitrary SQL.
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


    def direct_get_tables_and_columns(self, force_refresh: bool = False):
        """
        Returns a dictionary keyed by the table name with the column headings as the values.
        :return table_and_columns:
        """
        # If the information is already cached, return it unless it is stale.
        if self.tables_and_columns is not None and not force_refresh:
            current = self._get_schema_version()
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
        self._schema_version_cached = self._get_schema_version()

        return self.tables_and_columns


    def direct_get_highest_id(self, target_table):
        """
        Getting a random id from the database using u'SELECT * FROM {} ORDER BY RANDOM() LIMIT 1' is really slow in the
        case of large tables.
        Something a little snappier would be nice.
        Returns the highest id in the table.
        :param target_table:
        :return:
        """
        target_table = force_unicode(target_table)
        target_table_id = self._get_id_column(target_table)
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
        Returns the number of records in a given table.
        :param target_table:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
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

    def direct_get_row_count(self, table):
        """
        Gets the row count off the table.
        :param table:
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT COUNT(*) FROM {}".format(table)

        for row in c.execute(stmt):
            conn.close()
            return row[0]


    def direct_get_db_unique_id(self):
        """
        It is useful to embed certain information about the database in it directly (thus you can tell your dealing with
        the same database, even if it's been moved to a different place or converted into a different format).
        The database_unique_id is a uuid4 string for the database which is written into the database on creation to
        uniquely define it's instance number forwever more.
        :return:
        """
        stmt = "SELECT `database_metadata_unique_id` FROM `database_metadata`"
        conn = self.get_connection()
        unique_ids = []
        for row in conn.execute(stmt):
            unique_ids.append(row[0])
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
        Allows you to set the database unique id.

        If no force value is supplied, just uses uuid to generate one and inserts it instead.
        Prompts to proceed if it detects the value is already set
        :param force_value: Default None
        :return:
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
        Checks that the MetaData table has one and only one row.
        :return None: All changes are made internally to the database
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



    def direct_write_metadata(self, md_field_name, md_field_value):
        """
        Allows for storing data in the MetaData table of the database.
        The table only has one row - if another value is given then it will be written over.
        :param md_field_name: The name of then field where the value will be stored
        :param md_field_value: The value of the field.
        :return:
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



    def direct_read_metadata(self, md_field_name):
        """
        Read metadata from the database.
        :param md_field_name:
        :return:
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

