
"""
Represent mutable local database rows with explicit synchronization.

Row caches table/ID/schema metadata while retaining local values; construction alone does not persist a payload. Factory and blank-row helpers can insert records immediately. FixedTableStorageRow adds a declared table and subclass validation hook. These classes delegate database operations to the driver wrapper.
"""
from __future__ import annotations

import datetime
import pprint
from copy import deepcopy

from typing import Optional, Union, Iterator, Any, Self, ClassVar

from LiuXin_alpha.errors import DatabaseDriverError, InputIntegrityError, RowReadOnlyError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


class Row(RowAPI):
    """
    Keep a local row payload and cached identity for a database record.

    Deep-copy supplied values, infer schema metadata and defer persistence until an explicit writer/factory call. Read-only mode replaces sync but leaves local mutation available. Equality compares identity hashes; deepcopy returns a writable base Row sharing the database. Prefer repr or __unicode__ for diagnostics because the legacy __str__ returns bytes.

    Example:
        Given db, row = Row(db, {"work_title": "Draft"}) represents an unsaved Work; row.sync() assigns an ID and persists its values.
    """

    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None, read_only: bool = False) -> None:
        """
        Copy local values, infer schema metadata and configure synchronization.

        Initialize caches, then refresh_db_properties. Empty payloads cache only allowed tables and keep table=None. Read-only construction replaces the instance sync method with no_sync after metadata setup.

        Example:
            Given db, Row(db, {"work_title": "Draft"}, read_only=True) permits local inspection and editing while normal sync calls raise.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Optional payload deep-copied before schema inference; None starts unidentified and empty.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :return: None; initialize state without inserting a database row.
        :raises DatabaseDriverError: database is None.
        :raises DatabaseIntegrityError: Supplied fields cannot be assigned unambiguously to a table.
        """
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        self.read_only = read_only

        # Preform checking on the inputs
        if database is None:
            err_str = "Row called without a DatabasePing"
            err_str = default_log.log_variables(err_str, "ERROR", ("row_dict", row_dict), ("database", database))
            raise DatabaseDriverError(err_str)
        self.db = database

        # Copy the given row_dict into the local row_dict
        local_row_dict = dict()
        if row_dict is not None:
            local_row_dict = deepcopy(row_dict)
        self.int_row_dict = local_row_dict

        # Properties that will be read off the database/derived from the row
        self._table = None
        self.allowed_tables = None
        self.row_id = None
        self.self_linkable = False
        self.linkable_tables = []
        self.allowed_columns = set()

        self.refresh_db_properties()

        if self.read_only:
            self.sync = self.no_sync

    @property
    def table(self) -> str:
        """
        Return the table name currently cached for this row.

        Example:
            Row(db, {"work_title": "A journey"}).table is "works" when those fields identify the Work table.


        :return: Inferred or explicitly loaded table name; None for an unidentified empty Row despite the str annotation.
        """
        return self._table

    @property
    def row_id(self) -> Optional[int]:
        """
        Read a cached row ID or fall back to the payload’s discovered ID column.

        A non-None cached ID wins over row_dict, including zero. The concrete getter does not coerce values to int or check database existence.

        Example:
            For a Row with a Work payload but no work_id, row.row_id is None until an ID is assigned or allocated.


        :return: Cached ID, or the payload value; None when a known table has no supplied ID.
        :raises InputIntegrityError: Neither a cached ID nor an identified table is available.
        """
        cached_row_id = getattr(self, "_row_id", None)
        if cached_row_id is not None or cached_row_id == 0:
            return cached_row_id

        table = self.table
        if table is None:
            raise InputIntegrityError("Cannot resolve row_id for a row with no identified table")

        # If the id column is present but None, omit it so SQLite assigns an id.
        id_col = self.db.driver_wrapper.get_id_column(table)

        try:
            return self.row_dict[id_col]
        except KeyError:
            return None

    @row_id.setter
    def row_id(self, value: Optional[int]) -> None:
        """
        Replace the cached row ID without changing payload fields.

        No schema, type or row-existence validation occurs. The cached value can diverge from the payload’s ID column; primary_id on fixed-table rows updates both.

        Example:
            For an existing Row, row.row_id = 7 changes its cached identity but leaves row.row_dict untouched.


        :param value: ID cached as supplied; None makes later reads fall back to row_dict.
        :return: None; update only _row_id.
        """
        object.__setattr__(self, "_row_id", value)

    def make_read_only(self):
        """
        Mark the row read-only and replace its instance sync method with no_sync.

        Local edits and other methods that write directly through the wrapper remain available. Changing the flag back to False does not restore the replaced sync method.

        Example:
            After row.make_read_only(), assigning row["work_title"] is still local, while row.sync() raises RowReadOnlyError.


        :return: None; future normal sync() calls raise RowReadOnlyError.
        """
        self.read_only = True
        self.sync = self.no_sync

    
    @staticmethod
    def _best_effort_sqlite_object_type(database: DatabaseAPI, name: str) -> Optional[str]:
        """
        Probe sqlite_master to classify a named schema object when possible.

        Open a driver connection through database.driver_wrapper.driver and close it in a finally block. Unsupported drivers, unavailable metadata and lookup failures are deliberately treated as unknown; cleanup errors are suppressed.

        Example:
            Given a SQLite-backed db, Row._best_effort_sqlite_object_type(db, "works") normally returns "table".


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param name: Object name bound to the sqlite_master lookup.
        :return: Stored schema type such as table, view or index, or None on no match or any probe error.
        """
        try:
            driver = getattr(database, "driver_wrapper", None)
            driver = getattr(driver, "driver", None)
            get_conn = getattr(driver, "get_connection", None)
            if get_conn is None:
                return None
            conn = get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT type FROM sqlite_master WHERE name = ? LIMIT 1;", (name,))
                row = cur.fetchone()
                if row:
                    return row[0]
                return None
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            return None

    @classmethod
    def from_idless_row_dict(
        cls,
        database: DatabaseAPI,
        row_dict: dict[str, Any],
        *,
        table: Optional[str] = None,
        read_only: bool = False,
        reload_from_db: bool = True,
    ) -> "Self":
        """
        Insert an ID-less payload and return a row representing the inserted record.

        A None ID field is removed before insertion; an explicit non-None ID is retained. Construction with read_only=True still inserts first. Insertion and reload are separate driver operations, so a later reload/construction failure need not undo the insert.

        Example:
            Given db, Row.from_idless_row_dict(db, {"work_title": "A journey"}) creates a Work and normally reloads its generated ID and defaults.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Column/value mapping; copied before being retained by the row.
        :param table: Optional target hint for view checks, ID-column lookup and reload; insertion still delegates to the wrapper’s payload-based table inference.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :param reload_from_db: Reload a nonzero returned ID when its ID column is known, to include defaults and trigger changes.
        :return: New instance of the requested row class, reloaded when possible or built from the copied payload.
        :raises DatabaseDriverError: No database is supplied, a SQLite view is targeted, or the underlying insert fails.
        :raises TypeError: row_dict is None.
        :raises DatabaseIntegrityError: Payload inference or a database constraint prevents insertion.
        """
        if database is None:
            raise DatabaseDriverError("from_idless_row_dict called without a database")
        if row_dict is None:
            raise TypeError("from_idless_row_dict requires a row_dict")
        local_row_dict: dict[str, Any] = deepcopy(row_dict)

        target_table = table or database.driver_wrapper.identify_table_from_row_dict(local_row_dict)

        # If the inferred target is a view, error early with a helpful message.
        obj_type = cls._best_effort_sqlite_object_type(database, target_table)
        if obj_type == "view":
            raise DatabaseDriverError(
                f"Cannot INSERT into '{target_table}' because it is a view. "
                f"Pass table='<base table>' explicitly to from_idless_row_dict()."
            )

        # If the id column is present but None, omit it so SQLite assigns an id.
        id_col: Optional[str] = None
        try:
            id_col = database.driver_wrapper.get_id_column(target_table)
        except Exception:
            id_col = None

        if id_col and id_col in local_row_dict and local_row_dict[id_col] is None:
            local_row_dict.pop(id_col, None)

        new_id = database.driver_wrapper.add_row(local_row_dict)

        # If we have a numeric id, prefer to reload from the DB so defaults/triggers are reflected.
        if reload_from_db and new_id not in (None, 0) and id_col:
            row = cls(database=database, row_dict=None, read_only=read_only)
            row.load_row_from_id(row_id=int(new_id), table=target_table)
            return row

        # Otherwise, return a row built from what we know.
        if id_col and new_id not in (None, 0):
            local_row_dict[id_col] = new_id

        return cls(database=database, row_dict=local_row_dict, read_only=read_only)

    @staticmethod
    def _json_sanitize(
        obj: Any,
        *,
        max_text: int = 500,
        max_items: int = 50,
        _depth: int = 0,
        _max_depth: int = 3,
    ) -> Any:
        """
        Convert common diagnostic values to primitives with best-effort truncation.

        Preserve None, numeric values and booleans, including non-finite floats. Dates/times become ISO strings, bytes become tagged hex, and nested Rows become table/ID references before the depth check. Map keys are stringified and can collide; sets retain iteration order. At the depth limit repr is returned without text truncation, so bounds are advisory. Container conversion failures fall back to repr, which itself may raise.

        Example:
            >>> Row._json_sanitize(b"AB")
            {'__type__': 'bytes', 'encoding': 'hex', 'value': '4142'}
            >>> Row._json_sanitize([1, 2, 3], max_items=2)
            [1, 2, {'__truncated__': True}]


        :param obj: Value to convert.
        :param max_text: Text or byte-hex threshold; an ellipsis can exceed thresholds smaller than three.
        :param max_items: Maximum mapping/collection entries before a truncation marker.
        :param _depth: Current container recursion depth.
        :param _max_depth: Depth at which non-scalar containers fall back directly to repr.
        :return: Primitive, list, mapping, tagged byte/Row reference, or representation string.
        """
        if obj is None or isinstance(obj, (str, int, float, bool)):
            if isinstance(obj, str) and len(obj) > max_text:
                return obj[: max(0, max_text - 3)] + '...'
            return obj

        # Dates / times
        import datetime as _dt
        if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
            try:
                return obj.isoformat()
            except Exception:
                return repr(obj)

        # Bytes: represent as hex, truncated
        if isinstance(obj, (bytes, bytearray, memoryview)):
            b = bytes(obj)
            hx = b.hex()
            if len(hx) > max_text:
                hx = hx[: max(0, max_text - 3)] + '...'
            return {'__type__': 'bytes', 'encoding': 'hex', 'value': hx}

        # Rows: avoid deep recursion/cycles
        if isinstance(obj, Row):
            return {'__type__': 'RowRef', 'table': obj.table, 'row_id': obj.row_id}

        if _depth >= _max_depth:
            return repr(obj)

        try:
            from collections.abc import Mapping
            if isinstance(obj, Mapping):
                out: dict[str, Any] = {}
                for i, (k, v) in enumerate(obj.items()):
                    if i >= max_items:
                        out['__truncated__'] = True
                        break
                    out[str(k)] = Row._json_sanitize(
                        v, max_text=max_text, max_items=max_items, _depth=_depth + 1, _max_depth=_max_depth
                    )
                return out

            if isinstance(obj, (list, tuple, set, frozenset)):
                out_list = []
                for i, v in enumerate(obj):
                    if i >= max_items:
                        out_list.append({'__truncated__': True})
                        break
                    out_list.append(
                        Row._json_sanitize(
                            v, max_text=max_text, max_items=max_items, _depth=_depth + 1, _max_depth=_max_depth
                        )
                    )
                return out_list
        except Exception:
            pass

        s = repr(obj)
        if len(s) > max_text:
            s = s[: max(0, max_text - 3)] + '...'
        return s

    def to_jsonable(
        self,
        *,
        include_values: bool = True,
        max_cols: int = 50,
        max_text: int = 500,
        include_db_uuid: bool = True,
    ) -> dict[str, Any]:
        """
        Build a lossy diagnostic mapping of row identity and optional local values.

        The concrete Row sanitizer handles common value types, but this is not a strict JSON or size guarantee: identity/UUID fields pass through, non-finite floats remain, and depth-limit repr strings are unbounded. Values are not redacted. Zero/negative limits are not rejected, and truncation markers can exceed the nominal item/text limits. No database refresh occurs.

        Example:
            For a loaded row with ordinary scalar identity fields, json.dumps(row.to_jsonable(include_values=False)) encodes its diagnostic identity without row values.


        :param include_values: Include the local row_dict snapshot, default True.
        :param max_cols: Maximum top-level columns and nested collection items before truncation markers.
        :param max_text: Text/byte-hex truncation threshold forwarded to the sanitizer.
        :param include_db_uuid: Include the database uuid attribute, or None when unavailable.
        :return: Dictionary with type, table, row_id and read_only, plus optional db_uuid and sanitized row_dict.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """
        payload: dict[str, Any] = {
            '__type__': 'Row',
            'table': object.__getattribute__(self, 'table'),
            'row_id': object.__getattribute__(self, 'row_id'),
            'read_only': bool(getattr(self, 'read_only', False)),
        }

        if include_db_uuid:
            payload['db_uuid'] = getattr(getattr(self, 'db', None), 'uuid', None)

        if include_values:
            rd = object.__getattribute__(self, 'int_row_dict') or {}
            out: dict[str, Any] = {}
            for i, (k, v) in enumerate(rd.items()):
                if i >= max_cols:
                    payload['row_dict_truncated'] = True
                    break
                out[str(k)] = Row._json_sanitize(v, max_text=max_text, max_items=max_cols)
            payload['row_dict'] = out

        return payload

    def refresh_db_properties(self) -> None:
        """
        Infer row identity and cache schema/link metadata from the current payload.

        For an empty or false payload, refresh only allowed_tables and retain other cached fields. Otherwise infer the table, read its ID column and linkability, and cache allowed columns. False ID values become None except values comparing equal to zero, which are cached as zero. Inference errors can leave earlier metadata changes in place.

        Example:
            After replacing row.row_dict with a new valid table payload, row.refresh_db_properties() recomputes its table, ID and allowed columns.


        :return: None; update cached properties without fetching stored row values.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if not row_dict:
            allowed_tables = self.db.driver_wrapper.get_allowed_tables_snapshot()
            object.__setattr__(self, "allowed_tables", allowed_tables)
            return None

        table = self.db.driver_wrapper.identify_table_from_row_dict(row_dict)
        object.__setattr__(self, "_table", table)

        allowed_tables = self.db.driver_wrapper.get_allowed_tables_snapshot()
        object.__setattr__(self, "allowed_tables", allowed_tables)

        row_id_column = self.db.driver_wrapper.get_id_column(table)
        row_id = row_dict.get(row_id_column)
        if row_id != 0:
            row_id = row_id if row_id else None
        elif row_id is None:
            pass
        else:
            row_id = 0

        object.__setattr__(self, "row_id", row_id)

        self_linkable = True if self.db.driver_wrapper.check_for_intralink_table(table) else False
        object.__setattr__(self, "self_linkable", self_linkable)

        linkable_tables = self.db.driver_wrapper.get_interlinked_tables(table)
        object.__setattr__(self, "linkable_tables", linkable_tables)

        allowed_columns = self.db.get_column_headings(table)
        object.__setattr__(self, "allowed_columns", allowed_columns)

    @property
    def row_dict(self):
        """
        Expose the mutable local column/value mapping directly.

        Direct dictionary mutation bypasses __setitem__ schema checks and does not refresh cached identity. Read-only mode does not freeze this object.

        Example:
            For a loaded row, row.row_dict["work_title"] = "Revised" changes only the local mapping until sync.


        :return: The internal mapping itself, not a copy; legacy missing-row loads may retain a false sentinel.
        """

        return self.int_row_dict

    @row_dict.setter
    def row_dict(self, val):
        """
        Deep-copy replacement local values without refreshing row metadata.

        Table, ID and allowed-column caches remain unchanged until explicitly refreshed. This setter also preserves driver missing-row sentinels such as False.

        Example:
            After row.row_dict = {"work_title": "New"}, call row.refresh_db_properties() before relying on the new payload’s identity.


        :param val: Replacement payload passed to deepcopy; no mapping validation occurs.
        :return: None; assign the deep copy to int_row_dict.
        """

        self.int_row_dict = deepcopy(val)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - OUTPUT OPTIONS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __unicode__(self):
        """
        Render the local payload and cached row/schema metadata as diagnostic text.

        The representation is unbounded and includes payload values. It reads local state rather than reloading the row.

        Example:
            For a loaded row, details = row.__unicode__() includes its row_dict and relationship metadata.


        :return: Multiline string with values, table, allowed tables/columns, ID and linkability.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """
        info_str = "LiuXin Row Object\n"

        info_str += "row_dict: \n" + pprint.pformat(object.__getattribute__(self, "row_dict")) + "\n"

        info_str += "table: " + six_unicode(object.__getattribute__(self, "table")) + "\n"
        info_str += "allowed_tables: " + pprint.pformat(object.__getattribute__(self, "allowed_tables")) + "\n"
        info_str += "row_id: " + six_unicode(object.__getattribute__(self, "row_id")) + "\n"
        info_str += "self_linkable: " + six_unicode(object.__getattribute__(self, "self_linkable")) + "\n"
        info_str += "linkable_tables: " + six_unicode(object.__getattribute__(self, "linkable_tables")) + "\n"
        info_str += "allowed_columns: " + six_unicode(object.__getattribute__(self, "allowed_columns")) + "\n"

        return info_str

    def __str__(self):
        """
        Return the legacy UTF-8 byte encoding of the detailed row representation.

        Python 3 str(row) requires __str__ to return text and therefore raises TypeError with this implementation. Use row.__unicode__() for the detailed text or repr(row) for a compact label.

        Example:
            Given a loaded row, row.__str__() returns encoded bytes; row.__unicode__() returns the corresponding text.


        :return: bytes from the concrete Row, despite the API’s str annotation.
        """

        return self.__unicode__().encode("utf-8")

    def __repr__(self):

        """
        Format a compact diagnostic label containing database, table and ID.

        Example:
            For a loaded row, repr(row) produces an LX ROW OBJECT label suitable for inspecting its database identity.


        :return: Text label using repr(db), the cached table and resolved row ID.
        :raises InputIntegrityError: An unidentified row cannot resolve its ID.
        """

        rtn_str = "|LX ROW OBJECT - DatabasePing {0} - Table {1} - Id {2}|".format(
            repr(self.db),
            object.__getattribute__(self, "table"),
            six_unicode(object.__getattribute__(self, "row_id")),
        )
        return rtn_str

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - I/O METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Stage a column value locally after checking its schema membership.

        The first assignment to an empty mapping triggers metadata inference. Later assignments require an allowed column but do not refresh cached identity, even when assigning the ID field. Read-only rows still permit this local operation.

        Example:
            Given a Work row, row["work_title"] = "Revised" stages a value; row.sync() is the separate persistence step.


        :param key: Known column belonging to the row’s table, or a column that identifies an empty row.
        :param value: Value retained as supplied; no database write occurs here.
        :return: None; the local mapping is changed.
        :raises KeyError: No table recognizes the key, or it is not allowed on this row.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        target_table = self.db.driver_wrapper.identify_table_from_column(key, error=False)
        if target_table is None:
            err_str = "Cannot set item - does not correspond to a column heading from any table in this database"
            err_str = default_log.log_variables(err_str, "ERROR", ("db", self.db), ("key", key), ("value", value))
            raise KeyError(err_str)

        # If the row_dict has nothing in it add the value and proceed
        if not row_dict:
            row_dict[key] = value
            self.refresh_db_properties()
            return None

        # Check to make sure the key is on the list of allowed column headings
        allowed_cols = object.__getattribute__(self, "allowed_columns")
        if key not in allowed_cols:
            err_str = "Cannot set item - key is not one of the column headings allowed for this table."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("db", self.db),
                ("key", key),
                ("value", value),
                ("allowed_cols", allowed_cols),
            )
            raise KeyError(err_str)

        row_dict[key] = value
        return None

    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        Read a local value, materializing None for an absent recognized column.

        An already stored key wins without schema revalidation. A missing allowed key is inserted into row_dict with value None; this is not a database refresh.

        Example:
            For a partial Work row, row["work_id"] can return None and add that key to the local mapping before the row has been inserted.


        :param item: Existing payload key or recognized column for the inferred table.
        :return: Stored value, including None despite the narrower return annotation.
        :raises KeyError: The key is neither stored nor allowed for the table.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if item in row_dict:
            return row_dict[item]

        allowed_columns = object.__getattribute__(self, "allowed_columns")
        if item in allowed_columns:
            row_dict[item] = None
            return row_dict[item]

        err_str = "item couldn't be found in the row_dict, and wasn't a recognized column heading for this table"
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("item", item),
            ("row_dict", row_dict),
            ("allowed_columns", allowed_columns),
        )
        raise KeyError(err_str)

    # ---------------------------
    #
    # - UPDATE METHODS START HERE

    def update_and_check(self) -> None:
        """
        Refresh cached schema and identity metadata from the local payload.

        Despite its name, this does not fetch database values, persist edits, or perform additional value validation.

        Example:
            After a direct row_dict replacement, row.update_and_check() recalculates the table/ID metadata.


        :return: None; delegates to refresh_db_properties.
        """
        self.refresh_db_properties()

    def load_row_from_id(self, row_id: int = None, table: str = None) -> None:
        """
        Replace local values with a driver lookup for the selected table and ID.

        Supplied identity values change the object before lookup, and unsynced local values are discarded. The concrete method does not translate a missing-row sentinel: the SQLite wrapper can leave row_dict=False and retain cached identity. Use Database.get_row_from_id for its higher-level missing-row handling.

        Example:
            Given row = Row(db), row.load_row_from_id(work_id, "works") loads an existing Work into that object.


        :param row_id: ID to cache before lookup; None reuses the existing ID.
        :param table: Table to cache before lookup; None reuses the existing table.
        :return: None after replacing row_dict and refreshing metadata.
        :raises InputIntegrityError: ID resolution fails while the table is still unknown.
        :raises TypeError: A required ID or table remains unset after resolution.
        """
        if row_id is not None:
            object.__setattr__(self, "row_id", row_id)
        if table is not None:
            object.__setattr__(self, "_table", table)

        row_id = object.__getattribute__(self, "row_id")
        table = object.__getattribute__(self, "table")
        if row_id is None or table is None:
            err_str = "Unable to load_from_id  - id or table has yet to be set."
            default_log.error(err_str)
            raise TypeError(err_str)

        row_dict = self.db.driver_wrapper.get_row_from_id(table=table, row_id=row_id)
        object.__setattr__(self, "row_dict", row_dict)

        self.refresh_db_properties()

    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Create a blank database row and replace this object’s local state with it.

        This allocates a stored row with an ID through the wrapper; it is not an empty in-memory constructor. Existing local contents are overwritten without a guard. Read-only mode does not prevent this factory operation. The wrapper clears its scratch marker in the returned mapping; persistence of that clearing requires a later sync.

        Example:
            Given row = Row(db), row.load_blank_row("works") allocates and loads a new blank Work.


        :param table: Target table, or None to reuse the cached table.
        :return: None; load the new blank mapping and refresh metadata.
        :raises InputIntegrityError: The target is invalid or is a view rather than a writable base table.
        """
        if table is not None:
            object.__setattr__(self, "_table", table)

        blank_row_dict = self.db.driver_wrapper.get_blank_row(object.__getattribute__(self, "table"))
        object.__setattr__(self, "int_row_dict", blank_row_dict)

        self.refresh_db_properties()

    def ensure_row_has_id(self) -> None:
        """
        Allocate a database-backed ID when the local payload has none.

        The wrapper preserves a non-None ID, including zero, without checking row existence. Otherwise it inserts a blank row and copies only that ID into the payload; the payload’s other values are written later by sync. This method itself is not disabled by read_only mode.

        Example:
            For an unsaved Row with work_title set, row.ensure_row_has_id() allocates its database ID before a later row.sync().


        :return: None; replace the local payload with the wrapper result and cache its extracted ID.
        """
        new_row_dict = self.db.driver_wrapper.ensure_row_has_id(object.__getattribute__(self, "row_dict"))
        new_id = self.db.driver_wrapper.get_id_from_row(new_row_dict)

        object.__setattr__(self, "row_dict", new_row_dict)
        object.__setattr__(self, "row_id", new_id)

    def sync(self) -> None:
        """
        Ensure an ID and write the local payload through the driver wrapper.

        When ID allocation is needed, blank-row insertion and payload update are separate wrapper calls without a transaction composed by Row. A failed update can therefore leave an allocated blank row. The method does not reload defaults or trigger changes afterward. Read-only instances normally replace this method with no_sync.

        Example:
            Given row = Row(db, {"work_title": "A journey"}), row.sync() allocates an ID if necessary and writes the staged title.


        :return: None; the driver’s update status is discarded.
        :raises RowReadOnlyError: An instance in read-only mode routes sync to no_sync.
        """
        if self.row_id is None:
            self.ensure_row_has_id()

        row_dict = object.__getattribute__(self, "int_row_dict")
        if row_dict:
            self.db.driver_wrapper.update_row(row_dict)

    def no_sync(self) -> None:
        """
        Reject a synchronization request for a row in read-only mode.

        Example:
            A read-only row routes row.sync() to this method and raises RowReadOnlyError.


        :return: Never returns normally.
        :raises RowReadOnlyError: Always, regardless of current payload or flag value.
        """
        raise RowReadOnlyError("You cannot sync this row - we're in read only mode.")

    # ---------------------------
    # -------------------------------
    # - COMPARISON METHODS START HERE

    def __hash__(self) -> int:
        """
        Hash the database UUID, resolved row ID and table as one tuple.

        Non-identity payload changes leave the hash alone. Changing the table, ID or database UUID can change it, so assign a stable identity before using a row as a dictionary key. An identified but unsaved row may hash with a None ID.

        Example:
            Given a persisted row and a separately loaded copy, hash(row) equals hash(copy) when database UUID, table and ID match.


        :return: Process-local hash integer for the current identity fields.
        :raises InputIntegrityError: The row has neither a cached ID nor an identified table.
        """
        uuid = self.db.uuid
        row_id = object.__getattribute__(self, "row_id")
        table = object.__getattribute__(self, "table")
        return hash((uuid, row_id, table))

    def __eq__(self, other: RowAPI) -> bool:
        """
        Compare this row’s hash with the other object’s hash.

        The concrete Row implementation compares hashes rather than comparing identity tuples directly. Hash collisions, including with a non-Row object, can therefore compare equal; local non-identity field values are ignored.

        Example:
            Two loaded rows with the same database UUID, table and ID compare equal even when their local display values differ.


        :param other: Object accepted by hash(); no Row type check is performed.
        :return: True when the two hash integers are equal.
        :raises TypeError: other is unhashable.
        :raises InputIntegrityError: This row’s ID cannot be resolved because its table is unknown.
        """
        self_hash = self.__hash__()
        other_hash = hash(other)
        if self_hash == other_hash:
            return True
        else:
            return False

    # -------------------------------
    # -----------------------------------------------
    #
    # - DICTIONARY EMULATION MAGIC METHODS START HERE

    def keys(self) -> None:
        """
        Expose the live dictionary view of materialized local column names.

        The view reflects later mapping mutations and does not enumerate unmaterialized schema columns.

        Example:
            For a partial row, tuple(row.keys()) lists only the fields currently in row.row_dict.


        :return: dict_keys view in the concrete Row, despite the None return annotation.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        return row_dict.keys()

    def __iter__(self) -> Iterator[str]:
        """
        Iterate the column keys currently stored in the local mapping.

        Partial rows expose only materialized keys. This does not enumerate all schema columns, fetch missing values or snapshot the mapping before iteration.

        Example:
            For a Row constructed from {"work_title": "A journey"}, tuple(row) initially contains only "work_title".


        :return: Iterator yielding keys in dictionary insertion order.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        keys_list = row_dict.keys()
        for key in keys_list:
            yield key

    def __contains__(self, item: str) -> bool:
        """
        Test whether a column is already present in the local row mapping.

        A recognized schema column may still be absent until loaded, assigned or accessed through __getitem__. No database query occurs.

        Example:
            For a Row containing only work_title, "work_title" in row is true even before sync assigns an ID.


        :param item: Column key to test without materializing a missing value.
        :return: True only for keys currently stored in row_dict.
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if item in row_dict.keys():
            return True
        else:
            return False

    # -----------------------------------------------
    # ------------------------
    #
    # - COPY MAGIC STARTS HERE

    def __deepcopy__(self, memo: dict[Any, Any]) -> RowAPI:
        """
        Copy local values into a new writable Row attached to the same database.

        The database object is shared. The concrete method does not preserve subclasses, the read_only flag, or an ID cached separately from row_dict. It starts a fresh deepcopy for the payload rather than using memo; recursive references to the Row are therefore unsupported. No new database record is inserted by copying.

        Example:
            Given a persisted row, copy.deepcopy(row) produces an independent value snapshot whose sync() updates the same stored ID.


        :param memo: Copy-protocol memo argument, ignored by the concrete implementation.
        :return: New base Row with deeply copied values and identity inferred again from those values.
        """
        # if memo:
        #     info_str = "Row __deepcopy__ passed a non-trivial memo"
        #     default_log.log_variables(info_str, "INFO", ("memo", memo))
        row_dict = object.__getattribute__(self, "int_row_dict")
        new_row_dict = deepcopy(row_dict)
        return Row(database=self.db, row_dict=new_row_dict)

    # ------------------------


# Todo: Consider this for all main tables?
class FixedTableStorageRow(Row):
    """
    Specialize Row with a declared table and a validation hook.

    Subclasses must define TABLE_NAME and may override ID_COLUMN and validate. Construction checks inferred table compatibility or binds metadata for an empty row. Factory constructors preserve the subclass; inherited deepcopy still produces a base Row. Generic loading and row_dict mutation remain inherited, so the fixed table is not an immutable boundary enforced on every operation.

    Example:
        Define class WorkRow(FixedTableStorageRow): with TABLE_NAME = "works"; WorkRow.from_row_id(db, work_id) loads that table into the subclass.
    """

    TABLE_NAME: ClassVar[Optional[str]] = None
    ID_COLUMN: ClassVar[Optional[str]] = None

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, Any]] = None,
            read_only: bool = False) -> None:
        """
        Initialize a Row, enforce its declared table and run subclass validation.

        The base Row first infers nonempty payloads. Empty payloads receive the declared table’s metadata. Validation also runs for read-only instances and empty factory drafts, so overrides must support those states.

        Example:
            Given a WorkRow subclass declaring TABLE_NAME="works", WorkRow(db) binds Work metadata without inserting a row.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Optional values deep-copied and inferred by Row before fixed-table checks.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :return: None; metadata is bound and validate has run once.
        :raises TypeError: TABLE_NAME is None.
        :raises ValueError: The payload identifies a different table.
        """

        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        table_name = self.TABLE_NAME
        if table_name is None:
            raise TypeError(f"{self.__class__.__name__} must define TABLE_NAME.")

        current_table = getattr(self, "table", None)
        if current_table is None:
            self._bind_fixed_table_metadata(table_name)
        elif current_table != table_name:
            raise ValueError(
                f"{self.__class__.__name__} expected table '{table_name}' but row_dict maps to '{current_table}'."
            )

        self.validate()

    def _bind_fixed_table_metadata(self, table_name: str) -> None:
        """
        Bind cached row/schema properties explicitly to one table.

        Read the ID from the existing payload using ID_COLUMN when provided, otherwise schema discovery. This does not populate missing payload fields, query a stored row, check TABLE_NAME equality or invoke validate.

        Example:
            A fixed-table subclass can use self._bind_fixed_table_metadata(self.TABLE_NAME) to initialize metadata for an empty payload.


        :param table_name: Existing table name to bind.
        :return: None; set table, allowed tables/columns, linkability and cached ID.
        """
        object.__setattr__(self, "_table", table_name)
        object.__setattr__(self, "allowed_tables", self.db.driver_wrapper.get_allowed_tables_snapshot())
        object.__setattr__(self, "self_linkable", bool(self.db.driver_wrapper.check_for_intralink_table(table_name)))
        object.__setattr__(self, "linkable_tables", self.db.driver_wrapper.get_interlinked_tables(table_name))
        object.__setattr__(self, "allowed_columns", self.db.get_column_headings(table_name))

        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(table_name)
        object.__setattr__(self, "row_id", self.int_row_dict.get(id_column))

    @classmethod
    def blank(cls, database: "DatabaseAPI", *, read_only: bool = False) -> Self:
        """
        Construct the subclass, then allocate and load a blank row for its table.

        Construction validates the empty draft before load_blank_row writes. The loaded blank payload is not separately validated here. read_only=True still allocates the row while blocking later normal sync calls.

        Example:
            Given WorkRow with TABLE_NAME="works", WorkRow.blank(db) creates a stored blank Work and returns its wrapper.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :return: Subclass instance whose newly inserted blank row has an assigned ID.
        """
        row = cls(database=database, row_dict=None, read_only=read_only)
        row.load_blank_row(table=cls.TABLE_NAME)
        return row

    @classmethod
    def from_row_id(
        cls,
        database: "DatabaseAPI",
        row_id: int,
        *,
        read_only: bool = False,
    ) -> Self:
        """
        Construct the subclass and load the requested ID from its declared table.

        Construction validates the empty draft before loading; no second validate call follows the load. The inherited loader does not convert a missing-row sentinel into a dedicated missing-row error.

        Example:
            Given WorkRow with TABLE_NAME="works", WorkRow.from_row_id(db, work_id) loads an existing Work.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_id: Existing row ID to pass to load_row_from_id.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :return: Subclass instance containing the driver lookup result.
        """
        row = cls(database=database, row_dict=None, read_only=read_only)
        row.load_row_from_id(row_id=row_id, table=cls.TABLE_NAME)
        return row

    @classmethod
    def from_idless_row_dict(
        cls,
        database: "DatabaseAPI",
        row_dict: dict[str, Any],
        *,
        table: Optional[str] = None,
        read_only: bool = False,
        reload_from_db: bool = True,
    ) -> Self:
        """
        Validate a fixed-table draft, then insert through the inherited factory.

        Draft construction already invokes validate, then this method invokes it again. The draft is discarded and the original input mapping is passed to the insertion factory, so mutations made only to draft.row_dict by validation are not inserted. The inherited factory can construct and validate another instance before/after reload. No enclosing transaction spans these operations.

        Example:
            Given WorkRow, WorkRow.from_idless_row_dict(db, {"work_title": "Draft"}) validates and inserts a Work using its declared table.


        :param database: Open database exposing driver_wrapper, schema headings and a stable uuid.
        :param row_dict: Column/value mapping; copied before being retained by the row.
        :param table: Optional table name; if supplied it must equal TABLE_NAME.
        :param read_only: Whether normal sync() calls should raise RowReadOnlyError; local field edits remain possible.
        :param reload_from_db: Reload the inserted row through the inherited factory when its nonzero ID is available.
        :return: New subclass instance returned by Row.from_idless_row_dict.
        :raises ValueError: An explicit table differs from TABLE_NAME, or the draft payload identifies another table.
        """
        if table is not None and table != cls.TABLE_NAME:
            raise ValueError(f"{cls.__name__} only supports table '{cls.TABLE_NAME}', not '{table}'.")

        draft = cls(database=database, row_dict=row_dict, read_only=read_only)
        draft.validate()

        return super().from_idless_row_dict(
            database=database,
            row_dict=row_dict,
            table=cls.TABLE_NAME,
            read_only=read_only,
            reload_from_db=reload_from_db,
        )

    @property
    def primary_id(self) -> Optional[int]:
        """
        Read the declared ID column directly from the local row payload.

        Use ID_COLUMN when set, otherwise discover the table’s ID column. This getter does not consult Row’s separately cached row_id.

        Example:
            For a loaded WorkRow, row.primary_id reads work_id from row.row_dict.


        :return: Payload ID value, or None if the field is absent.
        """
        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(self.TABLE_NAME)
        return self.row_dict.get(id_column)

    @primary_id.setter
    def primary_id(self, value: Optional[int]) -> None:
        """
        Assign the declared payload ID and update the cached row ID together.

        Column validation occurs through Row.__setitem__, but value type and database existence are not checked. No persistence occurs.

        Example:
            For a WorkRow, row.primary_id = 7 updates both its work_id payload field and cached row_id.


        :param value: ID value stored as supplied.
        :return: None; update the local field through __setitem__, then cache it.
        """

        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(self.TABLE_NAME)
        self[id_column] = value
        object.__setattr__(self, "row_id", value)

    def sync(self) -> None:
        """
        Run subclass validation before delegating persistence to Row.sync.

        Validation errors prevent this method’s persistence step. Read-only instances replace the instance sync method with no_sync during construction, so normal calls on those instances raise before this hook runs.

        Example:
            Given a writable WorkRow with a validation override, row.sync() checks the staged values before writing.


        :return: None; return status from the underlying wrapper update is not exposed.
        """
        self.validate()
        super().sync()

    def validate(self) -> None:
        """
        Provide a no-op hook for subclasses to check their local payload.

        Overrides may raise to reject values. The hook is called during construction and writable sync; factory paths can call it repeatedly and with empty drafts. It is not automatically invoked on each assignment.

        Example:
            A WorkRow subclass can override validate to reject an invalid work_title before sync, while accepting the empty draft used by from_row_id.


        :return: None; the base implementation accepts the current state without checks.
        """
        return None
