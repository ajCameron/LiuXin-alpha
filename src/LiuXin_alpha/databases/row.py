
"""
Container for a
"""

import datetime
import pprint
from copy import deepcopy

from typing import Optional, Union, Iterator, Any

from LiuXin_alpha.errors import DatabaseDriverError, RowReadOnlyError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


class Row(RowAPI):
    """
    Contains a row off the database.
    """

    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None, read_only: bool = False) -> None:
        """
        Represents a single row from the LiuXin database.

        :param database: A LiuXin database object
        :param row_dict: Keyed with the column names and valued with their values.
        :param read_only: If True then the row is loaded in read only mode
        :return:
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
        Return the table for the row.

        :return:
        """
        return self._table

    def make_read_only(self):
        """
        Makes the row read only.

        :return:
        """
        self.read_only = True
        self.sync = self.no_sync

    
    @staticmethod
    def _best_effort_sqlite_object_type(database: DatabaseAPI, name: str) -> Optional[str]:
        """
        Best-effort helper to classify a SQLite schema object as 'table' or 'view'.

        This is used purely to produce clearer error messages when an inferred target is a view.
        If the underlying driver is not SQLite (or does not expose sqlite_master), returns None.
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
    ) -> "Row":
        """
        Factory constructor: insert an id-less row_dict into the database and return a Row.

        This is intended to replace the "get_blank_row -> mutate -> sync" workflow.

        Notes:
          - If the target table has an INTEGER PRIMARY KEY and the id column is omitted (or set to None),
            SQLite will auto-assign an id.
          - If reload_from_db is True, we will re-read the inserted row from the database to pick up
            defaults and trigger-side changes.

        :param database: A LiuXin database instance
        :param row_dict: Column/value payload (may omit the id column)
        :param table: Optional explicit target table. Use this if inference would otherwise pick a view.
        :param read_only: If True, the returned Row will be read-only.
        :param reload_from_db: If True, reload the inserted row via id after insertion.
        :return: Row instance representing the inserted row
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
        """Convert arbitrary objects into JSON-safe primitives (bounded).

        This is for diagnostics/logging/reporting, not for persistence.
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
        """Return a JSON-serializable representation of this Row.

        Note: stdlib json.dumps() still needs a `default=` handler unless you're
        dumping the result of this method (or using a custom JSONEncoder).
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
        Read the properties for the row off the database.

        :return:
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
        return self.int_row_dict

    @row_dict.setter
    def row_dict(self, val):
        self.int_row_dict = deepcopy(val)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - OUTPUT OPTIONS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __unicode__(self):
        """
        Unicode representation of the row.
        :return:
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
        return self.__unicode__().encode("utf-8")

    def __repr__(self):

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
        Allows a dictionary like interface to the row.

        :param key:
        :param value:
        :return:
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
        Allows a dictionary like interface to the row.

        :param item:
        :return:
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
        Updates the metadata stored about the row in the class.

        :return:
        """
        self.refresh_db_properties()

    def load_row_from_id(self, row_id: int = None, table: str = None) -> None:
        """
        If an id is present, load or reload the row_dict from it.

        :param row_id: The id of the row to load - if None, tries to use the id already present
        :param table: The name of the table to load the row from
        :return:
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
        Load a blank row off the given database - will block if the table or row_dict fields are already full.

        :param table:
        :return:
        """
        if table is not None:
            object.__setattr__(self, "_table", table)

        blank_row_dict = self.db.driver_wrapper.get_blank_row(object.__getattribute__(self, "table"))
        object.__setattr__(self, "int_row_dict", blank_row_dict)

        self.refresh_db_properties()

    def ensure_row_has_id(self) -> None:
        """
        Makes sure that the row_dict has an id in it.

        :return:
        """
        new_row_dict = self.db.driver_wrapper.ensure_row_has_id(object.__getattribute__(self, "row_dict"))
        new_id = self.db.driver_wrapper.get_id_from_row(new_row_dict)

        object.__setattr__(self, "row_dict", new_row_dict)
        object.__setattr__(self, "row_id", new_id)

    def sync(self) -> None:
        """
        Sync the current contents of the row to the database.

        :return:
        """
        if self.row_id is None:
            self.ensure_row_has_id()

        row_dict = object.__getattribute__(self, "int_row_dict")
        if row_dict:
            self.db.driver_wrapper.update_row(row_dict)

    def no_sync(self) -> None:
        """
        Method to replace sync if we're in read only mode.

        :return:
        """
        raise RowReadOnlyError("You cannot sync this row - we're in read only mode.")

    # ---------------------------
    # -------------------------------
    # - COMPARISON METHODS START HERE

    def __hash__(self) -> int:
        """
        A hash for the row based on the table, id and database - will fail unless all three of these are filled.
        :return:
        """
        uuid = self.db.uuid
        row_id = object.__getattribute__(self, "row_id")
        table = object.__getattribute__(self, "table")
        return hash((uuid, row_id, table))

    def __eq__(self, other: RowAPI) -> bool:
        """
        Uses the hash function to test equality.

        :param other:
        :return:
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
        Returns the keys from the row_dict dictionary.

        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        return row_dict.keys()

    def __iter__(self) -> Iterator[str]:
        """
        Allows use of the in statement in content of a for loop.

        Iterates over all the column headings in the row.
        If the row has been loaded from the database then all column headings will be set - including if the row is
        black. If the row is being constructed rom the invididual keys, only the keys that have been set will be
        returned.
        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        keys_list = row_dict.keys()
        for key in keys_list:
            yield key

    def __contains__(self, item: str) -> bool:
        """
        Allows use of the in statement - returns true if the item is in the row_dict - false otherwise.

        :param item:
        :return:
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
        Allows for deep copying.

        :param memo:
        :return:
        """
        # if memo:
        #     info_str = "Row __deepcopy__ passed a non-trivial memo"
        #     default_log.log_variables(info_str, "INFO", ("memo", memo))
        row_dict = object.__getattribute__(self, "int_row_dict")
        new_row_dict = deepcopy(row_dict)
        return Row(database=self.db, row_dict=new_row_dict)

    # ------------------------
