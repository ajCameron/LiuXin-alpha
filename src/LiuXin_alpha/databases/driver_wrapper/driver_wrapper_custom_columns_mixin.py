
"""
Custom column method mixins -
"""

import re

from typing import List, TYPE_CHECKING, Any, Optional

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import to_json_str

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.macros_api import MacrosAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI

# Todo: Round these up and move them to the custom columns mixin - as with everything else
# Todo: Or, perhaps preferably, move them down into the driver and integrate properly
class CustomColumnsDriverWrapperMixin:
    """
    Custom columns driver wrapper methods.
    """


    def __init__(self, db: "DatabaseAPI", macros: "MacrosAPI") -> None:
        """
        Constructor.

        :param db:
        :param macros:
        """
        # Worker objects
        self.db = db
        self._conn_override = None  # prefer using the live driver connection via @property conn

        # Don't assign to self.macros directly: subclasses (e.g. DriverWrapper) may expose
        # macros as a read-only @property (no setter). Also avoid clobbering an already-set
        # macros when macros is None.
        if macros is not None:

            macros_setter = getattr(self, "set_macros", None)

            if callable(macros_setter):
                macros_setter(macros)
            else:
                try:
                    self.macros = macros
                except AttributeError:
                    # Last resort: common convention used by wrappers
                    setattr(self, "_macros", macros)

        # Todo: Might want to rename this to custom_column_tables
        # Stores properties of the database
        self.custom_tables = set()

    def _canonicalise_cc_in_table(self, in_table: str) -> str:
        """
        Resolve legacy/compat aliases for custom-column attachment tables.

        Calibre-style APIs historically default to 'books'. In a FRBR/WEMI-first schema
        that table may not exist; in that case we opportunistically map to a sensible
        analogue (typically 'manifestations').
        :param in_table:
        :return:
        """

        available = self.db.main_tables.union(self.db.interlink_tables).union(self.db.intralink_tables)
        if in_table in available:
            return in_table
        if in_table == "books":
            for candidate in ("manifestations", "items", "works"):
                if candidate in available:
                    return candidate
        return in_table


    @property
    def conn(self):
        """Return a live connection for this DB.

        Connection objects in LiuXin can be rotated/aliased during driver refresh or fixture provisioning.
        To avoid holding stale/closed connections, prefer resolving the connection from the owning driver.
        An explicit override can be supplied via the setter, but will be validated lazily and discarded if unusable.
        """
        override = getattr(self, "_conn_override", None)
        if override is not None:
            # If the override is stale/closed, drop it and fall back to the live driver connection.
            try:
                exec_fn = getattr(override, "execute", None)
                if callable(exec_fn):
                    exec_fn("SELECT 1")
                return override
            except Exception:
                try:
                    self._conn_override = None
                except Exception:
                    pass

        # Prefer db.driver.conn when available
        db = getattr(self, "db", None)
        if db is not None:
            drv = getattr(db, "driver", None)
            if drv is not None:
                try:
                    return drv.conn
                except Exception:
                    pass

        # Fallback for DriverWrapper, which may not have db set yet
        drv = getattr(self, "driver", None)
        if drv is not None:
            try:
                return drv.conn
            except Exception:
                pass

        return override

    @conn.setter
    def conn(self, value):
        # Backwards-compat: allow code to assign self.conn = <connection>.
        # Prefer leaving this unset so the property resolves a fresh connection from the driver.
        self._conn_override = value


    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN METHODS
    def deleted_marked_custom_columns(self) -> None:
        """
        Deleted custom columns which have been marked for removal.

        Should be done during a reload or load before the
        custom columns are read off the database.
        :return :
        """
        # Custom columns can be attached to any table (not just books). The link-table name
        # depends on the attachment table, so we must include custom_column_in_table when
        # computing table/link-table pairs.
        num_table_lt_map: dict[int, tuple[str, str]] = {}

        # Prefer executing via the connection bound to this instance to avoid
        # side effects from driver_wrapper connection/lock aliasing.
        try:
            rows = self.conn.get_row(
                "SELECT custom_column_id, custom_column_in_table FROM custom_columns "
                "WHERE custom_column_mark_for_delete=1"
            )
        except Exception:
            # Backwards-compat path for older DBs/schemas/macros (books-only).
            rows = [(num, "books") for num in self.db.macros.get_all_cc_ids_marked_for_delete(conn=self.conn)]

        for r in rows:
            if isinstance(r, dict):
                num = int(r.get("custom_column_id"))
                in_table = r.get("custom_column_in_table") or "books"
            else:
                num = int(r[0])
                in_table = (r[1] if len(r) > 1 else None) or "books"

            num_table_lt_map[num] = self.custom_table_names(num, in_table=in_table)

        if num_table_lt_map:
            self.db.macros.preform_cc_column_delete_from_map(num_table_lt_map, conn=self.conn)

    def get_custom_tables(self) -> set[str]:
        """
        Get the names of all the custom tables currently registered on the database.

        :return:
        """
        # Always use the driver's live connection to avoid stale db.conn aliases pointing
        # at a closed connection after driver/connection churn.
        return self.db.macros.direct_get_custom_tables(conn=self.db.driver.conn)

    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Get the extra value for the custom table at a given index.

        :param link_table:
        :param index:
        :return:
        """
        return self.db.macros.direct_get_custom_and_extra(link_table, index, conn=self.conn)

    def direct_get_custom_id_val_pairs(self, table: str) -> tuple[int, Any]:
        """
        Retrieve a list of pairs of the ids from the custom table and their values.

        :param table:
        :return:
        """
        return self.db.macros.get_all_cc_id_val_pairs(table, conn=self.conn)

    @staticmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Get the custom column table name and the link table name associated with it.

        :param num:
        :param in_table: The table the custom column is linked to - defaults to "books"
        :return:
        """
        try:
            num = int(num)
        except ValueError as e:
            err_str = "Cannot coerce table num (id) to an integer"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("num", num), ("num_type", type(num)))
            raise ValueError(err_str)

        return "custom_column_%d" % num, "%s_custom_column_%d_link" % (in_table, num)

    # Todo: Custom columns needed to be added to the appropriate table name cache after they've been created - check
    #       that this is happening
    def set_custom_column_metadata(
            self,
            num: int,
            name: Optional[str] = None,
            label: Optional[str] = None,
            is_editable: Optional[str] = None,
            display: Optional[str] = None,
            in_table: str = "books"):
        """
        Change the metadata for a custom column - identified with the num.

        :param num: The id integer for the custom column
        :param name: The new name of the custom column
        :param label:
        :param is_editable:
        :param display: Used by the interfaces to know what name to give the custom column
        :param in_table: Which table is the custom column being attached to? (defualt is "books")
        :return:
        """
        # Note: the caller is responsible for scheduling a metadata backup if necessary
        changed = self.db.macros.set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )

        # Note: the caller is responsible for scheduling a metadata backup if necessary
        return changed

    # Todo: Restrict multiple to the known values
    # Todo: The combination of name and table should be unique
    # Todo: Change data_type to datatype
    # Todo: in_table and table seme to do the same thing
    # Todo: datatype should be fully typeable
    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: Optional[str] = None,
        editable: bool = True,
        display: Optional[str] = None,
        in_table: bool = "books",
        table: Optional[str] = None,
        make_category = None,
    ):
        """
        Add a custom column to the books table.

        Also write the metadata describing the custom column out to the metadata tables.
        :param label:
        :param name:
        :param datatype: Must be one of the following - rating, int, text, comments, series, composite, enumeration,
                         float, datetime, bool
        :param is_multiple:
        :param editable: Is the column editable?
        :param display:
        :param in_table: Which table should the custom column be created in? (Defaults to books for historical reasons)
                         Should be in the main, intralink or interlink tables
        :param table:
        :param make_category: Add this custom  column to the category browser
        :return:
        """
        # Todo: Somewhere there are allowed cc datatypes - preform a check that we're being given one of them

        # Support newer/clearer keyword alias: `table=` (same as `in_table=`)
        if table is not None:
            if in_table != "books" and in_table != table:
                raise TypeError("Pass only one of table= or in_table= (or keep them identical).")
            in_table = table

        in_table = self._canonicalise_cc_in_table(in_table)

        assert in_table in self.db.main_tables.union(self.db.interlink_tables).union(
            self.db.intralink_tables
        ), "in_table {} not found in main, intralink or interlink tables".format(in_table)

        # Some datatypes just don't make much sense to be multiple - so throwing an error if we can some combinations
        if is_multiple and datatype in ("rating", "int", "float", "datetime", "bool"):
            err_str = "Cannot have a mutliple column of type {} - makes no sense".format(datatype)
            raise NotImplementedError(err_str)

        if display is None:
            display = {}

        # calibre: composite columns can optionally be shown in the (misnamed) "Tag Browser".
        # It is controlled by display['make_category'] rather than is_category.
        if make_category is not None and datatype == "composite":
            display = dict(display)
            display["make_category"] = bool(make_category)

        # Update the custom columns table with the new entry - once this has been done it will, at a minimum, be created
        # at the next startup
        label = label if label is not None else "{}__{}".format(in_table, name)

        if not label:
            raise ValueError(_("The label must be non-empty."))


        if re.match(r"^\w+$", label) is None or (not label) or (not label[0].isalpha()) or label.lower() != label:
            raise ValueError(
                _("The label must contain only lower case letters, digits and underscores, and start " "with a letter")
            )
        if datatype not in CUSTOM_DATA_TYPES:
            raise ValueError("%r is not a supported data type" % datatype)

        # If normalized - a link table is required and generated
        normalized = datatype not in (
            "datetime",
            "comments",
            "int",
            "bool",
            "float",
            "composite",
        )
        is_multiple = is_multiple and datatype in (
            "text",
            "composite",
            "comments",
            "series",
            "enumeration",
        )

        # need_order determines if the custom column needs an additional column to allow for re0ordering of the
        # values
        ordered = False
        if is_multiple and datatype in ("comments", "series"):
            ordered = True

        # In calibre, text might be somewhat badly named - I think it should be "tags" or something similar
        if datatype in ("rating", "int"):
            dt = "INTEGER"
        elif datatype in ("text", "comments", "series", "composite", "enumeration"):
            dt = "TEXT"
        elif datatype in ("float",):
            dt = "REAL"
        elif datatype == "datetime":
            dt = "timestamp"
        elif datatype == "bool":
            dt = "BOOL"
        else:
            err_str = "datatype not recognize and not supported"
            err_str = default_log.log_variables(err_str, "ERROR", ("datatype", datatype))
            raise NotImplementedError(err_str)

        # Todo: Really rating should point over to a rating table of some sort
        cc_row_dict = self.db.driver_wrapper.get_blank_row("custom_columns")
        cc = "custom_column_"
        cc_row_dict[cc + "label"] = label
        cc_row_dict[cc + "name"] = name
        cc_row_dict[cc + "datatype"] = datatype
        cc_row_dict[cc + "is_multiple"] = is_multiple
        cc_row_dict[cc + "editable"] = editable
        cc_row_dict[cc + "display"] = to_json_str(display)  # display is a dict, and so has to be serialized
        cc_row_dict[cc + "normalized"] = normalized
        cc_row_dict[cc + "in_table"] = in_table
        cc_row_dict[cc + "ordered"] = ordered
        self.db.driver_wrapper.update_row(cc_row_dict)

        num = cc_row_dict["custom_column_id"]

        collate = "COLLATE NOCASE" if dt == "TEXT" else ""
        cc_table, link_table = self.custom_table_names(num, in_table=in_table)

        self.db.macros.create_cc_table(
            normalized=normalized,
            datatype=datatype,
            dt=dt,
            table=cc_table,
            link_table=link_table,
            collate=collate,
            in_table=in_table,
            ordered=ordered,
        )
        # Todo: Need to notify the database that the custom columns have been updated

        # Update the tables name cache in the database to reflect the fact that new tables have just been created
        if normalized:
            self.custom_tables.add(cc_table)
            self.custom_tables.add(link_table)
        else:
            self.custom_tables.add(cc_table)

        return num

    def delete_custom_column(self, num: int) -> None:
        """
        Mark a custom column for later deletion.

        Deletion is not done at this time.
        It will be done at the next refresh.
        :param num:
        :return:
        """
        self.macros.mark_custom_column_for_delete(num=num)

    # Todo: CustomColumnRowAPI class?
    def _get_custom_column_row(self, in_table: str, cc_name: str) -> "RowAPI":
        """
        Return the custom column row for the given custom column.

        :return:
        """
        pass

    # Todo: We've got a known list of link table additional info - not just extra - use it
    def update_custom_column(self, in_table, cc_name, value, extra: Optional[str] = None) -> "RowAPI":
        """
        Preform an update on the given custom column - loading the data into the backend.

        We can fully spec an update on the given custom column with just two values.
        :param in_table: Table the custom column is in
        :param cc_name: Name of the custom column in the table
        :param value: Values to load into the database
        :param extra: Extra data to set for the link:
        :return:
        """
        raise NotImplementedError
