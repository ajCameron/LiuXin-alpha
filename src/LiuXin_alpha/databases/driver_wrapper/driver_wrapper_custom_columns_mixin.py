"""
Manage custom-column definitions and physical value tables.

The owner database supplies schema groups, macros and the live driver. Legacy books
attachment names can map to current main tables. Connection overrides are resolved
lazily, while unfinished custom-value mutation hooks retain their existing behavior.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Never, Protocol, cast

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES
from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    require_legacy_macros,
)
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
)
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import to_json_str

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI


class _MacroProvider(Protocol):
    """Read-only view of the macro provider installed by a concrete host."""

    @property
    def macros(self) -> DatabaseMacrosAPI: ...


# Todo: Round these up and move them to the custom columns mixin - as with everything else
# Todo: Or, perhaps preferably, move them down into the driver and integrate properly
class _MutableMacroProvider(Protocol):
    """Fallback host whose macro binding can be assigned during composition."""

    macros: DatabaseMacrosAPI


class CustomColumnStorageMixin[ConnectionT: QueryConnection]:
    """
    Manage custom-column definitions and physical value tables.

    The owner database supplies schema groups, macros and the live driver. Legacy books
    attachment names can map to current main tables. Connection overrides are resolved
    lazily, while unfinished custom-value mutation hooks retain their existing behavior.

    Example:
        >>> wrapper.create_custom_column("note", in_table="works")  # doctest: +SKIP
    """

    def __init__(
        self, db: DatabaseAPI | None, macros: DatabaseMacrosAPI | None
    ) -> None:
        """
        Initialize database ownership, connection override and custom-table tracking.

        When macros is provided, prefer a callable set_macros(), otherwise assign macros
        or fall back to _macros if assignment raises AttributeError. A None macros
        argument preserves an existing macro provider. This initializer does not open a
        connection.

        Example:
            >>> mixin = CustomColumnsDriverWrapperMixin(db=None, macros=None)
            >>> mixin.custom_tables, mixin.conn
            (set(), None)


        :param db: Owner database used for schema and macro operations; may be None
            during setup.
        :param macros: Optional macro provider; None leaves the existing provider
            unchanged.
        :return: None.
        """
        # Worker objects
        self.db = db
        self._conn_override: ConnectionT | None = (
            None  # prefer using the live driver connection via @property conn
        )

        # Don't assign to self.macros directly: subclasses (e.g. DriverWrapper) may expose
        # macros as a read-only @property (no setter). Also avoid clobbering an already-set
        # macros when macros is None.
        if macros is not None:
            macros_setter = getattr(self, "set_macros", None)

            if callable(macros_setter):
                macros_setter(macros)
            else:
                try:
                    cast(_MutableMacroProvider, self).macros = macros
                except AttributeError:
                    # Last resort: common convention used by wrappers
                    self._macros = macros

        # Todo: Might want to rename this to custom_column_tables
        # Stores properties of the database
        self._custom_tables: set[str] = set()

    @property
    def custom_tables(self) -> set[str]:
        """Expose storage names; service facades may resolve them from their owner."""
        return self._custom_tables

    def _require_database(self) -> DatabaseAPI:
        """Reject owner-dependent operations before the facade has been wired."""
        if self.db is None:
            raise RuntimeError("Custom-column operations require an owning database")
        return self.db

    def _canonicalise_cc_in_table(self, in_table: str) -> str:
        """
        Resolve the legacy books attachment name against available table groups.

        Existing main, interlink or intralink names pass through. If books is absent,
        prefer manifestations, then items, then works. Other unknown names pass through
        unchanged for later validation.

        Example:
            >>> from types import SimpleNamespace
            >>> host = CustomColumnsDriverWrapperMixin(
            ...     db=SimpleNamespace(
            ...         main_tables={"works", "items"},
            ...         interlink_tables=set(),
            ...         intralink_tables=set(),
            ...     ), macros=None
            ... )
            >>> host._canonicalise_cc_in_table("books")
            'items'


        :param in_table: Table to which the custom column is attached.
        :return: Resolved attachment name, or the original name.
        """

        db = self._require_database()
        available = db.main_tables.union(db.interlink_tables).union(
            db.intralink_tables or ()
        )
        if in_table in available:
            return in_table
        if in_table == "books":
            for candidate in ("manifestations", "items", "works"):
                if candidate in available:
                    return candidate
        return in_table

    @property
    def conn(self) -> ConnectionT | None:
        """
        Resolve an override or the current owner/driver connection.

        An explicit override is probed with SELECT 1 when it exposes execute; an
        override without that method is accepted as-is. Probe failures clear the stored
        override where possible. Then prefer db.driver.conn, followed by
        self.driver.conn, suppressing lookup errors. If neither works, returns the
        original local override, which may still be stale after a failed probe. No new
        connection is opened.

        Example:
            >>> from types import SimpleNamespace
            >>> token = object()
            >>> mixin = CustomColumnsDriverWrapperMixin(
            ...     SimpleNamespace(driver=SimpleNamespace(conn=token)), None
            ... )
            >>> mixin.conn is token
            True


        :return: Override or live driver connection; possibly None or the failed
            override when all fallbacks fail.
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
    def conn(self, value: ConnectionT | None) -> None:
        # Backwards-compat: allow code to assign self.conn = <connection>.
        # Prefer leaving this unset so the property resolves a fresh connection from the driver.
        """
        Store a connection override for lazy resolution on the next read.

        No validation or cleanup occurs at assignment. Assign None to prefer the live
        owner/driver connection.

        Example:
            >>> mixin = CustomColumnsDriverWrapperMixin(None, None)
            >>> token = object()
            >>> mixin.conn = token
            >>> mixin._conn_override is token
            True


        :param value: Replacement connection-like object, or None to clear the override.
        :return: None.
        """
        self._conn_override = value

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN METHODS
    def deleted_marked_custom_columns(self) -> None:
        """Atomically remove marked definitions and their physical SQLite storage.

        The macro reuses the current transaction through a savepoint, or owns a
        new unit on an idle connection. Selection and deletion share that unit.
        """
        require_legacy_macros(
            self._require_database().macros
        ).delete_marked_custom_columns()

    def get_custom_tables(self) -> set[str]:
        """
        Read registered custom-table names through the owner driver's live connection.

        Calls db.macros.direct_get_custom_tables(conn=db.driver.conn), ignoring this
        mixin's connection override. This is a fresh macro result, not a copy of the
        local custom_tables set.

        Example:
            >>> wrapper.get_custom_tables()  # doctest: +SKIP


        :return: Set of custom table/link-table names produced by the macro.
        """
        # Always use the driver's live connection to avoid stale db.conn aliases pointing
        # at a closed connection after driver/connection churn.
        return require_legacy_macros(
            self._require_database().macros
        ).direct_get_custom_tables(conn=self._require_database().driver.conn)

    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Read the extra cell for one owner in a legacy custom link table.

        Passes the resolved connection to db.macros.direct_get_custom_and_extra().
        Shared SQL looks up the _book owner column and selects only the first _extra
        value, without ordering; it does not return the custom value itself.

        Example:
            >>> wrapper.direct_get_custom_extra(
            ...     "books_custom_column_1_link", 1
            ... )  # doctest: +SKIP


        :param link_table: Trusted legacy custom link-table name.
        :param index: Owner identifier matched against the link _book column.
        :return: Selected extra scalar or the connection adapter's missing-value result.
        """
        return require_legacy_macros(
            self._require_database().macros
        ).direct_get_custom_and_extra(link_table, index, conn=self.conn)

    def direct_get_custom_id_val_pairs(self, table: str) -> list[tuple[int, Any]]:
        """
        Read all ID/value pairs from a custom table on the resolved connection.

        Passes table and conn to the owner macro without sorting or flattening. The
        historical tuple annotation does not describe the usual list of pairs.

        Example:
            >>> wrapper.direct_get_custom_id_val_pairs(
            ...     "custom_column_1"
            ... )  # doctest: +SKIP


        :param table: Table name in the current schema.
        :return: Macro result, normally a list of (ID, value) tuples.
        """
        return require_legacy_macros(
            self._require_database().macros
        ).get_all_cc_id_val_pairs(table, conn=self.conn)

    @staticmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Construct custom-value and owner-link table names from a numeric ID.

        Converts num with int(), logging and re-raising ValueError; other conversion
        errors propagate. Does not check table existence or validate the attachment
        name.

        Example:
            >>> CustomColumnsDriverWrapperMixin.custom_table_names(
            ...     "7", in_table="works"
            ... )
            ('custom_column_7', 'works_custom_column_7_link')


        :param num: Custom-column metadata identifier.
        :param in_table: Table to which the custom column is attached.
        :return: Pair (custom_column_<id>, <in_table>_custom_column_<id>_link).
        """
        try:
            num = int(num)
        except ValueError as e:
            err_str = "Cannot coerce table num (id) to an integer"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("num", num), ("num_type", type(num))
            )
            raise ValueError(err_str) from e

        return f"custom_column_{num}", f"{in_table}_custom_column_{num}_link"

    # Todo: Custom columns needed to be added to the appropriate table name cache after they've been created - check
    #       that this is happening
    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
    ) -> bool:
        """Atomically update definition fields, preserving omitted fields.

        Attachment changes require a storage migration and are rejected here.
        The macro joins the current transaction without committing unrelated
        writes. The caller remains responsible for facade metadata refresh.
        """
        return require_legacy_macros(
            self._require_database().macros
        ).set_custom_column_metadata(
            num=num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            in_table=in_table,
        )

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
        label: str | None = None,
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
        in_table: str = "books",
        table: str | None = None,
        make_category: bool | None = None,
    ) -> int:
        """
        Create a custom-column definition and its physical value storage.

        The table alias overrides the default books attachment; conflicting nondefault
        aliases raise TypeError. Resolves legacy attachment names and asserts membership
        in the owner table groups. Rejects unsupported datatypes, invalid labels, and
        multiple numeric/date/bool/rating columns. Labels default to
        <attachment>__<name> and must begin with a letter and contain lowercase word
        characters.

        Text, rating, series and enumeration use normalized value/link storage. Multiple
        comments and series request ordering. display is serialized as JSON; for
        composite columns, make_category updates that mapping. Definition insertion
        and physical DDL share one owned transaction: either both commit or both roll
        back. Nested macro transactions are refused before writing, so facade caches
        are updated only after a real commit. Returns the definition ID.

        Example:
            >>> wrapper.create_custom_column("note")  # doctest: +SKIP


        :param name: Custom-column display name.
        :param datatype: Supported CUSTOM_DATA_TYPES name, such as text, int, comments,
            series or composite.
        :param is_multiple: Whether multiple values are requested; unsupported scalar
            combinations raise NotImplementedError.
        :param label: Optional lowercase registry label; None derives one from
            attachment and name.
        :param editable: Editable flag recorded in the definition.
        :param display: JSON-serializable options; None becomes an empty dict.
        :param in_table: Attachment table name; defaults to books.
        :param table: Optional alias for in_table; conflicting explicit names raise
            TypeError.
        :param make_category: Optional boolean-like composite browser-category setting;
            ignored for other datatypes.
        :return: New integer custom-column definition ID.
        """
        # Todo: Somewhere there are allowed cc datatypes - preform a check that we're being given one of them

        # Support newer/clearer keyword alias: `table=` (same as `in_table=`)
        if table is not None:
            if in_table != "books" and in_table != table:
                raise TypeError(
                    "Pass only one of table= or in_table= (or keep them identical)."
                )
            in_table = table

        in_table = self._canonicalise_cc_in_table(in_table)

        assert in_table in self._require_database().main_tables.union(
            self._require_database().interlink_tables
        ).union(self._require_database().intralink_tables or ()), (
            f"in_table {in_table} not found in main, intralink or interlink tables"
        )

        # Some datatypes just don't make much sense to be multiple - so throwing an error if we can some combinations
        if is_multiple and datatype in ("rating", "int", "float", "datetime", "bool"):
            err_str = (
                f"Cannot have a mutliple column of type {datatype} - makes no sense"
            )
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
        label = label if label is not None else f"{in_table}__{name}"

        if not label:
            raise ValueError(_("The label must be non-empty."))

        if (
            re.match(r"^\w+$", label) is None
            or (not label)
            or (not label[0].isalpha())
            or label.lower() != label
        ):
            raise ValueError(
                _(
                    "The label must contain only lower case letters, digits and underscores, and start "
                    "with a letter"
                )
            )
        if datatype not in CUSTOM_DATA_TYPES:
            raise ValueError(f"{datatype!r} is not a supported data type")

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
            err_str = default_log.log_variables(
                err_str, "ERROR", ("datatype", datatype)
            )
            raise NotImplementedError(err_str)

        # Register the complete definition and its storage in one transaction.
        cc = "custom_column_"
        cc_row_dict: dict[str, Any] = {cc + "scratch": ""}
        cc_row_dict[cc + "label"] = label
        cc_row_dict[cc + "name"] = name
        cc_row_dict[cc + "datatype"] = datatype
        cc_row_dict[cc + "is_multiple"] = int(is_multiple)
        cc_row_dict[cc + "editable"] = int(editable)
        cc_row_dict[cc + "display"] = to_json_str(
            display
        )  # display is a dict, and so has to be serialized
        cc_row_dict[cc + "normalized"] = int(normalized)
        cc_row_dict[cc + "in_table"] = in_table
        cc_row_dict[cc + "ordered"] = int(ordered)
        collate = "COLLATE NOCASE" if dt == "TEXT" else ""
        with self._require_database().macros.transaction(
            serialized=True, require_outermost=True
        ) as conn:
            num = self._require_database().macros.insert_row(
                "custom_columns", cc_row_dict
            )
            cc_table, link_table = self.custom_table_names(num, in_table=in_table)
            require_legacy_macros(self._require_database().macros).create_cc_table(
                normalized=normalized,
                datatype=datatype,
                dt=dt,
                table=cc_table,
                link_table=link_table,
                collate=collate,
                in_table=in_table,
                ordered=ordered,
                conn=conn,
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
        Mark a custom-column definition for deferred deletion.

        Calls the configured macro provider; tables are removed later by
        deleted_marked_custom_columns(), typically during reload. Does not update the
        local custom-table cache.

        Example:
            >>> wrapper.delete_custom_column(1)  # doctest: +SKIP


        :param num: Custom-column metadata identifier.
        :return: None.
        """
        require_legacy_macros(
            cast(_MacroProvider, self).macros
        ).mark_custom_column_for_delete(num=num)

    # Todo: CustomColumnRowAPI class?
    def _get_custom_column_row(self, in_table: str, cc_name: str) -> None:
        """
        Reserve a hook for retrieving a custom-column definition row.

        The current body is pass: it performs no lookup and ignores both arguments.

        Example:
            >>> CustomColumnsDriverWrapperMixin(None, None)._get_custom_column_row(
            ...     "works", "note"
            ... ) is None
            True


        :param in_table: Table to which the custom column is attached.
        :param cc_name: Custom column name.
        :return: None; this hook remains unimplemented.
        """
        pass

    # Todo: We've got a known list of link table additional info - not just extra - use it
    def update_custom_column(
        self, in_table: str, cc_name: str, value: Any, extra: str | None = None
    ) -> Never:
        """
        Reject the unfinished custom-column value update operation.

        Always raises NotImplementedError before reading the database or modifying
        values.

        Example:
            >>> mixin = CustomColumnsDriverWrapperMixin(None, None)
            >>> try:
            ...     mixin.update_custom_column("works", "note", "Example")
            ... except NotImplementedError:
            ...     print("not implemented")
            not implemented


        :param in_table: Table to which the custom column is attached.
        :param cc_name: Custom column name.
        :param value: Proposed custom value; currently unused.
        :param extra: Optional link metadata; currently unused.
        :return: No normal return; always raises NotImplementedError.
        """
        raise NotImplementedError


class CustomColumnsDriverWrapperMixin(CustomColumnStorageMixin[QueryConnection]):
    """Wrapper storage adapter retaining its mutable local table-name cache.

    The service facade uses the shared storage operations with a physical SQL
    connection and discovers table names from its owner on each access instead.
    """

    @property
    def custom_tables(self) -> set[str]:
        return self._custom_tables

    @custom_tables.setter
    def custom_tables(self, value: set[str]) -> None:
        self._custom_tables = value
