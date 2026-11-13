"""
Adapt legacy custom-column calls to the native LiuXin service.

The owner database supplies schema groups, macros and the live driver. Legacy books
attachment names can map to current main tables. Connection overrides are resolved
lazily, while unfinished custom-value mutation hooks retain their existing behavior.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol, cast

from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    SQLConnection,
)
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI


class _MacroProvider(Protocol):
    """Read-only view of the macro provider installed by a concrete host."""

    @property
    def macros(self) -> DatabaseMacrosAPI: ...


class _MutableMacroProvider(Protocol):
    """Fallback host whose macro binding can be assigned during composition."""

    macros: DatabaseMacrosAPI


class CustomColumnAdapter[ConnectionT: QueryConnection]:
    """
    Adapt legacy custom-column calls to the native LiuXin service.

    The owner database supplies schema groups, macros and the live driver. Legacy books
    attachment names can map to current main tables. Connection overrides are resolved
    lazily, while unfinished custom-value mutation hooks retain their existing behavior.

    Example:
        >>> wrapper.create_custom_column("note", in_table="works")  # doctest: +SKIP
    """

    def __init__(
        self, db: DatabaseAPI | None, macros: DatabaseMacrosAPI | None = None
    ) -> None:
        """
        Initialize database ownership, connection override and custom-table tracking.

        When macros is provided, prefer a callable set_macros(), otherwise assign macros
        or fall back to _macros if assignment raises AttributeError. A None macros
        argument preserves an existing macro provider. This initializer does not open a
        connection.

        Example:
            >>> mixin = CustomColumnAdapter(db=None, macros=None)
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
            >>> host = CustomColumnAdapter(
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
            >>> mixin = CustomColumnAdapter(
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
            >>> mixin = CustomColumnAdapter(None, None)
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
        """Retain retired storage; deletion requires an explicit native purge plan."""
        self.get_custom_tables()

    def get_custom_tables(self) -> set[str]:
        """Discover physical tables belonging to native field definitions."""
        from LiuXin_alpha.databases.custom_columns.native_repository import names
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )

        service = CustomFieldService(self._require_database())
        with service.read() as conn:
            result = set()
            for definition in service.repository.definitions(conn, retired=True):
                table, link = names(definition)
                for target in (table, link) if definition.normalized else (table,):
                    if service.repository.table_exists(conn, target):
                        result.add(target)
            return result

    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """Read a scalar Series position through the native value contract."""
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )
        from LiuXin_alpha.databases.custom_columns.native_types import (
            OwnerRef,
            SeriesValue,
        )

        match = re.fullmatch(r"(.+)_custom_column_(\d+)_link", link_table)
        if match is None:
            raise ValueError("Expected a custom-field membership table")
        service = CustomFieldService(self._require_database())
        value = service.get_value(OwnerRef(match[1], index), int(match[2]))
        return value.position if isinstance(value, SeriesValue) else None

    def direct_get_custom_id_val_pairs(self, table: str) -> list[tuple[int, Any]]:
        """Expose native vocabulary through the legacy pair shape."""
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )

        match = re.fullmatch(r"custom_column_(\d+)", table)
        if match is None:
            raise ValueError("Expected a custom-field value table")
        service = CustomFieldService(self._require_database())
        result: list[tuple[int, Any]] = []
        cursor = 0
        while True:
            page = service.inventory(int(match[1]), limit=1000, after_id=cursor)
            result.extend((item["id"], item["value"]) for item in page.items)
            if page.next_cursor is None:
                return result
            cursor = page.next_cursor

    @staticmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Construct custom-value and owner-link table names from a numeric ID.

        Converts num with int(), logging and re-raising ValueError; other conversion
        errors propagate. Does not check table existence or validate the attachment
        name.

        Example:
            >>> CustomColumnAdapter.custom_table_names(
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
    @staticmethod
    def _check_labels(
        service: Any,
        attachment: str,
        label: str,
        datatype: str,
        current_id: int | None = None,
    ) -> None:
        """Reserve synthetic Series keys only for Calibre-facing operations."""
        from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
            CompatibilityError,
        )

        requested = {label, label + "_index"} if datatype == "series" else {label}
        for other in service.list_fields(include_retired=True):
            if other.id == current_id or other.attachment != attachment:
                continue
            keys = (
                {other.label, other.label + "_index"}
                if other.datatype == "series"
                else {other.label}
            )
            if requested & keys:
                raise CompatibilityError(
                    "Field label conflicts with a field or Series companion"
                )

    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
    ) -> bool:
        """Validate every definition edit through native migrations and revisions."""
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )
        from LiuXin_alpha.databases.transactions import transaction_active
        from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
            CompatibilityError,
        )

        db = self._require_database()
        if (
            transaction_active(db.driver.conn)
            and db.macros.current_transaction_connection() is None
        ):
            raise RuntimeError(
                "Native definition edits require an owned or managed transaction"
            )
        service = CustomFieldService(db)
        with service.write():
            definition = service.get_field(num)
            if in_table is not None and in_table != definition.attachment:
                raise CompatibilityError(
                    "Attachment changes require an explicit native migration"
                )
            changes = {
                key: value
                for key, value in {
                    "name": name,
                    "label": label,
                    "editable": is_editable,
                    "display": display,
                }.items()
                if value is not None
            }
            if not changes:
                return False
            self._check_labels(
                service,
                definition.attachment,
                label or definition.label,
                definition.datatype,
                definition.id,
            )
            service.apply_field_migration(service.plan_field_migration(num, changes))
        return True

    # Todo: Restrict multiple to the known values
    # Todo: The combination of name and table should be unique
    # Todo: Change data_type to datatype
    # Todo: in_table and table seme to do the same thing
    # Todo: datatype should be fully typeable
    def _prepare_custom_column_publication(
        self, connection: SQLConnection
    ) -> Callable[[], None]:
        """Let a service validate its metadata before storage creation commits."""
        return lambda: None

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
        """Translate the legacy creation signature into the sole native storage format.

        The facade may prepare its metadata within this owned transaction. Publication
        follows commit; validation failure rolls back both definition and storage.
        """
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )
        from LiuXin_alpha.databases.custom_columns.native_types import CollectionMode
        from LiuXin_alpha.databases.transactions import transaction_active

        db = self._require_database()
        if (
            transaction_active(db.driver.conn)
            or db.macros.current_transaction_connection() is not None
        ):
            raise RuntimeError("Custom-column creation requires its own transaction")
        if table is not None:
            if in_table not in ("books", table):
                raise TypeError(
                    "Pass only one of table= or in_table= (or keep them identical)."
                )
            in_table = table
        attachment = self._canonicalise_cc_in_table(in_table)
        if is_multiple and datatype != "text":
            raise NotImplementedError("Use the native API for non-text collections")
        mode: CollectionMode = "scalar"
        if is_multiple:
            mode = "set"
        options = dict(display or {})
        if make_category is not None and datatype == "composite":
            options["make_category"] = bool(make_category)
        service = CustomFieldService(db)
        with service.write() as conn:
            self._check_labels(
                service,
                attachment,
                label if label is not None else f"{attachment}__{name}",
                datatype,
            )
            field = service.create_field(
                attachment,
                label if label is not None else f"{attachment}__{name}",
                name=name,
                datatype=datatype,
                mode=mode,
                editable=editable,
                display=options,
            )
            publish = self._prepare_custom_column_publication(conn)
        publish()
        return field.id

    def delete_custom_column(self, num: int) -> None:
        """Retire a native field; preserve storage until explicit purge."""
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )
        from LiuXin_alpha.databases.transactions import transaction_active

        db = self._require_database()
        if (
            transaction_active(db.driver.conn)
            and db.macros.current_transaction_connection() is None
        ):
            raise RuntimeError(
                "Native retirement requires an owned or managed transaction"
            )
        CustomFieldService(db).retire_field(num)

    def _get_custom_column_row(self, in_table: str, cc_name: str) -> dict[str, Any]:
        """Resolve one scoped definition by its label, returning stored metadata."""
        if self.db is None:
            raise RuntimeError("Custom columns require an attached database")
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )

        service = CustomFieldService(self.db)
        with service.read() as conn:
            definition = service.repository.resolve(conn, (in_table, cc_name))
            cursor = conn.execute(
                "SELECT * FROM custom_columns WHERE custom_column_id=?",
                (definition.id,),
            )
            try:
                record = cursor.fetchone()
                if record is None:
                    raise ValueError("Custom-column definition disappeared")
                return dict(
                    zip(
                        (c[0] for c in cursor.description or ()),
                        record,
                        strict=True,
                    )
                )
            finally:
                cursor.close()

    def update_custom_column(
        self,
        in_table: str,
        cc_name: str,
        value: Any,
        extra: str | None = None,
        *,
        owner_id: int,
    ) -> None:
        """Write a scoped field for an explicit owner through native storage."""
        from LiuXin_alpha.databases.custom_columns.native_service import (
            CustomFieldService,
        )
        from LiuXin_alpha.databases.custom_columns.native_types import (
            OwnerRef,
            SeriesValue,
        )

        service = CustomFieldService(self._require_database())
        definition = service.get_field((in_table, cc_name))
        if extra is not None:
            if definition.datatype != "series" or definition.mode != "scalar":
                raise ValueError("Extra requires a scalar Series field")
            value = SeriesValue(value, float(extra))
        service.set_value(OwnerRef(in_table, owner_id), definition.id, value)
