"""Legacy macro names delegating to the single native custom-field service.

These adapters require a bound database. Writes participate in native managed
transactions; arbitrary borrowed connections and raw table creation are unsupported.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Literal, overload

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_repository import names
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    OwnerRef,
    SeriesValue,
)
from LiuXin_alpha.databases.custom_columns.native_values import adapt, identity
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    SQLConnection,
    SQLCursor,
    SQLParameters,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.values import _inventory, _project
from LiuXin_alpha.utils.language_tools import plural_singular_mapper


class CustomColumnMacros:
    """Compatibility names; native operations own validation, storage and revisions."""

    def __init__(self, db: DatabaseAPI):
        self.db = db

    def _cc_call(self, operation: str, args: dict[str, Any]) -> Any:
        service = CustomFieldService(self.db)
        connection = args.get("conn")
        if (
            connection is not None
            and connection is not self.db.macros.current_transaction_connection()
        ):
            raise CompatibilityError(
                "Custom-column macros require the native managed transaction connection"
            )
        if operation in {
            "create_cc_table",
            "reconcile_cc_owner_cleanup",
            "do_cc_db_bulk_addition",
            "update_custom_column_additional_column_many",
        }:
            raise CompatibilityError(
                "Use native field creation, repair or value operations"
            )
        if operation in {
            "delete_marked_custom_columns",
            "preform_cc_column_delete_from_map",
            "do_custom_column_delete_by_id",
            "do_custom_column_delete_by_num",
        }:
            raise CompatibilityError(
                "Retired fields require an explicit native purge plan"
            )
        if operation in {"mark_cc_for_delete", "mark_custom_column_for_delete"}:
            service.retire_field(args["num"] if "num" in args else args["cc_column_id"])
            return None
        if operation == "set_custom_column_metadata":
            changes = {
                key: value
                for key, value in args.items()
                if key not in {"num", "conn"} and value is not None
            }
            if "is_editable" in changes:
                changes["editable"] = changes.pop("is_editable")
            if "in_table" in changes:
                changes["attachment"] = changes.pop("in_table")
            service.apply_field_migration(
                service.plan_field_migration(args["num"], changes)
            )
            return None
        if operation == "get_all_cc_ids_marked_for_delete":
            return [
                d.id for d in service.list_fields(include_retired=True) if d.retired
            ]
        if operation == "direct_get_custom_tables":
            return {
                physical
                for d in service.list_fields(include_retired=True)
                for physical in (names(d) if d.normalized else names(d)[:1])
            }
        if operation == "clean_custom":
            with service.write():
                for number in args["cc_num_map"]:
                    definition = service.get_field(number)
                    if definition.normalized:
                        for candidate in list(_inventory(service, number)):
                            if not candidate["owners"]:
                                service.delete_value(number, candidate["id"])
            return None
        supplied = next(
            (
                args[key]
                for key in (
                    "custom_column",
                    "target_table",
                    "cc_table",
                    "cc_column",
                    "lt",
                    "link_table",
                    "cc_series_link_table",
                    "table",
                )
                if key in args
            ),
            None,
        )
        if supplied is None and "num" in args:
            field_id = args["num"]
        else:
            match = re.fullmatch(
                r"(?:[a-zA-Z_]+_)?custom_column_(\d+)(?:_link)?", supplied or ""
            )
            if match is None:
                raise CompatibilityError(
                    "Expected a registered native custom-column table"
                )
            field_id = int(match[1])
        definition = service.get_field(field_id)
        if supplied is not None and supplied not in names(definition):
            raise CompatibilityError("Table does not belong to the native field")
        reads = {
            "get_cc_id_and_value_from_id",
            "get_cc_id_value_from_cc_id",
            "get_cc_id_from_value",
            "get_all_cc_custom_values",
            "get_all_cc_id_val_pairs",
            "get_cc_lt_books_from_lt_value",
            "get_cc_series_index_indices",
            "check_for_cc_link",
            "read_cc_value_from_meta_2",
            "get_cc_books_from_link_table",
            "get_cc_books_for_dirtying",
            "direct_get_custom_and_extra",
        }
        with service.read() if operation in reads else service.write():
            inventory = list(_inventory(service, field_id))

            def item(value_id: int) -> dict[str, Any]:
                for candidate in inventory:
                    if candidate["id"] == value_id:
                        return candidate
                raise IndexError(value_id)

            if operation in {
                "get_cc_id_and_value_from_id",
                "get_cc_id_value_from_cc_id",
            }:
                selected = item(
                    args["target_id"] if "target_id" in args else args["old_id"]
                )
                return selected["id"], selected["value"]
            if operation == "get_cc_id_from_value":
                key = identity(adapt(args["cc_value"], definition), definition)
                found = [
                    (v["id"],)
                    for v in inventory
                    if identity(v["value"], definition) == key
                ]
                return found if args.get("all") else (found[0][0] if found else None)
            if operation == "get_all_cc_custom_values":
                return [(v["value"],) for v in inventory]
            if operation == "get_all_cc_id_val_pairs":
                return [(v["id"], v["value"]) for v in inventory]
            with service.read() as conn:
                stored = service.repository.read_entries(conn, definition)
            if operation in {
                "get_cc_lt_books_from_lt_value",
                "get_cc_books_from_link_table",
                "get_cc_books_for_dirtying",
            }:
                target = args.get("value", args.get("lt_value", args.get("id")))
                return [
                    (owner,)
                    for owner, values in stored.items()
                    if any(v.value_id == target for v in values)
                ]
            if operation == "get_cc_series_index_indices":
                return [
                    (v.position,)
                    for values in stored.values()
                    for v in values
                    if v.value_id == args["series_id"]
                ]
            if operation == "check_for_cc_link":
                return any(
                    v.value_id == args["value_id"]
                    for v in stored.get(args["book_id"], ())
                )
            if operation in {
                "direct_get_custom_and_extra",
                "read_cc_value_from_meta_2",
            }:
                value = service.get_value(
                    OwnerRef(
                        definition.attachment,
                        (args["index"] if "index" in args else args["book_id"]),
                    ),
                    field_id,
                )
                projected, extra = _project(definition, value)
                return (
                    (projected, extra)
                    if operation == "direct_get_custom_and_extra"
                    else (
                        "|".join(projected)
                        if isinstance(projected, list)
                        else projected
                    )
                )
            if operation in {"add_cc_table_value", "ensure_custom_column_value"}:
                return service.ensure_value(field_id, args["value"])
            if operation == "insert_multiple_values_into_cc_table":
                for value in args["values"]:
                    service.ensure_value(field_id, value)
                return None
            if operation == "update_cc_value":
                service.rename_value(field_id, args["cc_id"], args["cc_value"])
            elif operation in {"repoint_cc_lt_values", "update_cc_lt_value_by_value"}:
                service.rename_value(
                    field_id,
                    (args["old_id"] if "old_id" in args else args["old_value_id"]),
                    item(args["new_id"] if "new_id" in args else args["new_value_id"])[
                        "value"
                    ],
                )
            elif operation in {"delete_cc_item", "delete_from_cc_table_by_id"}:
                service.delete_value(field_id, args["target_id"])
            elif operation == "delete_from_cc_table_by_value":
                key = identity(adapt(args["target_id"], definition), definition)
                for candidate in inventory:
                    if identity(candidate["value"], definition) == key:
                        service.delete_value(field_id, candidate["id"])
            elif operation == "clear_cc_unused_table_entries":
                for candidate in inventory:
                    if not candidate["owners"]:
                        service.delete_value(field_id, candidate["id"])
            elif operation in {
                "add_cc_link_with_extra",
                "add_cc_link_with_extra_multi",
            }:
                if args.get("target_column", "value") != "value":
                    raise CompatibilityError(
                        "Only native value memberships are supported"
                    )
                sequence = (
                    args["sequence"]
                    if operation.endswith("multi")
                    else [(args["book_id"], args["value_id"], args["extra"])]
                )
                for row in sequence:
                    owner, value_id, *positions = row
                    value = item(value_id)["value"]
                    if definition.datatype == "series":
                        value = SeriesValue(
                            value,
                            positions[0]
                            if positions and positions[0] is not None
                            else 1.0,
                        )
                    reference = OwnerRef(definition.attachment, int(owner))
                    if definition.mode == "scalar":
                        service.set_value(reference, field_id, value)
                    else:
                        service.edit_entries(reference, field_id, add=[value])
            elif operation in {
                "break_cc_lt_link",
                "break_cc_links_by_book_id",
                "break_cc_links_by_book_id_and_value",
                "clear_cc_entries_from_table",
            }:
                owners = args["book_id"] if "book_id" in args else args["book"]
                owners = [owners] if isinstance(owners, (int, str)) else owners
                target = args.get("value_id", args.get("value"))
                for owner in owners:
                    owner = owner[0] if isinstance(owner, tuple) else owner
                    reference = OwnerRef(definition.attachment, int(owner))
                    values = stored.get(int(owner), [])
                    retained = [
                        v for v in values if target is not None and v.value_id != target
                    ]
                    service.set_value(
                        reference,
                        field_id,
                        retained
                        if definition.mode != "scalar"
                        else (retained[0] if retained else None),
                    )
            else:
                raise CompatibilityError(
                    f"Unsupported custom-column macro: {operation}"
                )
        return None

    def ensure_custom_column_value(self, cc_table: str, value: Any) -> Any:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "ensure_custom_column_value", {"cc_table": cc_table, "value": value}
        )

    def reconcile_cc_owner_cleanup(
        self,
        in_table: str,
        storage_tables: Iterable[str],
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "reconcile_cc_owner_cleanup",
            {"in_table": in_table, "storage_tables": storage_tables, "conn": conn},
        )

    def mark_cc_for_delete(self, cc_column_id: int) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("mark_cc_for_delete", {"cc_column_id": cc_column_id})

    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        conn: SQLConnection | None = None,
    ) -> bool:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "set_custom_column_metadata",
            {
                "num": num,
                "name": name,
                "label": label,
                "is_editable": is_editable,
                "display": display,
                "in_table": in_table,
                "conn": conn,
            },
        )

    def delete_marked_custom_columns(self, conn: SQLConnection | None = None) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("delete_marked_custom_columns", {"conn": conn})

    def create_cc_table(
        self,
        normalized: bool,
        datatype: str,
        dt: str,
        table: str,
        link_table: str,
        collate: str,
        in_table: str = "books",
        ordered: bool = False,
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "create_cc_table",
            {
                "normalized": normalized,
                "datatype": datatype,
                "dt": dt,
                "table": table,
                "link_table": link_table,
                "collate": collate,
                "in_table": in_table,
                "ordered": ordered,
                "conn": conn,
            },
        )

    def do_custom_column_delete_by_num(self, num: int) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("do_custom_column_delete_by_num", {"num": num})

    def do_custom_column_delete_by_id(self, cc_id: int) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("do_custom_column_delete_by_id", {"cc_id": cc_id})

    def mark_custom_column_for_delete(self, num: int) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("mark_custom_column_for_delete", {"num": num})

    def get_all_cc_ids_marked_for_delete(
        self, conn: QueryConnection | None = None
    ) -> list[int]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("get_all_cc_ids_marked_for_delete", {"conn": conn})

    def preform_cc_column_delete_from_map(
        self,
        num_table_lt_map: dict[int, tuple[str, str]],
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "preform_cc_column_delete_from_map",
            {"num_table_lt_map": num_table_lt_map, "conn": conn},
        )

    @staticmethod
    def _get_cc_id_val(custom_column: str) -> tuple[str, str]:
        """
        Derive the legacy ID and value column names from an inflected table name.

        Delegate singularization to plural_singular_mapper and append _id and _value.
        This performs no database lookup or identifier validation; exceptional names
        follow the inflector's rules.

        Example:
            >>> SQLiteDatabaseCustomColumnMacros._get_cc_id_val("tags")
            ('tag_id', 'tag_value')


        :param custom_column: Trusted custom-table name used to derive its ID/value
            column prefixes.
        :return: A pair of generated (id_column, value_column) strings.
        """
        cc_col = plural_singular_mapper(custom_column)
        return (f"{cc_col}_id", f"{cc_col}_value")

    @staticmethod
    def _cc_table_col_mapper(table: str) -> str:
        """
        Return the shared inflector's singular form used as a custom-column prefix.

        No schema lookup, suffix removal or separate identifier validation is performed.

        Example:
            >>> SQLiteDatabaseCustomColumnMacros._cc_table_col_mapper("custom_column_1")
            'custom_column_1'


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :return: The prefix returned by plural_singular_mapper.
        """
        return plural_singular_mapper(table)

    def get_dirtied_cache(self) -> dict[Any, int]:
        """
        Map dirty book IDs to enumeration positions while accommodating two queue
        schemas.

        Inspect metadata_dirtied_books using PRAGMA. Prefer the legacy
        metadata_dirtied_book column when present; otherwise read non-null
        metadata_dirtied_table_id values, filtering table=books when the table
        discriminator exists. Failed introspection or an unknown layout returns an empty
        dictionary; errors during the selected query propagate. Query order is
        unspecified, duplicate IDs keep their last enumeration position, and legacy null
        IDs are retained.

        Example:
            >>> from types import SimpleNamespace
            >>> import sqlite3
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute(
            ...     "CREATE TABLE metadata_dirtied_books (metadata_dirtied_table TEXT, "
            ...     "metadata_dirtied_table_id INTEGER)"
            ... )
            >>> _ = conn.executemany(
            ...     "INSERT INTO metadata_dirtied_books VALUES (?, ?)",
            ...     [("books", 7), ("tags", 8), ("books", None)],
            ... )
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.db = SimpleNamespace(
            ...     driver_wrapper=SimpleNamespace(execute=conn.execute)
            ... )
            >>> macros.get_dirtied_cache()
            {7: 0}
            >>> conn.close()


        :return: A dictionary of dirty IDs to zero-based positions, possibly empty.
        """
        try:
            cols = [
                row["name"] if isinstance(row, Mapping) else row[1]
                for row in self.db.driver_wrapper.execute(
                    "PRAGMA table_info(metadata_dirtied_books)"
                )
            ]
        except Exception:
            cols = []
        if "metadata_dirtied_book" in cols:
            stmt = "SELECT metadata_dirtied_book FROM metadata_dirtied_books"
        elif "metadata_dirtied_table_id" in cols:
            if "metadata_dirtied_table" in cols:
                stmt = "SELECT metadata_dirtied_table_id FROM metadata_dirtied_books WHERE metadata_dirtied_table='books' AND metadata_dirtied_table_id IS NOT NULL"
            else:
                stmt = "SELECT metadata_dirtied_table_id FROM metadata_dirtied_books WHERE metadata_dirtied_table_id IS NOT NULL"
        else:
            return {}
        dirtied_cache = {
            x: i for i, (x,) in enumerate(self.db.driver_wrapper.execute(stmt))
        }
        return dirtied_cache

    def get_cc_id_and_value_from_id(
        self, custom_column: str, target_id: int, conn: QueryConnection | None = None
    ) -> tuple[int, Any]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_id_and_value_from_id",
            {"custom_column": custom_column, "target_id": target_id, "conn": conn},
        )

    def get_cc_id_value_from_cc_id(
        self, table: str, old_id: int, conn: QueryConnection | None = None
    ) -> tuple[int, Any]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_id_value_from_cc_id",
            {"table": table, "old_id": old_id, "conn": conn},
        )

    @overload
    def get_cc_id_from_value(
        self,
        target_table: str,
        cc_value: Any,
        all: Literal[False] = False,
        conn: QueryConnection | None = None,
    ) -> int | None: ...

    @overload
    def get_cc_id_from_value(
        self,
        target_table: str,
        cc_value: Any,
        all: Literal[True],
        conn: QueryConnection | None = None,
    ) -> list[Sequence[Any]]: ...

    @overload
    def get_cc_id_from_value(
        self,
        target_table: str,
        cc_value: Any,
        all: bool,
        conn: QueryConnection | None = None,
    ) -> int | None | list[Sequence[Any]]: ...

    def get_cc_id_from_value(
        self,
        target_table: str,
        cc_value: Any,
        all: bool = False,
        conn: QueryConnection | None = None,
    ) -> int | None | list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_id_from_value",
            {
                "target_table": target_table,
                "cc_value": cc_value,
                "all": all,
                "conn": conn,
            },
        )

    def get_cc_lt_books_from_lt_value(
        self, lt: str, value: Any, conn: QueryConnection | None = None
    ) -> list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_lt_books_from_lt_value", {"lt": lt, "value": value, "conn": conn}
        )

    def get_all_cc_custom_values(
        self, cc_table: str, distinct: bool = False, conn: QueryConnection | None = None
    ) -> list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_all_cc_custom_values",
            {"cc_table": cc_table, "distinct": distinct, "conn": conn},
        )

    def get_cc_series_index_indices(
        self,
        cc_series_link_table: str,
        series_id: int,
        conn: QueryConnection | None = None,
    ) -> list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_series_index_indices",
            {
                "cc_series_link_table": cc_series_link_table,
                "series_id": series_id,
                "conn": conn,
            },
        )

    def check_for_cc_link(
        self,
        link_table: str,
        book_id: int,
        value_id: int,
        conn: QueryConnection | None = None,
    ) -> int | None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "check_for_cc_link",
            {
                "link_table": link_table,
                "book_id": book_id,
                "value_id": value_id,
                "conn": conn,
            },
        )

    def read_cc_value_from_meta_2(
        self, num: int, book_id: int, conn: QueryConnection | None = None
    ) -> Any:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "read_cc_value_from_meta_2", {"num": num, "book_id": book_id, "conn": conn}
        )

    def get_all_cc_id_val_pairs(
        self, table: str, conn: QueryConnection | None = None
    ) -> list[tuple[int, Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("get_all_cc_id_val_pairs", {"table": table, "conn": conn})

    def get_cc_books_from_link_table(
        self, lt: str, lt_value: Any, conn: QueryConnection | None = None
    ) -> list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_books_from_link_table",
            {"lt": lt, "lt_value": lt_value, "conn": conn},
        )

    def get_cc_books_for_dirtying(
        self, table: str, link: str, id: int, conn: QueryConnection | None = None
    ) -> list[Sequence[Any]]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "get_cc_books_for_dirtying",
            {"table": table, "link": link, "id": id, "conn": conn},
        )

    def direct_get_custom_and_extra(
        self, link_table: str, index: int, conn: QueryConnection | None = None
    ) -> Any:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "direct_get_custom_and_extra",
            {"link_table": link_table, "index": index, "conn": conn},
        )

    def add_cc_table_value(
        self, table: str, value: Any, conn: SQLConnection | None = None
    ) -> int | None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "add_cc_table_value", {"table": table, "value": value, "conn": conn}
        )

    def add_cc_link_with_extra(
        self,
        lt: str,
        book_id: int,
        value_id: int,
        extra: Any = None,
        conn: SQLConnection | None = None,
        target_column: str = "value",
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "add_cc_link_with_extra",
            {
                "lt": lt,
                "book_id": book_id,
                "value_id": value_id,
                "extra": extra,
                "conn": conn,
                "target_column": target_column,
            },
        )

    def add_cc_link_with_extra_multi(
        self,
        lt: str,
        sequence: Iterable[SQLParameters],
        extra: bool = False,
        conn: SQLConnection | None = None,
        target_column: str = "value",
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "add_cc_link_with_extra_multi",
            {
                "lt": lt,
                "sequence": sequence,
                "extra": extra,
                "conn": conn,
                "target_column": target_column,
            },
        )

    def insert_multiple_values_into_cc_table(
        self, table: str, values: Iterable[Any], conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "insert_multiple_values_into_cc_table",
            {"table": table, "values": values, "conn": conn},
        )

    def do_cc_db_bulk_addition(
        self,
        temp_tables: tuple[str, str, str],
        custom_table: str,
        link_table: str,
        add: Sequence[Any],
        remove: Sequence[Any],
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "do_cc_db_bulk_addition",
            {
                "temp_tables": temp_tables,
                "custom_table": custom_table,
                "link_table": link_table,
                "add": add,
                "remove": remove,
                "conn": conn,
            },
        )

    def update_cc_value(
        self,
        cc_column: str,
        cc_id: int,
        cc_value: Any,
        conn: SQLConnection | None = None,
    ) -> SQLCursor | None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "update_cc_value",
            {
                "cc_column": cc_column,
                "cc_id": cc_id,
                "cc_value": cc_value,
                "conn": conn,
            },
        )

    def repoint_cc_lt_values(self, lt: str, new_id: int, old_id: int) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "repoint_cc_lt_values", {"lt": lt, "new_id": new_id, "old_id": old_id}
        )

    def update_cc_lt_value_by_value(
        self,
        lt: str,
        new_value_id: int,
        old_value_id: int,
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "update_cc_lt_value_by_value",
            {
                "lt": lt,
                "new_value_id": new_value_id,
                "old_value_id": old_value_id,
                "conn": conn,
            },
        )

    def update_custom_column_additional_column_many(
        self, table: str, column: str, sequence: Iterable[SQLParameters]
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "update_custom_column_additional_column_many",
            {"table": table, "column": column, "sequence": sequence},
        )

    def delete_cc_item(
        self, table: str, lt: str, target_id: int, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "delete_cc_item",
            {"table": table, "lt": lt, "target_id": target_id, "conn": conn},
        )

    def break_cc_lt_link(self, lt: str, book: int, value: int | None = None) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "break_cc_lt_link", {"lt": lt, "book": book, "value": value}
        )

    def delete_from_cc_table_by_id(
        self, table: str, target_id: int, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "delete_from_cc_table_by_id",
            {"table": table, "target_id": target_id, "conn": conn},
        )

    def delete_from_cc_table_by_value(
        self, table: str, target_id: Any, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "delete_from_cc_table_by_value",
            {"table": table, "target_id": target_id, "conn": conn},
        )

    def break_cc_links_by_book_id(
        self,
        lt: str,
        book_id: str | int | Iterable[str | int | tuple[str | int]],
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "break_cc_links_by_book_id", {"lt": lt, "book_id": book_id, "conn": conn}
        )

    def break_cc_links_by_book_id_and_value(
        self, lt: str, book_id: int, value_id: int, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "break_cc_links_by_book_id_and_value",
            {"lt": lt, "book_id": book_id, "value_id": value_id, "conn": conn},
        )

    def clear_cc_entries_from_table(
        self, table: str, book_id: int, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "clear_cc_entries_from_table",
            {"table": table, "book_id": book_id, "conn": conn},
        )

    def clear_cc_unused_table_entries(
        self, table: str, lt: str, conn: SQLConnection | None = None
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "clear_cc_unused_table_entries", {"table": table, "lt": lt, "conn": conn}
        )

    def clean_custom(
        self,
        cc_num_map: Mapping[int, Mapping[str, Any]],
        cc_table_name_factory: Callable[[int], tuple[str, str]] | None = None,
        conn: SQLConnection | None = None,
    ) -> None:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call(
            "clean_custom",
            {
                "cc_num_map": cc_num_map,
                "cc_table_name_factory": cc_table_name_factory,
                "conn": conn,
            },
        )

    def direct_get_custom_tables(self, conn: QueryConnection | None = None) -> set[str]:
        """Delegate to native storage through the compatibility boundary."""
        return self._cc_call("direct_get_custom_tables", {"conn": conn})
