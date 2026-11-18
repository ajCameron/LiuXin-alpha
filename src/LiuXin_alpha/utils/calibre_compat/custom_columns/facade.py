"""Translate Calibre-shaped custom-column calls onto native field storage."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES
from LiuXin_alpha.databases.custom_columns.native_repository import names
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    OwnerRef,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier as quote
from LiuXin_alpha.databases.transactions import transaction_active, transaction_scope
from LiuXin_alpha.databases.utils import (
    _get_next_series_num_for_list,
    _get_series_values,
    cleanup_tags,
)
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import (
    CustomColumnAdapter,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.notify import (
    dummy_dirtied,
    dummy_notify,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.transactions import (
    atomic_custom_write,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.values import (
    _dispatch,
    _inventory,
    _project,
)
from LiuXin_alpha.utils.calibre_compat.metadata.field_adaptors import (
    cc_adapt_bool,
    cc_adapt_datetime,
    cc_adapt_enum,
    cc_adapt_number,
    cc_adapt_rating,
    cc_adapt_text,
)
from LiuXin_alpha.utils.calibre_compat.metadata.field_metadata import FieldMetadata
from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.libraries.liuxin_six import basestring
from LiuXin_alpha.utils.logging import prints

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.utils.calibre_compat.custom_columns.api import (
        CustomColumnCache,
        CustomColumnDataAdapter,
        CustomColumnDirtied,
        CustomColumnFieldMetadata,
        CustomColumnMetadata,
        CustomColumnNotify,
        CustomColumnPreferences,
    )


class CustomColumns(CustomColumnAdapter[SQLConnection]):
    """Adapt legacy arguments and optional projections without owning another format."""

    CUSTOM_DATA_TYPES = CUSTOM_DATA_TYPES
    db: DatabaseAPI
    prefs: CustomColumnPreferences | None = None
    FIELD_MAP: MutableMapping[str | int, int]
    field_metadata: CustomColumnFieldMetadata
    custom_column_label_map: MutableMapping[str, CustomColumnMetadata]
    custom_column_num_map: MutableMapping[int, CustomColumnMetadata]
    custom_column_num_to_label_map: MutableMapping[int, str]
    custom_data_adapters: Mapping[str, CustomColumnDataAdapter]
    dirtied: CustomColumnDirtied
    notify: CustomColumnNotify
    _data_cache: CustomColumnCache | None = None

    def __init__(
        self,
        db: DatabaseAPI,
        conn: SQLConnection | None = None,
        table: str = "books",
        field_metadata: CustomColumnFieldMetadata | None = None,
        data: CustomColumnCache | None = None,
        field_map: MutableMapping[str | int, int] | None = None,
        embed: bool = False,
    ) -> None:
        super().__init__(db, db.macros)
        self.embed = embed
        if (
            table == "books"
            and "books" not in db.main_tables
            and "manifestations" in db.main_tables
        ):
            table = "manifestations"
        self.table = table
        if conn is not None:
            self.conn = conn
        self.field_metadata = (
            FieldMetadata() if field_metadata is None else field_metadata
        )
        self.data = data
        self.FIELD_MAP = (
            field_map
            if field_map is not None
            else {
                name: index
                for index, name in enumerate(
                    (
                        "id",
                        "title",
                        "authors",
                        "timestamp",
                        "size",
                        "rating",
                        "tags",
                        "comments",
                        "series",
                        "publisher",
                        "series_index",
                        "sort",
                        "author_sort",
                        "formats",
                        "path",
                        "pubdate",
                        "uuid",
                        "cover",
                        "au_map",
                        "last_modified",
                        "identifiers",
                        "languages",
                    )
                )
            }
        )
        self.custom_column_label_map, self.custom_column_num_map = {}, {}
        self.custom_column_num_to_label_map = {}
        self.triggers: list[str] = []
        self.remove: list[int] = []
        self.custom_data_adapters = {
            "float": cc_adapt_number,
            "int": cc_adapt_number,
            "rating": cc_adapt_rating,
            "bool": cc_adapt_bool,
            "comments": lambda value, _: cc_adapt_text(value, {"is_multiple": False}),
            "datetime": cc_adapt_datetime,
            "text": cc_adapt_text,
            "series": cc_adapt_text,
            "enumeration": cc_adapt_enum,
        }
        if not embed:
            self.dirtied, self.notify = self._default_dirtied, self._default_notify
        self.refresh_db_custom_columns_metadata()
        self.cc_tag_browser_categories_made = True

    @property
    def native(self) -> CustomFieldService:
        return CustomFieldService(self.db)

    @property
    def conn(self) -> SQLConnection | None:
        if getattr(self, "_conn_override", None) is None:
            active = self.db.macros.current_transaction_connection()
            if active is not None:
                return cast(SQLConnection, active)
        return super().conn

    @conn.setter
    def conn(self, value: SQLConnection | None) -> None:
        self._conn_override = value

    def _require_owned_write(self) -> None:
        if (
            transaction_active(self._custom_connection())
            or self.db.macros.current_transaction_connection() is not None
        ):
            raise RuntimeError("Custom-column writes require their own transaction")

    def refresh_db_custom_columns_metadata(self) -> None:
        connection = self._custom_connection()
        with transaction_scope(
            connection, owns_transaction=not transaction_active(connection)
        ):
            publish = self._prepare_custom_columns_refresh(connection)
        publish()

    def _prepare_custom_column_publication(
        self, connection: SQLConnection
    ) -> Callable[[], None]:
        return self._prepare_custom_columns_refresh(connection)

    def _prepare_custom_columns_refresh(
        self, connection: SQLConnection | None = None
    ) -> Callable[[], None]:
        connection = self._custom_connection() if connection is None else connection
        repository = self.native.repository
        definitions = repository.definitions(connection)
        label_map: dict[str, CustomColumnMetadata] = {}
        num_map: dict[int, CustomColumnMetadata] = {}
        fields = []
        for definition in definitions:
            if definition.attachment != self.table:
                continue
            repository.require_native(connection, definition)
            table, link = names(definition)
            for physical in (table, link) if definition.normalized else (table,):
                if not repository.table_exists(connection, physical):
                    raise DatabaseIntegrityError(
                        f"Custom column {definition.id} has missing backing tables"
                    )
            separators = (
                {}
                if definition.mode == "scalar"
                else {
                    "cache_to_list": "|",
                    "ui_to_list": "&" if definition.display.get("is_names") else ",",
                    "list_to_ui": " & " if definition.display.get("is_names") else ", ",
                }
            )
            metadata: CustomColumnMetadata = {
                "label": definition.label,
                "name": definition.name,
                "datatype": definition.datatype,
                "editable": definition.editable,
                "display": dict(definition.display),
                "normalized": definition.normalized,
                "num": definition.id,
                "is_multiple": definition.mode != "scalar",
                "in_table": definition.attachment,
                "storage_version": 2,
                "multiple_seps": separators,
            }
            label_map[definition.label] = num_map[definition.id] = metadata
            fields.append(
                dict(
                    label=definition.label,
                    table=table,
                    column="value",
                    datatype=definition.datatype,
                    colnum=definition.id,
                    name=definition.name,
                    display=metadata["display"],
                    is_multiple=separators,
                    is_category=definition.normalized
                    and definition.datatype != "composite",
                    is_editable=definition.editable,
                    is_csp=False,
                    in_table=definition.attachment,
                )
            )
        publish_fields = self.field_metadata.prepare_custom_fields(self.table, fields)

        def publish() -> None:
            publish_fields()
            self.custom_column_label_map, self.custom_column_num_map = (
                label_map,
                num_map,
            )
            self.custom_column_num_to_label_map = {
                number: data["label"] for number, data in num_map.items()
            }

        return publish

    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: str | None = None,
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        table: str | None = None,
        make_category: bool | None = None,
    ) -> int:
        self._require_owned_write()
        if table is not None:
            if in_table not in (None, "books", table):
                raise TypeError(
                    "Pass only one of table= or in_table= (or keep them identical)."
                )
            in_table = table
        number = super().create_custom_column(
            name,
            datatype,
            is_multiple,
            label,
            editable,
            display,
            self.table if in_table is None else in_table,
            make_category=make_category,
        )
        if self.prefs is not None:
            self.prefs.set("update_all_last_mod_dates_on_start", True)
        return number

    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        notify: bool = True,
        update_last_modified: bool = False,
    ) -> bool:
        self.custom_field_metadata(num=num)
        self._require_owned_write()
        with self.native.write() as connection:
            changed = super().set_custom_column_metadata(
                num, name, label, is_editable, display, in_table
            )
            publish = self._prepare_custom_columns_refresh(connection)
        publish()
        if update_last_modified and self.prefs is not None:
            self.prefs.set("update_all_last_mod_dates_on_start", True)
        if changed and notify:
            self.notify("metadata", [])
        return changed

    def delete_custom_column(
        self, num: int | None = None, *, label: str | None = None
    ) -> None:
        metadata = self.custom_field_metadata(label=label, num=num)
        self._require_owned_write()
        with self.native.write() as connection:
            self.native.retire_field(metadata["num"])
            publish = self._prepare_custom_columns_refresh(connection)
        publish()

    def custom_field_name(
        self, label: str | None = None, num: int | None = None
    ) -> str:
        if label is None:
            if num is None:
                raise KeyError(num)
            label = self.custom_column_num_to_label_map[num]
        return self.field_metadata.custom_field_prefix + label

    def clean_custom(self) -> None:
        self._require_owned_write()
        service = self.native
        with service.write():
            for definition in service.list_fields(attachment=self.table):
                if definition.normalized:
                    while page := service.inventory(
                        definition.id, limit=1000, unused=True
                    ).items:
                        for item in page:
                            service.delete_value(definition.id, item["id"])

    def is_item_used_in_multiple(
        self, item: str, label: str | None = None, num: int | None = None
    ) -> bool:
        metadata = self.custom_field_metadata(label=label, num=num)
        return self.native.is_shared(metadata["num"], item)

    def custom_dirty_books_referencing(
        self, field: str, id: int, commit: bool = True
    ) -> list[Sequence[Any]]:
        metadata = self.custom_field_metadata(
            label=field.removeprefix(self.field_metadata.custom_field_prefix)
        )
        service = self.native
        with service.read() as connection:
            definition = service.repository.resolve(connection, metadata["num"])
            owners: list[Sequence[Any]] = [
                (owner,)
                for owner, values in service.repository.read_entries(
                    connection, definition
                ).items()
                if any(entry.value_id == id for entry in values)
            ]
        self.dirtied([owner[0] for owner in owners], commit=commit)
        return owners

    @property
    def value_cache(self) -> CustomColumnCache | None:
        """Expose an optional projection; SQL value operations do not require it."""
        return self._data_cache

    @property
    def data(self) -> CustomColumnCache:
        """Require the caller-supplied cache before accessing value projections."""
        if self._data_cache is None:
            raise RuntimeError(
                "Custom-column positional/cache operations require a results cache"
            )
        return self._data_cache

    @data.setter
    def data(self, value: CustomColumnCache | None) -> None:
        self._data_cache = value

    def id(self, idx: int) -> int:
        """Resolve a displayed cache position through its persistent-ID field."""
        return int(self.data[idx][self.FIELD_MAP["id"]])

    def _custom_connection(self) -> SQLConnection:
        """Require a physical connection; never open or close a borrowed handle."""
        connection = self.conn
        if connection is None:
            raise RuntimeError("Custom-column operation requires an open connection")
        return connection

    def _default_dirtied(self, ids: Iterable[int], commit: bool = True) -> None:
        """Ignore standalone dirtying; embedded hosts must replace this hook."""
        dummy_dirtied(ids, commit, cc_class=self)

    def _default_notify(self, event: str, ids: Iterable[int]) -> None:
        """Ignore standalone events; embedded hosts must replace this hook."""
        dummy_notify(event, ids, cc_class=self)

    @property
    def custom_tables(self) -> set[str]:
        """
        Read the owning database’s global custom-table names through the wrapper.

        Example:
            For a configured facade cc, cc.custom_tables includes physical custom
            value/link tables across the database.


        :return: Collection returned by get_custom_tables, not restricted to this
            facade’s attachment table.
        """
        return self.get_custom_tables()

    def get_custom_tables(self) -> set[str]:
        """
        Discover native custom tables through the explicit compatibility adapter.

        Example:
            Given cc, cc.get_custom_tables() retrieves the physical names used during
            definition validation.


        :return: Set of custom value/link table names.
        """
        return super().get_custom_tables()

    @staticmethod
    def _get_next_series_num_for_list(
        series_indices: Sequence[Sequence[int | float]],
    ) -> int | float:
        """
        Delegate series numbering to the preference-driven legacy helper.

        Default unwrapping expects rows containing the index at position zero.
        The returned number follows the configured increment policy.

        Example:
            With the default next policy,
            CustomColumns._get_next_series_num_for_list([[3.5]]) returns 4.0.


        :param series_indices: Ordered indexable rows containing the numeric index at
            element zero.
        :return: Next/configured number from the shared helper.
        """
        return _get_next_series_num_for_list(series_indices)

    def custom_columns_in_meta(
        self,
        update_field_map: bool = True,
        field_metadata: CustomColumnFieldMetadata | None = None,
    ) -> dict[int, str]:
        """
        Generate legacy meta2 SQL projection strings for the current custom columns.

        Use the stored attachment table and its actual primary key. Multiple values
        use sorted-concatenation functions; Series adds an index projection. This
        compatibility helper creates no view and does not update FIELD_MAP. Value
        operations read their backing tables without depending on these fragments.

        Example:
            Given cc, fragments = cc.custom_columns_in_meta() produces SQL strings
            for a compatible projection over its attachment table.


        :param update_field_map: Compatibility flag currently ignored.
        :param field_metadata: Compatibility metadata argument currently ignored.
        :return: Dictionary mapping custom-column numbers to SQL projection strings.
        """
        if self.native.repository.postgres:
            raise CompatibilityError("Legacy meta2 SQL projections require SQLite")
        lines = {}

        for data in self.custom_column_label_map.values():
            definition = self.native.get_field(data["num"])
            _project(definition, None if definition.mode == "scalar" else ())
            in_table = data.get("in_table") or "books"
            table, lt = self.custom_table_names(data["num"], in_table=in_table)
            owner_reference = (
                quote(in_table)
                + "."
                + quote(self.db.driver_wrapper.get_id_column(in_table))
            )
            table_col = plural_singular_mapper(table)
            lt_col = plural_singular_mapper(lt)

            if data["normalized"]:
                query = "{table}.{table_col}_value"

                if data["is_multiple"]:
                    if data["multiple_seps"]["cache_to_list"] == "|":
                        query = "sortconcat_bar(link.{lt_col}_ordinal, {table}.{table_col}_value)"

                    elif data["multiple_seps"]["cache_to_list"] == "&":
                        query = "sortconcat_amper(link.{lt_col}_ordinal, {table}.{table_col}_value)"

                    else:
                        prints(
                            "WARNING: unknown value in multiple_seps",
                            data["multiple_seps"]["cache_to_list"],
                        )
                        query = "sortconcat_bar(link.{lt_col}_ordinal, {table}.{table_col}_value)"

                final_query = query.format(
                    table=table, table_col=table_col, lt_col=lt_col
                )

                line = """(SELECT {query} FROM {lt} AS link INNER JOIN
                    {table} ON(link.{lt_col}_value={table}.{table_col}_id) WHERE link.{lt_col}_book={owner_reference})
                    custom_{num}
                """.format(
                    query=final_query,
                    owner_reference=owner_reference,
                    lt=lt,
                    lt_col=lt_col,
                    table=table,
                    table_col=table_col,
                    num=data["num"],
                )

                if data["datatype"] == "series":
                    line += """,(SELECT {lt_col}_extra FROM {lt} WHERE {lt}.{lt_col}_book={owner_reference})
                        custom_index_{num}""".format(
                        lt=lt,
                        lt_col=lt_col,
                        num=data["num"],
                        owner_reference=owner_reference,
                    )
            else:
                line = """
                (SELECT {table_col}_value FROM {table} WHERE {table_col}_book={owner_reference}) custom_{num}
                """.format(
                    table=table,
                    table_col=table_col,
                    num=data["num"],
                    owner_reference=owner_reference,
                )
            lines[data["num"]] = line

        return lines

    def update_field_map_from_custom_columns_in_meta(
        self, lines: dict[int, str], update_field_metadata: bool = True
    ) -> None:
        """
        Append custom value/index slots and optionally update FieldMetadata indices.

        Start after the current maximum slot, add each custom value and an adjacent
        series index where required. Repeated calls append new positions rather than
        restoring original ones. The caller must ensure the actual view uses this order;
        failures can leave partial map updates.

        Example:
            After creating a compatible view in sorted custom-number order,
            cc.update_field_map_from_custom_columns_in_meta(fragments) appends its cache
            slots.


        :param lines: Mapping whose sorted numeric keys determine custom-column slot
            order; SQL values are not inspected.
        :param update_field_metadata: Whether to update FieldMetadata alongside
            FIELD_MAP, default True.
        :return: None; mutate FIELD_MAP and possibly field metadata in place.
        :raises ValueError: FIELD_MAP is empty.
        :raises KeyError: A requested number is missing from custom_column_num_map.
        """
        custom_map = lines

        # custom col labels are numbers (the id in the custom_columns table)
        custom_cols = list(sorted(custom_map.keys()))

        # Assume the field map is in its default state - before any custom columns have been registered to it
        base = max(self.FIELD_MAP.values())

        for col in custom_cols:
            self.FIELD_MAP[col] = base = base + 1
            if update_field_metadata:
                self.field_metadata.set_field_record_index(
                    self.custom_column_num_map[col]["label"], base, prefer_custom=True
                )

            if self.custom_column_num_map[col]["datatype"] == "series":
                # account for the series index column. Field_metadata knows that the series index is one larger than the
                # series. If you change it here, be sure to change it there as well.
                self.FIELD_MAP[str(col) + "_index"] = base = base + 1
                if update_field_metadata:
                    self.field_metadata.set_field_record_index(
                        self.custom_column_num_map[col]["label"] + "_index",
                        base,
                        prefer_custom=True,
                    )

    def custom_field_metadata(
        self, label: str | None = None, num: int | None = None
    ) -> CustomColumnMetadata:
        """
        Return the metadata record selected by label first, otherwise number.

        Example:
            Given cc, cc.custom_field_metadata(num=column_id) exposes the record shared
            with its numeric/label maps.


        :param label: Optional custom-column label; takes precedence over num.
        :param num: Numeric metadata key used when label is None.
        :return: Existing mutable metadata dictionary, not a copy.
        :raises KeyError: The chosen key is absent, including num=None when neither
            selector is supplied.
        """

        if label is not None:
            return self.custom_column_label_map[label]
        if num is None:
            raise KeyError(num)
        return self.custom_column_num_map[num]

    def _get_series_values(self, val: Any) -> tuple[str, float | None]:
        """
        Parse a series string or the last entry of a supplied list.

        Delegate parsing to the shared bracket-index helper. Earlier list elements are
        ignored; unsupported truthy types raise rather than being stringified.

        Example:
            For cc, cc._get_series_values(["ignored", "Dune [2]"]) returns ("Dune",
            2.0).


        :param val: False value, series string, or nonempty list whose last item can be
            parsed.
        :return: Parsed (name, optional float index); false input becomes ("", None).
        :raises NotImplementedError: A truthy value is neither a supported string nor a
            list.
        """
        if val is None or not val:
            return _get_series_values("")

        if isinstance(val, basestring):
            return _get_series_values(val)

        if isinstance(val, list):
            return _get_series_values(val[-1])

        else:
            raise NotImplementedError(f"val was not of an expected type {val}")

    @staticmethod
    def cleanup_tags(tags_list: Iterable[str | bytes]) -> list[str]:
        """
        Delegate text and byte tag cleanup to the shared metadata policy.

        Decode bytes before normalization; preserve first spelling and input order while
        removing case-insensitive duplicates.

        Example:
            >>> CustomColumns.cleanup_tags([])
            []


        :param tags_list: Iterable of tag entries passed through unchanged.
        :return: Cleaned, deduplicated tag strings.
        """
        return cleanup_tags(tags_list)

    def get_custom(
        self,
        idx: int,
        label: str | None = None,
        num: int | None = None,
        index_is_id: bool = False,
    ) -> Any:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "get_custom",
            {"idx": idx, "label": label, "num": num, "index_is_id": index_is_id},
        )

    def get_custom_extra(
        self,
        idx: int,
        label: str | None = None,
        num: int | None = None,
        index_is_id: bool = False,
    ) -> Any:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "get_custom_extra",
            {"idx": idx, "label": label, "num": num, "index_is_id": index_is_id},
        )

    def get_custom_and_extra(
        self,
        idx: int,
        label: str | None = None,
        num: int | None = None,
        index_is_id: bool = False,
    ) -> tuple[Any, Any]:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "get_custom_and_extra",
            {"idx": idx, "label": label, "num": num, "index_is_id": index_is_id},
        )

    def get_custom_items_with_ids(
        self, label: str | None = None, num: int | None = None
    ) -> list[tuple[int, Any]]:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self, "get_custom_items_with_ids", {"label": label, "num": num}
        )

    def get_next_cc_series_num_for(
        self,
        series: str,
        label: str | None = None,
        num: int | None = None,
    ) -> float | None:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "get_next_cc_series_num_for",
            {"series": series, "label": label, "num": num},
        )

    @atomic_custom_write
    def set_custom_bulk_multiple(
        self,
        cc_row_ids: Iterable[int],
        add: Iterable[str | bytes] | None = None,
        remove: Iterable[str] | None = None,
        label: str | None = None,
        num: int | None = None,
        notify: bool = False,
    ) -> None:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "set_custom_bulk_multiple",
            {
                "cc_row_ids": cc_row_ids,
                "add": add,
                "remove": remove,
                "label": label,
                "num": num,
                "notify": notify,
            },
        )

    @atomic_custom_write
    def set_custom_bulk(
        self,
        cc_row_ids: Sequence[int],
        val: Any,
        label: str | None = None,
        num: int | None = None,
        append: bool = False,
        notify: bool = True,
        extras: Sequence[Any] | Mapping[int, Any] | None = None,
    ) -> None:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "set_custom_bulk",
            {
                "cc_row_ids": cc_row_ids,
                "val": val,
                "label": label,
                "num": num,
                "append": append,
                "notify": notify,
                "extras": extras,
            },
        )

    @atomic_custom_write
    def set_custom(
        self,
        cc_row_id: int,
        val: Any,
        label: str | None = None,
        num: int | None = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        commit: bool = True,
        allow_case_change: bool = False,
    ) -> set[int]:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "set_custom",
            {
                "cc_row_id": cc_row_id,
                "val": val,
                "label": label,
                "num": num,
                "append": append,
                "notify": notify,
                "extra": extra,
                "commit": commit,
                "allow_case_change": allow_case_change,
            },
        )

    @atomic_custom_write
    def delete_custom_item_using_id(
        self,
        idx: int | None,
        label: str | None = None,
        num: int | None = None,
    ) -> None:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "delete_custom_item_using_id",
            {"idx": idx, "label": label, "num": num},
        )

    @atomic_custom_write
    def delete_item_from_multiple(
        self,
        item: str,
        label: str | None = None,
        num: int | None = None,
    ) -> list[int]:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "delete_item_from_multiple",
            {"item": item, "label": label, "num": num},
        )

    @atomic_custom_write
    def rename_custom_item(
        self,
        old_id: int,
        new_name: str,
        label: str | None = None,
        num: int | None = None,
    ) -> None:
        """Translate this compatibility operation onto the native field service."""
        return _dispatch(
            self,
            "rename_custom_item",
            {"old_id": old_id, "new_name": new_name, "label": label, "num": num},
        )

    def all_custom(self, label: str | None = None, num: int | None = None) -> set[Any]:
        metadata = self.custom_field_metadata(label=label, num=num)
        return {item["value"] for item in _inventory(self.native, metadata["num"])}

    def _read_custom_rows(
        self, data: Mapping[str, Any], owner_id: int
    ) -> list[tuple[Any, Any]]:
        service = self.native
        with service.read() as connection:
            definition = service.repository.resolve(connection, data["num"])
            return [
                (entry.value, entry.position)
                for entry in service.repository.read_entries(
                    connection, definition, [owner_id]
                ).get(owner_id, [])
            ]

    def _read_custom_value(self, data: Mapping[str, Any], owner_id: int) -> Any:
        definition = self.native.get_field(data["num"])
        value, _ = _project(
            definition,
            self.native.get_value(OwnerRef(self.table, owner_id), definition.id),
        )
        return "|".join(value) if isinstance(value, list) else value

    def _refresh_custom_cache(
        self, data: Mapping[str, Any], owner_ids: Iterable[int]
    ) -> None:
        definition = self.native.get_field(data["num"])
        for owner_id in owner_ids:
            value, extra = _project(
                definition,
                self.native.get_value(OwnerRef(self.table, owner_id), definition.id),
            )
            cache = self.value_cache
            if cache is not None:
                cache.set(
                    owner_id,
                    self.FIELD_MAP[definition.id],
                    ("|".join(value) or None) if isinstance(value, list) else value,
                    row_is_id=True,
                )
                if definition.datatype == "series":
                    slot = self.FIELD_MAP.get(
                        str(definition.id) + "_index", self.FIELD_MAP[definition.id] + 1
                    )
                    cache.set(owner_id, slot, extra, row_is_id=True)

    def _set_custom(
        self,
        id_: int,
        val: Any,
        label: str | None = None,
        num: int | None = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        allow_case_change: bool = False,
    ) -> set[int]:
        return self.set_custom(
            id_,
            val,
            label=label,
            num=num,
            append=append,
            notify=notify,
            allow_case_change=allow_case_change,
            extra=extra,
        )

    def rename_custom_item_in_data(
        self, book_ids: Iterable[Sequence[int]], column_num: int | str, new_value: Any
    ) -> None:
        """Replace optional cache cells without performing a storage mutation."""
        cache = self.value_cache
        if cache is not None:
            for row in book_ids:
                cache.set(row[0], self.FIELD_MAP[column_num], new_value, row_is_id=True)
