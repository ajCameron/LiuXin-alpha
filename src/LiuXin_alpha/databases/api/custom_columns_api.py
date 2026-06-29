"""
Structural API for calibre-style custom column helpers.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Protocol, TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName
    from LiuXin_alpha.databases.field_metadata_bridge import FieldMetadata


CustomColumnMetadata: TypeAlias = MutableMapping[str, Any]
CustomColumnDataAdapter: TypeAlias = Callable[[Any, Mapping[str, Any]], Any]


class CustomColumnsAPI(Protocol):
    """
    Protocol implemented by ``CustomColumns`` and expected by its mixins.

    The custom-column implementation is split across several mixins. Those
    mixins share state on the final host object, so a structural protocol is a
    better fit than forcing every mixin through another concrete base class.
    """

    db: "DatabaseAPI"
    table: "MainTableName"
    data: Any
    prefs: Any
    FIELD_MAP: MutableMapping[Any, int]
    field_metadata: "FieldMetadata"
    custom_column_label_map: MutableMapping[str, CustomColumnMetadata]
    custom_column_num_map: MutableMapping[int, CustomColumnMetadata]
    custom_column_num_to_label_map: MutableMapping[int, str]
    custom_data_adapters: Mapping[str, CustomColumnDataAdapter]

    @property
    def conn(self) -> Any:
        """
        Return the live database connection used by custom-column helpers.

        :return:
        """

    @conn.setter
    def conn(self, value: Any) -> None:
        """
        Set an explicit connection override.

        :param value:
        :return:
        """

    @property
    def custom_tables(self) -> Iterable[str]:
        """
        Return custom-column storage/link table names.
        :return:
        """

    @property
    def direct_custom_tables(self) -> set[str]:
        """Return custom-column storage/link table names directly from the database."""

    def all_custom(self, label: Optional[str] = None, num: Optional[int] = None) -> set[Any]:
        """
        Return all stored values for a custom column.

        :param label:
        :param num:
        :return:
        """

    @staticmethod
    def cleanup_tags(tags_list: list[str]) -> list[str]:
        """
        Normalize tag values before storing them.

        :param tags_list:
        :return:
        """

    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: Optional[str] = None,
        editable: bool = True,
        display: Optional[Mapping[str, Any]] = None,
        in_table: str = "books",
        table: Optional[str] = None,
        make_category: Optional[bool] = None,
    ) -> int:
        """
        Create a custom column definition and its backing tables.

        :param name:
        :param datatype:
        :param is_multiple:
        :param label:
        :param editable:
        :param display:
        :param in_table:
        :param table:
        :param make_category:
        :return:
        """

    def custom_dirty_books_referencing(self, field: str, book_id: Any, commit: bool = True) -> Iterable[Any]:
        """
        Dirty records referencing a custom-column value.

        :param field:
        :param book_id:
        :param commit:
        :return:
        """

    def custom_field_metadata(
        self,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> "CustomColumnMetadata":
        """
        Return the metadata record for a custom column.

        :param label:
        :param num:
        :return:
        """

    @staticmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Return ``(custom_table, link_table)`` for a custom-column id.

        :param num:
        :param in_table:
        :return:
        """

    def delete_custom_column(self, label: Optional[str] = None, num: Optional[int] = None) -> None:
        """
        Mark a custom column for later deletion.

        :param label:
        :param num:
        :return:
        """

    def delete_custom_item_using_id(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> None:
        """
        Delete a normalized custom-column item by id.

        :param idx:
        :param label:
        :param num:
        :return:
        """

    def delete_item_from_multiple(
        self,
        item: str,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> list[int]:
        """
        Delete an item from a multiple-value text custom column.

        :param item:
        :param label:
        :param num:
        :return:
        """

    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Return the link-table extra value for a custom column.

        :param link_table:
        :param index:
        :return:
        """

    def direct_get_custom_id_val_pairs(self, table: str) -> tuple[int, Any]:
        """
        Return id/value pairs from a custom-column table.

        :param table:
        :return:
        """

    def dirtied(self, ids: Iterable[int], commit: bool = True) -> None:
        """
        Mark records as dirty.

        :param ids:
        :param commit:
        :return:
        """

    def get_custom(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> Any:
        """
        Return the custom-column value for a record.

        :param idx:
        :param label:
        :param num:
        :param index_is_id:
        :return:
        """

    def get_custom_extra(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> Any:
        """
        Return a custom-column extra value for a record.

        :param idx:
        :param label:
        :param num:
        :param index_is_id:
        :return:
        """

    def get_custom_and_extra(
        self,
        idx: int,
        label: Optional[str] = None,
        num: Optional[int] = None,
        index_is_id: bool = False,
    ) -> tuple[Any, Any]:
        """
        Return both the custom-column value and extra value.

        :param idx:
        :param label:
        :param num:
        :param index_is_id:
        :return:
        """

    def get_custom_items_with_ids(
        self,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> Any:
        """Return id/value pairs for normalized custom-column values."""

    def get_next_cc_series_num_for(
        self,
        series: str,
        label: Optional[str] = None,
        num: Optional[int] = None,
    ) -> Optional[float]:
        """
        Return the next custom-series index for the given series.

        :param series:
        :param label:
        :param num:
        :return:
        """

    def id(self, idx: int) -> int:
        """
        Return the row id for a row index.

        :param idx:
        :return:
        """

    def notify(self, event: str, ids: Iterable[int]) -> None:
        """
        Notify listeners about metadata changes.

        :param event:
        :param ids:
        :return:
        """

    def rename_custom_item_in_data(
        self,
        target_ids: Iterable[Any],
        column_num: Any,
        new_value: Any,
    ) -> None:
        """
        Update cached custom-column values after a value rename/delete.

        :param target_ids:
        :param column_num:
        :param new_value:
        :return:
        """

    def set_custom_bulk_multiple(
        self,
        ids: Iterable[int],
        add: Optional[Iterable[str]] = None,
        remove: Optional[Iterable[str]] = None,
        label: Optional[str] = None,
        num: Optional[int] = None,
        notify: bool = False,
    ) -> None:
        """
        Bulk update a multiple-value custom column.

        :param ids:
        :param add:
        :param remove:
        :param label:
        :param num:
        :param notify:
        :return:
        """

    def set_custom_bulk(
        self,
        ids: Iterable[int],
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extras: Optional[Mapping[int, Any]] = None,
    ) -> None:
        """
        Bulk update custom-column values.

        :param ids:
        :param val:
        :param label:
        :param num:
        :param append:
        :param notify:
        :param extras:
        :return:
        """

    def set_custom(
        self,
        id: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        commit: bool = True,
        allow_case_change: bool = False,
    ) -> set[int]:
        """
        Update a custom-column value.

        :param id:
        :param val:
        :param label:
        :param num:
        :param append:
        :param notify:
        :param extra:
        :param commit:
        :param allow_case_change:
        :return:
        """

    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        in_table: Optional[str] = None,
        notify: bool = True,
        update_last_modified: bool = False,
    ) -> Any:
        """
        Update a custom-column metadata row.

        :param num:
        :param name:
        :param label:
        :param is_editable:
        :param display:
        :param in_table:
        :param notify:
        :param update_last_modified:
        :return:
        """

    def _get_next_series_num_for_list(self, series_indices: Iterable[int | float]) -> Optional[float]:
        """
        Return the next series index for an existing series index list.

        :param series_indices:
        :return:
        """

    def _get_series_values(self, val: Any) -> tuple[str, Optional[float]]:
        """
        Parse a calibre-formatted series string.

        :param val:
        :return:
        """

    def _set_custom(
        self,
        id_: int,
        val: Any,
        label: Optional[str] = None,
        num: Optional[int] = None,
        append: bool = False,
        notify: bool = True,
        extra: Any = None,
        allow_case_change: bool = False,
    ) -> set[int]:
        """
        Implementation helper for ``set_custom``.

        :param id_:
        :param val:
        :param label:
        :param num:
        :param append:
        :param notify:
        :param extra:
        :param allow_case_change:
        :return:
        """
