
"""Macros API contract."""

from __future__ import annotations

import abc
import sqlite3

from typing import Optional, Union, TYPE_CHECKING, Any, Iterable
import datetime

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class MacrosAPI(abc.ABC):
    """
    Abstract API for SQL macros implementations.

    Macros are complex operations which can be sped up by implementing them in SQL.
    """

    @abc.abstractmethod
    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the macros class.

        :param db:
        """

    @staticmethod
    @abc.abstractmethod
    def _cc_table_col_mapper(table: str) -> str:
        ...

    @staticmethod
    @abc.abstractmethod
    def _get_cc_id_val(custom_column: str) -> tuple[str, str]:
        ...

    @abc.abstractmethod
    def add_cc_link_with_extra(self, lt, book_id, value_id, extra=None, conn=None, target_column='value'):
        ...

    @abc.abstractmethod
    def add_cc_link_with_extra_multi(self, lt, sequence, extra=False, conn=None, target_column='value'):
        ...

    @abc.abstractmethod
    def add_cc_table_value(self, table, value, conn=None):
        ...







    @abc.abstractmethod
    def break_cc_links_by_book_id(self, lt, book_id, conn=None):
        ...

    @abc.abstractmethod
    def break_cc_links_by_book_id_and_value(self, lt, book_id, value_id, conn=None):
        ...

    @abc.abstractmethod
    def break_cc_lt_link(self, lt, book, value=None):
        ...









    @abc.abstractmethod
    def bulk_add_links(self, link_table, src_col, dst_col, values):
        ...

    @abc.abstractmethod
    def bulk_delete_in_table(self, table, column, column_values):
        ...

    @abc.abstractmethod
    def bulk_delete_items_in_table_two_matching_cols(self, table, col_1, col_2, column_values):
        ...

    @abc.abstractmethod
    def bulk_update_link_table(self, link_table, update_column, other_column, values):
        ...

    @abc.abstractmethod
    def check_for_cc_link(self, link_table: str, book_id: int, value_id: int, conn: Optional[sqlite3.Connection]=None) -> bool:
        ...







    @abc.abstractmethod
    def clean_custom(self, cc_num_map, cc_table_name_factory=None, conn=None):
        ...

    @abc.abstractmethod
    def clear_cc_entries_from_table(self, table, book_id, conn=None):
        ...

    @abc.abstractmethod
    def clear_cc_unused_table_entries(self, table, lt, conn=None):
        ...








    @abc.abstractmethod
    def create_cc_table(
        self,
        normalized: bool,
        datatype: str,
        dt,
        table: str,
        link_table,
        collate,
        in_table='books',
        ordered=False,
        conn=None,
    ):
        ...

    @abc.abstractmethod
    def create_cc_temp_tables(self, temp_tables: Iterable[str], conn: Any=None) -> None:
        ...



    @abc.abstractmethod
    def delete_cc_item(self, table, lt, target_id, conn=None):
        ...





    @abc.abstractmethod
    def delete_from_cc_table_by_id(self, table, target_id, conn=None):
        ...

    @abc.abstractmethod
    def delete_from_cc_table_by_value(self, table, target_id):
        ...

    @abc.abstractmethod
    def delete_in_table(self, table, column, value):
        ...





    @abc.abstractmethod
    def destroy_cc_temp_tables(self, temp_tables: Iterable[str], conn: Any=None) -> None:
        ...

    @abc.abstractmethod
    def direct_get_custom_and_extra(self, link_table, index, conn=None):
        ...

    @abc.abstractmethod
    def direct_get_custom_tables(self, conn=None):
        ...

    @abc.abstractmethod
    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        ...

    @abc.abstractmethod
    def do_cc_db_bulk_addition(self, temp_tables, custom_table, link_table, add, remove, conn=None):
        ...

    @abc.abstractmethod
    def do_custom_column_delete_by_id(self, cc_id: int) -> None:
        ...

    @abc.abstractmethod
    def do_custom_column_delete_by_num(self, num: int) -> None:
        ...

    @abc.abstractmethod
    def ensure_custom_column_value(self, cc_table: str, value: Any) -> Any:
        ...

    @property
    @abc.abstractmethod
    def execute(self):
        ...

    @property
    @abc.abstractmethod
    def executemany(self):
        ...

    @abc.abstractmethod
    def generic_clean_update(self, link_table, link_col, value_for_clear):
        ...

    @property
    @abc.abstractmethod
    def get(self):
        ...

    @abc.abstractmethod
    def get_all_cc_custom_values(self, cc_table: str, distinct: bool=False, conn: Optional[sqlite3.Connection]=None) -> Iterable[Union[int, str, float]]:
        ...

    @abc.abstractmethod
    def get_all_cc_id_val_pairs(self, table, conn: Optional[sqlite3.Connection]=None):
        ...

    @abc.abstractmethod
    def get_all_cc_ids_marked_for_delete(self, conn=None) -> list[int]:
        ...

    @abc.abstractmethod
    def get_all_table_link_data(self, table1, table2, typed=False, priority=False):
        ...

    @abc.abstractmethod
    def get_cc_books_for_dirtying(self, table: str, link: str, id: int, conn: Optional[Any]=None) -> Iterable[str]:
        ...

    @abc.abstractmethod
    def get_cc_books_from_link_table(self, lt: str, lt_value: Any) -> Iterable[int]:
        ...

    @abc.abstractmethod
    def get_cc_id_and_value_from_id(self, custom_column: str, target_id: int, conn: Optional[sqlite3.Connection]=None) -> tuple[int, str]:
        ...

    @abc.abstractmethod
    def get_cc_id_from_value(self, target_table: str, cc_value: Union[str, int, datetime.datetime], all: bool=False, conn: Optional[sqlite3.Connection]=None) -> int:
        ...

    @abc.abstractmethod
    def get_cc_id_value_from_cc_id(self, table: str, old_id: int) -> tuple[int, str]:
        ...

    # Todo: This seems to be an interface weirdness
    @abc.abstractmethod
    def get_cc_lt_books_from_lt_value(
            self,
            lt: str,
            value: Union[str, int, datetime.datetime],
            conn: Optional[sqlite3.Connection]=None) -> Iterable[int]:
        ...

    @abc.abstractmethod
    def get_cc_series_index_indices(
            self,
            cc_series_link_table: str,
            series_id: int,
            conn: Optional[sqlite3.Connection] = None) -> tuple[Union[float, int], ...]:
        ...



    @abc.abstractmethod
    def get_dirtied_cache(self):
        ...

    @abc.abstractmethod
    def get_foreign_key_replacement_trigger(self, target_table, search_column='book', target_id='book_id', old=True):
        ...

    @abc.abstractmethod
    def get_link_data(self, table1, table2, table1_id, typed=False, priority=False):
        ...

    @abc.abstractmethod
    def get_linked_ids(self, link_table, left_id_col, right_id_col, left_id, type_filter=None):
        ...





    @abc.abstractmethod
    def get_unique_values(self, table, column):
        ...

    @abc.abstractmethod
    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        ...

    @abc.abstractmethod
    def hash_table(self, target_table: str, columns: Iterable[str]) -> str:
        ...

    @abc.abstractmethod
    def insert_multiple_values_into_cc_table(self, table, values, conn=None):
        ...

    @abc.abstractmethod
    def insert_values_into_temp_table(self, temp_table: str, values: Iterable[Any], conn: Any=None) -> None:
        ...





    @abc.abstractmethod
    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        ...

    @abc.abstractmethod
    def make_generic_link_no_priority(self, link_table, left_link_col, right_link_col, left_id=None, right_id=None, id_pairs=None):
        ...

    @abc.abstractmethod
    def mark_cc_for_delete(self, cc_column_id: int) -> None:
        ...

    @abc.abstractmethod
    def mark_custom_column_for_delete(self, num: int) -> None:
        ...

    @abc.abstractmethod
    def preform_cc_column_delete_from_map(self, num_table_lt_map: dict[int, tuple[str, str]], conn=None) -> None:
        ...








    @abc.abstractmethod
    def read_cc_value_from_meta_2(self, num: int, book_id: int, conn: Optional[sqlite3.Connection]=None) -> Iterable[Union[int, str, float]]:
        ...




    @abc.abstractmethod
    def read_link_property_trios(self, link_table, link_property_col, first_id, second_id):
        ...

    @abc.abstractmethod
    def replace_in_folder_path(self, target_str: str, replacement: str) -> None:
        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        ...

    @abc.abstractmethod
    def replace_in_folder_store_path(self, target_str: str, replacement: str) -> None:
        ...








    @abc.abstractmethod
    def repoint_cc_lt_values(self, lt, new_id, old_id):
        ...

    @abc.abstractmethod
    def reprioritize_link(self, link_table, left_link_col, right_link_col, left_id, right_id, new_type=None, new_priority='MAX'):
        ...



    @abc.abstractmethod
    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str]=None,
        label: Optional[str]=None,
        is_editable: Optional[bool]=None,
        display: Optional[str]=None,
        in_table: Optional[str]=None,
        conn=None,
    ) -> bool:
        ...

    @abc.abstractmethod
    def set_database_version(self, new_val):
        ...






    @abc.abstractmethod
    def set_library_id(self, new_val):
        ...








    @abc.abstractmethod
    def update_cc_lt_value_by_value(self, lt, new_value_id, old_value_id, conn=None):
        ...

    @abc.abstractmethod
    def update_cc_value(self, cc_column, cc_id, cc_value, conn=None):
        ...

    @abc.abstractmethod
    def update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        ...



    @abc.abstractmethod
    def update_custom_column_additional_column_many(self, table, column, sequence):
        ...
