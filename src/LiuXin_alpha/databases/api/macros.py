"""Macros API contract."""

from __future__ import annotations

import abc
import sqlite3

from typing import Optional


class MacrosAPI(abc.ABC):
    """Abstract API for SQL macros implementations."""

    @abc.abstractmethod
    def __init__(self, db):
        ...

    @staticmethod
    @abc.abstractmethod
    def _cc_table_col_mapper(self, table: str) -> str:
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
    def add_creator_tag_link(self, creator_id, tag_id):
        ...

    @abc.abstractmethod
    def add_feed(self, title, script):
        ...

    @abc.abstractmethod
    def add_series_tag_link(self, series_id, tag_id):
        ...

    @abc.abstractmethod
    def add_tag(self, tag_value):
        ...

    @abc.abstractmethod
    def add_tag_title_link(self, title_id, tag_id):
        ...

    @abc.abstractmethod
    def add_title_identifier(self, title_id, id_type, id_val):
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
    def break_creator_tag_link(self, tag_id, creator_id):
        ...

    @abc.abstractmethod
    def break_creator_title_links(self, title_id, creator_type=('author', 'authors')):
        ...

    @abc.abstractmethod
    def break_generic_link(self, link_table, link_col, remove_id, link_type=None):
        ...

    @abc.abstractmethod
    def break_generic_single_link(self, link_table, left_link_col, right_link_col, left_id, right_id):
        ...

    @abc.abstractmethod
    def break_lang_title_links(self, title_id, link_type=None):
        ...

    @abc.abstractmethod
    def break_lang_title_primary_link(self, title_id):
        ...

    @abc.abstractmethod
    def break_series_title_link(self, title_id, series_id=0):
        ...

    @abc.abstractmethod
    def break_tag_title_link(self, tag_id, title_id):
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
    def check_for_creator_tag_link(self, creator_id, tag_id):
        ...

    @abc.abstractmethod
    def check_for_series_tag_link(self, series_id, tag_id):
        ...

    @abc.abstractmethod
    def check_for_series_title_link(self, series_id, title_id):
        ...

    @abc.abstractmethod
    def check_for_tag_title_link(self, title_id, tag_id):
        ...

    @abc.abstractmethod
    def check_for_title_author_link(self, title_id, creator_id):
        ...

    @abc.abstractmethod
    def check_for_title_id_publisher_id_link(self, pub_id, title_id):
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
    def clear_creator_tag_links_for_creator(self, creator_id):
        ...

    @abc.abstractmethod
    def clear_null_publisher_links_from_title(self, title_id):
        ...

    @abc.abstractmethod
    def clear_publisher_title_links_by_title_id(self, title_id):
        ...

    @abc.abstractmethod
    def clear_series_tag_links_for_series(self, series_id):
        ...

    @abc.abstractmethod
    def clear_tag_title_links_for_title(self, title_id):
        ...

    @abc.abstractmethod
    def clear_title_comments_from_title_id(self, title_id):
        ...

    @abc.abstractmethod
    def clear_title_creator_links_for_given_type_and_title(self, title_id):
        ...

    @abc.abstractmethod
    def create_cc_table(self, normalized, datatype, dt, table, link_table, collate, in_table='books', ordered=False, conn=None):
        ...

    @abc.abstractmethod
    def create_cc_temp_tables(self, temp_tables, conn=None):
        ...

    @abc.abstractmethod
    def creator_clear_unused(self):
        ...

    @abc.abstractmethod
    def delete_book(self, book_id):
        ...

    @abc.abstractmethod
    def delete_cc_item(self, table, lt, target_id, conn=None):
        ...

    @abc.abstractmethod
    def delete_conversion_options(self, book_id, fmt, commit=True):
        ...

    @abc.abstractmethod
    def delete_feed(self, feed_id):
        ...

    @abc.abstractmethod
    def delete_file_by_id(self, file_id):
        ...

    @abc.abstractmethod
    def delete_files_by_id(self, file_ids):
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
    def delete_item_by_id(self, item_table, item_id_col, item_id):
        ...

    @abc.abstractmethod
    def delete_tag_by_value(self, tag):
        ...

    @abc.abstractmethod
    def delete_title(self, title_id):
        ...

    @abc.abstractmethod
    def delete_title_identifiers(self, title_id, id_type=None):
        ...

    @abc.abstractmethod
    def destroy_cc_temp_tables(self, temp_tables, conn=None):
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
    def do_custom_column_delete_by_id(self, cc_id):
        ...

    @abc.abstractmethod
    def do_custom_column_delete_by_num(self, num):
        ...

    @abc.abstractmethod
    def ensure_custom_column_value(self, cc_table, value):
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
    def get_all_cc_ids_marked_for_delete(self, conn=None):
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

    @abc.abstractmethod
    def get_cc_lt_books_from_lt_value(self, lt: str, value: Union[str, int, datetime.datetime], conn: Optional[sqlite3.Connection]=None) -> Iterable[int]:
        ...

    @abc.abstractmethod
    def get_cc_series_index_indices(self, cc_series_link_table: str, series_id: int, conn: Optional[sqlite3.Connection]=None) -> tuple[Union[float, int], ...]:
        ...

    @abc.abstractmethod
    def get_creator_link(self, creator_id):
        ...

    @abc.abstractmethod
    def get_creator_sort(self, creator_id):
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
    def get_primary_series_index(self, title_id):
        ...

    @abc.abstractmethod
    def get_series_id_from_value(self, series):
        ...

    @abc.abstractmethod
    def get_tag_id_from_value(self, tag):
        ...

    @abc.abstractmethod
    def get_title_series_ids_set(self, title_id):
        ...

    @abc.abstractmethod
    def get_unique_values(self, table, column):
        ...

    @abc.abstractmethod
    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        ...

    @abc.abstractmethod
    def hash_table(self, target_table, columns):
        ...

    @abc.abstractmethod
    def insert_multiple_values_into_cc_table(self, table, values, conn=None):
        ...

    @abc.abstractmethod
    def insert_values_into_temp_table(self, temp_table, values, conn=None):
        ...

    @abc.abstractmethod
    def library_unset_series(self, title_id, series_id):
        ...

    @abc.abstractmethod
    def link_null_series_to_title(self, title_id, series_index):
        ...

    @abc.abstractmethod
    def link_publisher_to_null_publisher_row(self, title_id):
        ...

    @abc.abstractmethod
    def make_creator_title_links(self, title_id=None, creator_id=None, id_pairs=None, creator_type='authors'):
        ...

    @abc.abstractmethod
    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        ...

    @abc.abstractmethod
    def make_generic_link_no_priority(self, link_table, left_link_col, right_link_col, left_id=None, right_id=None, id_pairs=None):
        ...

    @abc.abstractmethod
    def mark_cc_for_delete(self, cc_column_id):
        ...

    @abc.abstractmethod
    def mark_custom_column_for_delete(self, num):
        ...

    @abc.abstractmethod
    def preform_cc_column_delete_from_map(self, num_table_lt_map, conn=None):
        ...

    @abc.abstractmethod
    def publisher_clear_unused(self):
        ...

    @abc.abstractmethod
    def read_all_identifiers(self):
        ...

    @abc.abstractmethod
    def read_book_id_with_cover_id_and_cover_nmame(self):
        ...

    @abc.abstractmethod
    def read_book_id_with_file_id_file_ext_file_name_and_file_size(self):
        ...

    @abc.abstractmethod
    def read_book_sizes_max_mode(self):
        ...

    @abc.abstractmethod
    def read_book_sizes_min_mode(self):
        ...

    @abc.abstractmethod
    def read_book_sizes_sum_mode(self):
        ...

    @abc.abstractmethod
    def read_cc_value_from_meta_2(self, num: int, book_id: int, conn: Optional[sqlite3.Connection]=None) -> Iterable[Union[int, str, float]]:
        ...

    @abc.abstractmethod
    def read_creator_with_sort_and_link(self):
        ...

    @abc.abstractmethod
    def read_file_backups_for_book(self, book_id):
        ...

    @abc.abstractmethod
    def read_file_properties_for_book(self, book_id):
        ...

    @abc.abstractmethod
    def read_link_property_trios(self, link_table, link_property_col, first_id, second_id):
        ...

    @abc.abstractmethod
    def read_primary_title_series_id_from_meta(self, title_id):
        ...

    @abc.abstractmethod
    def remove_unused_series(self):
        ...

    @abc.abstractmethod
    def replace_in_cover_path(self, target_str, replacement):
        ...

    @abc.abstractmethod
    def replace_in_file_path(self, target_str, replacement):
        ...

    @abc.abstractmethod
    def replace_in_folder_path(self, target_str, replacement):
        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(self, target_str, replacement):
        ...

    @abc.abstractmethod
    def replace_in_folder_store_path(self, target_str, replacement):
        ...

    @abc.abstractmethod
    def repoint_cc_lt_values(self, lt, new_id, old_id):
        ...

    @abc.abstractmethod
    def reprioritize_link(self, link_table, left_link_col, right_link_col, left_id, right_id, new_type=None, new_priority='MAX'):
        ...

    @abc.abstractmethod
    def set_author_sort(self, title_id, sort):
        ...

    @abc.abstractmethod
    def set_conversion_options(self, book_id, fmt, options):
        ...

    @abc.abstractmethod
    def set_custom_column_metadata(self, num, name=None, label=None, is_editable=None, display=None, in_table=None, conn=None):
        ...

    @abc.abstractmethod
    def set_database_version(self, new_val):
        ...

    @abc.abstractmethod
    def set_feeds(self, feeds):
        ...

    @abc.abstractmethod
    def set_file_name(self, file_id, new_fname):
        ...

    @abc.abstractmethod
    def set_file_size(self, file_id, size):
        ...

    @abc.abstractmethod
    def set_file_size_and_name(self, file_id, size, fname):
        ...

    @abc.abstractmethod
    def set_has_cover(self, book_id, value):
        ...

    @abc.abstractmethod
    def set_library_id(self, new_val):
        ...

    @abc.abstractmethod
    def set_override_book_path(self, book_id, path):
        ...

    @abc.abstractmethod
    def set_title_identifier(self, title_id, id_type, id_val):
        ...

    @abc.abstractmethod
    def set_title_isbn(self, title_id, isbn):
        ...

    @abc.abstractmethod
    def set_title_primary_language(self, title_id, lang_id):
        ...

    @abc.abstractmethod
    def set_title_rating(self, title_id, rating):
        ...

    @abc.abstractmethod
    def unapply_series_tags(self, series_id, tags):
        ...

    @abc.abstractmethod
    def update_book_last_modified(self, book_id, last_modified):
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
    def update_creator_links(self, values):
        ...

    @abc.abstractmethod
    def update_creator_sorts(self, values):
        ...

    @abc.abstractmethod
    def update_custom_column_additional_column_many(self, table, column, sequence):
        ...

    @abc.abstractmethod
    def update_feed(self, feed_id, script, title):
        ...

    @abc.abstractmethod
    def update_index_for_series_title_link(self, title_id, series_id, index):
        ...

    @abc.abstractmethod
    def update_title(self, title_id, title):
        ...

    @abc.abstractmethod
    def update_title_author_link_priority(self, title_id, creator_id, new_priority):
        ...

    @abc.abstractmethod
    def update_title_creator_sort(self, title_id, creator_val):
        ...
