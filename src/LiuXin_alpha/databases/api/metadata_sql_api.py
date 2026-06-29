
"""
Metadata-aware SQL API contract.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


# Todo: This is not a great API. It needs parameters.
class MetadataSQLAPI(abc.ABC):
    """
    API for SQL operations that know about LiuXin metadata tables.
    """

    @abc.abstractmethod
    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the SQL based metadata API.

        :param db:
        """

    @property
    @abc.abstractmethod
    def get(self):
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
    def add_creator_tag_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def add_feed(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def add_series_tag_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def add_tag(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def add_tag_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def add_title_identifier(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_creator_tag_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_creator_title_links(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_generic_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_generic_single_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_lang_title_links(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_lang_title_primary_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_series_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def break_tag_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_creator_tag_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_series_tag_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_series_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_tag_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_title_author_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def check_for_title_id_publisher_id_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_creator_tag_links_for_creator(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_null_publisher_links_from_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_publisher_title_links_by_title_id(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_series_tag_links_for_series(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_tag_title_links_for_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_title_comments_from_title_id(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def clear_title_creator_links_for_given_type_and_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def creator_clear_unused(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_book(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_conversion_options(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_feed(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_file_by_id(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_files_by_id(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_item_by_id(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_tag_by_value(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def delete_title_identifiers(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_creator_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_creator_sort(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_primary_series_index(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_series_id_from_value(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_tag_id_from_value(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def get_title_series_ids_set(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def library_unset_series(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def link_null_series_to_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def link_publisher_to_null_publisher_row(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def make_creator_title_links(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def publisher_clear_unused(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_all_identifiers(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_book_id_with_cover_id_and_cover_nmame(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_book_id_with_file_id_file_ext_file_name_and_file_size(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_book_sizes_max_mode(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_book_sizes_min_mode(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_book_sizes_sum_mode(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_creator_with_sort_and_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_file_backups_for_book(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_file_properties_for_book(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def read_primary_title_series_id_from_meta(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def remove_unused_series(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def replace_in_cover_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def replace_in_file_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def replace_in_folder_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def replace_in_folder_store_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_author_sort(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_conversion_options(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_feeds(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_file_name(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_file_size(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_file_size_and_name(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_has_cover(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_override_book_path(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_title_identifier(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_title_isbn(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_title_primary_language(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def set_title_rating(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def unapply_series_tags(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_book_last_modified(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_creator_links(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_creator_sorts(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_feed(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_index_for_series_title_link(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_title(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_title_author_link_priority(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    def update_title_creator_sort(self, *args, **kwargs):
        ...
