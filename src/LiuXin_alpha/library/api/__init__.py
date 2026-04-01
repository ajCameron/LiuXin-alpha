from __future__ import annotations

import abc
from typing import Iterable, Optional

from LiuXin_alpha.utils.text.icu import lower as icu_lower


class DatabaseCacheAPI(abc.ABC):
    """API contract for cache objects tied to the database."""

    @abc.abstractmethod
    def __init__(self, backend):
        ...

    @abc.abstractmethod
    def _initialize_dynamic_categories(self) -> None:
        ...

    @abc.abstractmethod
    def add_books(self, books, add_duplicates=True, apply_import_tags=True, preserve_uuid=False, run_hooks=True, dbapi=None):
        ...

    @abc.abstractmethod
    def add_cover_cache(self, cover_cache) -> bool:
        ...

    @abc.abstractmethod
    def add_custom_book_data(self, name: str, val_map: dict[int, Any], delete_first: bool=False) -> bool:
        ...

    @abc.abstractmethod
    def add_format(self, book_id: int, fmt: str, stream_or_path: Union[bytes, BinaryIO], replace: bool=False, run_hooks: bool=True, dbapi=None) -> bool:
        ...

    @abc.abstractmethod
    def all_book_ids(self, rtn_type=frozenset):
        ...

    @abc.abstractmethod
    def all_field_for(self, field, book_ids, default_value=None):
        ...

    @abc.abstractmethod
    def all_field_ids(self, name: str) -> frozenset[int]:
        ...

    @abc.abstractmethod
    def all_field_names(self, field: str) -> frozenset[str]:
        ...

    @abc.abstractmethod
    def author_data(self, author_ids=None):
        ...

    @abc.abstractmethod
    def author_sort_from_authors(self, authors: Iterable[str], key_func: Callable[[str], str]=icu_lower) -> str:
        ...

    @abc.abstractmethod
    def author_sort_strings_for_books(self, book_ids: Iterable[int]) -> dict[int, tuple[str, ...]]:
        ...

    @abc.abstractmethod
    def book_formats(self, book_id: int) -> tuple[str, ...]:
        ...

    @abc.abstractmethod
    def books_for_field(self, name: str, item_id: int) -> set[int]:
        ...

    @abc.abstractmethod
    def books_in_virtual_library(self, vl, search_restriction=None) -> set[int]:
        ...

    @abc.abstractmethod
    def change_search_locations(self, newlocs):
        ...

    @abc.abstractmethod
    def clear_caches(self, book_ids=None, template_cache=True, search_cache=True):
        ...

    @abc.abstractmethod
    def clear_composite_caches(self, book_ids=None):
        ...

    @abc.abstractmethod
    def clear_dirtied(self, book_id: int, sequence):
        ...

    @abc.abstractmethod
    def clear_search_caches(self, book_ids=None):
        ...

    @abc.abstractmethod
    def close(self):
        ...

    @abc.abstractmethod
    def commit_dirty_cache(self) -> bool:
        ...

    @abc.abstractmethod
    def composite_for(self, name, book_id, mi=None, default_value=''):
        ...

    @abc.abstractmethod
    def conversion_options(self, book_id: int, fmt: str='PIPE'):
        ...

    @abc.abstractmethod
    def copy_cover_to(self, book_id: int, dest: Union[str, BinaryIO], use_hardlink: bool=False, report_file_size=None) -> bool:
        ...

    @abc.abstractmethod
    def copy_format_to(self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool=False, report_file_size=None) -> bool:
        ...

    @abc.abstractmethod
    def copy_formats_to(self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool=False, report_file_size=None):
        ...

    @abc.abstractmethod
    def cover(self, book_id: int, as_file: bool=False, as_image: bool=False, as_path: bool=False) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    def cover_last_modified(self, book_id: int) -> int:
        ...

    @abc.abstractmethod
    def cover_or_cache(self, book_id: int, timestamp: int) -> tuple[bool, bytes, int]:
        ...

    @abc.abstractmethod
    def create_book_entry(self, mi, cover=None, add_duplicates: bool=True, force_id: int=None, apply_import_tags: bool=True, preserve_uuid: bool=False):
        ...

    @abc.abstractmethod
    def create_custom_column(self, label: str, name: str, datatype, is_multiple: bool, editable: bool=True, display: Optional[str]=None) -> Union[int, Literal[False,]]:
        ...

    @abc.abstractmethod
    def data_for_find_identical_books(self):
        ...

    @abc.abstractmethod
    def data_for_has_book(self):
        ...

    @abc.abstractmethod
    def delete_conversion_options(self, book_ids, fmt: str='PIPE'):
        ...

    @abc.abstractmethod
    def delete_custom_book_data(self, name: str, book_ids: Iterable[int]=()) -> bool:
        ...

    @abc.abstractmethod
    def delete_custom_column(self, label: str=None, num: int=None) -> bool:
        ...

    @abc.abstractmethod
    def dirty_queue_length(self) -> int:
        ...

    @abc.abstractmethod
    def dump_and_restore(self, callback=None, sql=None):
        ...

    @abc.abstractmethod
    def dump_metadata(self, book_ids: Optional[Iterable[str]]=None, remove_from_dirtied: bool=True, callback=None) -> bool:
        ...

    @abc.abstractmethod
    def embed_metadata(self, book_ids: Iterable[int], only_fmts: Iterable[str]=None, report_error=None, report_progress=None) -> bool:
        ...

    @abc.abstractmethod
    def export_library(self, library_key, exporter, progress=None, abort=None):
        ...

    @abc.abstractmethod
    def fast_field_for(self, field_obj, book_id, default_value=None):
        ...

    @abc.abstractmethod
    def field_for(self, name, book_id, default_value=None):
        ...

    @abc.abstractmethod
    def field_ids_for(self, name: str, book_id: int) -> tuple[int]:
        ...

    @property
    @abc.abstractmethod
    def field_metadata(self):
        ...

    @abc.abstractmethod
    def find_identical_books(self, mi, search_restriction='', book_ids=None):
        ...

    @abc.abstractmethod
    def format(self, book_id: int, fmt: str, as_file: bool=False, as_path: str=False, preserve_filename: bool=False) -> bytes:
        ...

    @abc.abstractmethod
    def format_abspath(self, book_id, fmt):
        ...

    @abc.abstractmethod
    def format_files(self, book_id):
        ...

    @abc.abstractmethod
    def format_hash(self, book_id, fmt):
        ...

    @abc.abstractmethod
    def format_metadata(self, book_id, fmt, allow_cache=True, update_db=False):
        ...

    @abc.abstractmethod
    def formats(self, book_id, verify_formats=True):
        ...

    @abc.abstractmethod
    def get_a_dirtied_book(self) -> int:
        ...

    @abc.abstractmethod
    def get_books_for_category(self, category, item_id_or_composite_value):
        ...

    @abc.abstractmethod
    def get_categories(self, sort: str='name', book_ids: Iterable[int]=None, already_fixed=None, first_letter_sort: bool=False):
        ...

    @abc.abstractmethod
    def get_custom_book_data(self, name: str, book_ids: Iterable[int]=(), default: Optional[Any]=None) -> dict[int, Any]:
        ...

    @abc.abstractmethod
    def get_id_map(self, field: str) -> dict[int, str]:
        ...

    @abc.abstractmethod
    def get_ids_for_custom_book_data(self, name: str) -> set[int]:
        ...

    @abc.abstractmethod
    def get_item_id(self, field: str, item_name: str) -> int:
        ...

    @abc.abstractmethod
    def get_item_ids(self, field: str, item_names: Iterable[str]) -> dict[str, int]:
        ...

    @abc.abstractmethod
    def get_item_name(self, field: str, item_id: int) -> str:
        ...

    @abc.abstractmethod
    def get_last_read_positions(self, book_id: int, fmt: str, user: str) -> bool:
        ...

    @abc.abstractmethod
    def get_metadata(self, book_id: int, get_cover: bool=False, get_user_categories: bool=True, cover_as_data: bool=False):
        ...

    @abc.abstractmethod
    def get_metadata_for_dump(self, book_id):
        ...

    @abc.abstractmethod
    def get_next_series_num_for(self, series, field='series', current_indices=False):
        ...

    @abc.abstractmethod
    def get_proxy_metadata(self, book_id: str):
        ...

    @abc.abstractmethod
    def get_top_level_move_items(self):
        ...

    @abc.abstractmethod
    def get_usage_count_by_id(self, field: str) -> dict[int, int]:
        ...

    @abc.abstractmethod
    def has_book(self, mi) -> bool:
        ...

    @abc.abstractmethod
    def has_conversion_options(self, book_ids: Iterable[int], fmt: str='PIPE'):
        ...

    @abc.abstractmethod
    def has_format(self, book_id: int, fmt: str) -> bool:
        ...

    @abc.abstractmethod
    def has_id(self, book_id: int) -> bool:
        ...

    @abc.abstractmethod
    def init(self):
        ...

    @abc.abstractmethod
    def initialize_custom_columns(self) -> None:
        ...

    @abc.abstractmethod
    def initialize_dynamic(self):
        ...

    @abc.abstractmethod
    def initialize_tables(self) -> None:
        ...

    @abc.abstractmethod
    def initialize_template_cache(self):
        ...

    @abc.abstractmethod
    def last_modified(self):
        ...

    @property
    @abc.abstractmethod
    def library_id(self):
        ...

    @abc.abstractmethod
    def lookup_by_uuid(self, uuid: str) -> int:
        ...

    @abc.abstractmethod
    def mark_as_dirty(self, book_ids: Iterable[int]) -> bool:
        ...

    @abc.abstractmethod
    def move_library_to(self, newloc, progress=None, abort=None):
        ...

    @abc.abstractmethod
    def multisort(self, fields, ids_to_sort=None, virtual_fields=None):
        ...

    @property
    @abc.abstractmethod
    def new_api(self):
        ...

    @abc.abstractmethod
    def pref(self, name: str, default: Optional[T]=None) -> T:
        ...

    @abc.abstractmethod
    def read_backup(self, book_id):
        ...

    @abc.abstractmethod
    def read_tables(self) -> None:
        ...

    @abc.abstractmethod
    def refresh_format_cache(self):
        ...

    @abc.abstractmethod
    def refresh_ondevice(self):
        ...

    @abc.abstractmethod
    def refresh_search_locations(self):
        ...

    @abc.abstractmethod
    def reload_from_db(self, clear_caches=True):
        ...

    @abc.abstractmethod
    def remove_books(self, book_ids: Iterable[int], permanent: bool=False):
        ...

    @abc.abstractmethod
    def remove_cover_cache(self, cover_cache) -> bool:
        ...

    @abc.abstractmethod
    def remove_formats(self, formats_map: dict[int, str], db_only: bool=False) -> bool:
        ...

    @abc.abstractmethod
    def remove_items(self, field: str, item_ids: Iterable[str], restrict_to_book_ids: set[int]=None):
        ...

    @abc.abstractmethod
    def rename_items(self, field: str, item_id_to_new_name_map: dict[int, str], change_index: bool=True, restrict_to_book_ids: Optional[set[int]]=None):
        ...

    @abc.abstractmethod
    def restore_book(self, book_id, mi, last_modified, path, formats):
        ...

    @abc.abstractmethod
    def restore_original_format(self, book_id: int, original_fmt: str) -> bool:
        ...

    @property
    @abc.abstractmethod
    def safe_read_lock(self):
        ...

    @abc.abstractmethod
    def save_original_format(self, book_id: int, fmt: str) -> bool:
        ...

    @abc.abstractmethod
    def saved_search_add(self, name: str, val):
        ...

    @abc.abstractmethod
    def saved_search_delete(self, name: str) -> None:
        ...

    @abc.abstractmethod
    def saved_search_lookup(self, name: str):
        ...

    @abc.abstractmethod
    def saved_search_names(self) -> list[str]:
        ...

    @abc.abstractmethod
    def saved_search_rename(self, old_name, new_name):
        ...

    @abc.abstractmethod
    def saved_search_set_all(self, smap):
        ...

    @abc.abstractmethod
    def search(self, query, restriction='', virtual_fields=None, book_ids=None):
        ...

    @abc.abstractmethod
    def set_conversion_options(self, options, fmt='PIPE'):
        ...

    @abc.abstractmethod
    def set_cover(self, book_id_data_map: dict[int:Optional[Union[str, bytes]]]) -> bool:
        ...

    @abc.abstractmethod
    def set_custom_column_metadata(self, num: int, name: Optional[str]=None, label: Optional[str]=None, is_editable: Optional[bool]=None, display: Optional[str]=None, update_last_modified: bool=False) -> bool:
        ...

    @abc.abstractmethod
    def set_field(self, name: str, book_id_to_val_map: dict[int, str], allow_case_change: bool=True, do_path_update: bool=True) -> set[int]:
        ...

    @abc.abstractmethod
    def set_last_read_position(self, book_id, fmt, user='_', device='_', cfi=None, epoch=None, pos_frac=0):
        ...

    @abc.abstractmethod
    def set_link_for_authors(self, author_id_to_link_map: dict[int, str]) -> set[int]:
        ...

    @abc.abstractmethod
    def set_metadata(self, book_id: int, mi, ignore_errors=False, force_changes=False, set_title=True, set_authors=True, allow_case_change=False):
        ...

    @abc.abstractmethod
    def set_pref(self, name: str, val: Any) -> None:
        ...

    @abc.abstractmethod
    def set_sort_for_authors(self, author_id_to_sort_map: dict[int, str], update_books: bool=True) -> set[int]:
        ...

    @abc.abstractmethod
    def set_user_template_functions(self, user_template_functions):
        ...

    @abc.abstractmethod
    def tags_older_than(self, tag: str, delta=None, must_have_tag: Optional[Iterable[str]]=None, must_have_authors=None):
        ...

    @abc.abstractmethod
    def update_data_for_find_identical_books(self, book_id, data):
        ...

    @abc.abstractmethod
    def update_last_modified(self, book_ids, now=None):
        ...

    @abc.abstractmethod
    def update_path(self, book_ids: Iterable[int], mark_as_dirtied: bool=True) -> bool:
        ...

    @abc.abstractmethod
    def user_categories_for_books(self, book_ids, proxy_metadata_map=None):
        ...

    @abc.abstractmethod
    def vacuum(self) -> bool:
        ...

    @abc.abstractmethod
    def virtual_libraries_for_books(self, book_ids: Iterable[int]):
        ...

    @abc.abstractmethod
    def write_backup(self, book_id, raw):
        ...
