
"""
Expose the supported api compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import annotations

import abc
from typing import Iterable, Optional, Union, Any

from LiuXin_alpha.utils.text.icu import lower as icu_lower


class DatabaseCacheAPI(abc.ABC):
    """
    API contract for cache objects tied to the database.

    Example:
        Exercise DatabaseCacheAPI through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    @abc.abstractmethod
    def __init__(self, backend):
        """
        Initialize and validate the databasecacheapi state.

        Example:
            Exercise DatabaseCacheAPI.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param backend: Value supplied for backend under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        ...

    @abc.abstractmethod
    def _initialize_dynamic_categories(self) -> None:
        """
        Perform the initialize dynamic categories operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI. initialize dynamic categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def add_books(self, books, add_duplicates=True, apply_import_tags=True, preserve_uuid=False, run_hooks=True, dbapi=None):
        """
        Perform the add books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.add books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param books: Value supplied for books under the utility contract.
        :param add_duplicates: Value supplied for add duplicates under the utility contract.
        :param apply_import_tags: Value supplied for apply import tags under the utility
            contract.
        :param preserve_uuid: Value supplied for preserve uuid under the utility contract.
        :param run_hooks: Value supplied for run hooks under the utility contract.
        :param dbapi: Value supplied for dbapi under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def add_cover_cache(self, cover_cache) -> bool:
        """
        Perform the add cover cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.add cover cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param cover_cache: Value supplied for cover cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def add_custom_book_data(self, name: str, val_map: dict[int, Any], delete_first: bool=False) -> bool:
        """
        Perform the add custom book data operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.add custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val_map: Value supplied for val map under the utility contract.
        :param delete_first: Value supplied for delete first under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def add_format(self, book_id: int, fmt: str, stream_or_path: Union[bytes, BinaryIO], replace: bool=False, run_hooks: bool=True, dbapi=None) -> bool:
        """
        Perform the add format operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.add format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param stream_or_path: Value supplied for stream or path under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :param run_hooks: Value supplied for run hooks under the utility contract.
        :param dbapi: Value supplied for dbapi under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def all_book_ids(self, rtn_type=frozenset):
        """
        Perform the all book ids operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.all book ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param rtn_type: Value supplied for rtn type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def all_field_for(self, field, book_ids, default_value=None):
        """
        Perform the all field for operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.all field for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def all_field_ids(self, name: str) -> frozenset[int]:
        """
        Perform the all field ids operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.all field ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def all_field_names(self, field: str) -> frozenset[str]:
        """
        Perform the all field names operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.all field names through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def author_data(self, author_ids=None):
        """
        Perform the author data operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.author data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param author_ids: Value supplied for author ids under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def author_sort_from_authors(self, authors: Iterable[str], key_func: Callable[[str], str]=icu_lower) -> str:
        """
        Perform the author sort from authors operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.author sort from authors through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param authors: Value supplied for authors under the utility contract.
        :param key_func: Value supplied for key func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def author_sort_strings_for_books(self, book_ids: Iterable[int]) -> dict[int, tuple[str, ...]]:
        """
        Perform the author sort strings for books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.author sort strings for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def book_formats(self, book_id: int) -> tuple[str, ...]:
        """
        Perform the book formats operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.book formats through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def books_for_field(self, name: str, item_id: int) -> set[int]:
        """
        Perform the books for field operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.books for field through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def books_in_virtual_library(self, vl, search_restriction=None) -> set[int]:
        """
        Perform the books in virtual library operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.books in virtual library through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param vl: Value supplied for vl under the utility contract.
        :param search_restriction: Value supplied for search restriction under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def change_search_locations(self, newlocs):
        """
        Perform the change search locations operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.change search locations through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param newlocs: Value supplied for newlocs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def clear_caches(self, book_ids=None, template_cache=True, search_cache=True):
        """
        Perform the clear caches operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param template_cache: Value supplied for template cache under the utility contract.
        :param search_cache: Value supplied for search cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def clear_composite_caches(self, book_ids=None):
        """
        Perform the clear composite caches operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.clear composite caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def clear_dirtied(self, book_id: int, sequence):
        """
        Perform the clear dirtied operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.clear dirtied through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param sequence: Value supplied for sequence under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def clear_search_caches(self, book_ids=None):
        """
        Perform the clear search caches operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.clear search caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def close(self):
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.close through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def commit_dirty_cache(self) -> bool:
        """
        Perform the commit dirty cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.commit dirty cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def composite_for(self, name, book_id, mi=None, default_value=''):
        """
        Perform the composite for operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.composite for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def conversion_options(self, book_id: int, fmt: str='PIPE'):
        """
        Perform the conversion options operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def copy_cover_to(self, book_id: int, dest: Union[str, BinaryIO], use_hardlink: bool=False, report_file_size=None) -> bool:
        """
        Perform the copy cover to operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.copy cover to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def copy_format_to(self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool=False, report_file_size=None) -> bool:
        """
        Perform the copy format to operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.copy format to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def copy_formats_to(self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool=False, report_file_size=None):
        """
        Perform the copy formats to operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.copy formats to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def cover(self, book_id: int, as_file: bool=False, as_image: bool=False, as_path: bool=False) -> Optional[bytes]:
        """
        Perform the cover operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.cover through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param as_file: Value supplied for as file under the utility contract.
        :param as_image: Value supplied for as image under the utility contract.
        :param as_path: Value supplied for as path under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def cover_last_modified(self, book_id: int) -> int:
        """
        Perform the cover last modified operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.cover last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def cover_or_cache(self, book_id: int, timestamp: int) -> tuple[bool, bytes, int]:
        """
        Perform the cover or cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.cover or cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param timestamp: Value supplied for timestamp under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def create_book_entry(self, mi, cover=None, add_duplicates: bool=True, force_id: int=None, apply_import_tags: bool=True, preserve_uuid: bool=False):
        """
        Create book entry under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.create book entry through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :param cover: Value supplied for cover under the utility contract.
        :param add_duplicates: Value supplied for add duplicates under the utility contract.
        :param force_id: Value supplied for force id under the utility contract.
        :param apply_import_tags: Value supplied for apply import tags under the utility
            contract.
        :param preserve_uuid: Value supplied for preserve uuid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def create_custom_column(self, label: str, name: str, datatype, is_multiple: bool, editable: bool=True, display: Optional[str]=None) -> Union[int, Literal[False,]]:
        """
        Create custom column under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.create custom column through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param label: Value supplied for label under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param datatype: Value supplied for datatype under the utility contract.
        :param is_multiple: Value supplied for is multiple under the utility contract.
        :param editable: Value supplied for editable under the utility contract.
        :param display: Value supplied for display under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def data_for_find_identical_books(self):
        """
        Perform the data for find identical books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.data for find identical books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def data_for_has_book(self):
        """
        Perform the data for has book operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.data for has book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def delete_conversion_options(self, book_ids, fmt: str='PIPE'):
        """
        Perform the delete conversion options operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.delete conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def delete_custom_book_data(self, name: str, book_ids: Iterable[int]=()) -> bool:
        """
        Perform the delete custom book data operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.delete custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def delete_custom_column(self, label: str=None, num: int=None) -> bool:
        """
        Perform the delete custom column operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.delete custom column through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def dirty_queue_length(self) -> int:
        """
        Perform the dirty queue length operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.dirty queue length through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def dump_and_restore(self, callback=None, sql=None):
        """
        Perform the dump and restore operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.dump and restore through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param callback: Value supplied for callback under the utility contract.
        :param sql: Value supplied for sql under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def dump_metadata(self, book_ids: Optional[Iterable[str]]=None, remove_from_dirtied: bool=True, callback=None) -> bool:
        """
        Perform the dump metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.dump metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param remove_from_dirtied: Value supplied for remove from dirtied under the utility
            contract.
        :param callback: Value supplied for callback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def embed_metadata(self, book_ids: Iterable[int], only_fmts: Iterable[str]=None, report_error=None, report_progress=None) -> bool:
        """
        Perform the embed metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.embed metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param only_fmts: Value supplied for only fmts under the utility contract.
        :param report_error: Value supplied for report error under the utility contract.
        :param report_progress: Value supplied for report progress under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def export_library(self, library_key, exporter, progress=None, abort=None):
        """
        Perform the export library operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.export library through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param library_key: Value supplied for library key under the utility contract.
        :param exporter: Value supplied for exporter under the utility contract.
        :param progress: Value supplied for progress under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def fast_field_for(self, field_obj, book_id, default_value=None):
        """
        Perform the fast field for operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.fast field for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field_obj: Value supplied for field obj under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def field_for(self, name, book_id, default_value=None):
        """
        Perform the field for operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.field for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def field_ids_for(self, name: str, book_id: int) -> tuple[int]:
        """
        Perform the field ids for operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.field ids for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @property
    @abc.abstractmethod
    def field_metadata(self):
        """
        Perform the field metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.field metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def find_identical_books(self, mi, search_restriction='', book_ids=None):
        """
        Find identical books under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.find identical books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :param search_restriction: Value supplied for search restriction under the utility
            contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def format(self, book_id: int, fmt: str, as_file: bool=False, as_path: str=False, preserve_filename: bool=False) -> bytes:
        """
        Perform the format operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param as_file: Value supplied for as file under the utility contract.
        :param as_path: Value supplied for as path under the utility contract.
        :param preserve_filename: Value supplied for preserve filename under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def format_abspath(self, book_id, fmt):
        """
        Perform the format abspath operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.format abspath through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def format_files(self, book_id):
        """
        Perform the format files operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.format files through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def format_hash(self, book_id, fmt):
        """
        Perform the format hash operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.format hash through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def format_metadata(self, book_id, fmt, allow_cache=True, update_db=False):
        """
        Perform the format metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.format metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param allow_cache: Value supplied for allow cache under the utility contract.
        :param update_db: Value supplied for update db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def formats(self, book_id, verify_formats=True):
        """
        Perform the formats operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.formats through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param verify_formats: Value supplied for verify formats under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_a_dirtied_book(self) -> int:
        """
        Return a dirtied book under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get a dirtied book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_books_for_category(self, category, item_id_or_composite_value):
        """
        Return books for category under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get books for category through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param category: Value supplied for category under the utility contract.
        :param item_id_or_composite_value: Value supplied for item id or composite value
            under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_categories(self, sort: str='name', book_ids: Iterable[int]=None, already_fixed=None, first_letter_sort: bool=False):
        """
        Return categories under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param sort: Value supplied for sort under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param already_fixed: Value supplied for already fixed under the utility contract.
        :param first_letter_sort: Value supplied for first letter sort under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_custom_book_data(self, name: str, book_ids: Iterable[int]=(), default: Optional[Any]=None) -> dict[int, Any]:
        """
        Return custom book data under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :param default: Value supplied for default under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_id_map(self, field: str) -> dict[int, str]:
        """
        Return id map under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get id map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_ids_for_custom_book_data(self, name: str) -> set[int]:
        """
        Return ids for custom book data under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get ids for custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_item_id(self, field: str, item_name: str) -> int:
        """
        Return item id under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get item id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param item_name: Value supplied for item name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_item_ids(self, field: str, item_names: Iterable[str]) -> dict[str, int]:
        """
        Return item ids under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get item ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param item_names: Value supplied for item names under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_item_name(self, field: str, item_id: int) -> str:
        """
        Return item name under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get item name through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_last_read_positions(self, book_id: int, fmt: str, user: str) -> bool:
        """
        Return last read positions under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get last read positions through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param user: Value supplied for user under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_metadata(self, book_id: int, get_cover: bool=False, get_user_categories: bool=True, cover_as_data: bool=False):
        """
        Return normalized metadata parsed from the supplied document.

        Example:
            Exercise DatabaseCacheAPI.get metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param get_cover: Value supplied for get cover under the utility contract.
        :param get_user_categories: Value supplied for get user categories under the utility
            contract.
        :param cover_as_data: Value supplied for cover as data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_metadata_for_dump(self, book_id):
        """
        Return metadata for dump under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get metadata for dump through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_next_series_num_for(self, series, field='series', current_indices=False):
        """
        Return next series num for under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get next series num for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param series: Value supplied for series under the utility contract.
        :param field: Metadata or template field addressed by the operation.
        :param current_indices: Value supplied for current indices under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_proxy_metadata(self, book_id: str):
        """
        Return proxy metadata under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get proxy metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_top_level_move_items(self):
        """
        Return top level move items under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get top level move items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def get_usage_count_by_id(self, field: str) -> dict[int, int]:
        """
        Return usage count by id under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.get usage count by id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def has_book(self, mi) -> bool:
        """
        Return whether has book holds for the supplied ebook data.

        Example:
            Exercise DatabaseCacheAPI.has book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :return: True when the documented condition holds; otherwise False.
        """
        ...

    @abc.abstractmethod
    def has_conversion_options(self, book_ids: Iterable[int], fmt: str='PIPE'):
        """
        Return whether has conversion options holds for the supplied ebook data.

        Example:
            Exercise DatabaseCacheAPI.has conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        ...

    @abc.abstractmethod
    def has_format(self, book_id: int, fmt: str) -> bool:
        """
        Return whether has format holds for the supplied ebook data.

        Example:
            Exercise DatabaseCacheAPI.has format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        ...

    @abc.abstractmethod
    def has_id(self, book_id: int) -> bool:
        """
        Return whether has id holds for the supplied ebook data.

        Example:
            Exercise DatabaseCacheAPI.has id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        ...

    @abc.abstractmethod
    def init(self):
        """
        Perform the init operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.init through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def initialize_custom_columns(self) -> None:
        """
        Perform the initialize custom columns operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.initialize custom columns through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def initialize_dynamic(self):
        """
        Perform the initialize dynamic operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.initialize dynamic through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def initialize_tables(self) -> None:
        """
        Perform the initialize tables operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.initialize tables through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def initialize_template_cache(self):
        """
        Perform the initialize template cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.initialize template cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def last_modified(self):
        """
        Perform the last modified operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @property
    @abc.abstractmethod
    def library_id(self):
        """
        Perform the library id operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.library id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def lookup_by_uuid(self, uuid: str) -> int:
        """
        Perform the lookup by uuid operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.lookup by uuid through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param uuid: Value supplied for uuid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def mark_as_dirty(self, book_ids: Iterable[int]) -> bool:
        """
        Perform the mark as dirty operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.mark as dirty through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def move_library_to(self, newloc, progress=None, abort=None):
        """
        Perform the move library to operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.move library to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param newloc: Value supplied for newloc under the utility contract.
        :param progress: Value supplied for progress under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def multisort(self, fields, ids_to_sort=None, virtual_fields=None):
        """
        Perform the multisort operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.multisort through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param fields: Value supplied for fields under the utility contract.
        :param ids_to_sort: Value supplied for ids to sort under the utility contract.
        :param virtual_fields: Value supplied for virtual fields under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @property
    @abc.abstractmethod
    def new_api(self):
        """
        Perform the new api operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.new api through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def pref(self, name: str, default: Optional[T]=None) -> T:
        """
        Perform the pref operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.pref through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def read_backup(self, book_id):
        """
        Read backup under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.read backup through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def read_tables(self) -> None:
        """
        Read tables under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.read tables through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def refresh_format_cache(self):
        """
        Perform the refresh format cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.refresh format cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def refresh_ondevice(self):
        """
        Perform the refresh ondevice operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.refresh ondevice through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def refresh_search_locations(self):
        """
        Perform the refresh search locations operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.refresh search locations through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def reload_from_db(self, clear_caches=True):
        """
        Perform the reload from db operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.reload from db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param clear_caches: Value supplied for clear caches under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def remove_books(self, book_ids: Iterable[int], permanent: bool=False):
        """
        Perform the remove books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param permanent: Value supplied for permanent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def remove_cover_cache(self, cover_cache) -> bool:
        """
        Perform the remove cover cache operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.remove cover cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param cover_cache: Value supplied for cover cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def remove_formats(self, formats_map: dict[int, str], db_only: bool=False) -> bool:
        """
        Perform the remove formats operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.remove formats through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param formats_map: Value supplied for formats map under the utility contract.
        :param db_only: Value supplied for db only under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def remove_items(self, field: str, item_ids: Iterable[str], restrict_to_book_ids: set[int]=None):
        """
        Perform the remove items operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.remove items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param item_ids: Value supplied for item ids under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def rename_items(self, field: str, item_id_to_new_name_map: dict[int, str], change_index: bool=True, restrict_to_book_ids: Optional[set[int]]=None):
        """
        Perform the rename items operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.rename items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param item_id_to_new_name_map: Value supplied for item id to new name map under the
            utility contract.
        :param change_index: Value supplied for change index under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def restore_book(self, book_id, mi, last_modified, path, formats):
        """
        Perform the restore book operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.restore book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param last_modified: Value supplied for last modified under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param formats: Value supplied for formats under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def restore_original_format(self, book_id: int, original_fmt: str) -> bool:
        """
        Perform the restore original format operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.restore original format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param original_fmt: Value supplied for original fmt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @property
    @abc.abstractmethod
    def safe_read_lock(self):
        """
        Perform the safe read lock operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.safe read lock through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def save_original_format(self, book_id: int, fmt: str) -> bool:
        """
        Perform the save original format operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.save original format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_add(self, name: str, val):
        """
        Perform the saved search add operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search add through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_delete(self, name: str) -> None:
        """
        Perform the saved search delete operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search delete through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_lookup(self, name: str):
        """
        Perform the saved search lookup operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search lookup through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_names(self) -> list[str]:
        """
        Perform the saved search names operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search names through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_rename(self, old_name, new_name):
        """
        Perform the saved search rename operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search rename through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_name: Value supplied for old name under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def saved_search_set_all(self, smap):
        """
        Perform the saved search set all operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.saved search set all through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param smap: Value supplied for smap under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def search(self, query, restriction='', virtual_fields=None, book_ids=None):
        """
        Perform the search operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.search through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param query: Search expression parsed or evaluated by the utility.
        :param restriction: Value supplied for restriction under the utility contract.
        :param virtual_fields: Value supplied for virtual fields under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_conversion_options(self, options, fmt='PIPE'):
        """
        Set conversion options under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param options: Value supplied for options under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_cover(self, book_id_data_map: dict[int:Optional[Union[str, bytes]]]) -> bool:
        """
        Replace the container's cover while preserving required package references.

        Example:
            Exercise DatabaseCacheAPI.set cover through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_data_map: Value supplied for book id data map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_custom_column_metadata(self, num: int, name: Optional[str]=None, label: Optional[str]=None, is_editable: Optional[bool]=None, display: Optional[str]=None, update_last_modified: bool=False) -> bool:
        """
        Set custom column metadata under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set custom column metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param num: Value supplied for num under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param label: Value supplied for label under the utility contract.
        :param is_editable: Value supplied for is editable under the utility contract.
        :param display: Value supplied for display under the utility contract.
        :param update_last_modified: Value supplied for update last modified under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_field(self, name: str, book_id_to_val_map: dict[int, str], allow_case_change: bool=True, do_path_update: bool=True) -> set[int]:
        """
        Set field under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set field through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :param do_path_update: Value supplied for do path update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_last_read_position(self, book_id, fmt, user='_', device='_', cfi=None, epoch=None, pos_frac=0):
        """
        Set last read position under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set last read position through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param user: Value supplied for user under the utility contract.
        :param device: Value supplied for device under the utility contract.
        :param cfi: Value supplied for cfi under the utility contract.
        :param epoch: Value supplied for epoch under the utility contract.
        :param pos_frac: Value supplied for pos frac under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_link_for_authors(self, author_id_to_link_map: dict[int, str]) -> set[int]:
        """
        Set link for authors under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set link for authors through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param author_id_to_link_map: Value supplied for author id to link map under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_metadata(self, book_id: int, mi, ignore_errors=False, force_changes=False, set_title=True, set_authors=True, allow_case_change=False):
        """
        Update document metadata while preserving unrelated package state.

        Example:
            Exercise DatabaseCacheAPI.set metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param ignore_errors: Value supplied for ignore errors under the utility contract.
        :param force_changes: Value supplied for force changes under the utility contract.
        :param set_title: Value supplied for set title under the utility contract.
        :param set_authors: Value supplied for set authors under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_pref(self, name: str, val: Any) -> None:
        """
        Set pref under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set pref through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_sort_for_authors(self, author_id_to_sort_map: dict[int, str], update_books: bool=True) -> set[int]:
        """
        Set sort for authors under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set sort for authors through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param author_id_to_sort_map: Value supplied for author id to sort map under the
            utility contract.
        :param update_books: Value supplied for update books under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def set_user_template_functions(self, user_template_functions):
        """
        Set user template functions under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.set user template functions through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param user_template_functions: Value supplied for user template functions under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def tags_older_than(self, tag: str, delta=None, must_have_tag: Optional[Iterable[str]]=None, must_have_authors=None):
        """
        Perform the tags older than operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.tags older than through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag: Value supplied for tag under the utility contract.
        :param delta: Value supplied for delta under the utility contract.
        :param must_have_tag: Value supplied for must have tag under the utility contract.
        :param must_have_authors: Value supplied for must have authors under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def update_data_for_find_identical_books(self, book_id, data):
        """
        Perform the update data for find identical books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.update data for find identical books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def update_last_modified(self, book_ids, now=None):
        """
        Perform the update last modified operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.update last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param now: Value supplied for now under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def update_path(self, book_ids: Iterable[int], mark_as_dirtied: bool=True) -> bool:
        """
        Perform the update path operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.update path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param mark_as_dirtied: Value supplied for mark as dirtied under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def user_categories_for_books(self, book_ids, proxy_metadata_map=None):
        """
        Perform the user categories for books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.user categories for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param proxy_metadata_map: Value supplied for proxy metadata map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def vacuum(self) -> bool:
        """
        Perform the vacuum operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.vacuum through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def virtual_libraries_for_books(self, book_ids: Iterable[int]):
        """
        Perform the virtual libraries for books operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseCacheAPI.virtual libraries for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    @abc.abstractmethod
    def write_backup(self, book_id, raw):
        """
        Write backup under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseCacheAPI.write backup through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...
