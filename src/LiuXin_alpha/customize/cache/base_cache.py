"""
Provide cached library metadata reads, writes and invalidation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base cache through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

from typing import Iterable, Optional, Callable, Union, BinaryIO, Any, TypeVar, Literal

from LiuXin_alpha.customize.cache.read_write_api import api, read_api, write_api
from LiuXin_alpha.databases.db_types import MainTableName
from LiuXin_alpha.databases.locking import create_locks, wrap_simple, SafeReadLock
from LiuXin_alpha.utils.text.icu import lower as icu_lower

T = TypeVar("T")


class CacheAPI:
    """
    Base class for LiuXin cache objects - part of the cache plugin system.

    Example:
        Exercise CacheAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self, backend):
        """
        Add a backend to the cache class

        Example:
            Exercise CacheAPI.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param backend: Value supplied for backend under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        # The backend of the backend is the actual connection out to the database
        self.backend = backend

    # ------------------------------------------------------------------------------------------------------------------
    #
    #  - STARTUP
    @api
    def init(self):
        """
        Initialize the cache with data from the backend.

        Example:
            Exercise CacheAPI.init through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def read_tables(self) -> None:
        """
        Reading the table definitions from the backend to produce table objects.

        Example:
            Exercise CacheAPI.read tables through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def initialize_tables(self) -> None:
        """
        Read data off the backend tables into the cache itself.

        Example:
            Exercise CacheAPI.initialize tables through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def initialize_custom_columns(self) -> None:
        """
        Set up the custom columns.

        Example:
            Exercise CacheAPI.initialize custom columns through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def _initialize_dynamic_categories(self) -> None:
        """
        Initialize any additional dynamic categories which need to be read.

        Example:
            Exercise CacheAPI. initialize dynamic categories through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    #
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def field_metadata(self):
        """
        Returns the field metadata object stored in the backend.

        Example:
            Exercise CacheAPI.field metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError(f"Have to override this on an implementational level")

    @field_metadata.setter
    def field_metadata(self, value: Any) -> None:
        """
        Field Metadata cannot be directly set.

        Example:
            Exercise CacheAPI.field metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise ValueError(f"field_metadata cannot be set to {value=} - change the database and reload")

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BASIC API

    @property
    def new_api(self):
        """
        Legacy compatibility - returns a self reference.

        Example:
            Exercise CacheAPI.new api through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    @property
    def library_id(self):
        """
        Returns the library id - WILL CURRENTLY FAIL, UNLESS WORK IS DONE TO THE DATABASE.

        Example:
            Exercise CacheAPI.library id through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.library_id

    @property
    def safe_read_lock(self):
        """
        A safe read lock is a lock that does nothing if the thread already has a write lock.

        Example:
            Exercise CacheAPI.safe read lock through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def initialize_dynamic(self):
        """
        Read the dirtied books/objects out of the database and add the user defined dynamic categories.

        Example:
            Exercise CacheAPI.initialize dynamic through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def initialize_template_cache(self):
        """
        Setup the formatter template cache and start it as an empty set.

        Example:
            Exercise CacheAPI.initialize template cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_user_template_functions(self, user_template_functions):
        """
        Set user template functions under the format's safety and compatibility rules.

        Example:
            Exercise CacheAPI.set user template functions through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param user_template_functions: Value supplied for user template functions under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def clear_composite_caches(self, book_ids=None):
        """
        Clear caches for the composite tables - tables whose values are composed of more than one field.

        Example:
            Exercise CacheAPI.clear composite caches through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def clear_search_caches(self, book_ids=None):
        """
        Perform the clear search caches operation under explicit file-format and conversion rules.

        Example:
            Exercise CacheAPI.clear search caches through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def last_modified(self):
        """
        When was the last change made to the database?

        Example:
            Exercise CacheAPI.last modified through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def clear_caches(self, book_ids=None, template_cache=True, search_cache=True):
        """
        Front end for clear internal caches in the cache.

        Example:
            Exercise CacheAPI.clear caches through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param template_cache: Value supplied for template cache under the utility contract.
        :param search_cache: Value supplied for search cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def reload_from_db(self, clear_caches=True):
        """
        Reload all internally stored cache data from the database.

        Example:
            Exercise CacheAPI.reload from db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param clear_caches: Value supplied for clear caches under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def field_metadata(self):
        """
        Returns the field metadata object stored in the backend.

        Example:
            Exercise CacheAPI.field metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SORT AND SEARCH METHODS
    @read_api
    def multisort(self, fields, ids_to_sort=None, virtual_fields=None):
        """
        Return a list of sorted book book_ids. If ids_to_sort is None, all book book_ids are returned.

        Example:
            Exercise CacheAPI.multisort through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param fields: Value supplied for fields under the utility contract.
        :param ids_to_sort: Value supplied for ids to sort under the utility contract.
        :param virtual_fields: Value supplied for virtual fields under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def search(self, query, restriction="", virtual_fields=None, book_ids=None):
        """
        Search the database for the specified query, returning a set of matched book book_ids.

        Example:
            Exercise CacheAPI.search through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param query: Search expression parsed or evaluated by the utility.
        :param restriction: Value supplied for restriction under the utility contract.
        :param virtual_fields: Value supplied for virtual fields under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def saved_search_names(self) -> list[str]:
        """
        Search strings can be assigned names - this method returns all the ones currently set.

        Example:
            Exercise CacheAPI.saved search names through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def saved_search_lookup(self, name: str):
        """
        Retrieve a saved search by name.

        Example:
            Exercise CacheAPI.saved search lookup through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def saved_search_set_all(self, smap):
        """
        Perform the saved search set all operation under explicit file-format and conversion rules.

        Example:
            Exercise CacheAPI.saved search set all through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param smap: Value supplied for smap under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def saved_search_delete(self, name: str) -> None:
        """
        Remove a saved search from the map by name.

        Example:
            Exercise CacheAPI.saved search delete through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def saved_search_add(self, name: str, val):
        """
        Add a value to a saved search.

        Example:
            Exercise CacheAPI.saved search add through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def saved_search_rename(self, old_name, new_name):
        """
        Change the name of a saved search.

        Example:
            Exercise CacheAPI.saved search rename through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param old_name: Value supplied for old name under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def change_search_locations(self, newlocs):
        """
        Not sure what this does.

        Example:
            Exercise CacheAPI.change search locations through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param newlocs: Value supplied for newlocs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def refresh_search_locations(self):
        """
        Not sure what this does.

        Example:
            Exercise CacheAPI.refresh search locations through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - GENERIC FIELD ACCESS METHODS
    # Methods to access metadata about the various fields in the database - including the values of those fields
    @read_api
    def field_for(self, name, book_id, default_value=None):
        """
        Return the value of the field ``name`` for the book identified by ``book_id``.

        Example:
            Exercise CacheAPI.field for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def fast_field_for(self, field_obj, book_id, default_value=None):
        """
        Same as field_for, except that it avoids the extra lookup to get the field object.

        Example:
            Exercise CacheAPI.fast field for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field_obj: Value supplied for field obj under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def field_ids_for(self, name, book_id):
        """
        Return the book_ids (as a tuple) for the values that the field ``name`` has on the book identified by ``book_id``.

        Example:
            Exercise CacheAPI.field ids for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def all_field_for(self, field, book_ids, default_value=None):
        """
        Same as field_for, except that it operates on multiple books at once.

        Example:
            Exercise CacheAPI.all field for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def composite_for(self, name, book_id, mi=None, default_value=""):
        """
        Return the value for a composite field for the specified book id.

        Example:
            Exercise CacheAPI.composite for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def field_ids_for(self, name: str, book_id: int) -> tuple[int]:
        """
        Return the book_ids (as a tuple) for the values that the field ``name`` has on the book identified by ``book_id``.

        Example:
            Exercise CacheAPI.field ids for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def books_for_field(self, name: str, item_id: int) -> set[int]:
        """
        Return all the books lined to the item identified by ``item_id``, where the item belongs to the field ``name``.

        Example:
            Exercise CacheAPI.books for field through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def all_book_ids(self, rtn_type=frozenset):
        """
        Return all book book_ids in an instance of the given type.

        Example:
            Exercise CacheAPI.all book ids through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param rtn_type: Value supplied for rtn type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def all_field_ids(self, name: str) -> frozenset[int]:
        """
        Frozen set of book_ids for all values in the field ``name``.

        Example:
            Exercise CacheAPI.all field ids through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def all_field_names(self, field: str) -> frozenset[str]:
        """
        Frozen set of all fields names.

        Example:
            Exercise CacheAPI.all field names through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_usage_count_by_id(self, field: str) -> dict[int, int]:
        """
        Return a mapping of id to usage count for all values of the specified field

        Example:
            Exercise CacheAPI.get usage count by id through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_id_map(self, field: str) -> dict[int, str]:
        """
        Return a mapping of book_ids to values for the specified field.

        Example:
            Exercise CacheAPI.get id map through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_item_name(self, field: str, item_id: int) -> str:
        """
        Return the item name for the item specified by item_id in the specified field.

        Example:
            Exercise CacheAPI.get item name through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_item_id(self, field: str, item_name: str) -> int:
        """
        Return the item id for item_name (case-insensitive).

        Example:
            Exercise CacheAPI.get item id through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param item_name: Value supplied for item name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_item_ids(self, field: str, item_names: Iterable[str]) -> dict[str, int]:
        """
        Return the item book_ids for the given item names.

        Example:
            Exercise CacheAPI.get item ids through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param item_names: Value supplied for item names under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_field(
        self, name: str, book_id_to_val_map: dict[int, str], allow_case_change: bool = True, do_path_update: bool = True
    ) -> set[int]:
        """
        Set the values of the field specified by ``name``.

        Example:
            Exercise CacheAPI.set field through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :param do_path_update: Value supplied for do path update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def data_for_has_book(self):
        """
        Return data suitable for use in :meth:`has_book`.

        Example:
            Exercise CacheAPI.data for has book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def has_book(self, mi) -> bool:
        """
        Return True iff the database contains an entry with the same title as the passed in Metadata object.

        Example:
            Exercise CacheAPI.has book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param mi: Metadata object exposed to the template function.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    @read_api
    def has_id(self, book_id: int) -> bool:
        """
        Return True iff the specified book_id exists in the db.

        Example:
            Exercise CacheAPI.has id through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    @write_api
    def rename_items(
        self,
        field: str,
        item_id_to_new_name_map: dict[int, str],
        change_index: bool = True,
        restrict_to_book_ids: Optional[set[int]] = None,
    ):
        """
        Rename items in one-to-many and many-to-one tables e.g. series and tags.

        Example:
            Exercise CacheAPI.rename items through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param item_id_to_new_name_map: Value supplied for item id to new name map under the
            utility contract.
        :param change_index: Value supplied for change index under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def remove_items(self, field: str, item_ids: Iterable[str], restrict_to_book_ids: set[int] = None):
        """
        Delete all items in the specified field with the specified book_ids.

        Example:
            Exercise CacheAPI.remove items through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param field: Metadata or template field addressed by the operation.
        :param item_ids: Value supplied for item ids under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_books_for_category(self, category, item_id_or_composite_value):
        """
        Return books for category under the format's safety and compatibility rules.

        Example:
            Exercise CacheAPI.get books for category through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param category: Value supplied for category under the utility contract.
        :param item_id_or_composite_value: Value supplied for item id or composite value
            under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - METADATA METHODS
    # Methods to return metadata objects containing all the metadata about a particular item
    @api
    def get_metadata(
        self, book_id: int, get_cover: bool = False, get_user_categories: bool = True, cover_as_data: bool = False
    ):
        """
        Return metadata for the book identified by book_id as specilized object.

        Example:
            Exercise CacheAPI.get metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param get_cover: Value supplied for get cover under the utility contract.
        :param get_user_categories: Value supplied for get user categories under the utility
            contract.
        :param cover_as_data: Value supplied for cover as data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_proxy_metadata(self, book_id: str):
        """
        Like :meth:`get_metadata` except that it returns a ProxyMetadata object.

        Example:
            Exercise CacheAPI.get proxy metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_metadata_for_dump(self, book_id):
        """
        Return all the metadata needed for a dump of the metadata to the contained book folder.

        Example:
            Exercise CacheAPI.get metadata for dump through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_metadata(
        self,
        book_id: int,
        mi,
        ignore_errors=False,
        force_changes=False,
        set_title=True,
        set_authors=True,
        allow_case_change=False,
    ):
        """
        Set metadata for the book `id` from the `Metadata` object `mi`.

        Example:
            Exercise CacheAPI.set metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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
        raise NotImplementedError

    # Effectively a metadata -> book method
    @write_api
    def create_book_entry(
        self,
        mi,
        cover=None,
        add_duplicates: bool = True,
        force_id: int = None,
        apply_import_tags: bool = True,
        preserve_uuid: bool = False,
    ):
        """
        Create a new entry in the books table - accepts as input either a LiuXin or calibre metadata object.

        Example:
            Exercise CacheAPI.create book entry through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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
        raise NotImplementedError

    @api
    def add_books(
        self,
        books,
        add_duplicates=True,
        apply_import_tags=True,
        preserve_uuid=False,
        run_hooks=True,
        dbapi=None,
    ):
        """
        Add the specified books to the library.

        Example:
            Exercise CacheAPI.add books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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
        raise NotImplementedError

    @write_api
    def remove_books(self, book_ids: Iterable[int], permanent: bool = False):
        """
        Remove the books specified by the book_ids from the database and delete their format files. If ``permanent`` is False, then the format files are not deleted.

        Example:
            Exercise CacheAPI.remove books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param permanent: Value supplied for permanent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def data_for_find_identical_books(self):
        """
        Return data that can be used to implement :meth:`find_identical_books` without access to the db.

        Example:
            Exercise CacheAPI.data for find identical books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def update_data_for_find_identical_books(self, book_id, data):
        """
        Update the data for find identicle books.

        Example:
            Exercise CacheAPI.update data for find identical books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def find_identical_books(self, mi, search_restriction="", book_ids=None):
        """
        Finds books that have a superset of the authors in mi and the same title (title is fuzzy matched).

        Example:
            Exercise CacheAPI.find identical books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param mi: Metadata object exposed to the template function.
        :param search_restriction: Value supplied for search restriction under the utility
            contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - AUTHOR SPECIFIC FIELD ACCESS METHODS
    # Specialized access methods for named fields - authors, identifiers, e.t.c
    @read_api
    def author_data(self, author_ids=None):
        """
        Return author data as a dictionary keyed with the author id and valued with a tuple of name, sort, link.

        Example:
            Exercise CacheAPI.author data through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param author_ids: Value supplied for author ids under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def author_sort_strings_for_books(self, book_ids: Iterable[int]) -> dict[int, tuple[str, ...]]:
        """
        Return a map keyed with the book_id and valued with a tuple of the author sorts for all the given books.

        Example:
            Exercise CacheAPI.author sort strings for books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def author_sort_from_authors(self, authors: Iterable[str], key_func: Callable[[str], str] = icu_lower) -> str:
        """
        Given a list of authors, return the author_sort string for the authors.

        Example:
            Exercise CacheAPI.author sort from authors through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param authors: Value supplied for authors under the utility contract.
        :param key_func: Value supplied for key func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_sort_for_authors(self, author_id_to_sort_map: dict[int, str], update_books: bool = True) -> set[int]:
        """
        Sets the sort field for any referenced authors.

        Example:
            Exercise CacheAPI.set sort for authors through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param author_id_to_sort_map: Value supplied for author id to sort map under the
            utility contract.
        :param update_books: Value supplied for update books under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_link_for_authors(self, author_id_to_link_map: dict[int, str]) -> set[int]:
        """
        Update the link field for the given authors.

        Example:
            Exercise CacheAPI.set link for authors through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param author_id_to_link_map: Value supplied for author id to link map under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LAST MODIFIED FIELD METHODS
    @write_api
    def update_last_modified(self, book_ids, now=None):
        """
        Updates the last modified date for the given book_ids - if :param now: is None, will default to utcnow().

        Example:
            Exercise CacheAPI.update last modified through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param now: Value supplied for now under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - ON DEVICE FIELD METHODS
    @write_api
    def refresh_ondevice(self):
        """
        Refresh the ondevice field.

        Example:
            Exercise CacheAPI.refresh ondevice through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FORMAT SPECIFIC FIELD ACCESS METHODS
    # Methods to get metadata stored in the cache about a given format
    @read_api
    def format_hash(self, book_id, fmt):
        """
        Return the hash of the specified format for the specified book.

        Example:
            Exercise CacheAPI.format hash through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @api
    def format_metadata(self, book_id, fmt, allow_cache=True, update_db=False):
        """
        Return the path, size and mtime for the specified format for the specified book.

        Example:
            Exercise CacheAPI.format metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param allow_cache: Value supplied for allow cache under the utility contract.
        :param update_db: Value supplied for update db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def book_formats(self, book_id: int) -> tuple[str, ...]:
        """
        Return the fmt_priorities available for a given book.

        Example:
            Exercise CacheAPI.book formats through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def format_files(self, book_id):
        """
        Returns a map keyed with the format name and valued with the file names.

        Example:
            Exercise CacheAPI.format files through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def has_format(self, book_id: int, fmt: str) -> bool:
        """
        Return True iff the book has the specified format.

        Example:
            Exercise CacheAPI.has format through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    @write_api
    def refresh_format_cache(self):
        """
        Reload the format cache from the database.

        Example:
            Exercise CacheAPI.refresh format cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES SPECIFIC ACCESS METHODS
    @read_api
    def get_next_series_num_for(self, series, field="series", current_indices=False):
        """
        Return the next series index for the given series using all series next value preferences.

        Example:
            Exercise CacheAPI.get next series num for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param series: Value supplied for series under the utility contract.
        :param field: Metadata or template field addressed by the operation.
        :param current_indices: Value supplied for current indices under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TAGS SPECIFIC ACCESS METHODS
    @read_api
    def tags_older_than(
        self, tag: str, delta=None, must_have_tag: Optional[Iterable[str]] = None, must_have_authors=None
    ):
        """
        Return the book_ids of all books having the tag ``tag`` that are older than the specified time.

        Example:
            Exercise CacheAPI.tags older than through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param tag: Value supplied for tag under the utility contract.
        :param delta: Value supplied for delta under the utility contract.
        :param must_have_tag: Value supplied for must have tag under the utility contract.
        :param must_have_authors: Value supplied for must have authors under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UUID SPECIFIC ACCESS METHODS
    @read_api
    def lookup_by_uuid(self, uuid: str) -> int:
        """
        UUID -> book_id.

        Example:
            Exercise CacheAPI.lookup by uuid through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param uuid: Value supplied for uuid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FORMAT FRONT END
    # Methods to manipulate the physical format files
    @read_api
    def copy_format_to(
        self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool = False, report_file_size=None
    ) -> bool:
        """
        Copy the format ``fmt`` to the file like object ``dest``.

        Example:
            Exercise CacheAPI.copy format to through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def copy_formats_to(
        self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool = False, report_file_size=None
    ):
        """
        Copy the format ``fmt`` to the file like object ``dest``.

        Example:
            Exercise CacheAPI.copy formats to through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def format_abspath(self, book_id, fmt):
        """
        Return a path to the ebook file of format `format`.

        Example:
            Exercise CacheAPI.format abspath through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @api
    def save_original_format(self, book_id: int, fmt: str) -> bool:
        """
        Save a copy of the specified format as ORIGINAL_FORMAT, overwriting any existing ORIGINAL_FORMAT.

        Example:
            Exercise CacheAPI.save original format through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Book id, format marker, e.t.c should be their own classes
    @api
    def restore_original_format(self, book_id: int, original_fmt: str) -> bool:
        """
        Restore the specified format from the previously saved ORIGINAL_FORMAT, if any.

        Example:
            Exercise CacheAPI.restore original format through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param original_fmt: Value supplied for original fmt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def formats(self, book_id, verify_formats=True):
        """
        Return tuple of all formats for the specified book. If verify_formats is True, verifies that the files exist on disk.

        Example:
            Exercise CacheAPI.formats through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param verify_formats: Value supplied for verify formats under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @api
    def format(
        self, book_id: int, fmt: str, as_file: bool = False, as_path: str = False, preserve_filename: bool = False
    ) -> bytes:
        """
        Return the ebook format as a bytestring or `None` if it doesn't exist, or we can't read the file.

        Example:
            Exercise CacheAPI.format through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param as_file: Value supplied for as file under the utility contract.
        :param as_path: Value supplied for as path under the utility contract.
        :param preserve_filename: Value supplied for preserve filename under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @api
    def add_format(
        self,
        book_id: int,
        fmt: str,
        stream_or_path: Union[bytes, BinaryIO],
        replace: bool = False,
        run_hooks: bool = True,
        dbapi=None,
    ) -> bool:
        """
        Add a format to the specified book.

        Example:
            Exercise CacheAPI.add format through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param stream_or_path: Value supplied for stream or path under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :param run_hooks: Value supplied for run hooks under the utility contract.
        :param dbapi: Value supplied for dbapi under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Add capability to not track a file in the folder system
    @write_api
    def remove_formats(self, formats_map: dict[int, str], db_only: bool = False) -> bool:
        """
        Remove the specified formats from the specified books.

        Example:
            Exercise CacheAPI.remove formats through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param formats_map: Value supplied for formats map under the utility contract.
        :param db_only: Value supplied for db only under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK FRONT END
    # Methods to update and manipulate books
    @write_api
    def update_path(self, book_ids: Iterable[int], mark_as_dirtied: bool = True) -> bool:
        """
        Run update on the given books to take into account any metadata changes which might affect their position.

        Example:
            Exercise CacheAPI.update path through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param mark_as_dirtied: Value supplied for mark as dirtied under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - COVER FRONT END
    @api
    def cover(
        self, book_id: int, as_file: bool = False, as_image: bool = False, as_path: bool = False
    ) -> Optional[bytes]:
        """
        Return the cover image or None.

        Example:
            Exercise CacheAPI.cover through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param as_file: Value supplied for as file under the utility contract.
        :param as_image: Value supplied for as image under the utility contract.
        :param as_path: Value supplied for as path under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def cover_or_cache(self, book_id: int, timestamp: int) -> tuple[bool, bytes, int]:
        """
        Provides a tuple of information as to if to read from the cache or read from the folder store cache.

        Example:
            Exercise CacheAPI.cover or cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param timestamp: Value supplied for timestamp under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def cover_last_modified(self, book_id: int) -> int:
        """
        When was the primary cover for a given book last modified.

        Example:
            Exercise CacheAPI.cover last modified through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def copy_cover_to(
        self, book_id: int, dest: Union[str, BinaryIO], use_hardlink: bool = False, report_file_size=None
    ) -> bool:
        """
        Copy the cover to the file like object ``dest``.

        Example:
            Exercise CacheAPI.copy cover to through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :param report_file_size: Value supplied for report file size under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_cover(self, book_id_data_map: dict[int : Optional[Union[str, bytes]]]) -> bool:
        """
        Set the covers for a number of books.

        Example:
            Exercise CacheAPI.set cover through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id_data_map: Value supplied for book id data map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def add_cover_cache(self, cover_cache) -> bool:
        """
        Adds a cover_cache object to the set of internal cover caches.

        Example:
            Exercise CacheAPI.add cover cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param cover_cache: Value supplied for cover cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def remove_cover_cache(self, cover_cache) -> bool:
        """
        Remove a registered cover cache from the system.

        Example:
            Exercise CacheAPI.remove cover cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param cover_cache: Value supplied for cover cache under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - PREFERENCES FRONT END
    @read_api
    def pref(self, name: str, default: Any = None) -> Any:
        """
        Return the value for the specified preference or ``default`` if the preference is not set.

        Example:
            Exercise CacheAPI.pref through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_pref(self, name: str, val: Any) -> Any:
        """
        Set the specified preference to the specified value.

        Example:
            Exercise CacheAPI.set pref through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - VIRTUAL LIBRARY FRONT END
    @read_api
    def books_in_virtual_library(self, vl, search_restriction=None) -> set[int]:
        """
        Return the set of books in the specified virtual library

        Example:
            Exercise CacheAPI.books in virtual library through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param vl: Value supplied for vl under the utility contract.
        :param search_restriction: Value supplied for search restriction under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TAG BROWSER
    @api
    def get_categories(
        self, sort: str = "name", book_ids: Iterable[int] = None, already_fixed=None, first_letter_sort: bool = False
    ):
        """
        Used internally to implement the Tag Browser

        Example:
            Exercise CacheAPI.get categories through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param sort: Value supplied for sort under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param already_fixed: Value supplied for already fixed under the utility contract.
        :param first_letter_sort: Value supplied for first letter sort under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - DIRTIED BOOKS FRONT END
    @write_api
    def mark_as_dirty(self, book_ids: Iterable[int]) -> bool:
        """
        Note that the following books are dirtied on the database.

        Example:
            Exercise CacheAPI.mark as dirty through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def commit_dirty_cache(self) -> bool:
        """
        Write the current dirtied cache out of the database.

        Example:
            Exercise CacheAPI.commit dirty cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_a_dirtied_book(self) -> int:
        """
        Return a dirty book randomly selected from the dirtied_cache.

        Example:
            Exercise CacheAPI.get a dirtied book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def clear_dirtied(self, book_id: int, sequence):
        """
        Clear the dirtied indicator for the given book.

        Example:
            Exercise CacheAPI.clear dirtied through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param sequence: Value supplied for sequence under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def dirty_queue_length(self) -> int:
        """
        The current size of the dirtied cache.

        Example:
            Exercise CacheAPI.dirty queue length through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BACKUP FRONT END
    @write_api
    def write_backup(self, book_id, raw):
        """
        Write backup metadata into the book's folder.

        Example:
            Exercise CacheAPI.write backup through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def read_backup(self, book_id):
        """
        Return the OPF metadata backup for the book's folder as a bytestring or None if no such backup exists.

        Example:
            Exercise CacheAPI.read backup through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def dump_metadata(
        self, book_ids: Optional[Iterable[str]] = None, remove_from_dirtied: bool = True, callback=None
    ) -> bool:
        """
        Write metadata for each record to an individual OPF file.

        Example:
            Exercise CacheAPI.dump metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param remove_from_dirtied: Value supplied for remove from dirtied under the utility
            contract.
        :param callback: Value supplied for callback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def restore_book(self, book_id, mi, last_modified, path, formats):
        """
        Restore the book entry in the database for a book that already exists on the filesystem

        Example:
            Exercise CacheAPI.restore book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param last_modified: Value supplied for last modified under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param formats: Value supplied for formats under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM BOOK DATA
    @write_api
    def add_custom_book_data(self, name: str, val_map: dict[int, Any], delete_first: bool = False) -> bool:
        """
        Add data for name where val_map is a map of book_ids to values.

        Example:
            Exercise CacheAPI.add custom book data through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val_map: Value supplied for val map under the utility contract.
        :param delete_first: Value supplied for delete first under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_custom_book_data(
        self, name: str, book_ids: Iterable[int] = (), default: Optional[Any] = None
    ) -> dict[int, Any]:
        """
        Get data from the given book_ids for the given custom value name.

        Example:
            Exercise CacheAPI.get custom book data through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :param default: Value supplied for default under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def delete_custom_book_data(self, name: str, book_ids: Iterable[int] = ()) -> bool:
        """
        Delete data for name.

        Example:
            Exercise CacheAPI.delete custom book data through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_ids_for_custom_book_data(self, name: str) -> set[int]:
        """
        Return the set of book book_ids for which name has data.

        Example:
            Exercise CacheAPI.get ids for custom book data through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CONVERSION DATA FRONT END
    @read_api
    def conversion_options(self, book_id: int, fmt: str = "PIPE"):
        """
        Return the conversion options for a given book_id of a given format - default to fmt='PIPE'

        Example:
            Exercise CacheAPI.conversion options through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def has_conversion_options(self, book_ids: Iterable[int], fmt: str = "PIPE"):
        """
        Check to see if the given books have a designated conversion option.

        Example:
            Exercise CacheAPI.has conversion options through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    @write_api
    def delete_conversion_options(self, book_ids, fmt: str = "PIPE"):
        """
        Remove the conversion options from the given book_ids.

        Example:
            Exercise CacheAPI.delete conversion options through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_conversion_options(self, options, fmt="PIPE"):
        """
        Options must be a map of the form {book_id : conversion_options}.

        Example:
            Exercise CacheAPI.set conversion options through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param options: Value supplied for options under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMNS FRONT END
    # Todo: Need an enum for the potential datatypes
    @write_api
    def create_custom_column(
        self, label: str, name: str, datatype, is_multiple: bool, editable: bool = True, display: Optional[str] = None
    ) -> Union[int, Literal[False,]]:
        """
        Make a custom column for the books table.

        Example:
            Exercise CacheAPI.create custom column through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param label: Value supplied for label under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param datatype: Value supplied for datatype under the utility contract.
        :param is_multiple: Value supplied for is multiple under the utility contract.
        :param editable: Value supplied for editable under the utility contract.
        :param display: Value supplied for display under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Add a separate, calibre compatible interface to this class
    @write_api
    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        update_last_modified: bool = False,
    ) -> bool:
        """
        Update the changeable metadata for a custom column.

        Example:
            Exercise CacheAPI.set custom column metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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
        raise NotImplementedError

    @write_api
    def delete_custom_column(self, label: str = None, num: int = None) -> bool:
        """
        Remove a custom column set for the books table.

        Example:
            Exercise CacheAPI.delete custom column through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MOVE METHODS
    @read_api
    def get_top_level_move_items(self):
        """
        Not sure that there is a good way to implement this - and if a plugin is using this I have questions.

        Example:
            Exercise CacheAPI.get top level move items through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def move_library_to(self, newloc, progress=None, abort=None):
        """
        Not sure that there is a good way to implement this - and if a plugin is using this I have questions.

        Example:
            Exercise CacheAPI.move library to through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param newloc: Value supplied for newloc under the utility contract.
        :param progress: Value supplied for progress under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - DATABASE MAINTENANCE METHODS
    @write_api
    def dump_and_restore(self, callback=None, sql=None):
        """
        Dump the database to disk and restore it.

        Example:
            Exercise CacheAPI.dump and restore through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param callback: Value supplied for callback under the utility contract.
        :param sql: Value supplied for sql under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def vacuum(self) -> bool:
        """
        Preforming vacuum (or equivalent) - an SQL maintenance task.

        Example:
            Exercise CacheAPI.vacuum through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def close(self):
        """
        Close the database connection.

        Example:
            Exercise CacheAPI.close through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def export_library(self, library_key, exporter, progress=None, abort=None):
        """
        Save the database in some format - this will depend on the exporter function used.

        Example:
            Exercise CacheAPI.export library through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param library_key: Value supplied for library key under the utility contract.
        :param exporter: Value supplied for exporter under the utility contract.
        :param progress: Value supplied for progress under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - VIRTUAL LIBRARIES FRONT END
    @read_api
    def virtual_libraries_for_books(self, book_ids: Iterable[int]):
        """
        Return all the virtual libraries that the given books are in.

        Example:
            Exercise CacheAPI.virtual libraries for books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - USER CATEGORIES FRONT END
    @read_api
    def user_categories_for_books(self, book_ids, proxy_metadata_map=None):
        """
        Return the user categories for the specified books.

        Example:
            Exercise CacheAPI.user categories for books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param proxy_metadata_map: Value supplied for proxy metadata map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - EDIT BOOKS METHODS
    @write_api
    def embed_metadata(
        self, book_ids: Iterable[int], only_fmts: Iterable[str] = None, report_error=None, report_progress=None
    ) -> bool:
        """
        Update metadata in all formats of the specified book_ids to current metadata in the database.

        Example:
            Exercise CacheAPI.embed metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param only_fmts: Value supplied for only fmts under the utility contract.
        :param report_error: Value supplied for report error under the utility contract.
        :param report_progress: Value supplied for report progress under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @read_api
    def get_last_read_positions(self, book_id: int, fmt: str, user: str) -> bool:
        """
        Return the stored last read position for the book.

        Example:
            Exercise CacheAPI.get last read positions through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param user: Value supplied for user under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_last_read_position(self, book_id, fmt, user="_", device="_", cfi=None, epoch=None, pos_frac=0):
        """
        Update the last read position of a book on the database.

        Example:
            Exercise CacheAPI.set last read position through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------

    @read_api
    def pref(self, name: str, default: Optional[T] = None) -> T:
        """
        Return the value for the specified preference or ``default`` if the preference is not set.

        Example:
            Exercise CacheAPI.pref through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @write_api
    def set_pref(self, name: str, val: Any) -> None:
        """
        Set the specified preference to the specified value. See also :meth:`pref`.

        Example:
            Exercise CacheAPI.set pref through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseCache(CacheAPI):
    """
    Provide the basecache contract for validated ebook processing.

    Example:
        Exercise BaseCache through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    def __init__(self, backend):
        """
        Add a backend to the cache class

        Example:
            Exercise BaseCache.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param backend: Value supplied for backend under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(backend=backend)

        # Flag to check if data has been read off the backend
        self.init_called: bool = False

        # Tables which are known and relevant to the database
        self.tables: set[MainTableName] = backend.tables
        # Will store the inbuilt fields and custom fields
        self.fields = {}
        # Will store the composite fields made up of information from other fields.
        self.composites = {}

        self.read_lock, self.write_lock = create_locks()

        # CacheAPI is the base class for any caches - used here as it has all the public functions of any implemented
        # cache - and each of their signatures should be the same
        self.unlock: CacheAPI = CacheAPI(backend=None)

        # Implement locking for all simple read/write API methods
        # An unlocked version of the method is stored with the name starting with a leading underscore.
        # You can use the unlocked versions when the lock has already been acquired.
        # Alternatives self.unlock should provide an alias to all the functions which should be present unlocked
        for name in dir(self):
            func = getattr(self, name)
            ira = getattr(func, "is_read_api", None)
            if ira is not None:
                # Save original function
                setattr(self, "_" + name, func)
                setattr(self.unlock, name, func)

                # Wrap it in a lock
                lock = self.read_lock if ira else self.write_lock
                setattr(self, name, wrap_simple(lock, func))

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UTILITIES
    # Has to be here because we need a read lock
    @property
    def safe_read_lock(self) -> SafeReadLock:
        """
        A safe read lock does nothing if the thread already has a write lock, otherwise it acquires a read lock.

        Example:
            Exercise BaseCache.safe read lock through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return SafeReadLock(self.read_lock)

    #
    # ------------------------------------------------------------------------------------------------------------------
