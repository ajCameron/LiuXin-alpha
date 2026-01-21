"""
Base class for the cache.

All caches declared for LiuXin - including via plugins - should descend from here.
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

    An caches you define should inherit from this.
    Provided they have the appropriate API at cache level, the internal workings don't really matter.
    """

    def __init__(self, backend):
        """
        Add a backend to the cache class
        :param backend:
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

        Does any other generic startup tasks.
        At the end of this operation, the cache should be ready to use.
        :return:
        """
        raise NotImplementedError

    def read_tables(self) -> None:
        """
        Reading the table definitions from the backend to produce table objects.

        Data is not read into the tables at this stage - it just defined the tables which will, eventually, need to be
        populated.
        :return:
        """
        raise NotImplementedError

    def initialize_tables(self) -> None:
        """
        Read data off the backend tables into the cache itself.

        :return:
        """
        raise NotImplementedError

    def initialize_custom_columns(self) -> None:
        """
        Set up the custom columns.

        :return:
        """
        raise NotImplementedError

    def _initialize_dynamic_categories(self) -> None:
        """
        Initialize any additional dynamic categories which need to be read.

        Placeholder - should be overridden to actually have function.
        :return:
        """

    #
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def field_metadata(self):
        """
        Returns the field metadata object stored in the backend.

        This defines metadata for the individual fields.
        :return:
        """
        raise NotImplementedError(f"Have to override this on an implementational level")

    @field_metadata.setter
    def field_metadata(self, value: Any) -> None:
        """
        Field Metadata cannot be directly set.

        You need to change the fields defined on the database and then reload.
        :param value:
        :return:
        """
        raise ValueError(f"field_metadata cannot be set to {value=} - change the database and reload")

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BASIC API

    @property
    def new_api(self):
        """
        Legacy compatibility - returns a self reference.

        :return:
        """
        return self

    @property
    def library_id(self):
        """
        Returns the library id - WILL CURRENTLY FAIL, UNLESS WORK IS DONE TO THE DATABASE.

        :return:
        """
        return self.backend.library_id

    @property
    def safe_read_lock(self):
        """
        A safe read lock is a lock that does nothing if the thread already has a write lock.

        Otherwise it acquires a read lock.
        This is necessary to prevent DowngradeLockErrors, which can happen when updating the search cache in
        the presence of composite columns. Updating the search cache holds an exclusive lock, but searching a composite
        column involves reading field values via ProxyMetadata which tries to get a shared lock.

        There may be other scenarios that trigger this as well.

        This property returns a new lock object on every access. This lock object is not recursive (for performance) and
        must only be used in a with statement as ``with cache.safe_read_lock:`` otherwise bad things will happen.
        :return:
        """
        raise NotImplementedError

    @write_api
    def initialize_dynamic(self):
        """
        Read the dirtied books/objects out of the database and add the user defined dynamic categories.

        dirtied books are books/objects whose
        :return:
        """
        raise NotImplementedError

    @write_api
    def initialize_template_cache(self):
        """
        Setup the formatter template cache and start it as an empty set.
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_user_template_functions(self, user_template_functions):
        raise NotImplementedError

    @write_api
    def clear_composite_caches(self, book_ids=None):
        """
        Clear caches for the composite tables - tables whose values are composed of more than one field.

        :param book_ids:
        :return:
        """
        raise NotImplementedError

    @write_api
    def clear_search_caches(self, book_ids=None):
        raise NotImplementedError

    @read_api
    def last_modified(self):
        """
        When was the last change made to the database?

        :return:
        """
        raise NotImplementedError

    @write_api
    def clear_caches(self, book_ids=None, template_cache=True, search_cache=True):
        """
        Front end for clear internal caches in the cache.

        :param book_ids: Clear the format metadata cache for the given book book_ids.
        :param template_cache: Clear the template cache?
        :param search_cache: Clear the search_cache
        :return:
        """
        raise NotImplementedError

    @write_api
    def reload_from_db(self, clear_caches=True):
        """
        Reload all internally stored cache data from the database.

        After this, it should be as if the cache has been freshly loaded.
        :param clear_caches:
        :return:
        """
        raise NotImplementedError

    @property
    def field_metadata(self):
        """
        Returns the field metadata object stored in the backend.

        Use with care - and please try not to change unless you know what you're doing.
        :return:
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

        Fields must be a list of 2-tuples of the form (field_name, ascending=True or False). The most significant field
        is the first 2-tuple.
        :param fields:
        :param ids_to_sort:
        :param virtual_fields:
        :return:
        """
        raise NotImplementedError

    @read_api
    def search(self, query, restriction="", virtual_fields=None, book_ids=None):
        """
        Search the database for the specified query, returning a set of matched book book_ids.

        :param restriction: A restriction that is ANDed to the specified query. Note that
            restrictions are cached, therefore the search for a AND b will be slower than a with restriction b.
        :param virtual_fields: Used internally (virtual fields such as on_device to search over).
        :param book_ids: If not None, a set of book book_ids for which books will be searched instead of searching all books.
        :param query:
        :param restriction:
        :return:
        """
        raise NotImplementedError

    @read_api
    def saved_search_names(self) -> list[str]:
        """
        Search strings can be assigned names - this method returns all the ones currently set.

        :return:
        """
        raise NotImplementedError

    @read_api
    def saved_search_lookup(self, name: str):
        """
        Retrieve a saved search by name.

        :param name:
        :return:
        """
        raise NotImplementedError

    @write_api
    def saved_search_set_all(self, smap):
        raise NotImplementedError

    @write_api
    def saved_search_delete(self, name: str) -> None:
        """
        Remove a saved search from the map by name.

        :param name:
        :return:
        """
        raise NotImplementedError

    @write_api
    def saved_search_add(self, name: str, val):
        """
        Add a value to a saved search.

        named search must exist in the map or KeyError will be raised.
        :param name:
        :param val:
        :return:
        """
        raise NotImplementedError

    @write_api
    def saved_search_rename(self, old_name, new_name):
        """
        Change the name of a saved search.

        named search must exist in the map or KeyError will be raised.
        :param old_name:
        :param new_name:
        :return:
        """
        raise NotImplementedError

    @write_api
    def change_search_locations(self, newlocs):
        """
        Not sure what this does.

        :param newlocs:
        :return:
        """
        raise NotImplementedError

    @write_api
    def refresh_search_locations(self):
        """
        Not sure what this does.

        :return:
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

        If no such book exists or it has no defined value for the field ``name`` or no such field exists, then
        ``default_value`` is returned.
        ``default_value`` is not used for title, title_sort, authors, author_sort and series_index. This is because
        these always have values in the db.
        ``default_value`` is used for all custom columns.
        The returned value for is_multiple fields are always tuples, even when no values are found (in other words,
        default_value is ignored). The exception is identifiers for which the returned value is always a dict.
        The returned tuples are always in link order, that is, the order in which they were created.
        :param name:
        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    @read_api
    def fast_field_for(self, field_obj, book_id, default_value=None):
        """
        Same as field_for, except that it avoids the extra lookup to get the field object.

        You have to have the field object in hand before you can use this method - you can get it from the fields
        property.
        :param field_obj: The field object representing that database field
        :param book_id: The id of the book to look up the field value for
        :param default_value: Return this if the lookup fails
        :return:
        """
        raise NotImplementedError

    @read_api
    def field_ids_for(self, name, book_id):
        """
        Return the book_ids (as a tuple) for the values that the field ``name`` has on the book identified by ``book_id``.

        If there are no values, or no such book, or no such field, an empty tuple is returned.
        :param name: The name of the field to return for
        :param book_id: The id of the book to return the value for
        :return field_ids_tuple: A tuple book_ids in the linked field
        """
        raise NotImplementedError

    @read_api
    def all_field_for(self, field, book_ids, default_value=None):
        """
        Same as field_for, except that it operates on multiple books at once.

        :param field:
        :param book_ids:
        :param default_value: This value will be added to the map if there isn't another value to record.
        :return book_id_val_map:
        """
        raise NotImplementedError

    @read_api
    def composite_for(self, name, book_id, mi=None, default_value=""):
        """
        Return the value for a composite field for the specified book id.

        :param name:
        :param book_id:
        :param mi:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    @read_api
    def field_ids_for(self, name: str, book_id: int) -> tuple[int]:
        """
        Return the book_ids (as a tuple) for the values that the field ``name`` has on the book identified by ``book_id``.

        If there are no values, or no such book, or no such field, an empty tuple is returned.
        :param name: The name of the field to return for
        :param book_id: The id of the book to return the value for
        :return field_ids_tuple: A tuple book_ids in the linked field
        """
        raise NotImplementedError

    @read_api
    def books_for_field(self, name: str, item_id: int) -> set[int]:
        """
        Return all the books lined to the item identified by ``item_id``, where the item belongs to the field ``name``.

        Returned value is a set of book book_ids, or the empty set if the item or the field does not exist.
        :param name:
        :param item_id:
        :return:
        """
        raise NotImplementedError

    @read_api
    def all_book_ids(self, rtn_type=frozenset):
        """
        Return all book book_ids in an instance of the given type.

        :param rtn_type: e.g. frozenset
        :return:
        """
        raise NotImplementedError

    @read_api
    def all_field_ids(self, name: str) -> frozenset[int]:
        """
        Frozen set of book_ids for all values in the field ``name``.

        :param name: The name of the field to return
        :return:
        """
        raise NotImplementedError

    @read_api
    def all_field_names(self, field: str) -> frozenset[str]:
        """
        Frozen set of all fields names.

        All duplicates will be removed by adding the values to a frozen set.
        i.e. all the values of those fields.
        :param field:
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_usage_count_by_id(self, field: str) -> dict[int, int]:
        """
        Return a mapping of id to usage count for all values of the specified field

        This should be a many-one or many-many field.
        You can get it for a one-to-one field, but the results will (probably) not be what you want.
        :param field: The name of the field to return the count for
        :return field_val_usage_count_map: Keyed with the id of the resource and valued with how often it's been used.
        """
        raise NotImplementedError

    @read_api
    def get_id_map(self, field: str) -> dict[int, str]:
        """
        Return a mapping of book_ids to values for the specified field.

        The field must be a many-one or many-many field (or title), otherwise a ValueError is raised.
        :param field:
        :return item_id_to_val_map:
        """
        raise NotImplementedError

    @read_api
    def get_item_name(self, field: str, item_id: int) -> str:
        """
        Return the item name for the item specified by item_id in the specified field.

        See also :meth:`get_id_map`.
        The field must be a many-one or many-many field, otherwise a ValueError is raised.
        Note - in calibre, this would raise a AttributeError - this has been changed to Value to be consistent with
        the get_id_map function.
        :param field:
        :param item_id:
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_item_id(self, field: str, item_name: str) -> int:
        """
        Return the item id for item_name (case-insensitive).

        :param field:
        :param item_name:
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_item_ids(self, field: str, item_names: Iterable[str]) -> dict[str, int]:
        """
        Return the item book_ids for the given item names.

        :param field: Search in this field
        :param item_names: Iterable of names to look for
        :return item_name_id_map: Keyed with the item name and valued with the id found for the item
        """
        raise NotImplementedError

    @write_api
    def set_field(
        self, name: str, book_id_to_val_map: dict[int, str], allow_case_change: bool = True, do_path_update: bool = True
    ) -> set[int]:
        """
        Set the values of the field specified by ``name``.

        Returns the set of all book book_ids that were affected by the change.
        :param name:
        :param book_id_to_val_map: Mapping of book_ids to values that should be applied.
        :param allow_case_change: If True, the case of many-one or many-many fields will be changed.
            For example, if a  book has the tag ``tag1`` and you set the tag for another book to ``Tag1``
            then the both books will have the tag ``Tag1`` if allow_case_change is True, otherwise they will
            both have the tag ``tag1``.
        :param do_path_update: Used internally, you should never change it.
                               Should the db path be updated as a consequence of this change.
        :return:
        """
        raise NotImplementedError

    @read_api
    def data_for_has_book(self):
        """
        Return data suitable for use in :meth:`has_book`.

        This can be used for an implementation of :meth:`has_book` in a worker process without access to the db.
        :return:
        """
        raise NotImplementedError

    @read_api
    def has_book(self, mi) -> bool:
        """
        Return True iff the database contains an entry with the same title as the passed in Metadata object.

        The comparison is case-insensitive.
        See also :meth:`data_for_has_book`.
        :param mi:
        :return:
        """
        raise NotImplementedError

    @read_api
    def has_id(self, book_id: int) -> bool:
        """
        Return True iff the specified book_id exists in the db.

        :param book_id:
        :return:
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

        Cannot handle one-to-one fields - such as titles - but this seems to be a flaw which should be fixed.
        :param field: The field to update the items for
        :type field: str
        :param item_id_to_new_name_map: Keyed with the id of the item (as an int) and valued with the new name that
                                        the field should be changed to.
                                        Thus - if you where updating the names of a tag - would be keyed with the id of
                                        the tag your updating and valued with the new name for the tag.
        :param change_index: When renaming in a series-like field also change the series_index values.
        :param restrict_to_book_ids: An optional set of book book_ids for which the rename is to be performed, defaults to
                                     all books. Used when there's an active virtual library.
        :return:
        """
        raise NotImplementedError

    @write_api
    def remove_items(self, field: str, item_ids: Iterable[str], restrict_to_book_ids: set[int] = None):
        """
        Delete all items in the specified field with the specified book_ids.

        Returns the set of affected book book_ids.
        ``restrict_to_book_ids`` is an optional set of books book_ids.
        If specified the items will only be removed from those books.
        This is intended to be used with a virtual library - the entries will only be removed from the books in the
        virtual library.
        :param field:
        :param item_ids:
        :param restrict_to_book_ids:
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_books_for_category(self, category, item_id_or_composite_value):
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

        A :class:`calibre.ebooks.metadata.book.base.Metadata` object, in particular.

        Note that the list of formats is not verified. If get_cover is True, the cover is returned, either a path to
        temp file as mi.cover or if cover_as_data is True then as mi.cover_data.
        :param book_id: The id of the book to retrieve the cover for
        :param get_cover: If True then tries to read the cover - else ignored the cover
        :param get_user_categories: If True then tries to retrieve the user categories
        :param cover_as_data: If True returns the cover as a stream - else returns the cover as a path
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_proxy_metadata(self, book_id: str):
        """
        Like :meth:`get_metadata` except that it returns a ProxyMetadata object.

        This only reads values from the database on demand.
        This is much faster than get_metadata when only a small number of fields need to be accessed from the returned
        metadata object.
        :param book_id:
        :return:
        """
        raise NotImplementedError

    @read_api
    def get_metadata_for_dump(self, book_id):
        """
        Return all the metadata needed for a dump of the metadata to the contained book folder.

        :param book_id:
        :return:
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

        Setting force_changes=True will force set_metadata to update fields even if mi contains empty values.
        In this case, 'None' is distinguished from 'empty'. If mi.XXX is None, the XXX is not replaced, otherwise it is.
        The tags, identifiers, and cover attributes are special cases. Tags and identifiers cannot be set to None so
        then will always be replaced if force_changes is true.
        You must ensure that mi contains the values you want the book to have.
        Covers are always changed if a new cover is provided, but are never deleted.
        Also note that force_changes has no effect on setting title or authors.
        :param book_id:
        :param mi:
        :param ignore_errors:
        :param force_changes:
        :param set_title:
        :param set_authors:
        :param allow_case_change:
        :return:
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

        :param mi: The metadata for the new book
        :param cover: The cover for the new book
        :param add_duplicates: Should the book add even if duplicate detection trips?
        :param force_id: If force_if then the book is guaranteed to have a specified id
        :param apply_import_tags: Should i,port tags be applied to the book before it's added
        :param preserve_uuid: Use the uuid from the metadata instead of coming up with a new one.
        :return:
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

        Books should be an iterable of 2-tuples, each 2-tuple of the form :code:`(mi, format_map)` where mi is a
        Metadata object and format_map is a dictionary of the form :code:`{fmt: path_or_stream}`,
        for example: :code:`{'EPUB': '/path/to/file.epub'}`.

        If you want to add multiple examples of the same fmt to the book at the same time you can pass an iterable
        of paths as the value for the fmt map.
        for example :code:`{'EPUB': ['/path/to/file.epub', 'another/path/to/another_file.epub']}`.

        Returns a pair of lists: :code:`book_ids, duplicates`. ``book_ids`` contains the book book_ids for all newly created books in
        the database. ``duplicates`` contains the :code:`(mi, format_map)` for all books that already exist in the
        database as per the simple duplicate detection heuristic used by :meth:`has_book`

        Modifies the given fmt map as it goes.
        As entries are processed adds new entries keyed with the lower case fmt that's being added and valued with the
        either the name of the resource that was copied in or <stream> if the resource was a stream.
        :param books:
        :param add_duplicates: If True, then no effort will be made to find duplicates in the added books
        :param apply_import_tags: Apply the new book tags (stored in preferences)
        :param preserve_uuid: Keep the UUID stored in the metadata object
        :param run_hooks: Run the import and post import hooks
        :param dbapi: For internal use
        :return:
        """
        raise NotImplementedError

    @write_api
    def remove_books(self, book_ids: Iterable[int], permanent: bool = False):
        """
        Remove the books specified by the book_ids from the database and delete their format files.
        If ``permanent`` is False, then the format files are not deleted.
        :param book_ids:
        :param permanent:
        :return:
        """
        raise NotImplementedError

    @read_api
    def data_for_find_identical_books(self):
        """
        Return data that can be used to implement :meth:`find_identical_books` without access to the db.

        E.g. in a seperate worker thread.
        See databases.utils for an implementation.
        :return:
        """
        raise NotImplementedError

    @read_api
    def update_data_for_find_identical_books(self, book_id, data):
        """
        Update the data for find identicle books.

        :param book_id:
        :param data:
        :return:
        """
        raise NotImplementedError

    @read_api
    def find_identical_books(self, mi, search_restriction="", book_ids=None):
        """
        Finds books that have a superset of the authors in mi and the same title (title is fuzzy matched).

        See also :meth:`data_for_find_identical_books`.
        :param mi:
        :param search_restriction:
        :param book_ids:
        :return:
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

        Defaults to returning data for all authors.
        :param author_ids:
        :return:
        """
        raise NotImplementedError

    @read_api
    def author_sort_strings_for_books(self, book_ids: Iterable[int]) -> dict[int, tuple[str, ...]]:
        """
        Return a map keyed with the book_id and valued with a tuple of the author sorts for all the given books.

        :param book_ids:
        :return:
        """
        raise NotImplementedError

    @read_api
    def author_sort_from_authors(self, authors: Iterable[str], key_func: Callable[[str], str] = icu_lower) -> str:
        """
        Given a list of authors, return the author_sort string for the authors.

        Preferring the author sort associated with the author over the computed string.
        :param authors:
        :param key_func:
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_sort_for_authors(self, author_id_to_sort_map: dict[int, str], update_books: bool = True) -> set[int]:
        """
        Sets the sort field for any referenced authors.

        :param author_id_to_sort_map: Keyed with the author id, valued with the new sort string
        :param update_books:
        :return changed_books:
        """
        raise NotImplementedError

    @write_api
    def set_link_for_authors(self, author_id_to_link_map: dict[int, str]) -> set[int]:
        """
        Update the link field for the given authors.

        :param author_id_to_link_map:
        :return changed_books:
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

        :param book_ids:
        :param now:
        :return:
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

        :return:
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

        The kind of hash is backend dependent, but is usually SHA-256.
        Multiple hashes may be stored for any given book.
        The hash should now be LiuXin's custom hash - (SHA-512 + length of file in bytes)
        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @api
    def format_metadata(self, book_id, fmt, allow_cache=True, update_db=False):
        """
        Return the path, size and mtime for the specified format for the specified book.

        The path is a LiuXin Location object - which should contain all the information needed to actually get the file.
        You should not use path unless you absolutely have to, since accessing it directly breaks the threadsafe
        guarantees of this API. Instead use the :meth:`copy_format_to` method - this also ensures that there is a local
        copy of the file - as, by default, the FolderStore in question might not offer local file access.
        :param book_id: The book_id to search in
        :param fmt: The format to look for
        :param allow_cache: If ``True`` cached values are used, otherwise a
                            slow filesystem access is done. The cache values could be out of date
                            if access was performed to the filesystem outside of this API.
        :param update_db: If ``True`` The max_size field of the database is updates for this book.
        :return:
        """
        raise NotImplementedError

    @read_api
    def book_formats(self, book_id: int) -> tuple[str, ...]:
        """
        Return the fmt_priorities available for a given book.

        Returns then as a tuple, ordered by priority.
        :param book_id:
        :return:
        """
        raise NotImplementedError

    @read_api
    def format_files(self, book_id):
        """
        Returns a map keyed with the format name and valued with the file names.

        Keys will be the fmt_priority - value will be the name of that format file.
        e.g. :code:`{"EPUB_1": "some_book_by_x.epub", "EPUB_2": "another_version_of_x_by_y.epub"}`
        :param book_id: Retrieve the formats for this book
        :type book_id: int
        :return:
        """
        raise NotImplementedError

    @read_api
    def has_format(self, book_id: int, fmt: str) -> bool:
        """
        Return True iff the book has the specified format.

        If the format is bare - e.g. "EPUB" then this method
        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @write_api
    def refresh_format_cache(self):
        """
        Reload the format cache from the database.

        :return:
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

        There are a number of preferences which can control the next number generation.
        :param series:
        :param field: The series-like field (defaults to the builtin series column)
        :param current_indices: If True, returns a mapping of book_id to current series_index value instead.
        :return:
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

        tag comparison is case insensitive.
        Used extensively internally with the tag browser.
        :param tag:
        :param delta: A timedelta object or None. If None, then all book_ids with the tag are returned.
        :param must_have_tag: If not None the list of matches will be restricted to books that have this tag
        :param must_have_authors: A list of authors. If not None the list of matches will be restricted to books that
                                  have these authors (case insensitive).
        :return:
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

        The UUID for the given book is stored in the books table.
        :param uuid:
        :return:
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

        If the specified format does not exist, raises :class:`NoSuchFormat` error.

        dest can also be a path, in which case the format is copied to it, iff the path is different from the current
        path (taking case sensitivity into account).
        :param book_id: The id of the book to copy from
        :param fmt: The name of the format to copy (must be a format priority string e.g. "EPUB_1")
                    Use ``copy_formats_to`` if you want all the formats.
        :param dest: The destination to copy the format to
        :param use_hardlink:
        :param report_file_size:
        :return status: True if successful, False otherwise.
        """
        raise NotImplementedError

    @read_api
    def copy_formats_to(
        self, book_id: int, fmt: str, dest: Union[BinaryIO, str], use_hardlink: bool = False, report_file_size=None
    ):
        """
        Copy the format ``fmt`` to the file like object ``dest``.

        If the specified format does not exist, raises :class:`NoSuchFormat` error.

        dest can also be a path, in which case the format is copied to it, iff the path is different from the current
        path (taking case sensitivity into account).
        :param book_id: The id of the book to copy from
        :param fmt: The name of the format to copy (must be a format priority string e.g. "EPUB_1")
                    Use copy_formats_to if you want all the formats.
        :param dest: The destination to copy the format to
        :param use_hardlink:
        :param report_file_size:
        :return:
        """
        raise NotImplementedError

    @read_api
    def format_abspath(self, book_id, fmt):
        """
        Return a path to the ebook file of format `format`.

        You should almost never use this, as it breaks the threadsafe promise of this API.
        Instead, use, :meth:`copy_format_to` to get a local copy of the file that you can then manipulate.

        Currently, used only in calibredb list, the viewer, edit book, compare_format to original format, open with and
        the catalogs (via get_data_as_dict()).
        Apart from the viewer, open with and edit book, I don't believe any of the others do any file write I/O with the
        results of this call.
        Edit will be moved over to editing a copy - this is a calibre compatibility thing.

        WARNING! In calibre, this function will return a path to the actual book.
        In LiuXin this method returns a copy in a scratch folder.
        You will need to upload the book back to the folder store after you've finished IO with it.
        There was no good way to expose books across different types of folder stores.
        This technique seems to be the least bad - but I would not defend it as good.
        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @api
    def save_original_format(self, book_id: int, fmt: str) -> bool:
        """
        Save a copy of the specified format as ORIGINAL_FORMAT, overwriting any existing ORIGINAL_FORMAT.

        ORIGINAL_FMT is added to the cache.
        Reference will be made the format that was originally backed up - EPUB_1 would be backed up as
        ORIGINAL_EPUB_1.
        Calling this method with a bare format, e.g. "EPUB" will return a copy of the highest priority epub file.
        :param book_id:
        :param fmt:
        :return status: Was the backup successful?
        """
        raise NotImplementedError

    # Todo: Book id, format marker, e.t.c should be their own classes
    @api
    def restore_original_format(self, book_id: int, original_fmt: str) -> bool:
        """
        Restore the specified format from the previously saved ORIGINAL_FORMAT, if any.

        Return True on success.
        The ORIGINAL_FORMAT is deleted after a successful restore.
        ORIGINAL_FMT should be an ORIGINAL_FMT string - e.g. something of the form ORIGINAL_EPUB_1 e.t.c
        If just "ORIGINAL_EPUB" is passed, this method will fail as it's ambiguous.
        :param book_id:
        :param original_fmt:
        :return:
        """
        raise NotImplementedError

    @read_api
    def formats(self, book_id, verify_formats=True):
        """
        Return tuple of all formats for the specified book. If verify_formats is True, verifies that the files exist on
        disk.
        :param book_id: The book to return the formats list for
        :param verify_formats:
        :return:
        """
        raise NotImplementedError

    @api
    def format(
        self, book_id: int, fmt: str, as_file: bool = False, as_path: str = False, preserve_filename: bool = False
    ) -> bytes:
        """
        Return the ebook format as a bytestring or `None` if it doesn't exist, or we can't read the file.

        E.g. if we do not have read permissions on the file.
        :param book_id: Id of the book to read the format from.
        :param fmt: Format string - bare or with priority.
        :param as_file: If True the ebook format is returned as a file object. Note that the file object is a
                        SpooledTemporaryFile, so if what you want to do is copy the format to another file, use
                        :meth:`copy_format_to` instead for performance.
        :param as_path: Copies the format file to a temp file and returns the path to the temp file
        :param preserve_filename: If True and returning a path the filename is the same as that used in the library.
                                  Note that using this means that repeated calls yield the same temp file
                                  (which is re-created each time)
        :return:
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

        Return True of the format was added successfully.
        Format will be added to the book with the highest priority - all other formats will be relegated.
        If the fmt is given in the form of a priority fmt (e.g EPUB_1) then, if replace is True, that fmt will be
        replaced. If not returns False.

        :param replace: If True replace the existing highest priority fmt - unless another format is specified.
                        E.g. calling with "EPUB" will replace "EPUB_1"
                        E.g. calling with "EPUB_3" will replace "EPUB_3" - if it exists.
        :param run_hooks: If True, file type plugins are run on the format before and after being added.
        :param dbapi: Internal use only.
        :param book_id:
        :param fmt:
        :param stream_or_path:
        :param replace:
        :param run_hooks:
        :param dbapi:
        :return:
        """
        raise NotImplementedError

    # Todo: Add capability to not track a file in the folder system
    @write_api
    def remove_formats(self, formats_map: dict[int, str], db_only: bool = False) -> bool:
        """
        Remove the specified formats from the specified books.

        :param formats_map: A mapping of book_id to a list of formats to be removed from the book.
        :param db_only: If True, only remove the record for the format from the db, do not delete the actual format file
                        from the FolderStore.
                        Files removed in this way will be marked as "untracked".
        :return status: Was the operation successful?
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

        :param book_ids:
        :param mark_as_dirtied:
        :return status: Did the operation succeed?
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

        By default, returns the cover as a bytestring.
        WARNING: Using as_path will copy the cover to a temp file and return the path to the temp file.
        You should delete the temp file when you are done with it.
        :param book_id:
        :param as_file: If True return the image as an open file object (a SpooledTemporaryFile)
        :param as_image: If True return the image as a QImage object
        :param as_path: If True return the image as a path pointing to a temporary file
        :return:
        """
        raise NotImplementedError

    @read_api
    def cover_or_cache(self, book_id: int, timestamp: int) -> tuple[bool, bytes, int]:
        """
        Provides a tuple of information as to if to read from the cache or read from the folder store cache.

        See backend.cover_or_cache method.
        :param book_id:
        :param timestamp: Internally, LiuXin uses epoch time in nanoseconds.
        :return (read_status, cover_data, new_timestamp):
        """
        raise NotImplementedError

    @read_api
    def cover_last_modified(self, book_id: int) -> int:
        """
        When was the primary cover for a given book last modified.

        :param book_id:
        :return timestamp: Timestamp of last modification in epoch time.
        """
        raise NotImplementedError

    @read_api
    def copy_cover_to(
        self, book_id: int, dest: Union[str, BinaryIO], use_hardlink: bool = False, report_file_size=None
    ) -> bool:
        """
        Copy the cover to the file like object ``dest``.

        Returns False if no cover exists or dest is the same file as the current cover.
        dest can also be a path in which case the cover is copied to it if and only if the path is different from the
        current path (taking case sensitivity into account).
        :param book_id:
        :param dest:
        :param use_hardlink:
        :param report_file_size:
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_cover(self, book_id_data_map: dict[int : Optional[Union[str, bytes]]]) -> bool:
        """
        Set the covers for a number of books.

        data can be either a QImage, QPixmap, file object or bytestring.
        It can also be None, in which case any existing cover is removed.
        :param book_id_data_map:
        :return status:
        """
        raise NotImplementedError

    @write_api
    def add_cover_cache(self, cover_cache) -> bool:
        """
        Adds a cover_cache object to the set of internal cover caches.

        Allows multiple cover caches to be used at the same time. Which ... could be useful. I guess?
        :param cover_cache:
        :return status: Was the cover cache successfully registered?
        """
        raise NotImplementedError

    @write_api
    def remove_cover_cache(self, cover_cache) -> bool:
        """
        Remove a registered cover cache from the system.

        :param cover_cache:
        :return status: Was the cover cache successfully de-registered?
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

        Raises KeyError if the name of the preference is not known to the system.
        :param name: Name of the preference to return
        :param default:
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_pref(self, name: str, val: Any) -> Any:
        """
        Set the specified preference to the specified value.

        See also :meth:`pref`.
        :param name:
        :param val:
        :return:
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
        :param vl:
        :param search_restriction:
        :return:
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
        :param sort:
        :param book_ids:
        :param already_fixed:
        :param first_letter_sort:
        :return:
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

        :param book_ids:
        :return status: Where the given book book_ids successfully marked as dirty?
        """
        raise NotImplementedError

    @write_api
    def commit_dirty_cache(self) -> bool:
        """
        Write the current dirtied cache out of the database.

        :return: Did the database update write successfully?
        """
        raise NotImplementedError

    @read_api
    def get_a_dirtied_book(self) -> int:
        """
        Return a dirty book randomly selected from the dirtied_cache.

        Used by the maintenance methods.
        :return book_id: The id of a dirtied book
        """
        raise NotImplementedError

    @write_api
    def clear_dirtied(self, book_id: int, sequence):
        """
        Clear the dirtied indicator for the given book.

        This is used when fetching metadata, creating an OPF, and writing a file e.t.c. are separated into steps.
        The last step is clearing the indicator
        :param book_id:
        :param sequence:
        :return:
        """
        raise NotImplementedError

    @read_api
    def dirty_queue_length(self) -> int:
        """
        The current size of the dirtied cache.

        :return:
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

        :param book_id:
        :param raw:
        :return:
        """
        raise NotImplementedError

    @read_api
    def read_backup(self, book_id):
        """
        Return the OPF metadata backup for the book's folder as a bytestring or None if no such backup exists.

        :param book_id:
        :return:
        """
        raise NotImplementedError

    @write_api
    def dump_metadata(
        self, book_ids: Optional[Iterable[str]] = None, remove_from_dirtied: bool = True, callback=None
    ) -> bool:
        """
        Write metadata for each record to an individual OPF file.

        If callback is not None, it is called once at the beginning with the number of book_ids being processed.
        And once for every book_id, with arguments (book_id, mi, ok).
        :param book_ids:
        :param remove_from_dirtied:
        :param callback:
        :return status: True if all writes succeeded - False otherwise
        """
        raise NotImplementedError

    @write_api
    def restore_book(self, book_id, mi, last_modified, path, formats):
        """
        Restore the book entry in the database for a book that already exists on the filesystem

        :param book_id:
        :param mi:
        :param last_modified:
        :param path:
        :param formats:
        :return:
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

        If delete_first is True, all previously stored data for name will be removed.
        :param name: The name of the custom data to write
        :param val_map: Keyed with the id of the book and valued with its value
        :param delete_first:
        :return status: True if writing succeeded, False otherwise
        """
        raise NotImplementedError

    @read_api
    def get_custom_book_data(
        self, name: str, book_ids: Iterable[int] = (), default: Optional[Any] = None
    ) -> dict[int, Any]:
        """
        Get data from the given book_ids for the given custom value name.

        By default, returns data for _all_ book_ids, pass in a list of book book_ids if you only want some data.
        Returns a map of book_id to values. If a particular value could not be decoded, uses default for it.
        :param name: The name of the
        :param book_ids:
        :param default:
        :return book_id_custom_val_map:
        """
        raise NotImplementedError

    @write_api
    def delete_custom_book_data(self, name: str, book_ids: Iterable[int] = ()) -> bool:
        """
        Delete data for name.

        Defaults to deleting all data, if you only want to delete data for some book book_ids, pass in a
        list of book book_ids.
        :param name:
        :param book_ids:
        :return status: Did deletion go through?
        """
        raise NotImplementedError

    @read_api
    def get_ids_for_custom_book_data(self, name: str) -> set[int]:
        """
        Return the set of book book_ids for which name has data.

        :param name:
        :return book_ids_with_data:
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

        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @read_api
    def has_conversion_options(self, book_ids: Iterable[int], fmt: str = "PIPE"):
        """
        Check to see if the given books have a designated conversion option.

        :param book_ids:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @write_api
    def delete_conversion_options(self, book_ids, fmt: str = "PIPE"):
        """
        Remove the conversion options from the given book_ids.

        :param book_ids:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_conversion_options(self, options, fmt="PIPE"):
        """
        Options must be a map of the form {book_id : conversion_options}.

        :param options:
        :param fmt:
        :return:
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

        :param label: You can uniquely identify the column either through a label, or it's number.
                      This allows you to declare the label for the column.
        :param name: The name of the column.
        :param datatype: The datatype of the column - must be one of the SQLite datatypes.
        :param is_multiple: Is the custom column multiple?
                            (multiple values for each book)
        :param editable: Is the column editable?
        :param display:
        :return column_num: Either the column number of the new column or False
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

        :param num: The number of the custom column.
                    This serves as the id of the custom column.
                    It might be better to call it "column_id" or something, but we're aiming for calibre compatibility.
        :param name: New name for the column - or None if there isn't going to be an update.
        :param label: New label for the column - or None if there isn't going to be an update.
        :param is_editable: Set the column as editable or not.
        :param display: Optionally set the display name for the column.
        :param update_last_modified:
        :return status: Did the update go through?
        """
        raise NotImplementedError

    @write_api
    def delete_custom_column(self, label: str = None, num: int = None) -> bool:
        """
        Remove a custom column set for the books table.

        You must provide either of the label or the num.
        If you provide both, they should be consistent.
        :param label: The label identifying the custom column.
        :param num: The number identifying the custom column
        :return:
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

        :return:
        """
        raise NotImplementedError

    @write_api
    def move_library_to(self, newloc, progress=None, abort=None):
        """
        Not sure that there is a good way to implement this - and if a plugin is using this I have questions.

        :param newloc:
        :param progress:
        :param abort:
        :return:
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

        Can fix consistency problems.
        Does not always make sense with every database backend - or might be a really bad idea.
        :param callback: Progress indicator.
        :param sql:
        :return:
        """
        raise NotImplementedError

    @write_api
    def vacuum(self) -> bool:
        """
        Preforming vacuum (or equivalent) - an SQL maintenance task.

        :return status: Did the vacuum succeed?
        """
        raise NotImplementedError

    @write_api
    def close(self):
        """
        Close the database connection.

        :return status: Did we manage to close the database?
        """
        raise NotImplementedError

    @read_api
    def export_library(self, library_key, exporter, progress=None, abort=None):
        """
        Save the database in some format - this will depend on the exporter function used.

        :param library_key:
        :param exporter:
        :param progress:
        :param abort:
        :return:
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

        :param book_ids:
        :return:
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

        User categories are a custom thing which can be set on a book by book basis.
        proxy_metadata_map is optional and is useful for a
        performance boost, in contexts where a ProxyMetadata object for the books already exists.
        It should be a mapping of book_ids to their corresponding ProxyMetadata objects.
        :param book_ids:
        :param proxy_metadata_map:
        :return:
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

        Changes the files on disk to include the new metadata.
        :param book_ids: The books to update with the new metadata
        :param only_fmts: Only preform updates for specific formats within those books.
                          Can be a collectio of format strings e.g. ("EPUB_1", ) - in which case only the highest
                          priority epub will be updated.
                          Or can be a generic format string e.g. ("EPUB", ) - in which case all EPUBs will be updated.
        :param report_error: A callback system to report on errors if something goes wrong.
        :param report_progress: A callback to report progress
        :return status: Did the embed run complete without error?
        """
        raise NotImplementedError

    @read_api
    def get_last_read_positions(self, book_id: int, fmt: str, user: str) -> bool:
        """
        Return the stored last read position for the book.

        Used to return to your last position on some devices and the ebook reader.
        :param book_id: The book to update
        :param fmt: The format in that book - must be a format string
        :param user:
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_last_read_position(self, book_id, fmt, user="_", device="_", cfi=None, epoch=None, pos_frac=0):
        """
        Update the last read position of a book on the database.

        :param book_id:
        :param fmt:
        :param user:
        :param device:
        :param cfi:
        :param epoch:
        :param pos_frac:
        :return:
        """
        raise NotImplementedError

    #
    # ------------------------------------------------------------------------------------------------------------------

    @read_api
    def pref(self, name: str, default: Optional[T] = None) -> T:
        """
        Return the value for the specified preference or ``default`` if the preference is not set.

        :param name: The name of the preference to get
        :param default: The default value for the preference - or None
        :return:
        """
        raise NotImplementedError

    @write_api
    def set_pref(self, name: str, val: Any) -> None:
        """
        Set the specified preference to the specified value. See also :meth:`pref`.

        :param name:
        :param val:
        :return:
        """
        raise NotImplementedError


class BaseCache(CacheAPI):
    def __init__(self, backend):
        """
        Add a backend to the cache class
        :param backend:
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

        This is necessary to prevent DowngradeLockErrors, which can happen when updating the search cache in
        the presence of composite columns. Updating the search cache holds an exclusive lock, but searching a composite
        column involves reading field values via ProxyMetadata which tries to get a shared lock.
        There may be other scenarios that trigger this as well.
        This property returns a new lock object on every access. This lock object is not recursive (for performance) and
        must only be used in a with statement as ``with cache.safe_read_lock:`` otherwise bad things will happen.
        :return:
        """
        return SafeReadLock(self.read_lock)

    #
    # ------------------------------------------------------------------------------------------------------------------
