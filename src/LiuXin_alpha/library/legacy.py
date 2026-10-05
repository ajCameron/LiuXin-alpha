#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Preserve the legacy library facade and compatibility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise legacy through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import traceback
import types
import warnings

from LiuXin_alpha.databases.adaptors import clean_identifier, get_series_values
from LiuXin_alpha.databases.utils import (
    _get_next_series_num_for_list,
    _get_series_values,
    cleanup_tags,
    get_data_as_dict,
)
from LiuXin_alpha.exceptions import NoSuchFormat
from LiuXin_alpha.folder_stores.location import Location
from LiuXin_alpha.ingest.adding import (
    add_catalog,
    add_news,
    find_books_in_directory,
    import_book_directory,
    import_book_directory_multiple,
    recursive_import,
)
from LiuXin_alpha.library.backend import DB
from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
from LiuXin_alpha.library.caches.calibre.view import CalibreView
from LiuXin_alpha.metadata import validate_identifier
from LiuXin_alpha.surfaces.categories import CATEGORY_SORTS
from LiuXin_alpha.utils.calibre import force_unicode
from LiuXin_alpha.utils.date import utcnow
from LiuXin_alpha.utils.icu import lower as icu_lower
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems
from LiuXin_alpha.utils.logger import default_log
from LiuXin_alpha.utils.search_query_parser import set_saved_searches
from past.builtins import basestring

# In the nicest possible way, yet another ------- database interface.
# Built on the backend, which is, in turn, built on the database.
# This is used by the calibre gui to access the database. Provides some extra utility functions.


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


# Todo: Surely should be stored with the backend DB class?
def create_backend(
    library_path,
    default_prefs=None,
    read_only=False,
    progress_callback=lambda x, y: True,
    restore_all_prefs=False,
    load_user_formatter_functions=True,
    needs_create=False,
    existing_fsm=None,
):
    """
    Create a DB object from LiuXin.databases.backend - this adds additional methods to the base DatabasePing located as LiuXin.databases.database.

    Example:
        Exercise create backend through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param library_path: Value supplied for library path under the utility contract.
    :param default_prefs: Value supplied for default prefs under the utility contract.
    :param read_only: Value supplied for read only under the utility contract.
    :param progress_callback: Value supplied for progress callback under the utility
        contract.
    :param restore_all_prefs: Value supplied for restore all prefs under the utility
        contract.
    :param load_user_formatter_functions: Value supplied for load user formatter
        functions under the utility contract.
    :param needs_create: Value supplied for needs create under the utility contract.
    :param existing_fsm: Value supplied for existing fsm under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return DB(
        library_path,
        default_prefs=default_prefs,
        read_only=read_only,
        restore_all_prefs=restore_all_prefs,
        progress_callback=progress_callback,
        load_user_formatter_functions=load_user_formatter_functions,
        create=needs_create,
        with_cache=False,
    )


class LibraryDatabase(object):
    """
    In calibre this emulates the old LibraryDatabase2 interface. This is the interface used by the gui. Provides a number of useful methods.

    Example:
        Exercise LibraryDatabase through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    PATH_LIMIT = DB.PATH_LIMIT
    WINDOWS_LIBRARY_PATH_LIMIT = DB.WINDOWS_LIBRARY_PATH_LIMIT
    CATEGORY_SORTS = CATEGORY_SORTS
    MATCH_TYPE = ("any", "all")
    CUSTOM_DATA_TYPES = frozenset(
        [
            "rating",
            "text",
            "comments",
            "datetime",
            "int",
            "float",
            "bool",
            "series",
            "composite",
            "enumeration",
        ]
    )

    @classmethod
    def exists_at(cls, path):
        """
        Checks to see if the library exists at a specific path.

        Example:
            Exercise LibraryDatabase.exists at through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return path and os.path.exists(os.path.join(path, "metadata.db"))

    def __init__(
        self,
        library_path,
        default_prefs=None,
        read_only=False,
        is_second_db=False,
        progress_callback=lambda x, y: True,
        restore_all_prefs=False,
        existing_fsm=None,
    ):
        """
        Startup the database class.

        Example:
            Exercise LibraryDatabase.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param library_path: Value supplied for library path under the utility contract.
        :param default_prefs: Value supplied for default prefs under the utility contract.
        :param read_only: Value supplied for read only under the utility contract.
        :param is_second_db: Value supplied for is second db under the utility contract.
        :param progress_callback: Value supplied for progress callback under the utility
            contract.
        :param restore_all_prefs: Value supplied for restore all prefs under the utility
            contract.
        :param existing_fsm: Value supplied for existing fsm under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if read_only:
            raise NotImplementedError("Currently opens a scratch copy of the database - which is not quite read only")

        if os.path.isdir(library_path):
            library_path = os.path.join(library_path, "liuxin_metadata.db")

            info_str = (
                "legacy:LibraryDatabase was passed a path to a folder - joining it with the name of the "
                "default LiuXin database"
            )
            default_log.log_variables(info_str, "INFO", ("library_path", library_path))

        needs_create = not (os.path.exists(library_path) and os.path.isfile(library_path))

        self.is_second_db = is_second_db
        self.listeners = set()

        # Initialize the backend - creating the database if required
        backend = self.backend = create_backend(
            library_path,
            default_prefs=default_prefs,
            read_only=read_only,
            restore_all_prefs=restore_all_prefs,
            progress_callback=progress_callback,
            load_user_formatter_functions=not is_second_db,
            needs_create=needs_create,
        )

        # Store an internal reference to the folder store manager which the database uses to manipulate database assets
        self.fsm = self.backend.fsm

        # Read data from the backend into the cache
        cache = self.new_api = CalibreCache(backend)
        cache.init()

        # Initialize the View with the cache
        self.data = CalibreView(cache)

        # Copy methods to this class to tidy up the interface
        self.id = self.data.index_to_id
        self.row = self.data.id_to_index
        for x in (
            "get_property",
            "count",
            "refresh_ids",
            "set_marked_ids",
            "multisort",
            "search",
            "search_getting_ids",
        ):
            setattr(self, x, getattr(self.data, x))
        self.is_case_sensitive = getattr(backend, "is_case_sensitive", False)
        self.custom_field_name = backend.custom_columns.custom_field_name

        self.last_update_check = self.last_modified()

        # If this is the primary db set the global saved searches
        if not self.is_second_db:
            set_saved_searches(self, "saved_searches")

    def close(self):
        """
        Close the cache connection to the database.

        Example:
            Exercise LibraryDatabase.close through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.close()

    def break_cycles(self):
        """
        Preform actual shutdown tasks - release elements so that they can be deleted - then preform the delete.

        Example:
            Exercise LibraryDatabase.break cycles through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        delattr(self.backend, "field_metadata")
        self.data.cache.backend = None
        self.data.cache = None
        for x in (
            "data",
            "backend",
            "new_api",
            "listeners",
        ):
            delattr(self, x)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - LIBRARY WIDE PROPERTIES {{{

    @property
    def prefs(self):
        """
        Returns a DBPrefs object representing preferences stored in the library database.

        Example:
            Exercise LibraryDatabase.prefs through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.backend.prefs

    @property
    def field_metadata(self):
        """
        Perform the field metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.field metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.field_metadata

    @property
    def user_version(self):
        """
        Perform the user version operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.user version through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.user_version

    @property
    def library_id(self):
        """
        Perform the library id operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.library id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.library_id

    @property
    def library_path(self):
        """
        Perform the library path operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.library path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.library_path

    @property
    def dbpath(self):
        """
        Perform the dbpath operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.dbpath through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.dbpath

    def last_modified(self):
        """
        Perform the last modified operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.last_modified()

    def check_if_modified(self):
        """
        Perform the check if modified operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.check if modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.last_modified() > self.last_update_check:
            self.backend.reopen()
            self.new_api.reload_from_db()
            self.data.refresh(clear_caches=False)  # caches are already cleared by reload_from_db()
        self.last_update_check = utcnow()

    # Todo: Remove this from the backend - it's all been shifted over to the cache
    @property
    def custom_column_num_map(self):
        """
        Perform the custom column num map operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.custom column num map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.custom_column_num_map

    # Todo: Remove this from the backend - it's all been shifted over to the cache
    @property
    def custom_column_label_map(self):
        """
        Perform the custom column label map operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.custom column label map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.backend.custom_column_label_map

    # Todo: Remove this from the backend - it's all been shifted over to the cache
    @property
    def FIELD_MAP(self):
        """
        Perform the FIELD MAP operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.FIELD MAP through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.FIELD_MAP

    @property
    def formatter_template_cache(self):
        """
        Perform the formatter template cache operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.formatter template cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data.cache.formatter_template_cache

    def initialize_template_cache(self):
        """
        Perform the initialize template cache operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.initialize template cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.data.cache.initialize_template_cache()

    def all_ids(self):
        """
        All book ids in the db. This cannot be a generator because of db locking.

        Example:
            Exercise LibraryDatabase.all ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return tuple(self.new_api.all_book_ids())

    def is_empty(self):
        """
        Return whether is empty holds for the supplied ebook data.

        Example:
            Exercise LibraryDatabase.is empty through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: True when the documented condition holds; otherwise False.
        """
        with self.new_api.safe_read_lock:
            return not bool(self.new_api.fields["title"].table.book_col_map)

    def get_usage_count_by_id(self, field):
        """
        Return usage count by id under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get usage count by id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [[k, v] for k, v in iteritems(self.new_api.get_usage_count_by_id(field))]

    def field_id_map(self, field):
        """
        Perform the field id map operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.field id map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [(k, v) for k, v in iteritems(self.new_api.get_id_map(field))]

    def get_custom_items_with_ids(self, label=None, num=None):
        """
        Return custom items with ids under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get custom items with ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return [[k, v] for k, v in iteritems(self.new_api.get_id_map(self.custom_field_name(label, num)))]
        except ValueError:
            return []

    def refresh(self, field=None, ascending=True):
        """
        Perform the refresh operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.refresh through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param ascending: Value supplied for ascending under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.data.refresh(field=field, ascending=ascending)

    def get_id_from_uuid(self, uuid):
        """
        Return id from uuid under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get id from uuid through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param uuid: Value supplied for uuid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if uuid:
            return self.new_api.lookup_by_uuid(uuid)

    def add_listener(self, listener):
        """
        Add a listener. Will be called on change events with two arguments. Event name and list of affected ids.

        Example:
            Exercise LibraryDatabase.add listener through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param listener: Value supplied for listener under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.listeners.add(listener)

    def notify(self, event, ids=()):
        """
        Notify all listeners

        Example:
            Exercise LibraryDatabase.notify through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param event: Value supplied for event under the utility contract.
        :param ids: Value supplied for ids under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for listener in self.listeners:
            try:
                listener(event, ids)
            except Exception as e:
                # Notify the main log that updating a listener has failed
                info_str = "Unable to notify listener - has it shutdown?"
                default_log.log_exception(info_str, e, "INFO")

                # Print the exception to screen
                traceback.print_exc()
                continue

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - PATH OPERATIONS {{{

    # Todo: Need a method to get the bad paths - paths which the system SHOULD NOT import from -
    # Todo: Write this method
    def check_import_path(self, path):
        """
        Checks that the given path doesn't include any of the bad paths - such as folder stores, scratch and the database.

        Example:
            Exercise LibraryDatabase.check import path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # lp = os.path.normcase(os.path.abspath(self.gui.current_db.library_path))
        # if lp.startswith(os.path.normcase(os.path.abspath(root)) + os.pathsep):
        return True

    def path(self, index, index_is_id=False, all_paths=False):
        """
        Return the relative path to the directory containing this books files as a unicode string.

        Example:
            Exercise LibraryDatabase.path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param all_paths: Value supplied for all paths under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        cand_paths = self.new_api.field_for("path", book_id, default_value="")
        if not cand_paths:
            return ""

        if all_paths:
            return cand_paths
        else:
            try:
                cand_path = cand_paths[0]
                if isinstance(cand_path, basestring):
                    return cand_path.replace("/", os.sep)
                elif isinstance(cand_path, Location):
                    return cand_path.get_db_path_string()
                else:
                    raise NotImplementedError("This type of cand_path cannot be parsed - {}".format(type(cand_path)))
            except IndexError:
                return ""
            except KeyError:
                return ""

    def abspath(self, index, index_is_id=False, create_dirs=True):
        """
        Return the absolute path to the directory containing this books files as a unicode string.

        Example:
            Exercise LibraryDatabase.abspath through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param create_dirs: Value supplied for create dirs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Check to see if paths for a folder associated with the book already exist - if not they will have to be
        # created
        cand_paths = self.path(index, index_is_id=index_is_id)
        if not cand_paths:
            if create_dirs:
                raise NotImplementedError
        else:
            return cand_paths[0]

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Adding books {{{

    def create_book_entry(self, mi, cover=None, add_duplicates=True, force_id=None):
        """
        Create an entry for a blank book - create using the metadata object.

        Example:
            Exercise LibraryDatabase.create book entry through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :param cover: Value supplied for cover under the utility contract.
        :param add_duplicates: Value supplied for add duplicates under the utility contract.
        :param force_id: Value supplied for force id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ret = self.new_api.create_book_entry(mi, cover=cover, add_duplicates=add_duplicates, force_id=force_id)
        if ret is not None:
            self.data.books_added((ret,))
        return ret

    def add_books(self, paths, formats, metadata, add_duplicates=True, return_ids=False):
        """
        Add a collection of books to the database.

        Example:
            Exercise LibraryDatabase.add books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param paths: Value supplied for paths under the utility contract.
        :param formats: Value supplied for formats under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :param add_duplicates: Value supplied for add duplicates under the utility contract.
        :param return_ids: Value supplied for return ids under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        books = [(mi, {fmt: path}) for mi, path, fmt in zip(metadata, paths, formats)]
        book_ids, duplicates = self.new_api.add_books(books, add_duplicates=add_duplicates, dbapi=self)
        if duplicates:
            paths, formats, metadata = [], [], []
            for mi, format_map in duplicates:
                metadata.append(mi)
                for fmt, path in iteritems(format_map):
                    formats.append(fmt)
                    paths.append(path)
            duplicates = (paths, formats, metadata)
        ids = book_ids if return_ids else len(book_ids)
        # Extend the cache with the new books
        if book_ids:
            self.data.books_added(book_ids)
        return duplicates or None, ids

    def import_book(
        self,
        mi,
        formats,
        notify=True,
        import_hooks=True,
        apply_import_tags=True,
        preserve_uuid=False,
    ):
        """
        Import a book from a calibre/LiuXin dictionary.

        Example:
            Exercise LibraryDatabase.import book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :param formats: Value supplied for formats under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param import_hooks: Value supplied for import hooks under the utility contract.
        :param apply_import_tags: Value supplied for apply import tags under the utility
            contract.
        :param preserve_uuid: Value supplied for preserve uuid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        format_map = {}
        for path in formats:
            ext = os.path.splitext(path)[1][1:].upper()
            if ext == "OPF":
                continue
            format_map[ext] = path
        book_ids, duplicates = self.new_api.add_books(
            [(mi, format_map)],
            add_duplicates=True,
            apply_import_tags=apply_import_tags,
            preserve_uuid=preserve_uuid,
            dbapi=self,
            run_hooks=import_hooks,
        )
        # Extend the cache with the new books
        if book_ids:
            self.data.books_added(book_ids)
        if notify:
            self.notify("add", book_ids)
        return book_ids[0]

    @staticmethod
    def find_books_in_directory(dirpath, single_book_per_directory=False, compiled_rules=()):
        """
        Iterates through a directory tree, finding all the books in it to later add.

        Example:
            Exercise LibraryDatabase.find books in directory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dirpath: Value supplied for dirpath under the utility contract.
        :param single_book_per_directory: Value supplied for single book per directory under
            the utility contract.
        :param compiled_rules: Value supplied for compiled rules under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return find_books_in_directory(dirpath, single_book_per_directory, compiled_rules=compiled_rules)

    def import_book_directory_multiple(self, dirpath, callback=None, added_ids=None, compiled_rules=()):
        """
        Import books from a single directory containing multiple books.

        Example:
            Exercise LibraryDatabase.import book directory multiple through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dirpath: Value supplied for dirpath under the utility contract.
        :param callback: Value supplied for callback under the utility contract.
        :param added_ids: Value supplied for added ids under the utility contract.
        :param compiled_rules: Value supplied for compiled rules under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return import_book_directory_multiple(
            self,
            dirpath,
            callback=callback,
            added_ids=added_ids,
            compiled_rules=compiled_rules,
        )

    def import_book_directory(self, dirpath, callback=None, added_ids=None, compiled_rules=()):
        """
        Import a book from a directory - assuming that the book contains a single book.

        Example:
            Exercise LibraryDatabase.import book directory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dirpath: Value supplied for dirpath under the utility contract.
        :param callback: Value supplied for callback under the utility contract.
        :param added_ids: Value supplied for added ids under the utility contract.
        :param compiled_rules: Value supplied for compiled rules under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return import_book_directory(
            self,
            dirpath,
            callback=callback,
            added_ids=added_ids,
            compiled_rules=compiled_rules,
        )

    def recursive_import(
        self,
        root,
        single_book_per_directory=False,
        callback=None,
        added_ids=None,
        compiled_rules=(),
    ):
        """
        Recursively import - walk a tree and import any directory that looks like a book.

        Example:
            Exercise LibraryDatabase.recursive import through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param root: Root directory that bounds path resolution or traversal.
        :param single_book_per_directory: Value supplied for single book per directory under
            the utility contract.
        :param callback: Value supplied for callback under the utility contract.
        :param added_ids: Value supplied for added ids under the utility contract.
        :param compiled_rules: Value supplied for compiled rules under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return recursive_import(
            self,
            root,
            single_book_per_directory=single_book_per_directory,
            callback=callback,
            added_ids=added_ids,
            compiled_rules=compiled_rules,
        )

    def add_catalog(self, path, title):
        """
        Add a catalog of books to the database.

        Example:
            Exercise LibraryDatabase.add catalog through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param title: Value supplied for title under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id, new_book_added = add_catalog(self.new_api, path, title, dbapi=self)
        if book_id is not None and new_book_added:
            self.data.books_added((book_id,))
        return book_id

    def add_news(self, path, arg):
        """
        Add a item of news to the database.

        Example:
            Exercise LibraryDatabase.add news through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param arg: Value supplied for arg under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = add_news(self.new_api, path, arg, dbapi=self)
        if book_id is not None:
            self.data.books_added((book_id,))
        return book_id

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Deleting books

    def delete_book(self, book_id, notify=True, commit=True, permanent=False, do_clean=True):
        """
        Remove books from the database.

        Example:
            Exercise LibraryDatabase.delete book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param permanent: Value supplied for permanent under the utility contract.
        :param do_clean: Value supplied for do clean under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Check for use of the depreciated variables
        if not commit:
            warnings.warn("Depreciation - variable no longer in use")
        if not do_clean:
            warnings.warn("Depreciation - variable no longer in use")

        self.new_api.remove_books((book_id,), permanent=permanent)
        self.data.books_deleted((book_id,))
        if notify:
            self.notify("delete", [book_id])

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Custom data {{{
    def add_custom_book_data(self, book_id, name, val):
        """
        Add data to the custom books table.

        Example:
            Exercise LibraryDatabase.add custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.add_custom_book_data(name, {book_id: val})

    def add_multiple_custom_book_data(self, name, val_map, delete_first=False):
        """
        Add multiple instances of the same type of data to a number of books

        Example:
            Exercise LibraryDatabase.add multiple custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val_map: Value supplied for val map under the utility contract.
        :param delete_first: Value supplied for delete first under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.add_custom_book_data(name, val_map, delete_first=delete_first)

    def get_custom_book_data(self, book_id, name, default=None):
        """
        Return custom book data for the given book_id.

        Example:
            Exercise LibraryDatabase.get custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.get_custom_book_data(name, book_ids={book_id}, default=default).get(book_id, default)

    def get_all_custom_book_data(self, name, default=None):
        """
        Return all custom book data under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get all custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.get_custom_book_data(name, default=default)

    def delete_custom_book_data(self, book_id, name):
        """
        Perform the delete custom book data operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.delete custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.delete_custom_book_data(name, book_ids=(book_id,))

    def delete_all_custom_book_data(self, name):
        """
        Perform the delete all custom book data operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.delete all custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.delete_custom_book_data(name)

    def get_ids_for_custom_book_data(self, name):
        """
        Return ids for custom book data under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get ids for custom book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(self.new_api.get_ids_for_custom_book_data(name))

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Cover Methods - {{{

    def cover(self, index, index_is_id=False, as_file=False, as_image=False, as_path=False):
        """
        Returns a cover as a stream.

        Example:
            Exercise LibraryDatabase.cover through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param as_file: Value supplied for as file under the utility contract.
        :param as_image: Value supplied for as image under the utility contract.
        :param as_path: Value supplied for as path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.cover(book_id, as_file=as_file, as_image=as_image, as_path=as_path)

    def copy_cover_to(
        self,
        index,
        dest,
        index_is_id=False,
        windows_atomic_move=None,
        use_hardlink=False,
    ):
        """
        Copy the cover of a book to a location.

        Example:
            Exercise LibraryDatabase.copy cover to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param windows_atomic_move: Value supplied for windows atomic move under the utility
            contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if windows_atomic_move is not None:
            warnings.warn("Depreciation warning - Window atomic move no longer in use")

        book_id = index if index_is_id else self.id(index)
        return self.new_api.copy_cover_to(book_id, dest, use_hardlink=use_hardlink)

    def cover_last_modified(self, index, index_is_id=False):
        """
        Get the cover last modification time.

        Example:
            Exercise LibraryDatabase.cover last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.cover_last_modified(book_id) or self.last_modified()

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Formats methods - {{{

    def add_format(
        self,
        index,
        fmt,
        stream,
        index_is_id=False,
        path=None,
        notify=True,
        replace=True,
        copy_function=None,
    ):
        """
        Add a format to the library - associate it with a book with the given index. Import hooks will not be run - thus the format won't be processed as it's added path and copy_function are ignored by the new API

        Example:
            Exercise LibraryDatabase.add format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param notify: Value supplied for notify under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :param copy_function: Value supplied for copy function under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if path is not None:
            warnings.warn("Depreciation warning - path variable no longer in use")
        if copy_function is not None:
            warnings.warn("Depreciation warning - copy_function variable no longer in use")
        if not notify:
            warnings.warn("Depreciation warning - copy_function parameter no longer in use")

        book_id = index if index_is_id else self.id(index)
        ret = self.new_api.add_format(book_id, fmt, stream, replace=replace, run_hooks=False, dbapi=self)
        self.notify("metadata", [book_id])
        return ret

    def add_format_with_hooks(self, index, fmt, fpath, index_is_id=False, path=None, notify=True, replace=True):
        """
        Add the format - with a run of the post import hooks. path is ignored by the new API

        Example:
            Exercise LibraryDatabase.add format with hooks through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param fpath: Value supplied for fpath under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param notify: Value supplied for notify under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if path is not None:
            warnings.warn("Depreciation warning - variable path no longer in use")
        if not notify:
            warnings.warn("Depreciation warning - copy_function parameter no longer in use")

        book_id = index if index_is_id else self.id(index)
        ret = self.new_api.add_format(book_id, fmt, fpath, replace=replace, run_hooks=True, dbapi=self)
        self.notify("metadata", [book_id])
        return ret

    def copy_format_to(
        self,
        index,
        fmt,
        dest,
        index_is_id=False,
        windows_atomic_move=None,
        use_hardlink=False,
    ):
        """
        Copy/download a format from the library to a location. Downloads the current primary format of that type, for that book to the location.

        Example:
            Exercise LibraryDatabase.copy format to through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param dest: Value supplied for dest under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param windows_atomic_move: Value supplied for windows atomic move under the utility
            contract.
        :param use_hardlink: Value supplied for use hardlink under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if windows_atomic_move is not None:
            warnings.warn("Depreciation warning - windows_atomic_move no longer needed")

        book_id = index if index_is_id else self.id(index)
        return self.new_api.copy_format_to(book_id, fmt, dest, use_hardlink=use_hardlink)

    # }}}
    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Conversion options API

    def conversion_options(self, book_id, fmt):
        """
        Returns the conversion options for the given format for the given book_id.

        Example:
            Exercise LibraryDatabase.conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.conversion_options(book_id, fmt=fmt)

    def has_conversion_options(self, ids, fmt="PIPE"):
        """
        Checks to see if the given conversion option exists for any of the given ids.

        Example:
            Exercise LibraryDatabase.has conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids: Value supplied for ids under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        return self.new_api.has_conversion_options(ids, fmt=fmt)

    def delete_conversion_options(self, book_id, fmt, commit=True):
        """
        Remove a conversion option.

        Example:
            Exercise LibraryDatabase.delete conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - parameter commit is no longer in use")

        self.new_api.delete_conversion_options((book_id,), fmt=fmt)

    def set_conversion_options(self, book_id, fmt, options):
        """
        Set or update the conversion options.

        Example:
            Exercise LibraryDatabase.set conversion options through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param options: Value supplied for options under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.set_conversion_options({book_id: options}, fmt=fmt)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Dirtied book API
    # - Dirtied books are books that have been changed on the database - this is important for things like updating the
    #   metadata - if a book is dirtied then it's metadata needs to be updated as well

    def dirtied(self, book_ids, commit=True):
        """
        Mark all the books with the given book_ids as dirtied.

        Example:
            Exercise LibraryDatabase.dirtied through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit variable no longer in use")

        self.new_api.mark_as_dirty(frozenset(book_ids) if book_ids is not None else book_ids)

    def dirty_queue_length(self):
        """
        How many books are currently in the dirtied queue.

        Example:
            Exercise LibraryDatabase.dirty queue length through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.dirty_queue_length()

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METADATA APIS

    # Generic metadata API - {{{
    def dump_metadata(self, book_ids=None, remove_from_dirtied=True, commit=True, callback=None):
        """
        Preform a metadata dump - this writes metadata to an OPF file in the directory of the book.

        Example:
            Exercise LibraryDatabase.dump metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param remove_from_dirtied: Value supplied for remove from dirtied under the utility
            contract.
        :param commit: Value supplied for commit under the utility contract.
        :param callback: Value supplied for callback under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit parameter nbo longer used")

        self.new_api.dump_metadata(
            book_ids=book_ids,
            remove_from_dirtied=remove_from_dirtied,
            callback=callback,
        )

    def sort(self, field, ascending, subsort=False):
        """
        Preform a sort of the view of the books table.

        Example:
            Exercise LibraryDatabase.sort through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param ascending: Value supplied for ascending under the utility contract.
        :param subsort: Value supplied for subsort under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if subsort:
            warnings.warn("Depreciation user - subsort parameter is no longer in use")

        self.data.multisort([(field, ascending)])

    def get_field(self, index, key, default=None, index_is_id=False):
        """
        Return field under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get field through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        mi = self.new_api.get_metadata(book_id, get_cover=key == "cover")
        return mi.get(key, default)

    def set(self, index, field, val, allow_case_change=False):
        """
        Generic interface for set functionality.

        Example:
            Exercise LibraryDatabase.set through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param field: Metadata or template field addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = self.id(index)
        try:
            return self.new_api.set_field(field, {book_id: val}, allow_case_change=allow_case_change)
        finally:
            self.notify("metadata", [book_id])

    def set_metadata(
        self,
        book_id,
        mi,
        ignore_errors=False,
        set_title=True,
        set_authors=True,
        commit=True,
        force_changes=False,
        notify=True,
    ):
        """
        Update the metadata for a given book from a given API.

        Example:
            Exercise LibraryDatabase.set metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param ignore_errors: Value supplied for ignore errors under the utility contract.
        :param set_title: Value supplied for set title under the utility contract.
        :param set_authors: Value supplied for set authors under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param force_changes: Value supplied for force changes under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit variable no longer in use")

        self.new_api.set_metadata(
            book_id,
            mi,
            ignore_errors=ignore_errors,
            set_title=set_title,
            set_authors=set_authors,
            force_changes=force_changes,
        )
        if notify:
            self.notify("metadata", [book_id])

    # }}}

    # ------------------------------------------------------------------------------------------------------------------
    #  - AUTHOR METADATA FIELDS - {{{
    def authors_sort_strings(self, index, index_is_id=False):
        """
        Returns a list of all the author sort strings for a given book.

        Example:
            Exercise LibraryDatabase.authors sort strings through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return list(self.new_api.author_sort_strings_for_books((book_id,))[book_id])

    def author_sort_from_book(self, index, index_is_id=False):
        """
        Return the combined author sort string for a book.

        Example:
            Exercise LibraryDatabase.author sort from book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return " & ".join(self.authors_sort_strings(index, index_is_id=index_is_id))

    def authors_with_sort_strings(self, index, index_is_id=False):
        """
        Returns a list of the author data for the authors linked to the given book.

        Example:
            Exercise LibraryDatabase.authors with sort strings through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        with self.new_api.safe_read_lock:
            authors = self.new_api.unlock.field_ids_for("authors", book_id)
            adata = self.new_api.unlock.author_data(authors)
            return [(aid, adata[aid]["name"], adata[aid]["sort"], adata[aid]["link"]) for aid in authors]

    def set_sort_field_for_author(self, old_id, new_sort, commit=True, notify=False):
        """
        Set the sort field for an author.

        Example:
            Exercise LibraryDatabase.set sort field for author through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_id: Value supplied for old id under the utility contract.
        :param new_sort: Value supplied for new sort under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if commit:
            warnings.warn("Depreciation Warning - commit parameter is no longer used")

        changed_books = self.new_api.set_sort_for_authors({old_id: new_sort})
        if notify:
            self.notify("metadata", list(changed_books))

    def set_link_field_for_author(self, aid, link, commit=True, notify=False):
        """
        Set the link field for an author.

        Example:
            Exercise LibraryDatabase.set link field for author through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param aid: Value supplied for aid under the utility contract.
        :param link: Value supplied for link under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - parameter commit is no longer in use")

        changed_books = self.new_api.set_link_for_authors({aid: link})
        if notify:
            self.notify("metadata", list(changed_books))

    # }}} --------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Book on device API - {{{
    def book_on_device(self, book_id):
        """
        Are there currently files from the given book on a device?

        Example:
            Exercise LibraryDatabase.book on device through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self.new_api.safe_read_lock:
            return self.new_api.fields["ondevice"].book_on_device(book_id)

    def book_on_device_string(self, book_id):
        """
        Perform the book on device string operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.book on device string through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.field_for("ondevice", book_id)

    def set_book_on_device_func(self, func):
        """
        Set book on device func under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.set book on device func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param func: Value supplied for func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.fields["ondevice"].set_book_on_device_func(func)

    @property
    def book_on_device_func(self):
        """
        Perform the book on device func operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.book on device func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.new_api.fields["ondevice"].book_on_device_func

    # }}} --------------------------------------------------------------------------------------------------------------

    # Have considered moving these into the cache, but there is no need - keep the cache as minimal as possible to
    # avoid over complicating code associated with multithreading.

    # Identifiers API - {{{
    def set_identifier(self, book_id, typ, val, notify=True, commit=True):
        """
        Updates the identifier, of the given type, to the given value for the given book. Books is associated with the identifier with highest priority - if the identifier already exists then it's set as primary.

        Example:
            Exercise LibraryDatabase.set identifier through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if commit:
            warnings.warn("Depreciation error - variable commit is no longer used.")
        if not notify:
            warnings.warn("Depreciation error - variable notify is no longer used")

        with self.new_api.write_lock:

            id_field = self.new_api.fields["identifiers"]

            typ, val = clean_identifier(typ, val)
            if typ:
                validate_identifier(typ, val)
                id_field.set_identifier(book_id, typ, val)
        self.notify("metadata", [book_id])

    def set_identifiers(self, book_id, val, notify=True, commit=True, allow_case_change=False):
        """
        The set_identifiers method used to be generic - but it has now diverged so far from the original intent that it needs to be re-written for greater flexibility.

        Example:
            Exercise LibraryDatabase.set identifiers through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if commit:
            warnings.warn("Depreciation error - variable commit is no longer used.")
        if not notify:
            warnings.warn("Depreciation error - variable notify is no longer used")

        with self.new_api.write_lock:

            id_field = self.new_api.fields["identifiers"]

            cleaned_vals = self._id_val_dict_preflight(val)
            id_field.set_identifiers_from_set_dict(book_id=book_id, set_dict=cleaned_vals, db=self.backend)
        self.notify("metadata", [book_id])

    @staticmethod
    def _id_val_dict_preflight(val):
        """
        Bring the values dict which set_identifiers has been called with into a useful for (every id cleaned - the dictionary keyed with the type of the identifier and valued with a set of the id values.

        Example:
            Exercise LibraryDatabase. id val dict preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean_vals = dict()
        for typ, vals in iteritems(val):

            if isinstance(vals, basestring):
                clean_typ, clean_val = clean_identifier(typ, vals)
                clean_vals[clean_typ] = {
                    clean_val,
                }
            else:
                for one_val in vals:
                    clean_typ, clean_val = clean_identifier(typ, one_val)
                    if clean_typ in clean_vals:
                        clean_vals[clean_typ].add(clean_val)
                    else:
                        clean_vals[clean_typ] = {
                            clean_val,
                        }
        return clean_vals

    def set_isbn(self, book_id, isbn, notify=True, commit=True):
        """
        Set the ISBN for the given book. ISBN will be set with highest priority. Pass val=None to blank all the isbns associated with this book.

        Example:
            Exercise LibraryDatabase.set isbn through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param isbn: Value supplied for isbn under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.set_identifier(book_id, "isbn", isbn, notify=notify, commit=commit)

    # }}}

    # Tags API - {{{
    def set_tags(
        self,
        book_id,
        tags,
        append=False,
        notify=True,
        commit=True,
        allow_case_change=False,
    ):
        """
        Set the title tags for the given book_id.

        Example:
            Exercise LibraryDatabase.set tags through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param tags: Value supplied for tags under the utility contract.
        :param append: Value supplied for append under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit parameter is no longed used")

        tags = tags or []
        with self.new_api.write_lock:
            if append:
                otags = self.new_api.unlock.field_for("tags", book_id)
                existing = {icu_lower(x) for x in otags}
                tags = list(otags) + [x for x in tags if icu_lower(x) not in existing]
            ret = self.new_api.unlock.set_field("tags", {book_id: tags}, allow_case_change=allow_case_change)
        if notify:
            self.notify("metadata", [book_id])
        return ret

    def remove_all_tags(self, ids, notify=False, commit=True):
        """
        Removes all the tags from all the books with ids in the iterable of ids.

        Example:
            Exercise LibraryDatabase.remove all tags through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids: Value supplied for ids under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit parameter is no longer used")
        self.new_api.set_field("tags", {book_id: () for book_id in ids})
        if notify:
            self.notify("metadata", ids)

    def bulk_modify_tags(self, ids, add=(), remove=(), notify=False):
        """
        Convenience method for bulk modifying the tag content of books - allowing adding and removing tags from all given book ids.

        Example:
            Exercise LibraryDatabase.bulk modify tags through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids: Value supplied for ids under the utility contract.
        :param add: Value supplied for add under the utility contract.
        :param remove: Value supplied for remove under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._do_bulk_modify("tags", ids, add, remove, notify)

    def _do_bulk_modify(self, field, ids, add, remove, notify):
        """
        Perform the do bulk modify operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase. do bulk modify through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param ids: Value supplied for ids under the utility contract.
        :param add: Value supplied for add under the utility contract.
        :param remove: Value supplied for remove under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        add = cleanup_tags(add)
        remove = cleanup_tags(remove)
        remove = set(remove) - set(add)
        if not ids or (not add and not remove):
            return

        remove = {icu_lower(x) for x in remove}
        with self.new_api.write_lock:
            val_map = {}
            for book_id in ids:
                tags = list(self.new_api.unlock.field_for(field, book_id))
                existing = {icu_lower(x) for x in tags}
                tags.extend(t for t in add if icu_lower(t) not in existing)
                tags = tuple(t for t in tags if icu_lower(t) not in remove)
                val_map[book_id] = tags
            self.new_api.unlock.set_field(field, val_map, allow_case_change=False)

        if notify:
            self.notify("metadata", ids)

    def unapply_tags(self, book_id, tags, notify=True):
        """
        Remove all tags from the given iterable of tags from the book with the given book id.

        Example:
            Exercise LibraryDatabase.unapply tags through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param tags: Value supplied for tags under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bulk_modify_tags((book_id,), remove=tags, notify=notify)

    def is_tag_used(self, tag):
        """
        Check to see if the tag is currently linked to a title or not.

        Example:
            Exercise LibraryDatabase.is tag used through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag: Value supplied for tag under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        return icu_lower(tag) in {icu_lower(x) for x in self.new_api.all_field_names("tags")}

    def delete_tag(self, tag):
        """
        Remove a tag (specified with the tag text) from the tags table.

        Example:
            Exercise LibraryDatabase.delete tag through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.delete_tags((tag,))

    def delete_tags(self, tags):
        """
        Takes an iterable of tag strings and deletes them.

        Example:
            Exercise LibraryDatabase.delete tags through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tags: Value supplied for tags under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with self.new_api.write_lock:
            tag_map = {icu_lower(v): k for k, v in iteritems(self.new_api.unlock.get_id_map("tags"))}
            tag_ids = (tag_map.get(icu_lower(tag), None) for tag in tags)
            tag_ids = tuple(tid for tid in tag_ids if tid is not None)

            if tag_ids:
                self.new_api.unlock.remove_items("tags", tag_ids)

    # }}}

    def has_id(self, book_id):
        """
        Return whether has id holds for the supplied ebook data.

        Example:
            Exercise LibraryDatabase.has id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        return self.new_api.has_id(book_id)

    def format(
        self,
        index,
        fmt,
        index_is_id=False,
        as_file=False,
        mode="r+b",
        as_path=False,
        preserve_filename=False,
    ):
        """
        Perform the format operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param as_file: Value supplied for as file under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param as_path: Value supplied for as path under the utility contract.
        :param preserve_filename: Value supplied for preserve filename under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if mode != "r+b":
            warnings.warn("Depreciation warning - mode parameter is no longer used")
        book_id = index if index_is_id else self.id(index)
        return self.new_api.format(
            book_id,
            fmt,
            as_file=as_file,
            as_path=as_path,
            preserve_filename=preserve_filename,
        )

    def format_abspath(self, index, fmt, index_is_id=False):
        """
        Perform the format abspath operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.format abspath through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.format_abspath(book_id, fmt)

    def format_path(self, index, fmt, index_is_id=False):
        """
        Perform the format path operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.format path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        ans = self.new_api.format_abspath(book_id, fmt)
        if ans is None:
            raise NoSuchFormat("Record %d has no format: %s" % (book_id, fmt))
        return ans

    def format_files(self, index, index_is_id=False):
        """
        Perform the format files operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.format files through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return [(v, k) for k, v in iteritems(self.new_api.format_files(book_id))]

    def format_metadata(self, book_id, fmt, allow_cache=True, update_db=False, commit=False):
        """
        Returns the metadata for a format as a calibre metadata object.

        Example:
            Exercise LibraryDatabase.format metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param allow_cache: Value supplied for allow cache under the utility contract.
        :param update_db: Value supplied for update db under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not commit:
            warnings.warn("Depreciation warning - variable commit is no longer used")

        return self.new_api.format_metadata(book_id, fmt, allow_cache=allow_cache, update_db=update_db)

    def format_last_modified(self, book_id, fmt):
        """
        Perform the format last modified operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.format last modified through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        m = self.format_metadata(book_id, fmt)
        if m:
            return m["mtime"]

    def formats(self, index, index_is_id=False, verify_formats=True):
        """
        Perform the formats operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.formats through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param verify_formats: Value supplied for verify formats under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        ans = self.new_api.formats(book_id, verify_formats=verify_formats)
        if ans:
            return ",".join(ans)

    def has_format(self, index, fmt, index_is_id=False):
        """
        Return whether has format holds for the supplied ebook data.

        Example:
            Exercise LibraryDatabase.has format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.has_format(book_id, fmt)

    def refresh_format_cache(self):
        """
        Perform the refresh format cache operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.refresh format cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.refresh_format_cache()

    def refresh_ondevice(self):
        """
        Perform the refresh ondevice operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.refresh ondevice through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.refresh_ondevice()

    def tags_older_than(self, tag, delta, must_have_tag=None, must_have_authors=None):
        """
        Perform the tags older than operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.tags older than through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag: Value supplied for tag under the utility contract.
        :param delta: Value supplied for delta under the utility contract.
        :param must_have_tag: Value supplied for must have tag under the utility contract.
        :param must_have_authors: Value supplied for must have authors under the utility
            contract.
        :return: An iterator yielding the normalized values described above.
        """
        for book_id in sorted(
            self.new_api.tags_older_than(
                tag,
                delta=delta,
                must_have_tag=must_have_tag,
                must_have_authors=must_have_authors,
            )
        ):
            yield book_id

    def sizeof_format(self, index, fmt, index_is_id=False):
        """
        Perform the sizeof format operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.sizeof format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.format_metadata(book_id, fmt).get("size", None)

    def get_metadata(
        self,
        index,
        index_is_id=False,
        get_cover=False,
        get_user_categories=True,
        cover_as_data=False,
    ):
        """
        Returns the metaata for a given book as a calibreMetaData object.

        Example:
            Exercise LibraryDatabase.get metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param get_cover: Value supplied for get cover under the utility contract.
        :param get_user_categories: Value supplied for get user categories under the utility
            contract.
        :param cover_as_data: Value supplied for cover as data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        return self.new_api.get_metadata(
            book_id,
            get_cover=get_cover,
            get_user_categories=get_user_categories,
            cover_as_data=cover_as_data,
        )

    def rename_series(self, old_id, new_name, change_index=True):
        """
        Preforms

        Example:
            Exercise LibraryDatabase.rename series through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_id: Value supplied for old id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param change_index: Value supplied for change index under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.rename_items("series", {old_id: new_name}, change_index=change_index)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN DATA INTERFACE
    # Todo: All this is, I think, broken and needs to be tested

    def get_custom(self, index, label=None, num=None, index_is_id=False):
        """
        Return the custom data for the specified book_id and custom column.

        Example:
            Exercise LibraryDatabase.get custom through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        ans = self.new_api.field_for(self.custom_field_name(label, num), book_id)
        if isinstance(ans, tuple):
            ans = list(ans)
        return ans

    def get_custom_extra(self, index, label=None, num=None, index_is_id=False):
        """
        Return the extra value for the given custom column - this is the index value for a series type custom column.

        Example:
            Exercise LibraryDatabase.get custom extra through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = self.backend.custom_field_metadata(label, num)
        # add future datatypes with an extra column here
        if data["datatype"] != "series":
            return None
        book_id = index if index_is_id else self.id(index)
        return self.new_api.field_for(self.custom_field_name(label, num) + "_index", book_id)

    def get_custom_and_extra(self, index, label=None, num=None, index_is_id=False):
        """
        Return the value of a custom field and it's extra/index (if it has one).

        Example:
            Exercise LibraryDatabase.get custom and extra through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        data = self.backend.custom_field_metadata(label, num)
        ans = self.new_api.field_for(self.custom_field_name(label, num), book_id)
        if isinstance(ans, tuple):
            ans = list(ans)
        if data["datatype"] != "series":
            return ans, None
        return ans, self.new_api.field_for(self.custom_field_name(label, num) + "_index", book_id)

    def get_next_cc_series_num_for(self, series, label=None, num=None):
        """
        Return next cc series num for under the format's safety and compatibility rules.

        Example:
            Exercise LibraryDatabase.get next cc series num for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param series: Value supplied for series under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = self.backend.custom_field_metadata(label, num)
        if data["datatype"] != "series":
            return None
        return self.new_api.get_next_series_num_for(series, field=self.custom_field_name(label, num))

    def set_custom_bulk(self, ids, val, label=None, num=None, append=False, notify=True, extras=None):
        """
        Set custom metadata in bulk for the specified book ids.

        Example:
            Exercise LibraryDatabase.set custom bulk through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids: Value supplied for ids under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param append: Value supplied for append under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param extras: Value supplied for extras under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if extras is not None and len(extras) != len(ids):
            raise ValueError("Length of ids and extras is not the same")
        custom_field = self.custom_field_name(label, num)
        data = self.backend.custom_columns.custom_field_metadata(label, num)

        # Filter for the cases where the command cannot be completed
        if data["datatype"] == "composite":
            return set()
        if data["datatype"] == "enumeration" and (val and val not in data["display"]["enum_values"]):
            return
        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])

        if append:
            for book_id in ids:
                self.set_custom(book_id, val, label=label, num=num, append=True, notify=False)
        else:
            with self.new_api.write_lock:
                self.new_api.unlock.set_field(
                    custom_field,
                    {book_id: val for book_id in ids},
                    allow_case_change=False,
                )
                # Todo: 90%+ sure this is a bug in calibre - originally the lock was not kept while this was run
                if extras is not None:
                    self.new_api.unlock.set_field(
                        custom_field + "_index",
                        {book_id: val for book_id, val in zip(ids, extras)},
                    )
        if notify:
            self.notify("metadata", list(ids))

    def set_custom_bulk_multiple(self, ids, add=(), remove=(), label=None, num=None, notify=False):
        """
        Manipulate the values of a multiple valued custom column in bulk

        Example:
            Exercise LibraryDatabase.set custom bulk multiple through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids: Value supplied for ids under the utility contract.
        :param add: Value supplied for add under the utility contract.
        :param remove: Value supplied for remove under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        data = self.backend.custom_field_metadata(label, num)
        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])
        if data["datatype"] != "text" or not data["is_multiple"]:
            raise ValueError("Column %r is not text/multiple" % data["label"])
        field = self.custom_field_name(label, num)
        self._do_bulk_modify(field, ids, add, remove, notify)

    def is_item_used_in_multiple(self, item, label=None, num=None):
        """
        Return whether is item used in multiple holds for the supplied ebook data.

        Example:
            Exercise LibraryDatabase.is item used in multiple through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item: Value supplied for item under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        existing_tags = self.all_custom(label=label, num=num)
        return icu_lower(item) in {icu_lower(t) for t in existing_tags}

    def delete_custom_item_using_id(self, item_id, label=None, num=None):
        """
        Perform the delete custom item using id operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.delete custom item using id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.remove_items(self.custom_field_name(label, num), (item_id,))

    def rename_custom_item(self, old_id, new_name, label=None, num=None):
        """
        Perform the rename custom item operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.rename custom item through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_id: Value supplied for old id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.rename_items(self.custom_field_name(label, num), {old_id: new_name}, change_index=False)

    def delete_item_from_multiple(self, item, label=None, num=None):
        """
        Perform the delete item from multiple operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.delete item from multiple through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item: Value supplied for item under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        custom_field_name = self.custom_field_name(label, num)
        existing = self.new_api.get_id_map(custom_field_name)
        rmap = {icu_lower(v): k for k, v in iteritems(existing)}
        item_id = rmap.get(icu_lower(item), None)
        if item_id is None:
            return []
        return list(self.new_api.remove_items(custom_field_name, (item_id,)))

    def set_custom(
        self,
        book_id,
        val,
        label=None,
        num=None,
        append=False,
        notify=True,
        extra=None,
        commit=True,
        allow_case_change=False,
    ):
        """
        Set the value for a custom column for a book.

        Example:
            Exercise LibraryDatabase.set custom through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :param append: Value supplied for append under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param extra: Value supplied for extra under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit parameter is no longer used")

        custom_field_name = self.custom_field_name(label, num)
        data = self.backend.custom_field_metadata(label, num)
        if data["datatype"] == "composite":
            return set()
        if not data["editable"]:
            raise ValueError("Column %r is not editable" % data["label"])
        if data["datatype"] == "enumeration" and (val and val not in data["display"]["enum_values"]):
            return set()
        with self.new_api.write_lock:
            if append and data["is_multiple"]:
                current = self.new_api.unlock.field_for(custom_field_name, book_id)
                existing = {icu_lower(x) for x in current}
                val = current + tuple(
                    x
                    for x in self.new_api.fields[custom_field_name].writer.adapter(val)
                    if icu_lower(x) not in existing
                )
                affected_books = self.new_api.unlock.set_field(
                    custom_field_name,
                    {book_id: val},
                    allow_case_change=allow_case_change,
                )
            else:
                affected_books = self.new_api.unlock.set_field(
                    custom_field_name,
                    {book_id: val},
                    allow_case_change=allow_case_change,
                )
            if data["datatype"] == "series":
                s, sidx = get_series_values(val)
                if sidx is None:
                    extra = 1.0 if extra is None else extra
                    self.new_api.unlock.set_field(custom_field_name + "_index", {book_id: extra})
        if notify and affected_books:
            self.notify("metadata", list(affected_books))
        return affected_books

    def delete_custom_column(self, label=None, num=None):
        """
        Perform the delete custom column operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.delete custom column through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.new_api.delete_custom_column(label, num)

    def create_custom_column(self, label, name, datatype, is_multiple, editable=True, display=None):
        """
        Add a new custom column to the books table.

        Example:
            Exercise LibraryDatabase.create custom column through a consuming regression::

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
        if display is None:
            display = {}
        self.new_api.create_custom_column(label, name, datatype, is_multiple, editable=editable, display=display)

    def set_custom_column_metadata(
        self,
        num,
        name=None,
        label=None,
        is_editable=None,
        display=None,
        notify=True,
        update_last_modified=False,
    ):
        """
        Change the metadata for a custom column.

        Example:
            Exercise LibraryDatabase.set custom column metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param num: Value supplied for num under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param label: Value supplied for label under the utility contract.
        :param is_editable: Value supplied for is editable under the utility contract.
        :param display: Value supplied for display under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param update_last_modified: Value supplied for update last modified under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        changed = self.new_api.set_custom_column_metadata(
            num,
            name=name,
            label=label,
            is_editable=is_editable,
            display=display,
            update_last_modified=update_last_modified,
        )
        if changed and notify:
            self.notify("metadata", [])

    #
    # ----------------------------------------------------------------------------------------------------------------------

    def remove_cover(self, book_id, notify=True, commit=True):
        """
        Remove a cover from a book - will delete the primary cover of the book and note that the cover has been deliberately set to be None.

        Example:
            Exercise LibraryDatabase.remove cover through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - variable commit no longer in use")

        self.new_api.set_cover({book_id: None})
        if notify:
            self.notify("cover", [book_id])

    def set_cover(self, book_id, data, notify=True, commit=True):
        """
        Replace the container's cover while preserving required package references.

        Example:
            Exercise LibraryDatabase.set cover through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit parameter is no longer in use")

        self.new_api.set_cover({book_id: data})
        if notify:
            self.notify("cover", [book_id])

    def original_fmt(self, book_id, fmt):
        """
        Perform the original fmt operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.original fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        nfmt = ("ORIGINAL_%s" % fmt).upper()
        return nfmt if self.new_api.has_format(book_id, nfmt) else fmt

    def save_original_format(self, book_id, fmt, notify=True):
        """
        Perform the save original format operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.save original format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param notify: Value supplied for notify under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ret = self.new_api.save_original_format(book_id, fmt)
        if ret and notify:
            self.notify("metadata", [book_id])
        return ret

    def restore_original_format(self, book_id, original_fmt, notify=True):
        """
        Perform the restore original format operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.restore original format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param original_fmt: Value supplied for original fmt under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ret = self.new_api.restore_original_format(book_id, original_fmt)
        if ret and notify:
            self.notify("metadata", [book_id])
        return ret

    def remove_format(self, index, fmt, index_is_id=False, notify=True, commit=True, db_only=False):
        """
        Preform the removal of the highest rated format from a book.

        Example:
            Exercise LibraryDatabase.remove format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param fmt: Date, number or template format specification.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :param notify: Value supplied for notify under the utility contract.
        :param commit: Value supplied for commit under the utility contract.
        :param db_only: Value supplied for db only under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not commit:
            warnings.warn("Depreciation warning - commit variable not used")

        book_id = index if index_is_id else self.id(index)
        self.new_api.remove_formats({book_id: (fmt,)}, db_only=db_only)
        if notify:
            self.notify("metadata", [book_id])

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - Search and book matching functions

    def books_in_series(self, series_id):
        """
        Returns the ids of all the books in a given series in the order of their series index.

        Example:
            Exercise LibraryDatabase.books in series through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param series_id: Value supplied for series id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self.new_api.safe_read_lock:
            book_ids = self.new_api.unlock.books_for_field("series", series_id)
            ff = self.new_api.unlock.field_for
            return sorted(book_ids, key=lambda x: ff("series_index", x))

    def books_in_series_of(self, index, index_is_id=False):
        """
        Looks up the series that the book is - then returns a list of the ids of all the books in that series ordered by their position in the series.

        Example:
            Exercise LibraryDatabase.books in series of through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param index: Value supplied for index under the utility contract.
        :param index_is_id: Value supplied for index is id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id = index if index_is_id else self.id(index)
        series_ids = self.new_api.field_ids_for("series", book_id)
        if not series_ids:
            return []
        return self.books_in_series(series_ids[0])

    def books_with_same_title(self, mi, all_matches=True):
        """
        Returns a set of books_ids with the same title as that extracted from the metadata object.

        Example:
            Exercise LibraryDatabase.books with same title through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param mi: Metadata object exposed to the template function.
        :param all_matches: Value supplied for all matches under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        title = mi.title
        ans = set()
        if title:
            title = icu_lower(force_unicode(title))
            for book_id, x in iteritems(self.new_api.get_id_map("title")):
                if icu_lower(x) == title:
                    ans.add(book_id)
                    if not all_matches:
                        break
        return ans

    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Private interface {{{
    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: An iterator yielding the normalized values described above.
        """
        for row in self.data.iterall():
            yield row

    @staticmethod
    def _get_next_series_num_for_list(series_indices):
        """
        Perform the get next series num for list operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase. get next series num for list through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param series_indices: Value supplied for series indices under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _get_next_series_num_for_list(series_indices)

    @staticmethod
    def _get_series_values(val):
        """
        Perform the get series values operation under explicit file-format and conversion rules.

        Example:
            Exercise LibraryDatabase. get series values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _get_series_values(val)

    @staticmethod
    def all_custom(label, num):
        """
        Dummy method - should be overwritten below - if it isn't this method throughs an error.

        Example:
            Exercise LibraryDatabase.all custom through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param label: Value supplied for label under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError(label, num)

    # }}}


########################################################################################################################
# - ADD ADDITIONAL FUNCTIONS TO THE LIBRARYDATABASE
########################################################################################################################


method_type = lambda func: types.MethodType(func, LibraryDatabase)


# Legacy getter API {{{
# Uses the getter method from the view - and so will return the same result as calling on the view
for prop in (
    "author_sort",
    "authors",
    "comment",
    "comments",
    "publisher",
    "max_size",
    "rating",
    "series",
    "series_index",
    "tags",
    "title",
    "title_sort",
    "timestamp",
    "uuid",
    "pubdate",
    "ondevice",
    "metadata_last_modified",
    "languages",
):

    def getter(field_prop):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field_prop: Value supplied for field prop under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fm = {
            "comment": "comments",
            "metadata_last_modified": "last_modified",
            "title_sort": "sort",
            "max_size": "size",
        }.get(field_prop, field_prop)

        def get_property_func(self, index, index_is_id=False):
            """
            Return property func under the format's safety and compatibility rules.

            Example:
                Exercise getter.get property func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param index: Value supplied for index under the utility contract.
            :param index_is_id: Value supplied for index is id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.get_property(index, index_is_id=index_is_id, loc=self.FIELD_MAP[fm])

        return get_property_func

    setattr(LibraryDatabase, prop, method_type(getter(prop)))

for prop in ("series", "publisher"):

    def getter(db_field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db_field: Value supplied for db field under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def sp_local_func(self, index, index_is_id=False):
            """
            Perform the sp local func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.sp local func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param index: Value supplied for index under the utility contract.
            :param index_is_id: Value supplied for index is id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            book_id = index if index_is_id else self.id(index)
            ans = self.new_api.field_ids_for(db_field, book_id)
            try:
                return ans[0]
            except IndexError:
                pass
            except TypeError:
                pass

        return sp_local_func

    setattr(LibraryDatabase, prop + "_id", method_type(getter(prop)))

LibraryDatabase.format_hash = method_type(lambda self, book_id, fmt: self.new_api.format_hash(book_id, fmt))
LibraryDatabase.index = method_type(lambda self, book_id, cache=False: self.data.id_to_index(book_id))
LibraryDatabase.has_cover = method_type(lambda self, book_id: self.new_api.field_for("cover", book_id))
LibraryDatabase.get_tags = method_type(lambda self, book_id: set(self.new_api.field_for("tags", book_id)))
LibraryDatabase.get_categories = method_type(
    lambda self, sort="name", ids=None: self.new_api.get_categories(sort=sort, book_ids=ids)
)
LibraryDatabase.get_identifiers = method_type(
    lambda self, index, index_is_id=False: self.new_api.field_for(
        "identifiers", index if index_is_id else self.id(index)
    )
)
LibraryDatabase.isbn = method_type(
    lambda self, index, index_is_id=False: self.get_identifiers(index, index_is_id=index_is_id).get("isbn", None)
)
LibraryDatabase.get_books_for_category = method_type(
    lambda self, category, id_: self.new_api.get_books_for_category(category, id_)
)
LibraryDatabase.get_data_as_dict = method_type(get_data_as_dict)
LibraryDatabase.find_identical_books = method_type(lambda self, mi: self.new_api.find_identical_books(mi))
LibraryDatabase.get_top_level_move_items = method_type(lambda self: self.new_api.get_top_level_move_items())
# }}}

# Legacy setter API {{{
for field_name in (
    "!authors",
    "author_sort",
    "comment",
    "has_cover",
    "languages",
    "pubdate",
    "!publisher",
    "rating",
    "!series",
    "series_index",
    "timestamp",
    "uuid",
    "title",
    "title_sort",
):

    def setter(db_field):
        """
        Perform the setter operation under explicit file-format and conversion rules.

        Example:
            Exercise setter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db_field: Value supplied for db field under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        has_case_change = db_field.startswith("!")
        # Todo: Is comments still in use?
        db_field = {"comment": "comments", "title_sort": "sort"}.get(db_field, db_field)
        if has_case_change:
            db_field = db_field[1:]
            acc = db_field == "series"

            # Todo: This should be table_id - as it's more general than books
            def setter_func(self, book_id, val, notify=True, commit=True, allow_case_change=acc):
                """
                Perform the setter func operation under explicit file-format and conversion rules.

                Example:
                    Exercise setter.setter func through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :param book_id: Value supplied for book id under the utility contract.
                :param val: Template or metadata value evaluated by the operation.
                :param notify: Value supplied for notify under the utility contract.
                :param commit: Value supplied for commit under the utility contract.
                :param allow_case_change: Value supplied for allow case change under the utility
                    contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                if not commit:
                    warnings.warn("Depreciation warning - commit parameter is no longer used")

                ret = self.new_api.set_field(db_field, {book_id: val}, allow_case_change=allow_case_change)
                if notify:
                    self.notify([book_id])
                return ret

        elif db_field == "has_cover":

            def setter_func(self, book_id, val):
                """
                Perform the setter func operation under explicit file-format and conversion rules.

                Example:
                    Exercise setter.setter func through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :param book_id: Value supplied for book id under the utility contract.
                :param val: Template or metadata value evaluated by the operation.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                self.new_api.set_field("cover", {book_id: bool(val)})

        else:
            null_field = db_field in {"title", "sort", "uuid"}
            retval = True if db_field == "sort" else None

            def setter_func(self, book_id, val, notify=True, commit=True):
                """
                Perform the setter func operation under explicit file-format and conversion rules.

                Example:
                    Exercise setter.setter func through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :param book_id: Value supplied for book id under the utility contract.
                :param val: Template or metadata value evaluated by the operation.
                :param notify: Value supplied for notify under the utility contract.
                :param commit: Value supplied for commit under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                if not commit:
                    warnings.warn("Depreciation warning - commit parameter is no longer used")

                if not val and null_field:
                    return False if db_field == "sort" else None
                ret = self.new_api.set_field(db_field, {book_id: val})
                if notify:
                    self.notify([book_id])
                return ret if db_field == "languages" else retval

        return setter_func

    setattr(
        LibraryDatabase,
        "set_%s" % field_name.replace("!", ""),
        method_type(setter(field_name)),
    )

# Add the generic renamed functions to the LibraryDatabase
for field_name in ("authors", "tags", "publisher"):

    def renamer(affected_field):
        """
        Perform the renamer operation under explicit file-format and conversion rules.

        Example:
            Exercise renamer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param affected_field: Value supplied for affected field under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def renamer_func(self, old_id, new_name):
            """
            Perform the renamer func operation under explicit file-format and conversion rules.

            Example:
                Exercise renamer.renamer func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param old_id: Value supplied for old id under the utility contract.
            :param new_name: Value supplied for new name under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            id_map = self.new_api.rename_items(affected_field, {old_id: new_name})[1]
            if affected_field == "authors":
                return id_map[old_id]

        return renamer_func

    fname = field_name[:-1] if field_name in {"tags", "authors"} else field_name
    setattr(LibraryDatabase, "rename_%s" % fname, method_type(renamer(field_name)))

LibraryDatabase.update_last_modified = method_type(
    lambda self, book_ids, commit=False, now=None: self.new_api.update_last_modified(book_ids, now=now)
)

# }}}

# Legacy API to get information about many-(one, many) fields {{{
for field_name in ("authors", "tags", "publisher", "series"):

    def getter(field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def local_func_2(self):
            """
            Perform the local func 2 operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.local func 2 through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.new_api.all_field_names(field)

        return local_func_2

    final_name = field_name[:-1] if field_name in {"authors", "tags"} else field_name
    setattr(LibraryDatabase, "all_%s_names" % final_name, method_type(getter(field_name)))
# Setup the all_formats shortcut
LibraryDatabase.all_formats = method_type(lambda self: self.new_api.all_field_names("formats"))
# Setup the all_custom shortcut
LibraryDatabase.all_custom = method_type(
    lambda self, label=None, num=None: self.new_api.all_field_names(self.custom_field_name(label, num))
)

# Add other generic all shortcuts - will return the id map for each field
# Map is the form of a list of tuples - in order of the table id - the first element being the id of the entry on the
# table and the second element neing the name of the elements
# (e.g., for authors, [(0, u'Unknown'), (1, u'Neal Stephenson'), (2, u'Terry Pratchett'), ... ]
for local_func, field_name in iteritems(
    {
        "all_authors": "authors",
        "all_titles": "title",
        "all_tags2": "tags",
        "all_series": "series",
        "all_publishers": "publisher",
    }
):

    def getter(db_field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db_field: Value supplied for db field under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def func(self):
            """
            Perform the func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.field_id_map(db_field)

        return func

    setattr(LibraryDatabase, local_func, method_type(getter(field_name)))

# Will return a list of all the tags present on the database
LibraryDatabase.all_tags = method_type(lambda self: list(self.all_tag_names()))

# Returns all the identifier types in use in the database
LibraryDatabase.get_all_identifier_types = method_type(
    lambda self: list(self.new_api.fields["identifiers"].table.all_identifier_types())
)
LibraryDatabase.get_authors_with_ids = method_type(
    lambda self: [
        [aid, adata["name"], adata["sort"], adata["link"]] for aid, adata in iteritems(self.new_api.author_data())
    ]
)
LibraryDatabase.get_author_id = method_type(
    lambda self, author: {icu_lower(v): k for k, v in iteritems(self.new_api.get_id_map("authors"))}.get(
        icu_lower(author), None
    )
)

for field_name in ("tags", "series", "publishers", "ratings", "languages"):

    def getter(field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        local_fname = field[:-1] if field in {"publishers", "ratings"} else field

        def func(self):
            """
            Perform the func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [[tid, tag] for tid, tag in iteritems(self.new_api.get_id_map(local_fname))]

        return func

    setattr(LibraryDatabase, "get_%s_with_ids" % field_name, method_type(getter(field_name)))

# Return the name of the item from it's id
for field_name in ("author", "tag", "series"):

    def getter(field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        field = field if field == "series" else (field + "s")

        def func(self, item_id):
            """
            Perform the func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param item_id: Value supplied for item id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.new_api.get_item_name(field, item_id)

        return func

    setattr(LibraryDatabase, "%s_name" % field_name, method_type(getter(field_name)))

for field_name in ("publisher", "series", "tag"):

    def getter(field):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        local_fname = "tags" if field == "tag" else field

        def func(self, item_id):
            """
            Perform the func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param item_id: Value supplied for item id under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.new_api.remove_items(local_fname, (item_id,))

        return func

    setattr(
        LibraryDatabase,
        "delete_%s_using_id" % field_name,
        method_type(getter(field_name)),
    )
# }}}

# Legacy field API {{{
for local_func in (
    "standard_field_keys",
    "!custom_field_keys",
    "all_field_keys",
    "searchable_fields",
    "sortable_field_keys",
    "search_term_to_field_key",
    "!custom_field_metadata",
    "all_metadata",
):

    def getter(func):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param func: Value supplied for func under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if func.startswith("!"):
            func = func[1:]

            def local_meth(self, include_composites=True):
                """
                Perform the local meth operation under explicit file-format and conversion rules.

                Example:
                    Exercise getter.local meth through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :param include_composites: Value supplied for include composites under the utility
                    contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return getattr(self.field_metadata, func)(include_composites=include_composites)

        elif func == "search_term_to_field_key":

            def local_meth(self, term):
                """
                Perform the local meth operation under explicit file-format and conversion rules.

                Example:
                    Exercise getter.local meth through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :param term: Value supplied for term under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return self.field_metadata.search_term_to_field_key(term)

        else:

            def local_meth(self):
                """
                Perform the local meth operation under explicit file-format and conversion rules.

                Example:
                    Exercise getter.local meth through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param self: Value supplied for self under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return getattr(self.field_metadata, func)()

        return local_meth

    setattr(LibraryDatabase, local_func.replace("!", ""), method_type(getter(local_func)))
LibraryDatabase.metadata_for_field = method_type(lambda self, field: self.field_metadata.get(field))

# }}}

# Miscellaneous API {{{
for meth in (
    "get_next_series_num_for",
    "has_book",
):

    def getter(local_meth):
        """
        Perform the getter operation under explicit file-format and conversion rules.

        Example:
            Exercise getter through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param local_meth: Value supplied for local meth under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def meth_func(self, x):
            """
            Perform the meth func operation under explicit file-format and conversion rules.

            Example:
                Exercise getter.meth func through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param self: Value supplied for self under the utility contract.
            :param x: Value supplied for x under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return getattr(self.new_api, local_meth)(x)

        return meth_func

    setattr(LibraryDatabase, meth, method_type(getter(meth)))

LibraryDatabase.saved_search_names = method_type(lambda self: self.new_api.saved_search_names())
LibraryDatabase.saved_search_lookup = method_type(lambda self, x: self.new_api.saved_search_lookup(x))
LibraryDatabase.saved_search_set_all = method_type(lambda self, smap: self.new_api.saved_search_set_all(smap))
LibraryDatabase.saved_search_delete = method_type(lambda self, x: self.new_api.saved_search_delete(x))
LibraryDatabase.saved_search_add = method_type(lambda self, x, y: self.new_api.saved_search_add(x, y))
LibraryDatabase.saved_search_rename = method_type(lambda self, x, y: self.new_api.saved_search_rename(x, y))
LibraryDatabase.commit_dirty_cache = method_type(lambda self: self.new_api.commit_dirty_cache())
LibraryDatabase.author_sort_from_authors = method_type(lambda self, x: self.new_api.author_sort_from_authors(x))
# Cleaning is not required anymore
LibraryDatabase.clean = LibraryDatabase.clean_custom = method_type(lambda self: None)
LibraryDatabase.clean_standard_field = method_type(lambda self, field, commit=False: None)
# apsw operates in autocommit mode
LibraryDatabase.commit = method_type(lambda self: None)
# }}}

del method_type
