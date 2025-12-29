#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

# Slightly higher level than the database - some useful classes to access information on the database

import json
import os

import shutil
import time
from copy import deepcopy

# Todo: Replace all references to six with references to LiuXin_six
import six
from six import iteritems

from LiuXin.utils.logger import default_log

default_log.info("LiuXin.databases.backend - beginning import")
from LiuXin.constants import iswindows

from LiuXin.databases.caches.calibre.cache import CalibreCache

default_log.info("CalibreCache - imported")

from LiuXin.databases.dbprefs import DBPrefs
from LiuXin.databases.metadata_tools.add import Add
from LiuXin.databases.metadata_tools.ensure import Ensure
from LiuXin.databases.metadata_tools.apply import Apply
from LiuXin.databases.metadata_tools.intralinker import Intralinker

from LiuXin.databases.database import Database

default_log.info("DatabasePing import completed")
from LiuXin.databases.field_metadata import FieldMetadata

default_log.info("FieldMetadata import completed")
from LiuXin.databases.custom_columns import CustomColumns


from LiuXin.exceptions import FolderStoreError
from LiuXin.exceptions import NoSuchFormat

default_log.info("Exception imports complete.")


from LiuXin.folder_stores.cover_caches.on_disk import CoverCache

default_log.info("CoverCache import completed")
from LiuXin.folder_stores.folderstoremanager import FolderStoreManager

default_log.info("FolderStoreManager import completed")
from LiuXin.folder_stores.location import Location

default_log.info("Location import completed")


from LiuXin.metadata.ebook_metadata_tools import author_to_author_sort

from LiuXin.utils.calibre import isbytestring, filesystem_encoding, force_unicode
from LiuXin.utils.calibre.calibre_emulation import tweaks
from LiuXin.utils.config.config_tools import to_json, from_json, prefs
from LiuXin.utils.file_ops.file_ops import local_open as lopen
from LiuXin.utils.filenames import samefile, hardlink_file
from LiuXin.utils.icu import sort_key, lower as icu_lower
from LiuXin.utils.lx_libraries.liuxin_six import force_cmp
from LiuXin.utils.lx_libraries.liuxin_six import six_pickle
from LiuXin.utils.lx_libraries.liuxin_six import six_buffer
from LiuXin.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

default_log.info("LiuXin.databases.backend - import finished")

from past.builtins import basestring


# Slightly higher level than the database - some useful classes to access information on the database


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# Extra collators {{{
def pynocase(one, two, encoding="utf-8"):
    if isbytestring(one):
        try:
            one = one.decode(encoding, "replace")
        except:
            pass
    if isbytestring(two):
        try:
            two = two.decode(encoding, "replace")
        except:
            pass
    return force_cmp(one.lower(), two.lower())


def _author_to_author_sort(x):
    if not x:
        return ""
    return author_to_author_sort(x.replace("|", ","))


def icu_collator(s1, s2):
    return force_cmp(sort_key(force_unicode(s1, "utf-8")), sort_key(force_unicode(s2, "utf-8")))


# }}}


# Todo: Start this from a regular database - so we can upgrade to a backend if needed
class DB(Database):
    """
    Adds a layer of functions around the database.
    Also has an in memory cache.
    The databases.database class contains the basic operations. The databases.backend.DB class contains a few more
    functions. It's intended to act as a calibre compatibility layer, while also provided useful functionality.
    """

    PATH_LIMIT = 40 if iswindows else 100
    WINDOWS_LIBRARY_PATH_LIMIT = 75

    def __init__(
        self,
        library_path,
        default_prefs=None,
        read_only=False,
        restore_all_prefs=False,
        progress_callback=lambda x, y: True,
        load_user_formatter_functions=True,
        create=False,
        with_cache=True,
        existing_fsm=None,
    ):
        """
        Initialize the database.
        :param library_path: If a string, assumed that there is an SQLite database at the end of it. If a Location
                             parses that into metadata and initializes with that. If None, uses the default metadata
        :param default_prefs:
        :param read_only:
        :param restore_all_prefs:
        :param progress_callback:
        :param load_user_formatter_functions:
        :param create: Should a new database be created? False by default.
        :param with_cache: If True then will bootstrap a cache - which provides stored copies of the data on the
                           database - for access speed and efficiency.
        :param existing_fsm: Allows you to pass in an existing fsm if one has already been created.
                             NOTE - YOU MUST BE ABSOLUTELY SURE THAT THE FSM HAS BEEN STARTED ON THE SAME DATABASE AS
                             THE LIBRARY PATH. UTTER DISASTER WILL RESULT IF THIS IS NOT TRUE.
        :return:
        """
        if library_path is None:
            Database.__init__(self, create=create)

        elif isbytestring(library_path):
            try:
                library_path = library_path.decode(filesystem_encoding)
            except Exception as e:
                err_str = "Library path appeared to be a bytestring and couldn't be decoded without throwing an error"
                default_log.log_exception(err_str, e, "ERROR", ("library_path", library_path))
            metadata = {"database_path": library_path}
            Database.__init__(self, metadata=metadata, create=create)

        elif isinstance(library_path, six.string_types):
            metadata = {"database_path": library_path}
            Database.__init__(self, metadata=metadata, create=create)

        else:
            err_str = "Library path couldn't be parsed - it was not None, bytestring or basestring"
            default_log.log_variables(err_str, "ERROR")
            raise NotImplementedError(err_str)

        # FieldMetadata stores the metadata for the fields that are present on a calibre database - here they are
        # emulated
        self.field_metadata = FieldMetadata()

        # self.library_path is not a meaningful concept under LiuXin - so it throws a NotImplementedError
        self.dbpath = self.metadata["database_path"]
        self.dbpath = os.environ.get("LIUXIN_OVERRIDE_DATABASE_PATH", self.dbpath)

        # Makes a copy of the metadata file, then works on it to make sure that the file is not changed
        if read_only and os.path.exists(self.dbpath):
            self.lock_writing()

        # Will store a connection to the database
        self._conn = None
        self.conn = self.lock

        # Private variable to store the library_id
        self._library_id_ = None
        # Guarantee that the library_id is set
        self.library_id = None

        # Check to see if the database currently exists
        exists = self._exists

        # Custom columns metadata
        self.custom_column_label_map, self.custom_column_num_map = {}, {}
        self.custom_column_num_to_label_map = {}

        # Setup for the dbprefs - the preferences stored in the database
        self.prefs = DBPrefs(self)
        self.custom_columns_deleted = False
        self.FIELD_MAP = dict()
        self.custom_data_adapters = dict()

        self.default_prefs = default_prefs
        self.restore_all_prefs = restore_all_prefs
        self.pref_progress_callback = progress_callback

        # Load the FolderStoreManager into the backend - this is the class which actually deals with the files that
        # exist in the database.
        # This needs to be done before the table load - as some of the method in the FSM are needed in order to populate
        # some of the needed fields in the tables
        if existing_fsm is None:
            self.fsm = FolderStoreManager(self)
            self.existing_fsm = False
        else:
            self.fsm = existing_fsm
            self.existing_fsm = True

        self.tables = dict()

        # if load_user_formatter_functions:
        #     load_user_template_functions(self.library_id, self.prefs.get('user_template_functions', []))

        # For compatibility the DB object needs to have a self.db object - which has the same interface as the rest of
        # the class
        self.db = self

        # Load the cover cache into the backend - this is the class which is used while managing covers and the local
        # store of them
        if not self.existing_fsm:
            self.cover_cache = CoverCache(self.fsm)
        else:
            self.cover_cache = self.fsm.cover_cache

        # assert hasattr(self, "maintainer"), "DB object has no attribute maintainer"

        # Setup the custom_columns plugin - trying to isolate everything in one place
        self.custom_columns = CustomColumns(db=self.db, conn=None, field_metadata=self.field_metadata)

        # Additional utility methods
        self.add = Add(database=self)
        self.ensure = Ensure(database=self)
        self.apply = Apply(database=self)
        self.intralink = Intralinker(database=self)

        self.add.ensure = self.ensure
        self.add.apply = self.apply
        self.apply.add = self.add
        self.apply.ensure = self.ensure

        self.cache = None
        if with_cache:
            self.cache = CalibreCache(self)
            self.cache.init()

        # To try and keep the interface as much like calibre as possible
        # Though that ship may have sailed, exploded, and had the insurance claimed on it
        self.create_custom_column = self.driver_wrapper.create_custom_column
        self.custom_table_names = self.driver_wrapper.custom_table_names
        self.set_custom_column_metadata = self.driver_wrapper.set_custom_column_metadata
        self.direct_custom_tables = self.driver_wrapper.direct_custom_tables

    def initialize_prefs(self, default_prefs, restore_all_prefs, progress_callback=lambda x, y: True):
        """
        Initialize the database preferences (the preferences stored on the database).
        :param default_prefs:
        :param restore_all_prefs:
        :param progress_callback: First called with None, len(default_prefs). Then called with the position, name of the
                                  pref
        :return:
        """
        # Only apply the default prefs to a new database
        # Todo: Test for a new database
        if default_prefs is not None and not self._exists:
            progress_callback(None, len(default_prefs))

            # be sure that the prefs not to be copied are listed below
            for i, key in enumerate(default_prefs):
                if restore_all_prefs or key not in frozenset(["news_to_be_synced"]):
                    self.prefs[key] = default_prefs[key]
                    progress_callback(_("restored preference ") + key, i + 1)

            if "field_metadata" in default_prefs:
                fmvals = [f for f in default_prefs["field_metadata"].values() if f["is_custom"]]
                progress_callback(None, len(fmvals))
                for i, f in enumerate(fmvals):
                    progress_callback(_("creating custom column ") + f["label"], i)
                    self.create_custom_column(
                        f["label"],
                        f["name"],
                        f["datatype"],
                        (f["is_multiple"] is not None and len(f["is_multiple"]) > 0),
                        f["is_editable"],
                        f["display"],
                    )

        defs = self.prefs.defaults
        defs["gui_restriction"] = defs["cs_restriction"] = ""
        defs["categories_using_hierarchy"] = []
        defs["column_color_rules"] = []
        defs["column_icon_rules"] = []
        defs["cover_grid_icon_rules"] = []
        defs["grouped_search_make_user_categories"] = []
        defs["similar_authors_search_key"] = "authors"
        defs["similar_authors_match_kind"] = "match_any"
        defs["similar_publisher_search_key"] = "publisher"
        defs["similar_publisher_match_kind"] = "match_any"
        defs["similar_tags_search_key"] = "tags"
        defs["similar_tags_match_kind"] = "match_all"
        defs["similar_series_search_key"] = "series"
        defs["similar_series_match_kind"] = "match_any"
        defs["book_display_fields"] = [
            ("title", False),
            ("authors", True),
            ("formats", True),
            ("series", True),
            ("identifiers", True),
            ("tags", True),
            ("path", True),
            ("publisher", False),
            ("rating", False),
            ("author_sort", False),
            ("sort", False),
            ("timestamp", False),
            ("uuid", False),
            ("comments", True),
            ("id", False),
            ("pubdate", False),
            ("last_modified", False),
            ("size", False),
            ("languages", False),
        ]
        defs["virtual_libraries"] = {}
        defs["virtual_lib_on_startup"] = defs["cs_virtual_lib_on_startup"] = ""
        defs["virt_libs_hidden"] = defs["virt_libs_order"] = ()
        defs["update_all_last_mod_dates_on_start"] = False
        defs["field_under_covers_in_grid"] = "title"
        defs["cover_browser_title_template"] = "{title}"

        # Migrate the bool tristate tweak
        defs["bools_are_tristate"] = tweaks.get("bool_custom_columns_are_tristate", "yes") == "yes"
        if self.prefs.get("bools_are_tristate") is None:
            self.prefs.set("bools_are_tristate", defs["bools_are_tristate"])

        # # Migrate column coloring rules
        # if self.prefs.get('column_color_name_1', None) is not None:
        #     from calibre.library.coloring import migrate_old_rule
        #     old_rules = []
        #     for i in range(1, 6):
        #         col = self.prefs.get('column_color_name_'+str(i), None)
        #         templ = self.prefs.get('column_color_template_'+str(i), None)
        #         if col and templ:
        #             try:
        #                 del self.prefs['column_color_name_'+str(i)]
        #                 rules = migrate_old_rule(self.field_metadata, templ)
        #                 for templ in rules:
        #                     old_rules.append((col, templ))
        #             except:
        #                 pass
        #     if old_rules:
        #         self.prefs['column_color_rules'] += old_rules

        # Migrate saved search and user categories to db preference scheme
        def migrate_preference(key, default):
            old_val = prefs[key]
            if old_val != default:
                self.prefs[key] = old_val
                prefs[key] = default
            if key not in self.prefs:
                self.prefs[key] = default

        migrate_preference("user_categories", {})
        migrate_preference("saved_searches", {})

        # migrate grouped_search_terms
        if self.prefs.get("grouped_search_terms", None) is None:
            try:
                ogst = tweaks.get("grouped_search_terms", {})
                ngst = {}
                for t in ogst:
                    ngst[icu_lower(t)] = ogst[t]
                self.prefs.set("grouped_search_terms", ngst)
            except Exception as e:
                err_str = "Error while migrating grouped_search_terms"
                default_log.log_exception(err_str, e, "INFO")

        # migrate the gui_restriction preference to a virtual library
        gr_pref = self.prefs.get("gui_restriction", None)
        if gr_pref is not None:
            virt_libs = self.prefs.get("virtual_libraries", {})
            virt_libs[gr_pref] = 'search:"{}"'.format(gr_pref)
            self.prefs["virtual_libraries"] = virt_libs
            self.prefs["gui_restriction"] = ""
            self.prefs["virtual_lib_on_startup"] = gr_pref

        # migrate the cs_restriction preference to a virtual library
        gr_pref = self.prefs.get("cs_restriction", None)
        if gr_pref is not None:
            virt_libs = self.prefs.get("virtual_libraries", {})
            virt_libs[gr_pref] = 'search:"{}"'.format(gr_pref)
            self.prefs["virtual_libraries"] = virt_libs
            self.prefs["cs_restriction"] = ""
            self.prefs["cs_virtual_lib_on_startup"] = gr_pref

        # Rename any user categories with names that differ only in case
        user_cats = self.prefs.get("user_categories", [])
        catmap = {}
        for uc in user_cats:
            ucl = icu_lower(uc)
            if ucl not in catmap:
                catmap[ucl] = []
            catmap[ucl].append(uc)
        cats_changed = False
        for uc in catmap:
            if len(catmap[uc]) > 1:
                cat = catmap[uc][0]
                suffix = 1
                while icu_lower((cat + six_unicode(suffix))) in catmap:
                    suffix += 1
                info_str = "found user category case overlap" + six_unicode(catmap[uc])
                info_str += "Renaming user category %s to %s" % (
                    cat,
                    cat + six_unicode(suffix),
                )
                default_log.info(info_str)
                user_cats[cat + six_unicode(suffix)] = user_cats[cat]
                del user_cats[cat]
                cats_changed = True
        if cats_changed:
            self.prefs.set("user_categories", user_cats)

    def last_modified(self):
        """
        Return last modified time as a UTC datetime object
        :return:
        """
        return self.driver.direct_last_modified()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - ADDITIONAL DATABASE METHODS START HERE

    def get(self, *args, **kw):
        ans = self.driver_wrapper.execute(*args)
        if kw.get("all", True):
            return ans.fetchall()
        try:
            return ans.next()[0]
        except (StopIteration, IndexError):
            return None

    def last_insert_rowid(self):
        return self.conn.last_insert_rowid()

    def dump_and_restore(self, callback=None, sql=None):
        self.driver.dump_and_restore(self, callback=None, sql=None)

    @property
    def user_version(self):
        """
        The user version of the database.
        :return:
        """
        for row in self.driver_wrapper.execute("PRAGMA user_version;"):
            return row[0]

    @user_version.setter
    def user_version(self, val):
        """
        Set the user version of the database.
        :param val:
        :return:
        """
        self.driver_wrapper.execute("pragma user_version=%d" % int(val))

    def vacuum(self):
        self.driver_wrapper.execute("VACUUM")

    def copy_cover_to(
        self,
        path,
        dest,
        windows_atomic_move=None,
        use_hardlink=False,
        report_file_size=None,
    ):
        """
        Copy the primary cover in the folder at path to the given destination path.
        :param path: A LiuXin location object pointing to the cover or a path pointing to the cover
        :param dest: The place to copy the cover to
        :param windows_atomic_move: A class which provides atomic movement of files - file moves are completed in one
                                    pass. Provides a copy_path_to methods which does the copying.
        :param use_hardlink:
        :param report_file_size: Callback to report the file size copied
        :return:
        """
        if path is None:
            return

        # Todo: Handle the case of the dest being in another folder store
        # windows_atomic_move can only cope with
        if windows_atomic_move is not None and isinstance(dest, six.string_types):
            err_str = "Error, you must pass the dest as a path when using windows_atomic_move"
            default_log.error(err_str)
            raise Exception(err_str)

        # If the input path is in the form of a path (for an object in the cover cache) then copy the file to it
        if isinstance(path, six.string_types):

            if windows_atomic_move is not None:

                if os.access(path, os.R_OK) and dest and not samefile(dest, path):
                    windows_atomic_move.copy_path_to(path, dest)
                    return True

            else:
                # Check to see if the path can actually be read - if it can then read from it
                if os.access(path, os.R_OK):
                    try:
                        f = lopen(path, "rb")
                    except (IOError, OSError):
                        time.sleep(0.2)
                        try:
                            f = lopen(path, "rb")
                        except (IOError, OSError) as e:
                            # Ensure the path that caused this error is reported
                            raise Exception("Failed to open %r with error: %s" % (path, e))

                    with f:
                        if hasattr(dest, "write"):
                            if report_file_size is not None:
                                f.seek(0, os.SEEK_END)
                                report_file_size(f.tell())
                                f.seek(0)
                            shutil.copyfileobj(f, dest)
                            if hasattr(dest, "flush"):
                                dest.flush()
                            return True
                        elif dest and not samefile(dest, path):
                            if use_hardlink:
                                try:
                                    hardlink_file(path, dest)
                                    return True
                                except:
                                    pass
                            with lopen(dest, "wb") as d:
                                shutil.copyfileobj(f, d)
                            return True
            return False

        # If the input path is in the form of a location then
        elif isinstance(path, Location):

            # Read the path as a string and then write it out
            f = self.fsm.stores.read(target_location=path, mode="rb", string_io=True)
            if hasattr(dest, "write"):
                if hasattr(dest, "write"):
                    if report_file_size is not None:
                        f.seek(0, os.SEEK_END)
                        report_file_size(f.tell())
                        f.seek(0)
                    shutil.copyfileobj(f, dest)
                    if hasattr(dest, "flush"):
                        dest.flush()
                    return True
            elif dest:
                with lopen(dest, "wb") as d:
                    shutil.copyfileobj(f, d)
                return True

        else:
            err_str = "Unexpected path type - can only handle a path or a location"
            err_str = default_log.log_variables(err_str, "ERROR", ("path", path), ("type(path)", type(path)))
            raise NotImplementedError(err_str)

        return False

    def cover_or_cache(self, path, timestamp):
        """
        Checks to see if the cover should be read from the cache or read from the given path.
        Calls stat for the given path. If the stat is the same as the
        :param path: A LiuXin Location object pointing to the cover, or a path.
        :param timestamp: Unix timestamp object
        :return (read_status, cover_data, file_timestamp):
                If there has to be a new read then all data will be provided
                If there doesnt have to be a new read, the first element will be True and the other two None
                If the file can't be read the first element will be False and the other two None
                file_timestamp is when the file was last modified - this will be used to update the timestamp in the
                cache
        """
        if isinstance(path, basestring):
            try:
                stat = os.stat(path)
            except EnvironmentError:
                return False, None, None
            if abs(timestamp - stat.st_mtime) < 0.1:
                return True, None, None
            try:
                f = lopen(path, "rb")
            except (IOError, OSError):
                time.sleep(0.2)
            f = lopen(path, "rb")
            with f:
                return True, f.read(), stat.st_mtime

        # Todo: There's quite a good idea here - if the read fails wait for 0.2 seconds then try again - implement at the driver level
        elif isinstance(path, Location):
            try:
                stat = self.fsm.stores.stat(path)
            except EnvironmentError:
                return False, None, None
            if abs(timestamp - stat.st_mtime) < 0.1:
                return True, None, None
            f = self.fsm.stores.read(target_location=path, mode="rb", string_io=True)
            return True, f.read(), stat.st_mtime

        else:
            raise NotImplementedError("Can only handle a path or a Location")

    def set_cover(self, book_id, path, data, no_processing=False, add_to_cover_cache=True):
        """
        Set a cover for a book.
        :param book_id: The book to set the cover for
        :param path: The place to put the cover - might or might not be into the book.
        :type path: LiuXin Location object
        :param data: The cover data
        :param no_processing:
        :param add_to_cover_cache: Should the cover be added to the local cover cache in LiuXin_data
        :return:
        """
        # If data is a string, assume it's a path to a file - read the original name of the cover from this
        cover_o_name = None
        if isinstance(data, six.string_types) and os.path.exists(data):
            cover_o_name = os.path.splitext(os.path.basename(data))[0]

        # Add the data to the local cover cache - if it adds successfully then copy the cover into the book folder - if
        # it returns None then remove the cover
        sr_path = self.cover_cache.add_cover(
            resource_type="book",
            resource_id=book_id,
            data=data,
            no_processing=no_processing,
        )

        # If set_cover is being called with no data then mark any covers linked to the book as not primary and remove
        # the cover for this book from the local cover cache.
        book_row = self.get_row_from_id(table="books", row_id=book_id)
        cover_rows = self.get_interlinked_rows(target_row=book_row, secondary_table="covers")
        for cr in cover_rows:
            cr["cover_primary"] = 0
            cr.sync()

        if sr_path is None:
            return

        # Check that the provided override location exists and can be written to.
        if not self.fsm.path.exists(path):
            info_str = "Unable to save data to given location - it doesn't exist - defaulting to book_id"
            default_log.log_variables(info_str, "INFO", ("path", path))

            book_row = self.get_row_from_id(table="books", row_id=book_id)
            folder_row = self.fsm.ensure_book_folder(book_row)
            path = self.fsm.get_loc(asset_row=folder_row)
        else:
            folder_row = path["folder_row"]

        # Construct the cover row - will need the id from it when building the cover name
        cover_row = self.get_blank_row(table="covers")
        cover_id = cover_row["cover_id"]
        cover_name = "cover_id_{}.jpg".format(six_unicode(cover_id))
        cover_row["cover_name"] = cover_name

        # Todo: There must be a add cover method somewhere - use it here
        # Load the folder_id where the cover will end up into the row
        cover_row["cover_folder_id"] = folder_row["folder_id"]
        cover_row["cover_original_name"] = cover_o_name
        cover_row["cover_local"] = 1
        cover_row["cover_extension"] = ".jpg"

        # If you are setting something to be a cover assuming you want it to be the main cover
        cover_row["cover_primary"] = 1

        # Sync the row back to the database - load the cover into the folder store
        target_path = self.fsm.path.join(path, cover_name)
        self.fsm.stores.load(local_path=sr_path, dst=target_path)

    def cover_last_modified(self, path):
        """
        When was the cover physically last modified on disk (used to determine if the cover needs to be reloaded).
        :param path: Path to the cover object. Note this is different from calibre - where the path provided is the
                     path to the folder containing the cover.
        :type path: LiuXin Location object
        :return:
        """
        return self.fsm.path.last_modified(path=path, utc=True)

    # 1) Using the book_file_map to lookup the file_id from the book_id and the fmt
    # 2) From the file_id get the file_row
    # 3) Using the folder_store_manager get the location of that file
    # 4) Download the given file to the given dest - if dest is a path, then copy the file to it - if it's a file
    #    like object then write to it - if dest is a location then move the given file internally in the stores
    def copy_format_to(
        self,
        book_id,
        fmt,
        fname,
        path,
        dest,
        windows_atomic_move=None,
        use_hardlink=False,
        report_file_size=None,
        allow_overwrite=False,
    ):
        """
        Copy a format to a given dest.
        If the dst is a Location, then the format will be moved to that location.
        :param book_id: The id of the book to work with
        :param fmt: The format in the book to copy
        :param fname: Name of the file to copy (not all this information should be actually required)
        :param path: A path or Location - if this is a Location then the fmt and fname are ignored
        :param dest: The place to copy the format to - can be a file object, Location or path
        :type dst: file_object, Location or path
        :param windows_atomic_move:
        :param use_hardlink: Should a hardlink be used during the copy operation (seems to make a hardlink, then copy
                             the files to where they go - no clue).
        :param report_file_size:
        :param allow_overwrite: True if files can be overwritten with other files, False if not
        :return:
        """
        if isinstance(path, Location):
            src_loc = path
        else:
            # Todo: fmt_abspath should use book_id and fmt, unless fmt is ambiguous (not fmt_priority) then fall back on file name
            src_loc = self.format_abspath(book_id=book_id, fmt=fmt, fname=fname, path=path)

        if windows_atomic_move is not None and isinstance(dest, six.string_types):
            err_str = "Error, you must pass the dest as a path when using windows_atomic_move"
            default_log.error(err_str)
            raise FolderStoreError(err_str)

        if isinstance(dest, Location):

            if self.fsm.path.exists(dest) and not allow_overwrite:
                err_str = "Cannot overwrite given file"
                raise NotImplementedError(err_str)

            # Todo: This should include windows atomic move - use it to move the file from a temp store?
            return self.fsm.move.move_file_to(
                src=src_loc,
                dst=dest,
                use_hardlink=use_hardlink,
                report_file_size=report_file_size,
            )

        # If the dest is a string, assume it's a path and download the src file to it
        elif isinstance(dest, six.string_types):
            return self.fsm.smart_move(
                src=src_loc,
                dest=dest,
                use_hardlink=use_hardlink,
                report_file_size=report_file_size,
            )

        # If the dest is a file like object, write the file to it
        # Todo: Should include the option of returning a file like pointer
        elif hasattr(dest, "write"):
            f = self.fsm.stores.read(target_location=src_loc, mode="rb", string_io=True)
            if report_file_size is not None:
                f.seek(0, os.SEEK_END)
                report_file_size(f.tell())
                f.seek(0)
            shutil.copyfileobj(f, dest)
            if hasattr(dest, "flush"):
                dest.flush()
            return True

        return False

    def format_abspath(self, book_id, fmt, fname, path):
        """
        Returns the absolute location of a fmt, using the book_id and the fmt.
        :param book_id: The id of the book containing the format
        :param fmt: The format to get the path to - if not a LiuXin style format (i.e. EPUB_1) will default to the first
                    format - thus EPUB becomes EPUB_1
        :param fname: Ignored by LiuXin - here for calibre compatibility
        :param path: Ignored by LiuXin - here for calibre compatibility
        :return:
        """
        if "_" not in fmt:
            fmt = deepcopy(fmt) + "_1"

        book_file_map = self.tables["formats"].book_file_map
        try:
            book_fmts_file_id_map = book_file_map[book_id]
        except KeyError:
            info_str = "Book id not in the book_file_map"
            raise KeyError(info_str)

        if fmt in book_fmts_file_id_map:
            file_id = book_fmts_file_id_map[fmt]
            file_row = self.get_row_from_id(table="files", row_id=file_id)
            return self.fsm.get_loc(asset_row=file_row)
        else:
            err_str = "Format not found in book"
            raise KeyError(err_str)

    def add_format(self, book_id, fmt, stream, title, author, path, current_name, mtime=None):
        """
        Add a format to the database - front end for the methods in folder store manager.
        :param book_id:
        :param fmt:
        :param stream:
        :param title:
        :param author:
        :param path:
        :param current_name:
        :param mtime:
        :return:
        """
        return self.fsm.add.format(
            book_id=book_id,
            fmt=fmt,
            stream=stream,
            title=title,
            author=author,
            path=path,
            current_name=current_name,
            mtime=mtime,
            override_file_name=None,
        )

    def write_backup(self, path, raw):
        """
        Write a backup for an objects metadata to the given path.
        :param path: Should be a path to the location of the folder in which the metadata is to be written.
        :type path: LiuXin Location object
        :param raw: The metadata as a binary string.
        :return:
        """
        md_path = self.fsm.path.join(path, "metadata.opf")
        self.fsm.stores.write(target_location=md_path, string=raw, mode="wb")

    def read_backup(self, path):
        """
        Read the metadata.opf file from a location and return it as a string.
        :param path: The Location object specifying where the metadata folder is
        :return metadata_string: The OPF file as a binary string
        """
        metadata_loc = self.fsm.path.join(path, "metadata.opf")
        return self.fsm.stores.read(target_location=metadata_loc, mode="rb")

    def remove_books(self, book_ids, permanent=False):
        """
        Remove books from the database.
        :param book_ids: A list of the book ids to remove
        :param permanent: If True, remove the files permanently - if False, put them in the recycle bin
        :return:
        """
        # Register books for the delete service
        book_rows = [self.db.get_row_from_id("books", b_id) for b_id in book_ids]
        self.fsm.delete.book_delete_service(book_rows=book_rows, cleanup_folders=True, permanent=permanent)

        # Delete from the database
        self.driver_wrapper.executemany("DELETE FROM books WHERE book_id=?", [(x,) for x in book_ids])

    def remove_formats(self, remove_map):
        """
        Remove formats from books in the database.
        :param remove_map: Keyed with the id of the book and valued with the format to remove.
        :type remove_map: dict
        :return:
        """
        paths = []
        for book_id, removals in iteritems(remove_map):
            for fmt, fname, path in removals:
                if not isinstance(path, Location):
                    path = self.format_abspath(book_id, fmt, fname, path)
                    if path is not None:
                        paths.append(path)
                else:
                    paths.append(path)
        try:
            self.fsm.delete.delete_files(paths)
        except:
            import traceback

            traceback.print_exc()

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK CUSTOM DATA BACKEND

    def add_custom_data(self, name, val_map, delete_first):
        """
        Record custom data in the books_plugin_data table - allows plugins to store data for books.
        :param name: The name of the plugin
        :param val_map: Keyed with the book_id and valued with the value to
        :param delete_first:
        :return:
        """
        if delete_first:
            self.driver_wrapper.execute("DELETE FROM books_plugin_data WHERE book_plugin_data_name=?", (name,))
        self.driver_wrapper.executemany(
            "INSERT OR REPLACE INTO books_plugin_data "
            "(book_plugin_data_book, book_plugin_data_name, book_plugin_data_val) VALUES "
            "(?, ?, ?)",
            [(book_id, name, json.dumps(val, default=to_json)) for book_id, val in iteritems(val_map)],
        )

    def get_custom_book_data(self, name, book_ids, default=None):
        """
        Get data from the book_plugin_data for the book_ids.
        :param name: The name of the plugin
        :param book_ids: An iterable of the book_ids
        :param default: Default value to return if the value can't be retrieved
        :return:
        """
        book_ids = frozenset(book_ids)

        def safe_load(val):
            try:
                return json.loads(val, object_hook=from_json)
            except:
                return default

        if len(book_ids) == 1:
            bid = next(iter(book_ids))
            ans = {
                book_id: safe_load(val)
                for book_id, val in self.driver_wrapper.execute(
                    "SELECT book_plugin_data_book, book_plugin_data_val "
                    "FROM books_plugin_data "
                    "WHERE book_plugin_data_book=? "
                    "AND book_plugin_data_name=?",
                    (bid, name),
                )
            }
            return ans or {bid: default}

        ans = {}
        for book_id, val in self.driver_wrapper.execute(
            "SELECT book_plugin_data_book, book_plugin_data_val "
            "FROM books_plugin_data WHERE book_plugin_data_name=?",
            (name,),
        ):
            if not book_ids or book_id in book_ids:
                val = safe_load(val)
                ans[book_id] = val
        return ans

    def delete_custom_book_data(self, name, book_ids):
        """
        Delete from the books_plugin_data table.
        :param name: The name of the plugin to delete data for
        :param book_ids: An iterable of the book_ids
        :return:
        """
        if book_ids:
            self.driver_wrapper.executemany(
                "DELETE FROM books_plugin_data " "WHERE book_plugin_data_book=? AND book_plugin_data_name=?",
                [(book_id, name) for book_id in book_ids],
            )
        else:
            self.driver_wrapper.execute("DELETE FROM books_plugin_data WHERE book_plugin_data_name=?", (name,))

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CONVERSION OPTIONS MANAGER

    # Todo: These all need to be moved to macros
    def get_ids_for_custom_book_data(self, name):
        return frozenset(
            r[0]
            for r in self.driver_wrapper.execute(
                "SELECT book_plugin_data_book FROM books_plugin_data " "WHERE book_plugin_data_name=?",
                (name,),
            )
        )

    def conversion_options(self, book_id, fmt):
        """
        Returns conversion option data for the given book_id and fmt from the database.
        :param book_id:
        :param fmt:
        :return:
        """
        conn = self.driver.get_connection()
        c = conn.cursor()
        stmt = (
            "SELECT conversion_option_data FROM conversion_options "
            "WHERE conversion_option_book=? AND conversion_option_format=?"
        )
        data = None
        for row in c.execute(stmt, (book_id, fmt.upper())):
            data = row[0]
        if data:
            return six_pickle.loads(bytes(data))

    def has_conversion_options(self, ids, fmt="PIPE"):
        """
        Checks to see if any of the given ids have conversion option data for the specified format.
        :param ids:
        :param fmt:
        :return:
        """
        ids = frozenset(ids)
        conn = self.driver.get_connection()
        with conn:
            self.driver_wrapper.execute(
                "DROP TABLE IF EXISTS conversion_options_temp;"
                "CREATE TEMP TABLE conversion_options_temp (id INTEGER PRIMARY KEY);"
            )
            self.driver_wrapper.executemany("INSERT INTO conversion_options_temp VALUES (?)", [(x,) for x in ids])
            c = conn.cursor()
            select_stmt = (
                "SELECT conversion_option_book FROM conversion_options WHERE conversion_option_format=? "
                "AND conversion_option_book IN (SELECT id FROM conversion_options_temp)"
            )
            for row in c.execute(select_stmt, (fmt.upper(),)):
                return True
            return False

    def delete_conversion_options(self, book_ids, fmt):
        """
        Remove conversion options for the given formats for all book_ids.
        :param book_ids:
        :param fmt:
        :return:
        """
        self.driver_wrapper.executemany(
            "DELETE FROM conversion_options " "WHERE conversion_option_book=? AND conversion_option_format=?",
            [(book_id, fmt.upper()) for book_id in book_ids],
        )

    def set_conversion_options(self, options, fmt):
        """
        Stores data in the conversion options table for the specified format.
        :param options:
        :param fmt:
        :return:
        """
        options = [
            (book_id, fmt.upper(), six_buffer(six_pickle.dumps(data, -1))) for book_id, data in iteritems(options)
        ]
        self.driver_wrapper.executemany(
            "INSERT OR REPLACE INTO conversion_options(book,format,data) VALUES (?,?,?)",
            options,
        )

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - MOVE TOOLS

    # No real ways to implement these properly
    def get_top_level_move_items(self, all_paths):
        raise NotImplementedError

    def move_library_to(
        self,
        all_paths,
        newloc,
        progress=(lambda item_name, item_count, total: None),
        abort=None,
    ):
        raise NotImplementedError

    def restore_book(self, book_id, path, formats="all"):
        raise NotImplementedError

    def backup_database(self, path=None):
        """
        Backs up the database using the method in the database driver.
        :param path: The path to backup to
        :return:
        """
        self.driver.direct_backup(path=path)

    # Todo: Implement the ideas from this in the actual update method. To whit
    # Todo: Make sure that the paths recorded in the db on case insensitive systems are all lower case
    # Todo: Include wider use of windows Atomic file move in the right places
    def update_path(self, book_id, title, author, path_field, formats_field):
        """
        Runs the updates for a book folder - updates the paths to the book and all the assets in it.
        :param book_id: id of the book to run the update for.
        :param title:
        :param author:
        :param path_field:
        :param formats_field:
        :return:
        """
        book_row = self.db.get_row_from_id("books", book_id)

        # Run the update
        self.fsm.update.book(book_row)

    def windows_check_if_files_in_use(self, paths):
        """
        Raises an EACCES IOError if any of the files in the folder of book_id are opened in another program on windows.
        :param paths:
        :return:
        """
        if iswindows:
            for path in paths:
                self.fsm.path.windows_check_if_file_in_use(path)

    # Tood: As format_abspath has changed in function this will not work. At all.
    def has_format(self, book_id, fmt, fname, path):
        return self.format_abspath(book_id, fmt, fname, path) is not None

    def format_metadata_from_loc(self, loc):
        """
        Returns the metadata for the file located at the given location.
        :param loc: format location
        :return:
        """
        ans = {}
        if self.fsm.path.exists(loc):
            ans["path"] = loc
            ans["size"] = self.fsm.path.getsize(loc=loc)
            ans["mtime"] = self.fsm.path.last_modified(path=loc, utc=True)
        return ans

    def format_metadata(self, book_id, fmt, fname, path):
        """
        Return metadata for a format
        :param book_id: The id of the book to examine
        :param fmt: The format to return the metadata for
        :param fname: Not used in LiuXin
        :param path: Not used in LiuXin
        :return: Keyed with path, size and mtime
        :rtype dict:
        """
        path = self.format_abspath(book_id, fmt, fname, path)
        ans = {}
        if path is not None:
            ans["path"] = path
            ans["size"] = self.fsm.path.getsize(loc=path)
            ans["mtime"] = self.fsm.path.last_modified(path=path, utc=True)
        return ans

    def format_hash(self, book_id, fmt, fname, path):
        """
        Return the hash for a format of a book.
        :param book_id: The id of the book to examine
        :param fmt: The format in that book
        :param fname: Not used in LiuXin
        :param path: Not used in LiuXin
        :return:
        """
        path = self.format_abspath(book_id, fmt, fname, path)
        if path is None:
            raise NoSuchFormat("Record %d has no fmt: %s" % (book_id, fmt))
        return self.fsm.path.gethash(loc=path)

    # Todo: Implement the different read-write modes for the stream open methods in the folder store database_driver_plugins
    # Current working for on disk - but mostly by accident.
    def apply_to_format(self, book_id, path, fname, fmt, func, missing_value=None):
        """
        Apply a given function to a format and return the result (note that the format stream is closed after the
        function is applied).
        :param book_id:
        :param path:
        :param fname:
        :param fmt:
        :param func:
        :param missing_value:
        :return:
        """
        path = self.format_abspath(book_id, fmt, fname, path)
        if path is None:
            return missing_value
        with self.fsm.stores.read(path, "r+b") as f:
            return func(f)

    def cover_abspath(self, book_id, path):
        """
        Return an absolute path to the cover.
        :param book_id:
        :param path: Location of the object.
        :type path: LiuXin location object.
        :return:
        """
        return path

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - PATH TOOLS FOR SQLITE DATABASES

    @classmethod
    def exists_at(cls, path, db_name="metadata.db"):
        """
        Checks to see if the database exists at the given path.
        :param path:
        :return:
        """
        return path and os.path.exists(os.path.join(path, db_name))

    def normpath(self, path):
        path = os.path.abspath(os.path.realpath(path))
        if not self.is_case_sensitive:
            path = os.path.normcase(path).lower()
        return path

    def is_deletable(self, path):
        """

        :param path:
        :return:
        """
        return path and not self.normpath(self.library_path).startswith(self.normpath(path))

    @property
    def library_path(self):
        return self.metadata

    def reopen(self):
        raise NotImplementedError("Not currently in use")


#
# ----------------------------------------------------------------------------------------------------------------------
