#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Performance cache for data from the library database.
"""


from __future__ import unicode_literals, division, absolute_import, print_function

import json
import os
import pprint
import traceback
import random
import re
import shutil
import operator
from io import BytesIO
from collections import defaultdict
from functools import partial
from time import time

from past.builtins import unicode

from LiuXin.exceptions import NotInCache

from LiuXin.utils.calibre import isbytestring, as_unicode

from LiuXin.constants import iswindows, preferred_encoding

try:
    from LiuXin.customize.ui import run_plugins_on_import
except ImportError:

    def run_plugins_on_import(file):
        return file


try:
    from LiuXin.customize.ui import run_plugins_on_postimport
except ImportError:

    def run_plugins_on_postimport(file):
        return file


try:
    from LiuXin.customize.ui import run_plugins_on_postadd
except ImportError:

    def run_plugins_on_postadd(file, *args, **kwargs):
        return file


from LiuXin.databases.caches.calibre.tables.many_many_tables import (
    CalibreManyToManyTable,
)
from LiuXin.databases.caches.calibre.tables.many_one_tables import CalibreManyToOneTable
from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibreCompositeTable,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import CalibreOneToOneTable

from LiuXin.databases.caches.utils import api, read_api, write_api

from LiuXin.databases.caches.calibre.tables import calibre_create_table

from LiuXin.databases import SPOOL_SIZE, _get_next_series_num_for_list
from LiuXin.databases.adaptors import get_series_values
from LiuXin.databases.caches.calibre.fields import (
    calibre_create_field,
    IDENTITY,
    InvalidLinkTable,
)
from LiuXin.databases.caches.calibre.tables.base import CalibreVirtualTable
from LiuXin.databases.categories import get_categories
from LiuXin.databases.lazy import FormatMetadata, FormatsList, ProxyMetadata
from LiuXin.utils.general_ops.python_tools import uniq

from LiuXin.exceptions import NoSuchFormat

from LiuXin.file_formats import check_ebook_format
from LiuXin.file_formats.opf.opf2 import metadata_to_opf

from LiuXin.metadata import string_to_authors, author_to_author_sort
from LiuXin.metadata.book.base import calibreMetadata as Metadata
from LiuXin.metadata.metadata import MetaData as LiuXinMetadata

from LiuXin.utils.config.config_base import tweaks
from LiuXin.utils.date import now as nowf, utcnow, UNDEFINED_DATE
from LiuXin.utils.date import parse_date
from LiuXin.utils.icu import sort_key
from LiuXin.utils.icu import lower as icu_lower
from LiuXin.utils.general_ops.json_ops import smart_bool
from LiuXin.utils.file_ops.file_ops import local_open as lopen
from LiuXin.utils.localization import trans as _
from LiuXin.utils.logger import default_log
from LiuXin.utils.ptempfiles import (
    base_dir,
    PersistentTemporaryFile,
    SpooledTemporaryFile,
)

from LiuXin.databases.caches.utils import run_import_plugins
from LiuXin.databases.caches.utils import _add_newbook_tag

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_cmp
from LiuXin.utils.lx_libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin.utils.lx_libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types


from collections import defaultdict

from typing import Any, TypeVar, Optional

from LiuXin.customize.cache import BaseCache

from LiuXin.databases.caches.utils import read_api, write_api
from LiuXin.databases.locking import SafeReadLock
from LiuXin.databases.search import Search

from LiuXin.library.metadata import Metadata as LibraryMetadata

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import itervalues

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


T = TypeVar("T")


class BaseCalibreCache(BaseCache):
    """
    Base class for caches descending from the original calibre cache.
    """

    dynamic_category_preferences: frozenset[str] = frozenset(
        {"grouped_search_make_user_categories", "grouped_search_terms", "user_categories"}
    )

    def __init__(self, backend) -> None:
        """
        Initialize the cache - further action needs to be taken to actually load the data from the backend.

        The backend is the actual database connection.
        :param backend:
        """

        super().__init__(backend=backend)

        # Todo: This is awful. Make it go away.
        self.backend.execute = self.backend.driver_wrapper.execute
        self.backend.executemany = self.backend.driver_wrapper.executemany

        # In the original object the unlocked versions of the various objects are stored with an _ - here they are
        # stored both there and in the UnlockedCache class - so they can be accessed via self.unlock.method_name

        # This is to stop pycharm throwing function not found messages while preserving the original API
        # Common properties of the two classes will be stored in the common_dict

        self.format_metadata_cache = defaultdict(dict)

        # Used to store the templates for composite columns - will be passed to any metadata objects created by this
        # cache - so that they can return values for the field if queried for it
        self.formatter_template_cache = {}

        self.dirtied_cache = {}
        self.dirtied_sequence: int = 0

        self.cover_caches = set()
        self.clear_search_cache_count = 0

        # Add ins to assist with manipulating the database
        self.library_md = LibraryMetadata(database=self.backend, library=None, override_fsm=self.backend.fsm)

        self._search_api: Search = Search(self, "saved_searches", self.field_metadata.get_search_terms())
        self.initialize_dynamic()

        # Todo: Make this go away
        # Patching methods onto the backend - now safely removed to here - for backwards api compatibility
        backend.read_tables = self.read_tables
        backend.initialize_tables = self.initialize_tables
        backend.initialize_custom_columns = self.initialize_custom_columns

        # Properties
        self.FIELD_MAP = dict()

    @write_api
    def initialize_dynamic(self) -> None:
        """
        Initialize dynamic quantities which are stored in the cache.

        Dynamic quantities are a mixed bag of things which might change a lot.
        Read the dirtied books out of the database, add the user defined tag categories, add the grouped search terms
        e.t.c.
        :return:
        """
        self.dirtied_cache = self.backend.macros.get_dirtied_cache()
        if self.dirtied_cache:
            self.dirtied_sequence = max(itervalues(self.dirtied_cache)) + 1

        # Can be subclassed to do anything else that needs to be done
        self._initialize_dynamic_categories()

    @property
    def field_metadata(self):
        """
        Returns the field metadata object stored in the backend.

        This defines metadata for the individual fields.
        :return:
        """
        return self.backend.field_metadata

    def _backend_read_data(self) -> None:
        """
        Read data out of the database into the specialized internal stores for it.

        :return:
        """
        # Initialize_prefs must be called before initialize_custom_columns because icc can set a pref.
        self._do_backend_prefs_startup()

        self.initialize_custom_columns()
        self.initialize_tables()

    # Todo: This should ALL BE IN BACKEND - with a flag to indicate if it's run or not
    def _do_backend_prefs_startup(self) -> None:
        """
        Preform startup on the backend data preferences.
        :return:
        """
        # Initialize_prefs must be called before initialize_custom_columns because icc can set a pref.
        self.backend.initialize_prefs(
            self.backend.default_prefs,
            self.backend.restore_all_prefs,
            self.backend.pref_progress_callback,
        )

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
        return self.backend.prefs.get(name, default)

    @write_api
    def set_pref(self, name: str, val: Any) -> None:
        """
        Set the specified preference to the specified value. See also :meth:`pref`.

        :param name:
        :param val:
        :return:
        """
        self.backend.prefs.set(name, val)
        if name == "grouped_search_terms":
            self.unlock.clear_search_caches()
        if name in self.dynamic_category_preferences:
            self._initialize_dynamic_categories()


class CalibreCache(BaseCalibreCache):
    """
    An in-memory cache of the metadata.db file from a calibre library.
    This class also serves as a threadsafe API for accessing the database.
    The in-memory cache is maintained in normal form for maximum performance.

    SQLITE is simply used as a way to read and write from metadata.db robustly.
    All table reading/sorting/searching/caching logic is re-implemented. This was necessary for maximum performance and
    flexibility.
    """

    def __init__(self, backend) -> None:
        super(CalibreCache, self).__init__(backend=backend)

    @api
    def init(self) -> None:
        """
        Initialize this cache with data from the backend.
        :return:
        """
        # Read information describing the database into internal caches
        # Loads the database preferences, the custom column data and the declared table data into their stores
        self._backend_read_data()

        self.init_called = True

        with self.write_lock:

            self.read_tables()

            bools_are_tristate = self.backend.prefs["bools_are_tristate"]

            # Field creation occurs here
            for field, table in iteritems(self.tables):

                self.fields[field] = calibre_create_field(field, table, bools_are_tristate)
                if table.metadata["datatype"] == "composite":
                    self.composites[field] = self.fields[field]

                # Preform a read of the attribute tables
                self.fields[field].read_attribute_tables(db=self.backend)

            # The ondevice field is only ever a virtual field - has the book been copied to the device?
            self.fields["ondevice"] = calibre_create_field(
                "ondevice", CalibreVirtualTable("ondevice"), bools_are_tristate
            )

            # Cross linking the fields - some fields need access to others to work properly
            # Todo: Needs to go into the fields package
            for name, field in iteritems(self.fields):

                # Custom series field index
                if name[0] == "#" and name.endswith("_index"):
                    field.series_field = self.fields[name[: -len("_index")]]
                    self.fields[name[: -len("_index")]].index_field = field

                # Regular series has it's index field added
                elif name == "series_index":
                    field.series_field = self.fields["series"]
                    self.fields["series"].index_field = field

                # authors field has the author sort field added
                elif name == "authors":
                    field.author_sort_field = self.fields["author_sort"]

                # title field has the title sort field added
                elif name == "title":
                    field.title_sort_field = self.fields["sort"]

            # Todo: Render this obsolete and remove it
            self.fields["series"].internal_update_used = True

        if self.backend.prefs["update_all_last_mod_dates_on_start"]:
            self.update_last_modified(self.all_book_ids())
            self.backend.prefs.set("update_all_last_mod_dates_on_start", False)

    def initialize_tables(self) -> None:
        """
        Initialize Tables from databases.tables - store them internally.

        Called as part of the __init__ method in cache.
        This just sets up the tables - the actual read is preformed elsewhere - again in the cache.
        Loads both the builtins tables and the custom column tables.

        :return: None - all changes are made internally.
        """
        tables = self.tables = {}

        # Initialize the inbuilt tables
        # - one_to_one_tables
        for col in (
            "title",
            "sort",
            "author_sort",
            "series_index",
            "timestamp",
            "pubdate",
            "uuid",
            "path",
            "last_modified",
            "notes",
            "cover",
        ):
            tables[col] = calibre_create_table(name=col, metadata=self.field_metadata[col].copy(), fsm=self.backend.fsm)

        # - many_to_one tables
        for col in ("series", "publisher", "subjects", "synopses", "genre"):
            tables[col] = calibre_create_table(name=col, metadata=self.field_metadata[col].copy(), fsm=self.backend.fsm)

        # - one_many tables
        for col in ("comments",):
            tables[col] = calibre_create_table(name=col, metadata=self.field_metadata[col].copy(), fsm=self.backend.fsm)

        # - many_many tables
        for col in ("authors", "tags", "formats", "identifiers", "languages", "rating"):
            tables[col] = calibre_create_table(name=col, metadata=self.field_metadata[col].copy(), fsm=self.backend.fsm)

        # - virtual tables
        tables["size"] = calibre_create_table(
            name="size",
            metadata=self.field_metadata["size"].copy(),
            fsm=self.backend.fsm,
        )

        # The positions of the various columns read from one of the meta views
        # Todo: THIS IS PROBABLY ALL WRONG - NEED TO FIX IT
        self.FIELD_MAP = {
            "id": 0,
            "title": 1,
            "authors": 2,
            "timestamp": 3,
            "size": 4,
            "rating": 5,
            "tags": 6,
            "comments": 7,
            "series": 8,
            "publisher": 9,
            "series_index": 10,
            "sort": 11,
            "author_sort": 12,
            "formats": 13,
            "path": 14,
            "pubdate": 15,
            "uuid": 16,
            "cover": 17,
            "au_map": 18,
            "last_modified": 19,
            "identifiers": 20,
            "languages": 21,
        }

        # The field record index is subject to change and determined here
        for k, v in iteritems(self.FIELD_MAP):
            self.field_metadata.set_field_record_index(k, v, prefer_custom=False)

        # Where we start when adding additional fields to the field map
        base = max(itervalues(self.FIELD_MAP))

        # Initialize the custom tables - with their own place in the column map
        for label_ in sorted(self.backend.custom_column_label_map):

            data = self.backend.custom_column_label_map[label_]
            label = self.field_metadata.custom_field_prefix + label_
            metadata = self.field_metadata[label].copy()
            link_table = self.backend.custom_columns.custom_table_names(data["num"])[1]

            # Assign the field the next free slot in the FIELD_MAP
            self.FIELD_MAP[data["num"]] = base = base + 1
            self.field_metadata.set_field_record_index(label_, base, prefer_custom=True)

            if data["datatype"] == "series":
                # account for the series index column. Field_metadata knows that the series index is one larger than the
                # series. If you change it here, be sure to change it there as well.
                # By default the index of a series like field will always be assigned to one after the field in
                # FIELD_MAP
                self.FIELD_MAP[str(data["num"]) + "_index"] = base = base + 1
                self.field_metadata.set_field_record_index(label_ + "_index", base, prefer_custom=True)

            # Is the data normalized?
            if data["normalized"]:

                if metadata["is_multiple"]:
                    tables[label] = CalibreManyToManyTable(label, metadata, link_table=link_table, custom=True)
                else:
                    # Construct the series type table itself and note that it's custom
                    tables[label] = CalibreManyToOneTable(label, metadata, link_table=link_table, custom=True)
                    tables[label].custom = True

                    # Add the index type table - it will be noted that it's custom below
                    if metadata["datatype"] == "series":
                        # Create series index table
                        label += "_index"
                        metadata = self.field_metadata[label].copy()
                        metadata["column"] = "extra"
                        metadata["table"] = link_table
                        tables[label] = CalibreOneToOneTable(label, metadata, custom=True)

            else:

                if data["datatype"] == "composite":
                    tables[label] = CalibreCompositeTable(label, metadata, custom=True)
                else:
                    tables[label] = CalibreOneToOneTable(label, metadata, custom=True)

            # Note that the table is a custom column - needed to use the right read routine
            tables[label].custom = True
            try:
                tables[label].set_link_table(self)
            except AttributeError:
                pass

        self.FIELD_MAP["ondevice"] = base = base + 1
        self.field_metadata.set_field_record_index("ondevice", base, prefer_custom=False)
        self.FIELD_MAP["marked"] = base = base + 1
        self.field_metadata.set_field_record_index("marked", base, prefer_custom=False)
        self.FIELD_MAP["series_sort"] = base = base + 1
        self.field_metadata.set_field_record_index("series_sort", base, prefer_custom=False)

    # Todo: Merge with the custom_columns class over in library - again? No. There's just loads of custom column logic
    #       everywhere for no reason some of which is breaking everything
    def initialize_custom_columns(self) -> None:
        """
        Initialize the custom columns from the database.

        Needs to read and parse the custom columns defined in the database.
        :return None: All changes are made internally
        """
        self.backend.custom_columns_deleted = False
        all_tables = self.backend.all_tables

        # Delete custom columns previously marked for deletion
        with self.backend.conn:
            for record in self.backend.driver_wrapper.execute(
                "SELECT custom_column_id " "FROM custom_columns " "WHERE custom_column_mark_for_delete=1;"
            ):
                num = record[0]
                table, lt = self.backend.custom_table_names(num)
                stmt = """\
                        DROP INDEX   IF EXISTS {table}_idx;
                        DROP INDEX   IF EXISTS {lt}_aidx;
                        DROP INDEX   IF EXISTS {lt}_bidx;
                        DROP TRIGGER IF EXISTS fkc_update_{lt}_a;
                        DROP TRIGGER IF EXISTS fkc_update_{lt}_b;
                        DROP TRIGGER IF EXISTS fkc_insert_{lt};
                        DROP TRIGGER IF EXISTS fkc_delete_{lt};
                        DROP TRIGGER IF EXISTS fkc_insert_{table};
                        DROP TRIGGER IF EXISTS fkc_delete_{table};
                        DROP VIEW    IF EXISTS tag_browser_{table};
                        DROP VIEW    IF EXISTS tag_browser_filtered_{table};
                        DROP TABLE   IF EXISTS {table};
                        DROP TABLE   IF EXISTS {lt};
                        """.format(
                    table=table, lt=lt
                )
                self.backend.driver_wrapper.executemany(stmt)
                self.backend.prefs.set("update_all_last_mod_dates_on_start", True)
            self.backend.driver_wrapper.execute("DELETE FROM custom_columns WHERE custom_column_mark_for_delete=1")

        # Prepare to load metadata for the custom columns
        self.backend.custom_column_label_map, self.backend.custom_column_num_map = (
            {},
            {},
        )
        self.backend.custom_column_num_to_label_map = {}
        triggers = []
        remove = []

        # Todo: Not being able to guarentee that int results returned from the database are being converted properly is a real problem
        custom_tables = self.backend.custom_tables
        cc = "custom_column_"
        for record in self.backend.driver_wrapper.get_all_rows(table="custom_columns"):
            try:
                data = {
                    "datatype": record[cc + "datatype"],
                    "display": json.loads(record[cc + "display"]),
                    "editable": smart_bool(record[cc + "editable"]),
                    "is_multiple": smart_bool(record[cc + "is_multiple"]),
                    "label": record[cc + "label"],
                    "name": record[cc + "name"],
                    "normalized": smart_bool(int(record[cc + "normalized"])),
                    "num": int(record[cc + "id"]),
                }
            except Exception as e:
                err_str = "Parsing the record into a dict failed - deleting the record and continuing"
                default_log.log_exception(err_str, e, "ERROR", ("record", record))
                del_stmt = "DELETE FROM custom_columns WHERE custom_column_id=?;"
                self.backend.driver_wrapper.execute(del_stmt, record["custom_column_id"])
                continue

            if data["display"] is None:
                data["display"] = {}

            # set up the is_multiple separator dict
            if data["is_multiple"]:
                if data["display"].get("is_names", False):
                    seps = {
                        "cache_to_list": "|",
                        "ui_to_list": "&",
                        "list_to_ui": " & ",
                    }
                elif data["datatype"] == "composite":
                    seps = {"cache_to_list": ",", "ui_to_list": ",", "list_to_ui": ", "}
                else:
                    seps = {"cache_to_list": "|", "ui_to_list": ",", "list_to_ui": ", "}
            else:
                seps = {}
            data["multiple_seps"] = seps

            table, lt = self.backend.driver_wrapper.custom_table_names(data["num"])

            # If the data is not normalized just look for the custom column table - if the data is normalized then look
            # for the table and the link table connecting it to the books table.
            if table not in custom_tables or (data["normalized"] and lt not in all_tables):
                err_str = (
                    "custom column table not found in custom_tables or the table is normalized and the "
                    "link_table was not found in the all_tables category\n"
                )
                default_log.log_variables(
                    err_str,
                    "INFO",
                    ("table", table),
                    ("custom_tables", custom_tables),
                    ("data", data),
                    ("lt", lt),
                    ("all_tables", pprint.pformat(all_tables)),
                    ("table in custom_tables", table in custom_tables),
                    ("lt not in all tables", lt not in all_tables),
                )
                remove.append(data)
                continue

            self.backend.custom_column_num_map[data["num"]] = self.backend.custom_column_label_map[data["label"]] = data
            self.backend.custom_column_num_to_label_map[data["num"]] = data["label"]

            # Create Foreign Key triggers
            if data["normalized"]:
                trigger = "DELETE FROM %s WHERE book=OLD.id;" % lt
            else:
                trigger = "DELETE FROM %s WHERE book=OLD.id;" % table
            triggers.append(trigger)

        if remove:
            with self.backend.conn:
                for data in remove:
                    default_log.debug("WARNING: Custom column %r not found, removing." % data["label"])
                    self.backend.driver_wrapper.execute(
                        "DELETE FROM custom_columns WHERE custom_column_id=?",
                        (data["num"],),
                    )

        if triggers:
            with self.backend.conn:
                self.backend.driver_wrapper.execute(
                    """\
                    CREATE TEMP TRIGGER custom_books_delete_trg
                        AFTER DELETE ON books
                        BEGIN
                        %s
                    END;
                    """
                    % (" \n".join(triggers))
                )

        self._setup_custom_data_adaptors()

        # # Create Tag Browser categories for custom columns
        # for k in sorted(iterkeys(self.backend.custom_column_label_map)):
        #     v = self.backend.custom_column_label_map[k]
        #     if v['normalized']:
        #         is_category = True
        #     else:
        #         is_category = False
        #     is_m = v['multiple_seps']
        #     tn = 'custom_column_{0}'.format(v['num'])
        #     self.field_metadata.add_custom_field(label=v['label'], table=tn, column='value', datatype=v['datatype'],
        #                                          colnum=v['num'], name=v['name'], display=v['display'],
        #                                          is_multiple=is_m, is_category=is_category,
        #                                          is_editable=v['editable'],
        #                                          is_csp=False)

    def _setup_custom_data_adaptors(self) -> None:
        """
        Used to convert data stored in custom columns to usable values on the way back into the program from the db.

        :return None: All changes are made internally.
        """
        # Setup data adapters
        def adapt_text(x, d):
            if d["is_multiple"]:
                if x is None:
                    return []
                if isinstance(x, (str, unicode, bytes)):
                    x = x.split(d["multiple_seps"]["ui_to_list"])
                x = [y.strip() for y in x if y.strip()]
                x = [y.decode(preferred_encoding, "replace") if not isinstance(y, unicode) else y for y in x]
                return [" ".join(y.split()) for y in x]
            else:
                return x if x is None or isinstance(x, unicode) else x.decode(preferred_encoding, "replace")

        def adapt_datetime(x, d):
            if isinstance(x, (str, unicode, bytes)):
                x = parse_date(x, assume_utc=False, as_utc=False)
            return x

        def adapt_bool(x, d):
            if isinstance(x, (str, unicode, bytes)):
                x = x.lower()
                if x == "true":
                    x = True
                elif x == "false":
                    x = False
                elif x == "none":
                    x = None
                else:
                    x = bool(int(x))
            return x

        def adapt_enum(x, d):
            cand = adapt_text(x, d)
            if not cand:
                cand = None
            return cand

        def adapt_number(x, d):
            if x is None:
                return None
            if isinstance(x, (str, unicode, bytes)):
                if x.lower() == "none":
                    return None
            if d["datatype"] == "int":
                return int(x)
            return float(x)

        # Setup the custom data adaptors
        self.backend.custom_data_adapters = {
            "float": adapt_number,
            "int": adapt_number,
            "rating": lambda x, d: x if x is None else min(10.0, max(0.0, float(x))),
            "bool": adapt_bool,
            "comments": lambda x, d: adapt_text(x, {"is_multiple": False}),
            "datetime": adapt_datetime,
            "text": adapt_text,
            "series": adapt_text,
            "enumeration": adapt_enum,
        }

    def read_tables(self):
        """
        Read all data from the db into the python in-memory tables.
        Data is read from the backend and stored in the in-memory cache
        :return:
        """
        # Use a single transaction, to ensure nothing modifies the db while we are reading
        with self.backend.lock:

            for table in itervalues(self.tables):
                try:
                    table.read(self.backend)
                except:
                    print("Failed to read table:", table.name)
                    import pprint

                    pprint.pprint(table.metadata)
                    raise

    def _initialize_dynamic_categories(self):
        """
        Prepare the categories, including the user set categories.
        Reconstruct the user categories, putting them into field_metadata and add grouped search term user categories.
        :return:
        """
        # Reconstruct the user tag categories, putting them into field_metadata (which stores md about all currently
        # existing fields, real and virtual)
        fm = self.field_metadata
        fm.remove_dynamic_categories()
        for user_cat in sorted(iterkeys(self.unlock.pref("user_categories", {})), key=sort_key):
            cat_name = "@" + user_cat  # add the '@' to avoid name collision
            while cat_name:
                try:
                    fm.add_user_category(label=cat_name, name=user_cat)
                except ValueError:
                    break  # Can happen since we are removing dots and adding parent categories ourselves
                cat_name = cat_name.rpartition(".")[0]

        # add grouped search terms user categories
        muc = frozenset(self.unlock.pref("grouped_search_make_user_categories", []))
        for cat in sorted(iterkeys(self.unlock.pref("grouped_search_terms", {})), key=sort_key):
            if cat in muc:
                # There is a chance that these can be duplicates of an existing
                # user category. Print the exception and continue.
                try:
                    self.field_metadata.add_user_category(label="@" + cat, name=cat)
                except ValueError:
                    traceback.print_exc()

        if len(self._search_api.saved_searches.names()) > 0:
            self.field_metadata.add_search_category(label="search", name=_("Searches"))

        # Read existing grouped search terms off the database prefs and add them
        self.field_metadata.add_grouped_search_terms(self.unlock.pref("grouped_search_terms", {}))
        self.unlock.refresh_search_locations()

    @write_api
    def initialize_template_cache(self):
        """
        Setup the formatter template cache and start it as an empty set.
        :return:
        """
        self.formatter_template_cache = {}

    @write_api
    def set_user_template_functions(self, user_template_functions):
        self.backend.set_user_template_functions(user_template_functions)

    @write_api
    def clear_composite_caches(self, book_ids=None):
        """
        Clear caches for the composite tables - tables whose values are composed of more than one field.
        :param book_ids:
        :return:
        """
        for field in itervalues(self.composites):
            field.clear_caches(book_ids=book_ids)

    @write_api
    def clear_search_caches(self, book_ids=None):
        self.clear_search_cache_count += 1
        self._search_api.update_or_clear(self, book_ids)

    @read_api
    def last_modified(self):
        """
        When was the last change made to the database?
        :return:
        """
        return self.backend.last_modified()

    @write_api
    def clear_caches(self, book_ids=None, template_cache=True, search_cache=True):
        """
        Clear all the sub caches for the cache.
        :param book_ids: Clear the format metadata cache for the given book ids.
        :param template_cache: Clear the template cache?
        :param search_cache: Clear the search_cache
        :return:
        """
        if template_cache:
            self.unlock.initialize_template_cache()  # Clear the formatter template cache

        for field in itervalues(self.fields):
            if hasattr(field, "clear_caches"):
                field.clear_caches(book_ids=book_ids)  # Clear the composite cache and ondevice caches

        if book_ids:
            for book_id in book_ids:
                self.format_metadata_cache.pop(book_id, None)
        else:
            self.format_metadata_cache.clear()

        if search_cache:
            self.unlock.clear_search_caches(book_ids)

    @write_api
    def reload_from_db(self, clear_caches=True):
        """
        Reload some internally stored cache data from the database.
        This is not enough to account for the presence of custom columns - you need to reload the LibraryDatabase
        (effectively doing a restart) before they will show up.
        :param clear_caches:
        :return:
        """
        if clear_caches:
            self.unlock.clear_caches()

        # Prevent other processes, such as calibredb from interrupting the reload by locking the db
        with self.backend.conn:
            self.backend.prefs.load_from_db()
            self._search_api.saved_searches.load_from_db()
            for field in itervalues(self.fields):
                if hasattr(field, "table"):
                    field.table.read(self.backend)  # Reread data from metadata.db

    def _get_metadata(self, book_id, get_user_categories=True):  # {{{
        """
        Return a calibre metadata object for the given book id
        :param book_id:
        :param get_user_categories:
        :return:
        """
        mi = Metadata(None, template_cache=self.formatter_template_cache)

        mi._proxy_metadata = ProxyMetadata(self, book_id, formatter=mi.formatter)

        author_ids = self.unlock.field_ids_for("authors", book_id)
        adata = self.unlock.author_data(author_ids)
        aut_list = [adata[i] for i in author_ids]
        aum = []
        aus = {}
        aul = {}
        for rec in aut_list:
            aut = rec["name"]
            aum.append(aut)
            aus[aut] = rec["sort"]
            aul[aut] = rec["link"]
        mi.title = self.unlock.field_for("title", book_id, default_value=_("Unknown"))
        mi.authors = aum
        mi.author_sort = self.unlock.field_for("author_sort", book_id, default_value=_("Unknown"))
        # Todo: Add creator sort map to LiuXin metadata
        mi.author_sort_map = aus
        # Todo: What is this? Add analogues case to LiuXin metadata
        mi.author_link_map = aul
        mi.comments = self.unlock.field_for("comments", book_id)
        mi.publisher = self.unlock.field_for("publisher", book_id)
        n = utcnow()
        mi.timestamp = self.unlock.field_for("timestamp", book_id, default_value=n)
        mi.pubdate = self.unlock.field_for("pubdate", book_id, default_value=n)
        mi.uuid = self.unlock.field_for("uuid", book_id, default_value="dummy")
        mi.title_sort = self.unlock.field_for("sort", book_id, default_value=_("Unknown"))
        mi.last_modified = self.unlock.field_for("last_modified", book_id, default_value=n)
        formats = self.unlock.field_for("formats", book_id)
        mi.format_metadata = {}
        mi.languages = list(self.unlock.field_for("languages", book_id, default_value=()))
        if not formats:
            good_formats = None
        else:
            mi.format_metadata = FormatMetadata(self, book_id, formats)
            good_formats = FormatsList(sorted(formats), mi.format_metadata)

        # These three attributes are returned by the db2 get_metadata(), however, we dont actually use them anywhere
        # other than templates, so they have been removed, to avoid unnecessary overhead. The templates all use
        # _proxy_metadata.
        # mi.book_size   = self.unlock.field_for('size', book_id, default_value=0)
        # mi.ondevice_col = self.unlock.field_for('ondevice', book_id, default_value='')
        # mi.db_approx_formats = formats
        mi.formats = good_formats
        mi.has_cover = _("Yes") if self.unlock.field_for("cover", book_id, default_value=False) else ""
        mi.tags = list(self.unlock.field_for("tags", book_id, default_value=()))
        mi.series = self.unlock.field_for("series", book_id)
        if mi.series:
            mi.series_index = self.unlock.field_for("series_index", book_id, default_value=1.0)
        mi.rating = self.unlock.field_for("rating", book_id)
        mi.set_identifiers(self.unlock.field_for("identifiers", book_id, default_value={}))
        # Todo: This seems to be ... well ... not even wrong
        mi.application_id = book_id
        # Todo: Check that this has been properly set for the LiuXin metadata object
        mi.id = book_id
        composites = []
        for key, meta in self.field_metadata.custom_iteritems():
            mi.set_user_metadata(key, meta)
            if meta["datatype"] == "composite":
                composites.append(key)
            else:
                val = self.unlock.field_for(key, book_id)
                if isinstance(val, tuple):
                    val = list(val)
                extra = self.unlock.field_for(key + "_index", book_id)
                mi.set(key, val=val, extra=extra)
        for key in composites:
            mi.set(key, val=self.unlock.composite_for(key, book_id, mi))

        user_cat_vals = {}
        if get_user_categories:
            user_cats = self.backend.prefs["user_categories"]
            for ucat in user_cats:
                res = []
                for name, cat, ign in user_cats[ucat]:
                    v = mi.get(cat, None)
                    if isinstance(v, list):
                        if name in v:
                            res.append([name, cat])
                    elif name == v:
                        res.append([name, cat])
                user_cat_vals[ucat] = res
        mi.user_categories = user_cat_vals

        return mi

    # }}}

    # Cache Layer API {{{

    @read_api
    def field_for(self, name, book_id, default_value=None):
        """
        Return the value of the field ``name`` for the book identified by ``book_id``. If no such book exists or it has
        no defined value for the field ``name`` or no such field exists, then ``default_value`` is returned.
        ``default_value`` is not used for title, title_sort, authors, author_sort and series_index. This is because
        these always have values in the db.
        ``default_value`` is used for all custom columns.
        The returned value for is_multiple fields are always tuples, even when no values are found (in other words,
        default_value is ignored). The exception is identifiers for which the returned value is always a dict.
        The returned tuples are always in link order, that is, the order in which they were created.
        Will KeyError if the name doesn't correspond to one of the known fields.
        :param name:
        :param book_id:
        :param default_value:
        :return:
        """
        if self.composites and name in self.composites:
            return self.composite_for(name, book_id, default_value=default_value)
        try:
            field = self.fields[name]
        except KeyError:
            if name not in self.fields:
                err_str = "field not found in list of currently valid field"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("name", name),
                    ("self.fields.keys()", self.fields.keys()),
                )
                raise KeyError(err_str)
            return default_value

        if field.is_multiple:
            # If the default value is None, then substitute the field default value
            if default_value is None:
                default_value = field.default_value

        try:
            return field.for_book(book_id, default_value=default_value)
        # Book probably has no entries for that particular field
        except (KeyError, IndexError):
            return default_value

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
        if field_obj.is_composite:
            return field_obj.get_value_with_cache(book_id, self.unlock.get_proxy_metadata)
        if field_obj.is_multiple:
            default_value = field_obj.default_value
        try:
            return field_obj.for_book(book_id, default_value=default_value)
        except (KeyError, IndexError):
            return default_value

    @read_api
    def all_field_for(self, field, book_ids, default_value=None):
        """
        Same as field_for, except that it operates on multiple books at once.
        :param field:
        :param book_ids:
        :param default_value: This value will be added to the map if there isn't another value to record.
        :return book_id_val_map:
        """
        field_obj = self.fields[field]
        return {
            book_id: self.unlock.fast_field_for(field_obj, book_id, default_value=default_value) for book_id in book_ids
        }

    @read_api
    def composite_for(self, name, book_id, mi=None, default_value=""):
        """
        Return the value for a composite field for the specified book id.
        The function which does the work of
        :param name:
        :param book_id:
        :param mi:
        :param default_value:
        :return:
        """
        try:
            f = self.fields[name]
        except KeyError:
            return default_value

        if mi is None:
            return f.get_value_with_cache(book_id, self.unlock.get_proxy_metadata)
        else:
            return f._render_composite_with_cache(book_id, mi, mi.formatter, mi.template_cache)

    @read_api
    def field_ids_for(self, name, book_id):
        """
        Return the ids (as a tuple) for the values that the field ``name`` has on the book identified by ``book_id``.
        If there are no values, or no such book, or no such field, an empty tuple is returned.
        :param name: The name of the field to return for
        :param book_id: The id of the book to return the value for
        :return field_ids_tuple: A tuple ids in the linked field
        """
        field_obj = self.fields[name]
        try:
            return field_obj.ids_for_book(book_id)
        except (KeyError, IndexError):
            return ()

    @read_api
    def books_for_field(self, name, item_id):
        """
        Return all the books associated with the item identified by ``item_id``, where the item belongs to the field
        ``name``.
        Returned value is a set of book ids, or the empty set if the item or the field does not exist.
        :param name:
        :param item_id:
        :return:
        """
        field_obj = self.fields[name]
        # Todo: Errors should be thrown - not silently lost
        try:
            return field_obj.books_for(item_id)
        except (KeyError, IndexError):
            return set()

    @read_api
    def all_book_ids(self, type=frozenset):
        """
        Returns all the book_ids known to the database
        :param type: e.g. frozenset
        :return:
        """
        return type(self.fields["uuid"].table.book_col_map)

    @read_api
    def all_field_ids(self, name):
        """
        Frozen set of ids for all values in the field ``name``.
        :param name: The name of the field to return
        :return:
        """
        return frozenset(iter(self.fields[name]))

    @read_api
    def all_field_names(self, field):
        """
        Frozen set of all fields names (should only be used for many-one and many-many fields) - i.e. all the values of
        those fields.
        :param field:
        :return:
        """
        if field == "formats":
            return frozenset(self.fields[field].table.col_book_map)

        try:
            return frozenset(itervalues(self.fields[field].table.id_map))
        except AttributeError:
            raise ValueError("%s is not a many-one or many-many field" % field)

    # Todo: This is broken for the majority of fields - fix it and move the logic somewhere more helpful
    @read_api
    def get_usage_count_by_id(self, field):
        """
        Return a mapping of id to usage count for all values of the specified field, which must be a many-one or
        many-many field.
        :param field: The name of the field to return the count for
        :return field_val_usage_count_map: Keyed with the id of the resource and valued with how often it's been used.
        """
        try:
            return {k: len(v) for k, v in iteritems(self.fields[field].table.col_book_map)}
        except AttributeError:
            raise ValueError("%s is not a many-one or many-many field" % field)

    @read_api
    def get_id_map(self, field):
        """
        Return a mapping of ids to values for the specified field.
        The field must be a many-one or many-many field (or title), otherwise a ValueError is raised.
        :param field:
        :return item_id_to_val_map:
        """
        try:
            return self.fields[field].table.id_map.copy()
        except AttributeError:
            if field == "title":
                return self.fields[field].table.book_col_map.copy()
            raise ValueError("%s is not a many-one or many-many field" % field)

    @read_api
    def get_item_name(self, field, item_id):
        """
        Return the item name for the item specified by item_id in the specified field. See also :meth:`get_id_map`.
        The field must be a many-one or many-many field, otherwise a ValueError is raised.
        Note - in calibre, this would raise a AttributeError - this has been changed to Value to be consistent with
        the get_id_map function.
        :param field:
        :param item_id:
        :return:
        """
        try:
            return self.fields[field].table.id_map[item_id]
        except AttributeError:
            raise ValueError("%s is not a many-one or many-many field" % field)

    # Todo: Should be used in more places - in particualr when trying to match when preforming updates
    # Todo: Upgrade here with fuzzy matching - then use elsewhere
    @read_api
    def get_item_id(self, field, item_name):
        """
        Return the item id for item_name (case-insensitive).
        :param field:
        :param item_name:
        :return:
        """
        rmap = {icu_lower(v) if isinstance(v, unicode) else v: k for k, v in iteritems(self.fields[field].table.id_map)}
        return rmap.get(icu_lower(item_name) if isinstance(item_name, unicode) else item_name, None)

    @read_api
    def get_item_ids(self, field, item_names):
        """
        Return the item ids for the item names.
        :param field: Search in this field
        :param item_names: Iterable of names to look for
        :return item_name_id_map: Keyed with the item name and valued with the id found for the item
        """
        rmap = {icu_lower(v) if isinstance(v, unicode) else v: k for k, v in iteritems(self.fields[field].table.id_map)}
        return {name: rmap.get(icu_lower(name) if isinstance(name, unicode) else name, None) for name in item_names}

    @read_api
    def author_data(self, author_ids=None):
        """
        Return author data as a dictionary keyed with the author id and valued with a tuple of name, sort, link.

        Defaults to returning data for all authors.
        :param author_ids:
        :return:
        """
        af = self.fields["authors"]
        if author_ids is None:
            author_ids = tuple(af.table.id_map)
        return {aid: af.author_data(aid) for aid in author_ids if aid in af.table.id_map}

    @write_api
    def update_path(self, book_ids, mark_as_dirtied=True):
        """
        Run update on the given books to take into account any metadata changes which might affect their position.
        Does the update for book formats and covers.
        :param book_ids:
        :param mark_as_dirtied:
        :return:
        """
        for book_id in book_ids:
            title = self.unlock.field_for("title", book_id, default_value=_("Unknown"))
            try:
                author = self.unlock.field_for("authors", book_id, default_value=(_("Unknown"),))[0]
            except IndexError:
                author = _("Unknown")
            self.backend.update_path(book_id, title, author, self.fields["path"], self.fields["formats"])
            if mark_as_dirtied:
                self.unlock.mark_as_dirty(book_ids)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FORMAT METHODS
    @read_api
    def formats(self, book_id, verify_formats=True):
        """
        Return tuple of all formats for the specified book. If verify_formats is True, verifies that the files exist on
        disk.
        :param book_id: The book to return the formats list for
        :param verify_formats:
        :return:
        """
        ans = self.field_for("formats", book_id)
        if verify_formats and ans:

            fmts_field = self.fields["formats"]

            def verify(fmt):
                try:
                    loc = loc_from_formats_field(fmts_field, book_id, fmt)
                except Exception as e:
                    err_str = "Error while calling formats"
                    default_log.log_exception(err_str, e, "INFO")
                    return False
                return self.backend.fsm.path.exists(loc)

            ans = tuple(x for x in ans if verify(x))
        return ans

    @api
    def format(self, book_id, fmt, as_file=False, as_path=False, preserve_filename=False):
        """
        Return the ebook format as a bytestring or `None` if the format doesn't exist, or we don't have read permission
        to the file.
        :param book_id:
        :param fmt:
        :param as_file: If True the ebook format is returned as a file object. Note that the file object is a
                        SpooledTemporaryFile, so if what you want to do is copy the format to another file, use
                        :meth:`copy_format_to` instead for performance.
        :param as_path: Copies the format file to a temp file and returns the path to the temp file
        :param preserve_filename: If True and returning a path the filename is the same as that used in the library.
                                  Note that using this means that repeated calls yield the same temp file
                                  (which is re-created each time)
        :return:
        """
        fmt = (fmt or "").upper()
        fmt = normalize_fmt(fmt)

        # Will be used to set the fmt for the output file
        ext = ("." + fmt.lower()) if fmt else ""

        # If the file is returned as a path then make a folder for it in the scratch folders and then download the file
        # to that folder
        if as_path:
            if preserve_filename:
                with self.safe_read_lock:

                    # Todo: format_fname should not have an extension
                    # Make a name for the new file
                    try:
                        fname = self.fields["formats"].format_fname(book_id, fmt)
                    except Exception as e:
                        err_str = "Error while calling the format method of the cache"
                        default_log.log_exception(err_str, e, "INFO")
                        return None
                    fname += ext

                bd = base_dir()
                d = os.path.join(bd, "format_abspath")
                try:
                    os.makedirs(d)
                except Exception as e:
                    err_str = "Error while calling the format method of the cache"
                    default_log.log_exception(err_str, e, "INFO")

                ret = os.path.join(d, fname)
                try:
                    self.copy_format_to(book_id, fmt, ret)
                except NoSuchFormat:
                    return None
            else:
                # Copy the file into a persistent temporary file and return the path to it
                with PersistentTemporaryFile(ext) as pt:
                    try:
                        self.copy_format_to(book_id, fmt, pt)
                    except NoSuchFormat:
                        return None
                    ret = pt.name

        elif as_file:
            with self.safe_read_lock:
                try:
                    fname = self.fields["formats"].format_fname(book_id, fmt)
                except Exception as e:
                    err_str = "Error while calling the format method of the cache"
                    default_log.log_exception(err_str, e, "INFO")
                    return None
                fname += ext

            ret = SpooledTemporaryFile(SPOOL_SIZE)
            try:
                self.copy_format_to(book_id, fmt, ret)
            except NoSuchFormat:
                return None
            ret.seek(0)
            # Various bits of code try to use the name as the default title when reading metadata, so set it
            ret.name = fname

        else:
            buf = BytesIO()
            try:
                self.copy_format_to(book_id, fmt, buf)
            except NoSuchFormat:
                return None

            ret = buf.getvalue()

        return ret

    # Todo: file_extensions and cover_extensions should both be stored without a dot - make this so
    @read_api
    def format_hash(self, book_id, fmt):
        """
        Return the hash of the specified format for the specified book. The kind of hash is backend dependent, but is
        usually SHA-256.
        The hash should be LiuXin's custom hash - (SHA-512 + length of file in bytes)
        :param book_id:
        :param fmt:
        :return:
        """
        loc = loc_from_formats_field(self.fields["formats"], book_id, fmt)

        # Use the loc to retrieve the physical hash for the file using the Folder Store Manager's path module.
        # (which takes locations)
        return self.backend.fsm.path.gethash(loc)

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
        :param update_db: If ``True`` The max_size field of the database is updated for this book.
        :return:
        """
        if not fmt:
            return {}
        fmt = normalize_fmt(fmt)
        if allow_cache:
            x = self.format_metadata_cache[book_id].get(fmt, None)
            if x is not None:
                return x

        # Read directly from the file's actual location
        with self.safe_read_lock:
            try:
                loc = loc_from_formats_field(self.fields["formats"], book_id, fmt)
            except Exception as e:
                err_str = "loc_from_formats field has failed"
                default_log.log_exception(err_str, e, "DEBUG")
                return {}

            # If the fmt is malformed the loc_from_formats_field function might change it slightly to try and find
            # the format (drop the leading ., if there is one, add a _1 if the fmt isn't followed by a number
            # Thus, from then on, the format used should be the priority_fmt stored in the returned loc
            fmt_priority = loc.fmt_priority
            ans = self.backend.format_metadata_from_loc(loc)
            self.format_metadata_cache[book_id][fmt_priority] = ans

        if update_db and "size" in ans:
            with self.write_lock:
                max_size = self.fields["formats"].table.update_fmt(
                    book_id, fmt_priority, None, ans["size"], self.backend
                )
                self.fields["size"].table.update_sizes({book_id: max_size})

        return ans

    @read_api
    def book_formats(self, book_id):
        """
        Return the fmt_priorities available for a given book.
        Returns then as a tuple, ordered by priority.
        :param book_id:
        :return:
        """
        field = self.fields["formats"]
        return field.table.book_col_map.get(book_id, ())

    @read_api
    def format_files(self, book_id):
        """
        Returns a map keyed with the format name and valued with the file names.
        Keys will be the fmt_priority - value will be the name of that format file.
        :param book_id: Retrieve the formats for this book
        :type book_id: int
        :return:
        """
        field = self.fields["formats"]
        fmts = field.table.book_col_map.get(book_id, ())
        return {fmt: field.format_fname(book_id, fmt) for fmt in fmts}

    # Todo: Add a "replace_format" method - and/or an "update_format" method
    @read_api
    def format_abspath(self, book_id, fmt):
        """
        Return a path to the ebook file of format `format`. You should almost never use this, as it breaks the
        threadsafe promise of this API.
        Instead use, :meth:`copy_format_to`.
        Currently used only in calibredb list, the viewer, edit book, compare_format to original format, open with and
        the catalogs (via get_data_as_dict()).
        Apart from the viewer, open with and edit book, I don't believe any of the others do any file write I/O with the
        results of this call.
        WARNING! In calibre, this function will return a path to the actual book. This method returns a copy in a
        scratch folder - you will need to upload the book back to the folder store after you've finished IO with it.
        There was no elegant way to expose books across different types of folder stores.
        :param book_id:
        :param fmt:
        :return:
        """
        fmt = (fmt or "").upper()
        if fmt == "__COVER_INTERNAL__":
            # Todo: Upgrade cover_abspath so that it can take account of this
            return self.backend.cover_abspath(book_id, None)

        try:
            loc = loc_from_formats_field(self.fields["formats"], book_id, fmt)
        except Exception as e:
            err_str = "loc_from_formats_field has failed"
            default_log.log_exception(err_str, e, "DEBUG")
            return None

        if loc:
            return self.backend.fsm.stores.download(src=loc)

    # Todo: If called with a priority format then should return True iff that format exists - if called with a general
    #       format should return True if the book has any entries of that format
    @read_api
    def has_format(self, book_id, fmt):
        """
        Return True iff the format exists on disk.
        :param book_id:
        :param fmt:
        :return:
        """
        fmt = (fmt or "").upper()
        if fmt.startswith("."):
            fmt = fmt[1:]
        try:
            return self.fields["formats"].has_format(book_id, fmt)
        except Exception as e:
            err_str = "Error while trying to call has_format"
            default_log.log_exception(err_str, e, "INFO")
            return False

    @api
    def save_original_format(self, book_id, fmt):
        """
        Save a copy of the specified format as ORIGINAL_FORMAT, overwriting any existing ORIGINAL_FORMAT.
        ORIGINAL_FMT is added to the cache - as the format that was originally backed up - EPUB_1 would be backed up as
        ORIGINAL_EPUB_1.
        :param book_id:
        :param fmt:
        :return:
        """
        fmt = fmt.upper()
        if "ORIGINAL" in fmt:
            raise ValueError("Cannot save original of an original fmt")

        # Preform the physical update
        fmt_loc = loc_from_formats_field(self.fields["formats"], book_id, fmt)
        fmt = fmt_loc.fmt_priority

        fmt_file_id = int(fmt_loc["file_row"]["file_id"])
        if fmt_loc is None:
            return False
        backup_fmt_loc = self.backend.fsm.add.format_backup(file_id=fmt_file_id, note_backup=True, backup_pos="last")

        original_fmt = "ORIGINAL_{}".format(fmt)

        # Update the cache - if the backup has actually been preformed
        if backup_fmt_loc:
            self.fields["formats"].add_format(book_id=book_id, fmt=original_fmt, fmt_loc=backup_fmt_loc)

        return True if backup_fmt_loc else False

    # Todo: Preserve the noting of backups between sessions - done - now test
    @api
    def restore_original_format(self, book_id, original_fmt):
        """
        Restore the specified format from the previously saved ORIGINAL_FORMAT, if any. Return True on success.
        The ORIGINAL_FORMAT is deleted after a successful restore.
        ORIGINAL_FMT should be an ORIGINAL_FMT string - e.g. something of the form ORIGINAL_EPUB_1 e.t.c
        :param book_id:
        :param original_fmt:
        :return:
        """
        original_fmt = original_fmt.upper()

        # Tokenize and determine which of the formats is being restored from backup
        original_fmt_tokens = original_fmt.split("_")
        if len(original_fmt_tokens) == 3:
            fmt_for_restore = "_".join(original_fmt_tokens[1:])
        else:
            raise NotImplementedError("original_fmt couldn't be parsed")

        fmts_field = self.fields["formats"]
        src_fmt_loc = loc_from_formats_field(fmts_field, book_id, original_fmt)

        # src format can actually be found - restore can proceed
        if src_fmt_loc is not None:
            dst_fmt_loc = loc_from_formats_field(fmts_field, book_id, fmt_for_restore)

            # Move the physical file
            self.backend.fsm.move.overwrite_file(src=src_fmt_loc, dst=dst_fmt_loc)

            # Update the cache
            fmts_field.remove_fmt(book_id, original_fmt)

            return True

        return False

    @read_api
    def copy_format_to(self, book_id, fmt, dest, use_hardlink=False, report_file_size=None):
        """
        Copy the format ``fmt`` to the file like object ``dest``. If the specified format does not exist, raises
        :class:`NoSuchFormat` error.
        dest can also be a path, in which case the format is copied to it, iff the path is different from the current
        path (taking case sensitivity into account).
        :param book_id:
        :param fmt:
        :param dest:
        :param use_hardlink:
        :param report_file_size:
        :return:
        """
        fmt = (fmt or "").upper()
        try:
            loc = loc_from_formats_field(self.fields["formats"], book_id, fmt)
        except (KeyError, AttributeError):
            raise NoSuchFormat("Record %d has no %s file" % (book_id, fmt))

        return self.backend.copy_format_to(
            book_id=book_id,
            fmt=None,
            fname=None,
            path=loc,
            dest=dest,
            windows_atomic_move=None,
            use_hardlink=use_hardlink,
            report_file_size=report_file_size,
            allow_overwrite=False,
        )

    @api
    def add_format(self, book_id, fmt, stream_or_path, replace=False, run_hooks=True, dbapi=None):
        """
        Add a format to the specified book. Return True of the format was added successfully.
        Format will be added to the book with the highest priority - all other formats will be relegated.
        If the fmt is given in the form of a priority fmt (e.g EPUB_1) then, if replace is True, that fmt will be
        replaced. If not returns False.
        :param replace: If True replace the existing highest priority fmt
        :param run_hooks: If True, file type plugins are run on the format before and after being added.
        :param book_id:
        :param fmt:
        :param stream_or_path:
        :param replace:
        :param run_hooks:
        :param dbapi: Internal use only.
        :return:
        """
        if run_hooks:
            # Run import plugins, the write lock is not held to cater for broken plugins that might spin the event loop
            # by popping up a message in the GUI during the processing.
            npath = run_import_plugins(stream_or_path, fmt)
            fmt = os.path.splitext(npath)[-1].lower().replace(".", "").upper()
            stream_or_path = lopen(npath, "rb")
            fmt = check_ebook_format(stream_or_path, fmt)

        # Todo: Fiddle with the order the checks are done in to make behavior more like calibre
        with self.write_lock:

            fmt_loc = self.__do_actual_add_format(
                book_id=book_id, fmt=fmt, stream_or_path=stream_or_path, replace=replace
            )
            if not fmt_loc:
                return False

            size = int(fmt_loc["file_row"]["file_size"])
            fname = fmt_loc["file_row"]["file_name"]

            if hasattr(fmt_loc, "override_fmt"):
                fmt = fmt_loc.override_fmt

            max_size = self.fields["formats"].table.update_fmt(book_id, fmt, fname, size, self.backend)
            self.fields["size"].table.update_sizes({book_id: max_size})
            self.unlock.update_last_modified((book_id,))

        if run_hooks:
            # Run post import plugins, the write lock is released so the plugin
            # can call api without a locking violation.
            run_plugins_on_postimport(dbapi or self, book_id, fmt)
            stream_or_path.close()

        return True

    def __do_actual_add_format(self, book_id, fmt, stream_or_path, replace=False):
        """
        Actually add a book to the folder store.
        :param book_id:
        :param fmt:
        :param stream_or_path:
        :param replace:
        :return:
        """
        formats_field = self.fields["formats"]

        fmt = formats_field.stand_fmt(fmt)

        # Deal with the case where the fmt is NOT a priority fmt - in this case add the fmt with the highest
        # priority - preform no overwrite
        if not formats_field.check_fmt_is_priority_fmt(fmt):
            final_loc = self.backend.fsm.add.format(
                book_id, fmt, stream=stream_or_path, format_in_book_priority="highest"
            )
            formats_field.reload_book_from_db(db=self.backend, book_id=book_id)
            return final_loc

        # Given format is a fmt_priority - if appropriate do a replace of that format in the book
        base_fmt = formats_field.prep_base_fmt(fmt)
        priority_fmt = fmt
        if not formats_field.has_format(book_id=book_id, fmt=base_fmt):
            fmt_loc = self.backend.fsm.add.format(
                book_id, fmt, stream=stream_or_path, format_in_book_priority="highest"
            )
            formats_field.add_format(book_id=book_id, fmt=priority_fmt, fmt_loc=fmt_loc)
            return fmt_loc

        # Need to determine if the fmt already present in the book
        priority_fmt_status = formats_field.has_priority_fmt(book_id, priority_fmt=priority_fmt)
        if priority_fmt_status and not replace:
            return False

        if priority_fmt_status:
            # Need to replace the old fmt with the new one - use the move overwrite_file method
            old_fmt_loc = loc_from_formats_field(formats_field, book_id, priority_fmt)
            self.backend.fsm.move.overwrite_file(src=stream_or_path, dst=old_fmt_loc)
            formats_field.reload_book_from_db(db=self.backend, book_id=book_id)
            return old_fmt_loc

        # Presumably just been given the file with a fmt priority which does not exist
        # Interpret this to mean that the user wants the file added to the end of the priority stack
        fmt = fmt.split("_")[0]
        fmt_loc = self.backend.fsm.add.format(book_id, fmt, stream=stream_or_path, format_in_book_priority="lowest")
        formats_field.reload_book_from_db(db=self.backend, book_id=book_id)
        fmt_loc.override_fmt = formats_field.table.get_last_priority_fmt(book_id=1, fmt=fmt)
        return fmt_loc

    @write_api
    def remove_formats(self, formats_map, db_only=False):
        """
        Remove the specified formats from the specified books.
        :param formats_map: A mapping of book_id to a list of formats to be removed from the book.
        :param db_only: If True, only remove the record for the format from the db, do not delete the actual format file
                        from the filesystem.
        :return:
        """
        formats_map = self._formats_map_preflight(formats_map)

        table = self.fields["formats"].table

        # Remove the given formats from the formats metadata cache
        for book_id, fmts in iteritems(formats_map):
            for fmt in fmts:
                self.format_metadata_cache[book_id].pop(fmt, None)

        if not db_only:
            # Check the given format actually exists - if it does then tries to remove it
            removes = defaultdict(set)
            for book_id, fmts in iteritems(formats_map):
                for fmt in fmts:
                    try:
                        file_loc = self.fields["formats"].format_floc(book_id, fmt)
                    except Exception as e:
                        err_str = "Trying to get the format_floc (format file loc) failed"
                        default_log.log_exception(err_str, e, "DEBUG", ("book_id", book_id), ("fmt", fmt))
                        continue
                    if file_loc:
                        removes[book_id].add((fmt, "", file_loc))
            if removes:
                self.backend.remove_formats(removes)

        size_map = table.remove_formats(formats_map, self.backend)
        self.fields["size"].table.update_sizes(size_map)
        self.unlock.update_last_modified(tuple(iterkeys(formats_map)))

    def _formats_map_preflight(self, formats_map):
        """
        Takes the formats map - expands any base fmt out into a full priority_fmt.
        Thus {1: ['EPUB']} would become something like {1:['EPUB_1', 'EPUB_2']}.
        :param formats_map:
        :return:
        """
        formats_map = {book_id: frozenset((f or "").upper() for f in fmts) for book_id, fmts in iteritems(formats_map)}

        # Needed for tools
        fmts_table = self.fields["formats"].table

        new_formats_map = defaultdict(set)
        for book_id in formats_map:
            for trial_fmt in formats_map[book_id]:
                if not trial_fmt:
                    continue
                trial_fmt = trial_fmt.upper()

                if fmts_table.check_fmt_is_priority_fmt(trial_fmt):
                    new_formats_map[book_id].add(trial_fmt)
                else:
                    fmts_table_priority_fmts = fmts_table.get_all_priority_fmts(book_id=book_id, fmt=trial_fmt)
                    for new_trial_fmt in fmts_table_priority_fmts:
                        new_formats_map[book_id].add(new_trial_fmt)

        new_formats_map = {book_id: frozenset(fmts) for book_id, fmts in iteritems(new_formats_map)}

        return new_formats_map

    @write_api
    def update_last_modified(self, book_ids, now=None):
        """
        Updates the last modified date for the given book_ids - if :param now: is None, will default to utcnow()
        :param book_ids:
        :param now:
        :return:
        """
        if book_ids:
            if now is None:
                now = nowf()
            f = self.fields["last_modified"]
            f.writer.set_books({book_id: now for book_id in book_ids}, self.backend)
            if self.composites:
                self.unlock.clear_composite_caches(book_ids)
            self.unlock.clear_search_caches(book_ids)

    #
    # ------------------------------------------------------------------------------------------------------------------

    @api
    def get_metadata(self, book_id, get_cover=False, get_user_categories=True, cover_as_data=False):
        """
        Return metadata for the book identified by book_id as a :class:`calibre.ebooks.metadata.book.base.Metadata`
        object.
        Note that the list of formats is not verified. If get_cover is True, the cover is returned, either a path to
        temp file as mi.cover or if cover_as_data is True then as mi.cover_data.
        :param book_id: The id of the book to retrieve the cover for
        :param get_cover: If True then tries to read the cover - else ignored the cover
        :param get_user_categories: If True then tries to retrieve the user categories
        :param cover_as_data: If True returns the cover as a stream - else returns the cover as a path
        :return:
        """
        with self.safe_read_lock:
            mi = self._get_metadata(book_id, get_user_categories=get_user_categories)

        if get_cover:
            if cover_as_data:
                cdata = self.cover(book_id)
                if cdata:
                    mi.cover_data = ("jpeg", cdata)
            else:
                mi.cover = self.cover(book_id, as_path=True)

        return mi

    @read_api
    def get_proxy_metadata(self, book_id):
        """
        Like :meth:`get_metadata` except that it returns a ProxyMetadata object that only reads values from the database
        on demand. This is much faster than get_metadata when only a small number of fields need to be accessed from the
        returned metadata object.
        :param book_id:
        :return:
        """
        return ProxyMetadata(self, book_id)

    @write_api
    def set_metadata(
        self,
        book_id,
        mi,
        ignore_errors=False,
        force_changes=False,
        set_title=True,
        set_authors=True,
        allow_case_change=False,
    ):
        """
        Set metadata for the book `id` from the `Metadata` object `mi`
        Setting force_changes=True will force set_metadata to update fields even if mi contains empty values. In this
        case, 'None' is distinguished from 'empty'. If mi.XXX is None, the XXX is not replaced, otherwise it is.
        The tags, identifiers, and cover attributes are special cases. Tags and identifiers cannot be set to None so
        then will always be replaced if force_changes is true. You must ensure that mi contains the values you want the
        book to have. Covers are always changed if a new cover is provided, but are never deleted. Also note that
        force_changes has no effect on setting title or authors.
        :param book_id:
        :param mi:
        :param ignore_errors:
        :param force_changes:
        :param set_title:
        :param set_authors:
        :param allow_case_change:
        :return:
        """
        dirtied = set()

        try:
            # Handle code passing in an OPF object instead of a Metadata object
            mi = mi.to_book_metadata()
        except (AttributeError, TypeError):
            pass

        def set_field(name, local_val):
            dirtied.update(
                self.unlock.set_field(
                    name,
                    {book_id: local_val},
                    do_path_update=False,
                    allow_case_change=allow_case_change,
                )
            )

        path_changed = False
        if set_title and mi.title:
            path_changed = True
            set_field("title", mi.title)
        if set_authors:
            path_changed = True
            if not mi.authors:
                mi.authors = [_("Unknown")]
            authors = []
            for a in mi.authors:
                authors += string_to_authors(a)
            set_field("authors", authors)

        if path_changed:
            self.unlock.update_path({book_id})

        def protected_set_field(name, local_val):
            try:
                set_field(name, local_val)
            except Exception as local_e:
                local_err_str = "Failure while trying to set_metadata"
                default_log.log_exception(local_err_str, local_e, "INFO")
                if ignore_errors:
                    traceback.print_exc()
                else:
                    raise

        # force_changes has no effect on cover manipulation
        try:
            cdata = mi.cover_data[1]
            if cdata is None and isinstance(mi.cover, six_string_types) and mi.cover and os.access(mi.cover, os.R_OK):
                with lopen(mi.cover, "rb") as f:
                    cdata = f.read() or None
            if cdata is not None:
                self.unlock.set_cover({book_id: cdata})
        except Exception as e:
            err_str = "Failure while trying to set_metadata"
            default_log.log_exception(err_str, e, "INFO")
            if ignore_errors:
                traceback.print_exc()
            else:
                raise

        try:
            with self.backend.conn:  # Speed up set_metadata by not operating in autocommit mode
                for field in ("rating", "series_index", "timestamp"):
                    val = getattr(mi, field)
                    if val is not None:
                        protected_set_field(field, val)

                for field in (
                    "author_sort",
                    "publisher",
                    "series",
                    "tags",
                    "comments",
                    "languages",
                    "pubdate",
                ):
                    val = mi.get(field, None)
                    if (force_changes and val is not None) or not mi.is_null(field):
                        protected_set_field(field, val)

                val = mi.get("title_sort", None)
                if (force_changes and val is not None) or not mi.is_null("title_sort"):
                    protected_set_field("sort", val)

                # identifiers will always be replaced if force_changes is True
                mi_idents = mi.get_identifiers()
                if force_changes:
                    protected_set_field("identifiers", mi_idents)
                elif mi_idents:
                    identifiers = self.unlock.field_for("identifiers", book_id, default_value={})
                    for key, val in iteritems(mi_idents):
                        if val and val.strip():  # Don't delete an existing identifier
                            identifiers[icu_lower(key)] = val
                    protected_set_field("identifiers", identifiers)

                user_mi = mi.get_all_user_metadata(make_copy=False)
                fm = self.field_metadata
                for key in iterkeys(user_mi):
                    if (
                        key in fm
                        and user_mi[key]["datatype"] == fm[key]["datatype"]
                        and (
                            user_mi[key]["datatype"] != "text" or user_mi[key]["is_multiple"] == fm[key]["is_multiple"]
                        )
                    ):
                        val = mi.get(key, None)
                        if force_changes or val is not None:
                            protected_set_field(key, val)
                            idx = key + "_index"
                            if idx in self.fields:
                                extra = mi.get_extra(key)
                                if extra is not None or force_changes:
                                    protected_set_field(idx, extra)
        except:
            # sqlite will rollback the entire transaction, thanks to the with statement, so we have to re-read
            # everything form the db to ensure the db and Cache are in sync
            self.unlock.reload_from_db()
            raise
        return dirtied

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - COVER METHODS

    @api
    def cover(self, book_id, as_file=False, as_image=False, as_path=False):
        """
        Return the cover image or None. By default, returns the cover as a bytestring.
        WARNING: Using as_path will copy the cover to a temp file and return the path to the temp file. You should
        delete the temp file when you are done with it.
        :param book_id:
        :param as_file: If True return the image as an open file object (a SpooledTemporaryFile)
        :param as_image: If True return the image as a QImage object
        :param as_path: If True return the image as a path pointing to a temporary file
        :return:
        """
        if as_file:
            ret = SpooledTemporaryFile(SPOOL_SIZE)
            if not self.copy_cover_to(book_id, ret):
                return
            ret.seek(0)
        elif as_path:
            pt = PersistentTemporaryFile("_dbcover.jpg")
            with pt:
                if not self.copy_cover_to(book_id, pt):
                    return
            ret = pt.name
        else:
            buf = BytesIO()
            if not self.copy_cover_to(book_id, buf):
                return
            ret = buf.getvalue()
            if as_image:
                from PyQt5.Qt import QImage

                i = QImage()
                i.loadFromData(ret)
                ret = i
        return ret

    @read_api
    def cover_or_cache(self, book_id, timestamp):
        """
        Provides a tuple of information as to whether to read from the cache or read from the on_disk cover.
        See backend.cover_or_cache method.
        :param book_id:
        :param timestamp:
        :return (read_status, cover_data, new_timestamp):
        """
        try:
            covers_field = self.fields["cover"]
            cover_loc = covers_field.cover_loc(book_id=book_id)
        except AttributeError:
            return False, None, None
        return self.backend.cover_or_cache(cover_loc, timestamp)

    @read_api
    def cover_last_modified(self, book_id):
        """
        When was the primary cover for a given book last modified.
        :param book_id:
        :return:
        """
        try:
            path = self.unlock.field_for("path", book_id).replace("/", os.sep)
        except AttributeError:
            return
        return self.backend.cover_last_modified(path)

    @read_api
    def copy_cover_to(self, book_id, dest, use_hardlink=False, report_file_size=None):
        """
        Copy the cover to the file like object ``dest``. Returns False if no cover exists or dest is the same file as
        the current cover.
        dest can also be a path in which case the cover is copied to it if and only if the path is different from the
        current path (taking case sensitivity into account).
        :param book_id:
        :param dest:
        :param use_hardlink:
        :param report_file_size:
        :return:
        """
        try:
            covers_field = self.fields["cover"]
            cover_loc = covers_field.cover_loc(book_id=book_id)
        except AttributeError:
            return False

        return self.backend.copy_cover_to(
            cover_loc,
            dest,
            use_hardlink=use_hardlink,
            report_file_size=report_file_size,
        )

    @write_api
    def set_cover(self, book_id_data_map):
        """
        Set the cover for this book.  data can be either a QImage, QPixmap, file object or bytestring. It can also be
        None, in which case any existing cover is removed.
        :param book_id_data_map:
        :return:
        """
        for book_id, data in iteritems(book_id_data_map):
            try:
                path = self.unlock.field_for("path", book_id).replace("/", os.sep)
            except AttributeError:
                self.unlock.update_path((book_id,))
                path = self.unlock.field_for("path", book_id).replace("/", os.sep)

            self.backend.set_cover(book_id, path, data)
        for cc in self.cover_caches:
            cc.invalidate(book_id_data_map)
        return self.unlock.set_field(
            "cover",
            {book_id: (0 if data is None else 1) for book_id, data in iteritems(book_id_data_map)},
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SORT AND SEARCH API

    @read_api
    def multisort(self, fields, ids_to_sort=None, virtual_fields=None):
        """
        Return a list of sorted book ids. If ids_to_sort is None, all book ids are returned.
        fields must be a list of 2-tuples of the form (field_name, ascending=True or False). The most significant field
        is the first 2-tuple.
        :param fields:
        :param ids_to_sort:
        :param virtual_fields:
        :return:
        """
        ids_to_sort = self.unlock.all_book_ids() if ids_to_sort is None else ids_to_sort
        get_metadata = self.unlock.get_proxy_metadata
        lang_map = self.fields["languages"].book_value_map
        virtual_fields = virtual_fields or {}

        fm = {"title": "sort", "authors": "author_sort"}

        def sort_key_func(field):
            """
            Handle series type fields, virtual fields and the id field
            :param field:
            :return:
            """
            idx = field + "_index"
            is_series = idx in self.fields
            try:
                func = self.fields[fm.get(field, field)].sort_keys_for_books(get_metadata, lang_map)
            except KeyError:
                if field == "id":
                    return IDENTITY
                else:
                    return virtual_fields[fm.get(field, field)].sort_keys_for_books(get_metadata, lang_map)
            if is_series:
                idx_func = self.fields[idx].sort_keys_for_books(get_metadata, lang_map)

                def skf(book_id):
                    return func(book_id), idx_func(book_id)

                return skf
            return func

        # Sort only once on any given field
        fields = uniq(fields, operator.itemgetter(0))

        if len(fields) == 1:
            return sorted(ids_to_sort, key=sort_key_func(fields[0][0]), reverse=not fields[0][1])
        sort_key_funcs = tuple(sort_key_func(field) for field, order in fields)
        orders = tuple(1 if order else -1 for _, order in fields)
        lazy_obj = object()  # Lazy load the sort keys for sub-sort fields

        class SortKey(object):

            __slots__ = ("book_id", "sort_key")

            def __init__(self, book_id):
                self.book_id = book_id
                # Calculate only the first sub-sort key since that will always be used
                self.sort_key = [key(book_id) if i == 0 else lazy_obj for i, key in enumerate(sort_key_funcs)]

            def __cmp__(self, other):
                for i, (order, self_key, other_key) in enumerate(zip(orders, self.sort_key, other.sort_key)):
                    if self_key is lazy_obj:
                        self_key = self.sort_key[i] = sort_key_funcs[i](self.book_id)
                    if other_key is lazy_obj:
                        other_key = other.sort_key[i] = sort_key_funcs[i](other.book_id)
                    ans = six_cmp(self_key, other_key)
                    if ans != 0:
                        return ans * order
                return 0

        return sorted(ids_to_sort, key=SortKey)

    @read_api
    def search(self, query, restriction="", virtual_fields=None, book_ids=None):
        """
        Search the database for the specified query, returning a set of matched book ids.
        :param restriction: A restriction that is ANDed to the specified query. Note that
            restrictions are cached, therefore the search for a AND b will be slower than a with restriction b.
        :param virtual_fields: Used internally (virtual fields such as on_device to search over).
        :param book_ids: If not None, a set of book ids for which books will be searched instead of searching all books.
        :param query:
        :param restriction:
        :return:
        """
        return self._search_api(self, query, restriction, virtual_fields=virtual_fields, book_ids=book_ids)

    @read_api
    def books_in_virtual_library(self, vl, search_restriction=None):
        """
        Return the set of books in the specified virtual library
        :param vl:
        :param search_restriction:
        :return:
        """
        vl = self.unlock.pref("virtual_libraries", {}).get(vl) if vl else None
        if not vl and not search_restriction:
            return self.all_book_ids()
        # We utilize the search restriction cache to speed this up
        if vl:
            if search_restriction:
                return frozenset(self.unlock.search("", vl) & self.unlock.search("", search_restriction))
            return frozenset(self.unlock.search("", vl))
        return frozenset(self.unlock.search("", search_restriction))

    @api
    def get_categories(self, sort="name", book_ids=None, already_fixed=None, first_letter_sort=False):
        """
        Used internally to implement the Tag Browser.
        :param sort:
        :param book_ids:
        :param already_fixed:
        :param first_letter_sort:
        :return:
        """
        try:
            with self.safe_read_lock:
                return get_categories(
                    self,
                    sort=sort,
                    book_ids=book_ids,
                    first_letter_sort=first_letter_sort,
                )
        except InvalidLinkTable as err:
            bad_field = err.field_name
            if bad_field == already_fixed:
                raise
            with self.write_lock:
                self.fields[bad_field].table.fix_link_table(self.backend)
            return self.get_categories(sort=sort, book_ids=book_ids, already_fixed=bad_field)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - DIRTY API

    @write_api
    def mark_as_dirty(self, book_ids):
        """
        Note that the following books are dirtied on the database.
        :param book_ids:
        :return:
        """
        # Regardless of weather the book needs to be marked as dirty the last modification time does need to be updated
        self.unlock.update_last_modified(book_ids)

        try:
            already_dirtied = set(self.dirtied_cache).intersection(book_ids)
        except TypeError:
            err_str = "TypeError while trying to calculate already_dirtied"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("self.dirtied_cache", self.dirtied_cache),
                ("book_ids", book_ids),
            )
            raise TypeError(err_str)

        new_dirtied = book_ids - already_dirtied
        already_dirtied = {book_id: self.dirtied_sequence + i for i, book_id in enumerate(already_dirtied)}
        if already_dirtied:
            self.dirtied_sequence = max(itervalues(already_dirtied)) + 1
        self.dirtied_cache.update(already_dirtied)
        if new_dirtied:
            self.backend.executemany(
                "INSERT OR IGNORE INTO metadata_dirtied_books (metadata_dirtied_book) VALUES (?)",
                ((x,) for x in new_dirtied),
            )
            new_dirtied = {book_id: self.dirtied_sequence + i for i, book_id in enumerate(new_dirtied)}
            self.dirtied_sequence = max(itervalues(new_dirtied)) + 1
            self.dirtied_cache.update(new_dirtied)

    @write_api
    def commit_dirty_cache(self):
        """
        Write the current dirtied cache out of the database.
        :return:
        """
        book_ids = [(x,) for x in self.dirtied_cache]
        if book_ids:
            self.backend.executemany(
                "INSERT OR IGNORE INTO metadata_dirtied_books " "(metadata_dirtied_book) VALUES (?)",
                book_ids,
            )

    #
    # ------------------------------------------------------------------------------------------------------------------

    @write_api
    def set_field(self, name, book_id_to_val_map, allow_case_change=True, do_path_update=True):
        """
        Set the values of the field specified by ``name``. Returns the set of all book ids that were affected by the
        change.
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
        f = self.fields[name]

        is_series = f.metadata["datatype"] == "series"
        update_path = name in {"title", "authors"}
        if update_path and iswindows:
            # Todo: Should do something with this information? Surely?
            paths = (x for x in (self.unlock.field_for("path", book_id) for book_id in book_id_to_val_map) if x)
            self.backend.windows_check_if_files_in_use(paths)

        # - CUTTING HERE
        # Todo: This section should go in the specilized series fields
        simap = None
        if is_series:
            bimap, simap = {}, {}
            sfield = self.fields[name + "_index"]
            for k, v in iteritems(book_id_to_val_map):
                if isinstance(v, six_string_types):
                    v, sid = get_series_values(v)
                else:
                    v = sid = None
                if sid is None and name.startswith("#"):
                    try:
                        extra = self.unlock.fast_field_for(sfield, k)
                    except NotInCache:
                        extra = None
                    sid = extra or 1.0  # The value to be set the db link table
                bimap[k] = v
                if sid is not None:
                    simap[k] = sid
            book_id_to_val_map = bimap

        # Update the database (do this first and keep this separate - if this fails then we don't want to update the
        # cache)
        dirtied = f.update(book_id_to_val_map, self.backend, allow_case_change=allow_case_change)

        if is_series and simap:
            sf = self.fields[f.name + "_index"]
            dirtied |= sf.writer.set_books(simap, self.backend, allow_case_change=False)

        if dirtied and update_path and do_path_update:
            self.unlock.update_path(dirtied, mark_as_dirtied=False)

        self.unlock.mark_as_dirty(dirtied)

        return dirtied

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - DIRTIED API

    @read_api
    def get_a_dirtied_book(self):
        """
        Return a dirty book randomly selected from the dirtied_cache.
        :return:
        """
        if self.dirtied_cache:
            return random.choice(tuple(iterkeys(self.dirtied_cache)))
        return None

    @read_api
    def get_metadata_for_dump(self, book_id):
        """
        Return metadata for one of the dirtied books - so the updated form can be written out of the cache
        :param book_id:
        :return:
        """
        mi = None
        # get the current sequence number for this book to pass back to the backup thread. This will avoid double
        # calls in the case where the thread has not done the work between the put and the get_metadata
        sequence = self.dirtied_cache.get(book_id, None)
        if sequence is not None:
            try:
                # While a book is being created, the path is empty. Don't bother to
                # try to write the opf, because it will go to the wrong folder.
                if self.unlock.field_for("path", book_id):
                    mi = self.unlock.get_metadata(book_id)
                    # Always set cover to cover.jpg. Even if cover doesn't exist,
                    # no harm done. This way no need to call dirtied when
                    # cover is set/removed
                    mi.cover = "cover.jpg"
            except Exception as e:
                err_str = "Error while backing up book"
                default_log.log_exception(err_str, e, "INFO")
                # This almost certainly means that the book has been deleted while the backup operation sat in the
                # queue.
                # Todo: Work out an catch the errors that result from this - catch and log other types of error
                pass
        return mi, sequence

    @write_api
    def clear_dirtied(self, book_id, sequence):
        """
        Clear the dirtied indicator for the given book.
        This is used when fetching metadata, creating an OPF, and writing a file are separated into steps.
        The last step is clearing the indicator
        :param book_id:
        :param sequence:
        :return:
        """
        dc_sequence = self.dirtied_cache.get(book_id, None)
        if dc_sequence is None or sequence is None or dc_sequence == sequence:
            self.backend.execute(
                "DELETE FROM metadata_dirtied_books WHERE metadata_dirtied_book=?",
                (book_id,),
            )
            self.dirtied_cache.pop(book_id, None)

    @write_api
    def write_backup(self, book_id, raw):
        """
        Write backup metadata into the book's file.
        :param book_id:
        :param raw:
        :return:
        """
        try:
            path = self.unlock.field_for("path", book_id).replace("/", os.sep)
        except Exception as e:
            err_str = "Unable to get book path to write metadata backup"
            default_log.log_exception(err_str, e, "INFO")
            return

        self.backend.write_backup(path, raw)

    @read_api
    def dirty_queue_length(self):
        """
        The current size of the dirtied cache.
        :return:
        """
        return len(self.dirtied_cache)

    @read_api
    def read_backup(self, book_id):
        """
        Return the OPF metadata backup for the book as a bytestring or None if no such backup exists.
        :param book_id:
        :return:
        """
        try:
            path = self.unlock.field_for("path", book_id).replace("/", os.sep)
        except Exception as e:
            err_str = "Unable to get book path to read metadata backup"
            default_log.log_exception(err_str, e, "INFO")
            return

        try:
            return self.backend.read_backup(path)
        except EnvironmentError:
            return None

    @write_api
    def dump_metadata(self, book_ids=None, remove_from_dirtied=True, callback=None):
        """
        Write metadata for each record to an individual OPF file. If callback is not None, it is called once at the
        start with the number of book_ids being processed. And once for every book_id, with arguments (book_id, mi, ok).
        :param book_ids:
        :param remove_from_dirtied:
        :param callback:
        :return:
        """
        if book_ids is None:
            book_ids = set(self.dirtied_cache)

        if callback is not None:
            callback(len(book_ids), True, False)

        for book_id in book_ids:
            if self.unlock.field_for("path", book_id) is None:
                if callback is not None:
                    callback(book_id, None, False)
                continue
            mi, sequence = self.unlock.get_metadata_for_dump(book_id)
            if mi is None:
                if callback is not None:
                    callback(book_id, mi, False)
                continue
            try:
                raw = metadata_to_opf(mi)
                self.unlock.write_backup(book_id, raw)
                if remove_from_dirtied:
                    self.unlock.clear_dirtied(book_id, sequence)
            except Exception as e:
                err_str = "Error while preforming metadata dump"
                default_log.log_exception(err_str, e, "INFO")

            if callback is not None:
                callback(book_id, mi, True)

    #
    # ------------------------------------------------------------------------------------------------------------------

    @write_api
    def add_cover_cache(self, cover_cache):
        """
        Adds a cover_cache object to the set of internal cover caches.
        Allows multiple cover caches to be used at the same time. Which ... could be useful. I guess?
        :param cover_cache:
        :return:
        """
        if not callable(cover_cache.invalidate):
            raise ValueError("Cover caches must have an invalidate method")
        self.cover_caches.add(cover_cache)

    @write_api
    def remove_cover_cache(self, cover_cache):
        """
        Remove a registered cover cache from the system.
        :param cover_cache:
        :return:
        """
        self.cover_caches.discard(cover_cache)

    @read_api
    def get_next_series_num_for(self, series, field="series", current_indices=False):
        """
        Return the next series index for the specified series, taking into account the various preferences that
        control next series number generation.
        :param series:
        :param field: The series-like field (defaults to the builtin series column)
        :param current_indices: If True, returns a mapping of book_id to current series_index value instead.
        :return:
        """
        books = ()
        sf = self.fields[field]
        if series:
            q = icu_lower(series)
            for val, book_ids in sf.iter_searchable_values(
                self.unlock.get_proxy_metadata, frozenset(self.unlock.all_book_ids())
            ):
                if q == icu_lower(val):
                    books = book_ids
                    break
        idf = sf.index_field
        index_map = {book_id: self.unlock.fast_field_for(idf, book_id, default_value=1.0) for book_id in books}
        if current_indices:
            return index_map
        series_indices = sorted(itervalues(index_map))
        return _get_next_series_num_for_list(tuple(series_indices), unwrap=False)

    @read_api
    def author_sort_from_authors(self, authors, key_func=icu_lower):
        """
        Given a list of authors, return the author_sort string for the authors, preferring the author sort associated
        with the author over the computed string.
        :param authors:
        :param key_func:
        :return:
        """
        table = self.fields["authors"].table
        result = []
        rmap = {key_func(v): k for k, v in iteritems(table.id_map)}
        for aut in authors:
            aid = rmap.get(key_func(aut), None)
            result.append(author_to_author_sort(aut) if aid is None else table.asort_map[aid])
        return " & ".join(filter(None, result))

    @read_api
    def data_for_has_book(self):
        """
        Return data suitable for use in :meth:`has_book`. This can be used for an implementation of :meth:`has_book` in
        a worker process without access to the db.
        :return:
        """
        try:
            return {icu_lower(title) for title in itervalues(self.fields["title"].table.book_col_map)}
        except TypeError:
            # Some non-unicode titles in the db
            return {icu_lower(as_unicode(title)) for title in itervalues(self.fields["title"].table.book_col_map)}

    @read_api
    def has_book(self, mi):
        """
        Return True iff the database contains an entry with the same title as the passed in Metadata object.
        The comparison is case-insensitive.
        See also :meth:`data_for_has_book`.
        :param mi:
        :return:
        """
        title = mi.title
        if title:
            if isbytestring(title):
                title = title.decode(preferred_encoding, "replace")
            q = icu_lower(title).strip()
            for title in itervalues(self.fields["title"].table.book_col_map):
                if q == icu_lower(title):
                    return True
        return False

    @read_api
    def has_id(self, book_id):
        """
        Return True iff the specified book_id exists in the db
        :param book_id:
        :return:
        """
        return book_id in self.fields["title"].table.book_col_map

    @write_api
    def create_book_entry(
        self,
        mi,
        cover=None,
        add_duplicates=True,
        force_id=None,
        apply_import_tags=True,
        preserve_uuid=False,
    ):
        """
        Create a new entry in the books table - accepts as input either a LiuXin or calibre metadata object.
        :param mi: The metadata for the new book
        :param cover: The cover for the new book
        :param add_duplicates: Should the book add even if duplicate detection trips?
        :param force_id: If an int then the book is garanteed to have this id
        :param apply_import_tags: Should i,port tags be applied to the book before it's added
        :param preserve_uuid: Use the uuid from the metadata instead of coming up with a new one.
        :return:
        """
        if isinstance(mi, Metadata):
            liuxin_mi = LiuXinMetadata.from_calibre(mi)
            return self.__liuxin_create_book_entry(
                mi=liuxin_mi,
                cover=cover,
                add_duplicates=add_duplicates,
                force_id=force_id,
                apply_import_tags=apply_import_tags,
                preserve_uuid=preserve_uuid,
            )
        elif isinstance(mi, LiuXinMetadata):
            return self.__liuxin_create_book_entry(
                mi=mi,
                cover=cover,
                add_duplicates=add_duplicates,
                force_id=force_id,
                apply_import_tags=apply_import_tags,
                preserve_uuid=preserve_uuid,
            )
        else:
            raise NotImplementedError

    def __liuxin_create_book_entry(
        self,
        mi,
        cover=None,
        add_duplicates=True,
        force_id=None,
        apply_import_tags=True,
        preserve_uuid=False,
    ):
        """
        Create an entry on the database from a LiuXin metadata object.
        :param mi:
        :param cover:
        :param add_duplicates:
        :param force_id:
        :param apply_import_tags:
        :param preserve_uuid:
        :return:
        """
        # Todo: Implement
        if preserve_uuid:
            raise NotImplementedError

        if apply_import_tags:
            _add_newbook_tag(mi)
        if not add_duplicates and self.unlock.has_book(mi):
            return

        # Check to see if the primary series has an index set for it - if not then generate one from the entities
        # currently in the series on the database
        if mi.series:
            series = mi.series.keys()[0]
            if series not in mi.series_index:
                mi.series_index[series] = self.unlock.get_next_series_num_for(series)

        # Generate and set the author sort, if required
        if not mi.authors:
            mi.authors[_("Unknown")] = 0
        aus = mi.creator_sort if mi.creator_sort else self.unlock.author_sort_from_authors(mi.authors.keys())
        if isbytestring(aus):
            aus = aus.decode(preferred_encoding, "replace")
        mi.author_sort = aus

        # Ensure that a title is set
        mi.title = mi.title or _("Unknown")
        if isbytestring(mi.title):
            mi.title = mi.title.decode(preferred_encoding, "replace")

        mi.timestamp = utcnow() if mi.timestamp is None else mi.timestamp
        mi.pubdate = UNDEFINED_DATE if mi.pubdate is None else mi.pubdate

        # Todo: Cover should be set as the primary cover
        if cover is not None:
            mi.cover, mi.cover_data = None, (None, cover)

        book_row = self.library_md.to_book(mi, force_book_id=force_id)
        book_id = book_row["book_id"]

        for field, val in zip(("sort", "series_index", "author_sort", "uuid", "cover"), book_row):
            if field == "cover":
                val = bool(val)
            elif field == "uuid":
                self.fields[field].table.uuid_to_id_map[val] = book_id
            self.fields[field].table.book_col_map[book_id] = val

        return book_id

    # Todo: Merge and expand with a add book from file method
    # Todo: Add an add_book method which just adds a single book
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
        Add the specified books to the library. Books should be an iterable of 2-tuples, each 2-tuple of the form
        :code:`(mi, format_map)` where mi is a Metadata object and format_map is a dictionary of the form
        :code:`{fmt: path_or_stream}`,
        for example: :code:`{'EPUB': '/path/to/file.epub'}`.

        If you want to add multiple examples of the same fmt to the book at the same time you can pass an iterable
        of paths as the value for the fmt map.
        for example :code:`{'EPUB': ['/path/to/file.epub', 'another/path/to/another_file.epub']}`.

        Returns a pair of lists: :code:`ids, duplicates`. ``ids`` contains the book ids for all newly created books in
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
        duplicates, ids = [], []
        fmt_map = {}
        for mi, format_map in books:

            book_id = self.create_book_entry(
                mi,
                add_duplicates=add_duplicates,
                apply_import_tags=apply_import_tags,
                preserve_uuid=preserve_uuid,
            )

            if book_id is None:
                duplicates.append((mi, format_map))
            else:
                ids.append(book_id)
                for fmt, stream_or_path in iteritems(format_map):
                    if isinstance(stream_or_path, six_string_types) or hasattr(stream_or_path, "read"):
                        if self.add_format(
                            book_id,
                            fmt,
                            stream_or_path,
                            dbapi=dbapi,
                            run_hooks=run_hooks,
                        ):
                            fmt_map[fmt.lower()] = getattr(stream_or_path, "name", stream_or_path) or "<stream>"
                    elif hasattr(stream_or_path, "__iter__"):
                        # Need to add in reverse order - to make sure the first element of the list ends up associated
                        # with the book with the highest priority
                        stream_or_path.reverse()
                        new_fmt_file_names = []
                        for fmt_add_obj in stream_or_path:
                            if self.add_format(
                                book_id,
                                fmt,
                                fmt_add_obj,
                                dbapi=dbapi,
                                run_hooks=run_hooks,
                            ):
                                new_fmt_file_names.append(getattr(fmt_add_obj, "name", fmt_add_obj) or "<stream>")
                            else:
                                new_fmt_file_names.append(None)
                        stream_or_path.reverse()
                        new_fmt_file_names.reverse()
                        fmt_map[fmt.lower()] = new_fmt_file_names
                    else:
                        raise NotImplementedError

            run_plugins_on_postadd(dbapi or self, book_id, fmt_map)
        return ids, duplicates

    @write_api
    def remove_books(self, book_ids, permanent=False):
        """
        Remove the books specified by the book_ids from the database and delete their format files. If ``permanent`` is
        False, then the format files are not deleted.
        :param book_ids:
        :param permanent:
        :return:
        """
        self.backend.remove_books(book_ids, permanent=permanent)

        for field in itervalues(self.fields):
            try:
                table = field.table
            except AttributeError:
                continue  # Some fields like ondevice do not have tables
            else:
                table.remove_books(book_ids, self.backend)

        self._search_api.discard_books(book_ids)
        self.unlock.clear_caches(book_ids=book_ids, template_cache=False, search_cache=False)
        for cc in self.cover_caches:
            cc.invalidate(book_ids)

    @read_api
    def author_sort_strings_for_books(self, book_ids):
        """
        Return a map keyed with the book_id and valued with a tuple of the author sorts for all the given books.
        Author sort strings will be in the priority order of the authors.
        :param book_ids:
        :return:
        """
        val_map = {}
        for book_id in book_ids:
            authors = self.unlock.field_ids_for("authors", book_id)
            adata = self.unlock.author_data(authors)
            val_map[book_id] = tuple(adata[aid]["sort"] for aid in authors)
        return val_map

    # Todo: Are you sure change_index actually does that? seems a bit weird?
    # Todo: Probsbly should hive the logic for this off into the fields
    @write_api
    def rename_items(
        self,
        field,
        item_id_to_new_name_map,
        change_index=True,
        restrict_to_book_ids=None,
    ):
        """
        Rename items in one-to-many and many-to-one tables e.g. series and tags.
        Cannot handle one-to-one fields - such as titles.
        :param field: The field to update the items for
        :type field: str
        :param item_id_to_new_name_map: Keyed with the id of the item (as an int) and valued with the new name that
                                        the field should be changed to.
                                        Thus - if you where updating the names of a tag - would be keyed with the id of
                                        the tag your updating and valued with the new name for the tag.
        :param change_index: When renaming in a series-like field also change the series_index values.
        :param restrict_to_book_ids: An optional set of book ids for which the rename is to be performed, defaults to
                                     all books. Used when there's an active virtual library.
        :return:
        """
        f = self.fields[field]
        affected_books = set()
        try:
            sv = f.metadata["is_multiple"]["ui_to_list"]
        except (TypeError, KeyError, AttributeError):
            sv = None

        if restrict_to_book_ids is not None:

            # We have a Virtual Library. Only change the item name for those books
            if not isinstance(restrict_to_book_ids, frozenset):
                restrict_to_book_ids = frozenset(restrict_to_book_ids)
            id_map = {}
            default_process_map = {}

            # Process every item in the rename map
            for old_id, new_name in iteritems(item_id_to_new_name_map):
                new_names = tuple(x.strip() for x in new_name.split(sv)) if sv else (new_name,)

                # Build a list of all the books in the virtual library with the item
                books_with_id = f.books_for(old_id)

                books_to_process = books_with_id & restrict_to_book_ids

                # Determine the processing behavior to use for the individual rename
                if len(books_with_id) == len(books_to_process):
                    # All the books with the ID are in the VL, so we can use the normal processing
                    default_process_map[old_id] = new_name
                elif books_to_process:
                    affected_books.update(books_to_process)
                    newvals = {}
                    for book_id in books_to_process:

                        # Get the current values, remove the one being renamed, then add the new value(s) back.
                        vals = tuple(self.unlock.field_for(field, book_id))

                        # Check for is_multiple
                        if isinstance(vals, tuple):
                            # We must preserve order.
                            vals = list(vals)

                            # Don't need to worry about case here because we are fetching its one-true spelling.
                            # But lets be careful anyway
                            try:
                                dex = vals.index(self.unlock.get_item_name(field, old_id))

                                # This can put the name back with a different case
                                vals[dex] = new_names[0]

                                # now add any other items if they aren't already there
                                if len(new_names) > 1:
                                    set_vals = {icu_lower(x) for x in vals}
                                    for v in new_names[1:]:
                                        lv = icu_lower(v)
                                        if lv not in set_vals:
                                            vals.append(v)
                                            set_vals.add(lv)
                                newvals[book_id] = vals

                            except Exception as e:
                                print(e.message)
                                traceback.print_exc()
                        else:
                            newvals[book_id] = new_names[0]

                    # Allow case changes
                    self.unlock.set_field(field, newvals)
                    id_map[old_id] = self.unlock.get_item_id(field, new_names[0])

            if default_process_map:
                ab, idm = self.unlock.rename_items(field, default_process_map, change_index=change_index)
                affected_books.update(ab)
                id_map.update(idm)

            return affected_books, id_map

        try:
            rename_func = f.table.rename_item
        except AttributeError:
            raise ValueError("Cannot rename items for one-one fields: %s" % field)

        moved_books = set()
        id_map = {}
        for item_id, new_name in iteritems(item_id_to_new_name_map):
            new_names = tuple(x.strip() for x in new_name.split(sv)) if sv else (new_name,)
            books, new_id = rename_func(item_id, new_names[0], self.backend)
            affected_books.update(books)
            id_map[item_id] = new_id
            if new_id != item_id:
                moved_books.update(books)
            if len(new_names) > 1:
                # Add the extra items to the books
                extra = new_names[1:]
                self.unlock.set_field(
                    field,
                    {book_id: self.unlock.fast_field_for(f, book_id) + extra for book_id in books},
                )

        if affected_books:
            if field == "authors":
                self.unlock.set_field(
                    "author_sort",
                    {k: " & ".join(v) for k, v in iteritems(self.unlock.author_sort_strings_for_books(affected_books))},
                )
                self.unlock.update_path(affected_books, mark_as_dirtied=False)
            elif change_index and hasattr(f, "index_field") and tweaks["series_index_auto_increment"] != "no_change":
                for book_id in moved_books:
                    self.unlock.set_field(
                        f.index_field.name,
                        {
                            book_id: self.unlock.get_next_series_num_for(
                                self.unlock.fast_field_for(f, book_id), field=field
                            )
                        },
                    )
            self.unlock.mark_as_dirty(affected_books)
        return affected_books, id_map

    @write_api
    def remove_items(self, field, item_ids, restrict_to_book_ids=None):
        """
        Delete all items in the specified field with the specified ids.
        Returns the set of affected book ids. ``restrict_to_book_ids`` is an optional set of books ids. If specified the
        items will only be removed from those books.
        This is intended to be used with a virtual library - the entries will only be removed from the books in the
        virtual library.
        :param field:
        :param item_ids:
        :param restrict_to_book_ids:
        :return:
        """
        field = self.fields[field]

        if restrict_to_book_ids is not None and not isinstance(restrict_to_book_ids, frozenset):
            restrict_to_book_ids = frozenset(restrict_to_book_ids)
        affected_books = field.table.remove_items(item_ids, self.backend, restrict_to_book_ids=restrict_to_book_ids)
        if affected_books:
            # Todo: This method needs to deal with how we set indexes
            if hasattr(field, "index_field"):
                self.unlock.set_field(field.index_field.name, {bid: 1.0 for bid in affected_books})
            else:
                self.unlock.mark_as_dirty(affected_books)
        return affected_books

    # ------------------------------------------------------------------------------------------------------------------
    # - CUSTOM BOOK DATA
    # ------------------------------------------------------------------------------------------------------------------

    @write_api
    def add_custom_book_data(self, name, val_map, delete_first=False):
        """
        Records data in the books_plugin_data table.
        Add data for name where val_map is a map of book_ids to values. If delete_first is True, all previously stored
        data for name will be removed.
        :param name:
        :param val_map:
        :param delete_first:
        :return:
        """
        # Validate that the given books actually exist on the database
        missing = frozenset(val_map) - self.unlock.all_book_ids()
        if missing:
            raise ValueError("add_custom_book_data: no such book_ids: %d" % missing)
        self.backend.add_custom_data(name, val_map, delete_first)

    @read_api
    def get_custom_book_data(self, name, book_ids=(), default=None):
        """
        Get data for name. By default returns data for all book_ids, pass in a list of book ids if you only want some
        data. Returns a map of book_id to values. If a particular value could not be decoded, uses default for it.
        :param name:
        :param book_ids:
        :param default:
        :return:
        """
        return self.backend.get_custom_book_data(name, book_ids, default)

    @write_api
    def delete_custom_book_data(self, name, book_ids=()):
        """
        Delete data for name. By default deletes all data, if you only want to delete data for some book ids, pass in a
        list of book ids.
        :param name:
        :param book_ids:
        :return:
        """
        self.backend.delete_custom_book_data(name, book_ids)

    @read_api
    def get_ids_for_custom_book_data(self, name):
        """
        Return the set of book ids for which name has data.
        :param name:
        :return:
        """
        return self.backend.get_ids_for_custom_book_data(name)

    # ------------------------------------------------------------------------------------------------------------------
    # - CONVERSION DATA
    # ------------------------------------------------------------------------------------------------------------------

    @read_api
    def conversion_options(self, book_id, fmt="PIPE"):
        """
        Return the conversion options for a given book_id of a given format - default to fmt='PIPE'
        :param book_id:
        :param fmt:
        :return:
        """
        return self.backend.conversion_options(book_id, fmt)

    @read_api
    def has_conversion_options(self, ids, fmt="PIPE"):
        return self.backend.has_conversion_options(ids, fmt)

    @write_api
    def delete_conversion_options(self, book_ids, fmt="PIPE"):
        """
        Remove the conversion options for the given book ids.
        :param book_ids:
        :param fmt:
        :return:
        """
        return self.backend.delete_conversion_options(book_ids, fmt)

    @write_api
    def set_conversion_options(self, options, fmt="PIPE"):
        """
        options must be a map of the form {book_id:conversion_options}
        :param options:
        :param fmt:
        :return:
        """
        return self.backend.set_conversion_options(options, fmt)

    @write_api
    def refresh_format_cache(self):
        self.fields["formats"].table.read(self.backend)
        self.format_metadata_cache.clear()

    @write_api
    def refresh_ondevice(self):
        self.fields["ondevice"].clear_caches()
        self.clear_search_caches()
        self.clear_composite_caches()

    @read_api
    def tags_older_than(self, tag, delta=None, must_have_tag=None, must_have_authors=None):
        """
        Return the ids of all books having the tag ``tag`` that are older than the specified time.
        tag comparison is case insensitive.
        Used extensively internally with the tag browser.
        :param tag:
        :param delta: A timedelta object or None. If None, then all ids with the tag are returned.
        :param must_have_tag: If not None the list of matches will be restricted to books that have this tag
        :param must_have_authors: A list of authors. If not None the list of matches will be restricted to books that
                                  have these authors (case insensitive).
        :return:
        """
        tag_map = {icu_lower(v): k for k, v in iteritems(self.unlock.get_id_map("tags"))}
        tag = icu_lower(tag.strip())
        mht = icu_lower(must_have_tag.strip()) if must_have_tag else None
        tag_id, mht_id = tag_map.get(tag, None), tag_map.get(mht, None)
        ans = set()
        if mht_id is None and mht:
            return ans
        if tag_id is not None:
            tagged_books = self.unlock.books_for_field("tags", tag_id)
            if mht_id is not None and tagged_books:
                tagged_books = tagged_books.intersection(self.unlock.books_for_field("tags", mht_id))
            if tagged_books:
                if must_have_authors is not None:
                    amap = {icu_lower(v): k for k, v in iteritems(self.unlock.get_id_map("authors"))}
                    books = None
                    for author in must_have_authors:
                        abooks = self.unlock.books_for_field("authors", amap.get(icu_lower(author), None))
                        books = abooks if books is None else books.intersection(abooks)
                        if not books:
                            break
                    tagged_books = tagged_books.intersection(books or set())
                if delta is None:
                    ans = tagged_books
                else:
                    now = nowf()
                    for book_id in tagged_books:
                        ts = self.unlock.field_for("timestamp", book_id)
                        if (now - ts) > delta:
                            ans.add(book_id)
        return ans

    @write_api
    def set_sort_for_authors(self, author_id_to_sort_map, update_books=True):
        """
        Sets the sort field for any referenced authors.
        :param author_id_to_sort_map: Keyed with the author id, valued with the new sort string
        :param update_books:
        :return:
        """
        sort_map = self.fields["authors"].table.set_sort_names(author_id_to_sort_map, self.backend)
        changed_books = set()
        if update_books:
            val_map = {}
            for author_id in sort_map:
                books = self.unlock.books_for_field("authors", author_id)
                changed_books |= books
                for book_id in books:
                    authors = self.unlock.field_ids_for("authors", book_id)
                    adata = self.unlock.author_data(authors)
                    sorts = [adata[x]["sort"] for x in authors]
                    val_map[book_id] = " & ".join(sorts)
            if val_map:
                self.unlock.set_field("author_sort", val_map)
        if changed_books:
            self.unlock.mark_as_dirty(changed_books)
        return changed_books

    @write_api
    def set_link_for_authors(self, author_id_to_link_map):
        """
        Update the link field for the given authors.
        :param author_id_to_link_map:
        :return:
        """
        link_map = self.fields["authors"].table.set_links(author_id_to_link_map, self.backend)
        changed_books = set()
        for author_id in link_map:
            changed_books |= set([bid for bid in self.unlock.books_for_field("authors", author_id)])
        if changed_books:
            self.unlock.mark_as_dirty(changed_books)
        return changed_books

    @read_api
    def lookup_by_uuid(self, uuid):
        """
        UUID -> book_id
        The UUID for the given book is stored in the books table.
        :param uuid:
        :return:
        """
        return self.fields["uuid"].table.lookup_by_uuid(uuid)

    # ------------------------------------------------------------------------------------------------------------------
    # - CUSTOM COLUMNS
    # ------------------------------------------------------------------------------------------------------------------
    @write_api
    def create_custom_column(self, label, name, datatype, is_multiple, editable=True, display=None):
        """
        Make a custom column for the books table.
        :param label:
        :param name:
        :param datatype:
        :param is_multiple:
        :param editable:
        :param display:
        :return:
        """
        if display is None:
            display = {}
        return self.backend.create_custom_column(
            label=label,
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            editable=editable,
            display=display,
        )

    @write_api
    def set_custom_column_metadata(
        self,
        num,
        name=None,
        label=None,
        is_editable=None,
        display=None,
        update_last_modified=False,
    ):
        """
        Update the changeable metadata for a custom column.
        :param num:
        :param name:
        :param label:
        :param is_editable:
        :param display:
        :param update_last_modified:
        :return:
        """
        changed = self.backend.set_custom_column_metadata(
            num, name=name, label=label, is_editable=is_editable, display=display
        )
        if changed:
            if update_last_modified:
                self.unlock.update_last_modified(self.unlock.all_book_ids())
            else:
                self.backend.prefs.set("update_all_last_mod_dates_on_start", True)
        return changed

    # Todo: Make category and field distinct - category being a grouping of fields
    # Todo: This is really part of the search infrastructure
    @read_api
    def get_books_for_category(self, category, item_id_or_composite_value):
        """
        Category is an alternative term for field.
        :param category:
        :param item_id_or_composite_value:
        :return:
        """
        f = self.fields[category]
        # If the field has a specialized method for retrieving values, then use it
        if hasattr(f, "get_books_for_val"):
            # Composite field and others
            return f.get_books_for_val(
                item_id_or_composite_value,
                self.unlock.get_proxy_metadata,
                self.unlock.all_book_ids(),
            )
        return self.unlock.books_for_field(f.name, int(item_id_or_composite_value))

    @write_api
    def delete_custom_column(self, label=None, num=None):
        """
        Remove a custom column set for the books table.
        :param label:
        :param num:
        :return:
        """
        self.backend.delete_custom_column(label, num)

    # Todo: Should be part of the search infrastructure
    # ------------------------------------------------------------------------------------------------------------------
    # - FIND IDENTICAL BOOKS METHOD AND SUPPORT METHODS
    # ------------------------------------------------------------------------------------------------------------------

    @read_api
    def data_for_find_identical_books(self):
        """
        Return data that can be used to implement :meth:`find_identical_books` in a worker process without access to the
        db. See databases.utils for an implementation.
        :return author_map, authors_table, title_book_col_map:
        """
        at = self.fields["authors"].table
        author_map = defaultdict(set)
        for aid, author in iteritems(at.id_map):
            author_map[icu_lower(author)].add(aid)
        return (
            author_map,
            at.col_book_map.copy(),
            self.fields["title"].table.book_col_map.copy(),
        )

    @read_api
    def update_data_for_find_identical_books(self, book_id, data):
        """
        Update the data for find identicle books.
        :param book_id:
        :param data:
        :return:
        """
        author_map, author_book_map, title_map = data
        title_map[book_id] = self.unlock.field_for("title", book_id)
        at = self.fields["authors"].table
        for aid in at.book_col_map.get(book_id, ()):
            author_map[icu_lower(at.id_map[aid])].add(aid)
            try:
                author_book_map[aid].add(book_id)
            except KeyError:
                author_book_map[aid] = {book_id}

    @read_api
    def find_identical_books(self, mi, search_restriction="", book_ids=None):
        """
        Finds books that have a superset of the authors in mi and the same title (title is fuzzy matched). See also
        :meth:`data_for_find_identical_books`.
        :param mi:
        :param search_restriction:
        :param book_ids:
        :return:
        """
        from LiuXin.databases.utils import fuzzy_title

        identical_book_ids = set()
        if mi.authors:
            try:
                quathors = mi.authors[:20]  # Too many authors causes parsing of the search expression to fail
                query = " and ".join('authors:"=%s"' % (a.replace('"', "")) for a in quathors)
                qauthors = mi.authors[20:]
            except ValueError:
                return identical_book_ids

            try:
                book_ids = self.unlock.search(query, restriction=search_restriction, book_ids=book_ids)
            except Exception as e:
                err_str = "Exception while preforming search in find identical books - ignoring"
                default_log.log_exception(err_str, e, "DEBUG")

            if qauthors and book_ids:
                matches = set()
                qauthors = {icu_lower(x) for x in qauthors}
                for book_id in book_ids:
                    aut = self.unlock.field_for("authors", book_id)
                    if aut:
                        aut = {icu_lower(x) for x in aut}
                        if aut.issuperset(qauthors):
                            matches.add(book_id)
                book_ids = matches

            for book_id in book_ids:
                fbook_title = self.unlock.field_for("title", book_id)
                fbook_title = fuzzy_title(fbook_title)
                mbook_title = fuzzy_title(mi.title)
                if fbook_title == mbook_title:
                    identical_book_ids.add(book_id)
        return identical_book_ids

    # ------------------------------------------------------------------------------------------------------------------
    # - MOVE METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    # Todo: Actually write this method. Also write convenience methods for moving all the folder stores in one place
    @write_api
    def move_db_to(self, newloc, progress=None, abort=None):
        """
        Move the database file that we're currently running off to a different location.
        Moving the library as a whole also requires moving a bunch of the folder stores around - which needs to be done
        individually.
        :param newloc:
        :param progress:
        :param abort:
        :return:
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - SEARCH METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    @read_api
    def saved_search_names(self):
        return self._search_api.saved_searches.names()

    @read_api
    def saved_search_lookup(self, name):
        return self._search_api.saved_searches.lookup(name)

    @write_api
    def saved_search_set_all(self, smap):
        self._search_api.saved_searches.set_all(smap)
        self.unlock.clear_search_caches()

    @write_api
    def saved_search_delete(self, name):
        self._search_api.saved_searches.delete(name)
        self.unlock.clear_search_caches()

    @write_api
    def saved_search_add(self, name, val):
        self._search_api.saved_searches.add(name, val)

    @write_api
    def saved_search_rename(self, old_name, new_name):
        self._search_api.saved_searches.rename(old_name, new_name)
        self.unlock.clear_search_caches()

    @write_api
    def change_search_locations(self, newlocs):
        self._search_api.change_locations(newlocs)

    @write_api
    def refresh_search_locations(self):
        self._search_api.change_locations(self.field_metadata.get_search_terms())

    # ------------------------------------------------------------------------------------------------------------------
    # - DATABASE MAINTENANCE METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    @write_api
    def dump_and_restore(self, callback=None, sql=None):
        """
        Dump the database to disk and restore it. Can fix consistency problems.
        :param callback:
        :param sql:
        :return:
        """
        return self.backend.dump_and_restore(callback=callback, sql=sql)

    @write_api
    def vacuum(self):
        self.backend.vacuum()

    @write_api
    def close(self):
        from LiuXin.customize.ui import available_library_closed_plugins

        for plugin in available_library_closed_plugins():
            try:
                plugin.run(self)
            except Exception as e:
                err_str = "Exception while running library close plugin"
                default_log.log_exception(err_str, e, "DEBUG")
        self.backend.close()

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
        cover = mi.cover
        mi.cover = None
        self.unlock.create_book_entry(
            mi,
            add_duplicates=True,
            force_id=book_id,
            apply_import_tags=False,
            preserve_uuid=True,
        )
        self.unlock.update_last_modified((book_id,), last_modified)
        if cover and os.path.exists(cover):
            # Exists in the original calibre code
            self.unlock.set_field("cover", {book_id: 1})
        self.backend.restore_book(book_id, path, formats)

    @read_api
    def virtual_libraries_for_books(self, book_ids):
        """
        Return all the virtual libraries that the given books are in.
        :param book_ids:
        :return:
        """
        libraries = self.unlock.pref("virtual_libraries", {})
        ans = {book_id: [] for book_id in book_ids}
        for lib, expr in iteritems(libraries):
            books = self.unlock.search(expr)  # We deliberately dont use book_ids as we want to use the search cache
            for book in book_ids:
                if book in books:
                    ans[book].append(lib)
        return {k: tuple(sorted(v, key=sort_key)) for k, v in iteritems(ans)}

    @read_api
    def user_categories_for_books(self, book_ids, proxy_metadata_map=None):
        """
        Return the user categories for the specified books. proxy_metadata_map is optional and is useful for a
        performance boost, in contexts where a ProxyMetadata object for the books already exists.
        It should be a mapping of book_ids to their corresponding ProxyMetadata objects.
        :param book_ids:
        :param proxy_metadata_map:
        :return:
        """
        user_cats = self.backend.prefs["user_categories"]
        pmm = proxy_metadata_map or {}
        ans = {}

        for book_id in book_ids:
            proxy_metadata = pmm.get(book_id) or self.unlock.get_proxy_metadata(book_id)
            user_cat_vals = ans[book_id] = {}
            for ucat, categories in iteritems(user_cats):
                user_cat_vals[ucat] = res = []
                for name, cat, ign in categories:
                    try:
                        field_obj = self.fields[cat]
                    except KeyError:
                        continue

                    if field_obj.is_composite:
                        v = field_obj.get_value_with_cache(book_id, lambda x: proxy_metadata)
                    else:
                        v = self.unlock.fast_field_for(field_obj, book_id)

                    if isinstance(v, (list, tuple)):
                        if name in v:
                            res.append([name, cat])
                    elif name == v:
                        res.append([name, cat])
        return ans

    # ------------------------------------------------------------------------------------------------------------------
    # - EDIT METHODS FOR THE BOOKS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    @write_api
    def embed_metadata(self, book_ids, only_fmts=None, report_error=None, report_progress=None):
        """
        Update metadata in all formats of the specified book_ids to current metadata in the database.
        :param book_ids: The books to update with the new metadata
        :param only_fmts: Only ujpdate specific formats within those books
        :param report_error: A callback system to report if something goes wrong
        :param report_progress: A callback to report progress
        :return:
        """
        field = self.fields["formats"]
        from LiuXin.file_formats.opf.opf2 import pretty_print
        from LiuXin.customize.ui import apply_null_metadata
        from LiuXin.metadata.meta import set_metadata

        if only_fmts:
            only_fmts = {f.lower() for f in only_fmts}

        def doit(fmt, mi, stream):
            with apply_null_metadata, pretty_print:
                set_metadata(stream, mi, stream_type=fmt, report_error=report_error)
            stream.seek(0, os.SEEK_END)
            return stream.tell()

        for i, book_id in enumerate(book_ids):
            fmts = field.table.book_col_map.get(book_id, ())
            if not fmts:
                continue
            mi = self.get_metadata(book_id, get_cover=True, cover_as_data=True)
            try:
                path = self.unlock.field_for("path", book_id).replace("/", os.sep)
            except:
                continue
            for fmt in fmts:
                if only_fmts is not None and fmt.lower() not in only_fmts:
                    continue
                try:
                    name = self.fields["formats"].format_fname(book_id, fmt)
                except:
                    continue
                if name and path:
                    new_size = self.backend.apply_to_format(book_id, path, name, fmt, partial(doit, fmt, mi))
                    if new_size is not None:
                        self.format_metadata_cache[book_id].get(fmt, {})["size"] = new_size
                        max_size = self.fields["formats"].table.update_fmt(book_id, fmt, name, new_size, self.backend)
                        self.fields["size"].table.update_sizes({book_id: max_size})
            if report_progress is not None:
                report_progress(i + 1, len(book_ids), mi)

    @read_api
    def get_last_read_positions(self, book_id, fmt, user):
        """
        Lats read position records the users position within a document (used in the viewer). Record the data needed to
        reload that.
        :param book_id:
        :param fmt:
        :param user:
        :return:
        """
        fmt = fmt.upper()
        ans = []
        for device, cfi, epoch, pos_frac in self.backend.execute(
            "SELECT device,cfi,epoch,pos_frac FROM last_read_positions WHERE book=? AND format=? AND user=?",
            (book_id, fmt, user),
        ):
            ans.append({"device": device, "cfi": cfi, "epoch": epoch, "pos_frac": pos_frac})
        return ans

    @write_api
    def set_last_read_position(self, book_id, fmt, user="_", device="_", cfi=None, epoch=None, pos_frac=0):
        """
        Store data needed to retrieve a last read position for the user.
        :param book_id:
        :param fmt:
        :param user:
        :param device:
        :param cfi:
        :param epoch:
        :param pos_frac:
        :return:
        """
        fmt = fmt.upper()
        device = device or "_"
        user = user or "_"
        if not cfi:
            self.backend.execute(
                "DELETE FROM last_read_positions WHERE book=? AND format=? AND user=? AND device=?",
                (book_id, fmt, user, device),
            )
        else:
            self.backend.execute(
                "INSERT OR REPLACE INTO last_read_positions(book,format,user,device,cfi,epoch,pos_frac) VALUES (?,?,?,?,?,?,?)",
                (book_id, fmt, user, device, cfi, epoch or time(), pos_frac),
            )

    @read_api
    def export_library(self, library_key, exporter, progress=None, abort=None):
        from binascii import hexlify

        key_prefix = hexlify(library_key)
        book_ids = self.unlock.all_book_ids()
        total = len(book_ids) + 1
        format_metadata = {}
        if progress is not None:
            progress("metadata.db", 0, total)
        pt = PersistentTemporaryFile("-export.db")
        pt.close()
        self.backend.backup_database(pt.name)
        dbkey = key_prefix + ":::" + "metadata.db"
        with lopen(pt.name, "rb") as f:
            exporter.add_file(f, dbkey)
        os.remove(pt.name)
        metadata = {
            "format_data": format_metadata,
            "metadata.db": dbkey,
            "total": total,
        }
        for i, book_id in enumerate(book_ids):
            if abort is not None and abort.is_set():
                return
            if progress is not None:
                progress(self.unlock.field_for("title", book_id), i + 1, total)
            format_metadata[book_id] = {}
            for fmt in self.unlock.formats(book_id):
                mdata = self.format_metadata(book_id, fmt)
                key = "%s:%s:%s" % (key_prefix, book_id, fmt)
                format_metadata[book_id][fmt] = key
                with exporter.start_file(key, mtime=mdata.get("mtime")) as dest:
                    self.unlock.copy_format_to(book_id, fmt, dest, report_file_size=dest.ensure_space)
            cover_key = "%s:%s:%s" % (key_prefix, book_id, ".cover")
            with exporter.start_file(cover_key) as dest:
                if not self.copy_cover_to(book_id, dest, report_file_size=dest.ensure_space):
                    dest.discard()
                else:
                    format_metadata[book_id][".cover"] = cover_key
        exporter.set_metadata(library_key, metadata)
        if progress is not None:
            progress(_("Completed"), total, total)


def normalize_fmt(fmt):
    """
    Takes a fmt, in the form of a string - normalizes it.
    Upper case is ensured.
    If there is a leading '.', remove it.
    If there is not a priority number at the end of the string then assume that you want the highest priority file
    associated with the book - so add '_1' to the fmt and return it.
    :param fmt:
    :return:
    """
    fmt = fmt.upper()
    if fmt.startswith("."):
        fmt = fmt[1:]
    if re.match(r"^[a-zA-Z0-9]+$", fmt):
        fmt += "_1"
    return fmt


def loc_from_formats_field(formats_field, book_id, fmt):
    """
    Preforms the standardized search for the desired format (checks it for a dot, checks it for a trailing number).
    Tries to match the format with a corresponding priority fmt - something of the form fmt_number - which tells you the
    format and it's position in the formats stack for that particular book.
    Records the final fmt_priority used to find the format as an atribute of the returned loc.
    Thus, if you use this function to find the location of an object, be aware that you should use the fmt_priority
    returned with it from then on - as that's the effective format of the book.
    :param formats_field:
    :param book_id:
    :param fmt:
    :return:
    """
    try:
        loc = formats_field.format_floc(book_id=book_id, fmt=fmt)
        loc.fmt_priority = fmt
        return loc
    except:
        pass

    # Try the format without a leading dot, if one if present
    if fmt.startswith("."):
        fmt = fmt[1:]
    try:
        loc = formats_field.format_floc(book_id=book_id, fmt=fmt)
        loc.fmt_priority = fmt
        return loc
    except:
        pass

    # The fmt might not have a trailing _fmt_num - default to 1 and return
    if re.match(r"^[a-zA-Z0-9]+$", fmt):
        fmt += "_1"
    try:
        loc = formats_field.format_floc(book_id=book_id, fmt=fmt)
        loc.fmt_priority = fmt
        return loc
    except:
        raise NoSuchFormat("Record %d has no fmt: %s" % (book_id, fmt))


# Todo: This needs to actually be written and tested for a calibre library
def import_library(library_key, importer, library_path, progress=None, abort=None):
    from LiuXin.databases.backend import DB

    metadata = importer.metadata[library_key]
    total = metadata["total"]
    if progress is not None:
        progress("metadata.db", 0, total)
    if abort is not None and abort.is_set():
        return
    with open(os.path.join(library_path, "metadata.db"), "wb") as f:
        src = importer.start_file(metadata["metadata.db"], "metadata.db for " + library_path)
        shutil.copyfileobj(src, f)
        src.close()
    cache = CalibreCache(DB(library_path, load_user_formatter_functions=False))
    cache.init()
    format_data = {int(book_id): data for book_id, data in metadata["format_data"].iteritems()}
    for i, (book_id, fmt_key_map) in enumerate(format_data.iteritems()):
        if abort is not None and abort.is_set():
            return
        title = cache._field_for("title", book_id)
        if progress is not None:
            progress(title, i + 1, total)
        cache._update_path((book_id,), mark_as_dirtied=False)
        for fmt, fmtkey in fmt_key_map.iteritems():
            if fmt == ".cover":
                stream = importer.start_file(fmtkey, _("Cover for %s") % title)
                path = cache._field_for("path", book_id).replace("/", os.sep)
                cache.backend.set_cover(book_id, path, stream, no_processing=True)
            else:
                stream = importer.start_file(fmtkey, _("{0} format for {1}").format(fmt.upper(), title))
                size, fname = cache._do_add_format(book_id, fmt, stream, mtime=stream.mtime)
                cache.fields["formats"].table.update_fmt(book_id, fmt, fname, size, cache.backend)
            stream.close()
        cache.dump_metadata({book_id})
    if progress is not None:
        progress(_("Completed"), total, total)
    return cache


# }}}
