# -*- coding: utf-8 -*-

"""
Plugins provide means of extending LiuXin's functionality.

As LiuXin uses a lot of calibre parts, porting calibre plugins to LiuXin should be fairly easy.
Here you can find methods to assist with that.
WARNING - Compatibility is intended to be high, but there may well be some weird corner cases.
Testing is always advised.
There are some example methods in the tests to help with this.

E.g. If you want to test a database or cache like object, you can subclass a contructor which will patch in your
experimental object.
Then run the full suite of tests which have been marked as for it.

All base classes for any plugins anywhere in LiuXin should be stored here.
"""

from __future__ import with_statement, print_function

import os
import sys
import zipfile
import importlib
import pathlib
from copy import deepcopy

from typing import Union, Any, BinaryIO, NamedTuple, Iterable, Tuple, ClassVar, Literal, Optional

from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.constants import CALIBRE_NUMERIC_VERSION as numeric_version
from LiuXin_alpha.constants import CALIBRE_NUMERIC_VERSION as calibre_numeric_version
from LiuXin_alpha.utils.which_os import iswindows, isosx

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile as LiuXinZipFile
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory, PersistentTemporaryFile
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.errors import PluginNotFound, InvalidPlugin

from LiuXin_alpha.preferences import preferences

from typing import TypeVar

class Base:
    ...

T = TypeVar("T", bound=Base)


class CatalogCLIOption(NamedTuple):
    option: str
    default: str
    dest: str
    help: str


platform = "linux"
if iswindows:
    platform = "windows"
elif isosx:
    platform = "osx"

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class PluginPreferences:
    """
    Class that can be used to store the preferences of a plugin.
    """

    def __init__(self):
        self.defaults = dict()


class Plugin:  # {{{
    """
    Base class for a calibre plugin.

    Useful members include:

       * ``self.plugin_path``: Stores path to the zip file that contains this plugin or None if it is a builtin plugin
       * ``self.site_customization``: Stores a customization string entered by the user.

    Methods that should be overridden in sub classes:

       * :meth:`initialize`
       * :meth:`customization_help`

    Useful methods:

        * :meth:`temporary_file`
        * :meth:`__enter__`
        * :meth:`load_resources`
    """

    # List of platforms this plugin works on. For example: ``['windows', 'osx', 'linux']``
    supported_platforms = []

    # Todo: check this claim is actually true
    # The name of this plugin. You must set it something other than Trivial Plugin for it to work.
    name = "Trivial Plugin"

    # The version of this plugin as a 3-tuple (major, minor, revision)
    version = (1, 0, 0)

    # A short string describing what this plugin does
    description = _("Does absolutely nothing")

    # The author of this plugin
    author = _("Unknown")

    # When more than one plugin exists for a filetype, the plugins are run in order of decreasing priority
    # i.e. plugins with higher priority will be run first. The highest possible priority is ``sys.maxint``.
    # Default priority is 1.
    priority = 1

    # The earliest version of calibre this plugin requires
    minimum_calibre_version = (0, 4, 118)

    # If False, the user will not be able to disable this plugin. Use with care.
    can_be_disabled = True

    # The plugin_type of this plugin. Used for categorizing plugins in an interface
    # This allows you to declare the category your plugin should appear in an interface
    # For other purposes, the category will be inferred from the plugin type.
    plugin_type = _("Base")

    def __init__(self, plugin_path: Union[str, pathlib.Path]) -> None:
        """
        Startup the plugin.

        :param plugin_path:
        """
        self.plugin_path = plugin_path
        self.site_customization = None
        self.prefs = PluginPreferences()

    def initialize(self) -> None:
        """
        Called once when calibre plugins are initialized. Plugins are re-initialized
        every time a new plugin is added.

        Perform any plugin specific initialization here, such as extracting
        resources from the plugin zip file. The path to the zip file is
        available as ``self.plugin_path``.

        Note that ``self.site_customization`` is **not** available at this point.
        """
        pass

    def config_widget(self):
        """
        Implement this method and :meth:`save_settings` in your plugin to use a custom configuration dialog, rather then
        relying on the simple string based default customization.

        This method, if implemented, must return a QWidget. The widget can have an optional method validate() that takes
        no arguments and is called immediately after the user clicks OK. Changes are applied if and only if the method
        returns True.

        If for some reason you cannot perform the configuration at this time, return a tuple of two strings (message,
        details), these will be displayed as a warning dialog to the user and the process will be aborted.
        """
        raise NotImplementedError()

    def save_settings(self, config_widget):
        """
        Save the settings specified by the user with config_widget.

        :param config_widget: The widget returned by :meth:`config_widget`.

        """
        raise NotImplementedError()

    def do_user_config(self, parent=None):
        """
        This method shows a configuration dialog for this plugin.

        It returns True if the user clicks OK, False otherwise.
        The changes are automatically applied - if you use the default logic.
        If you don't, it's up to you.

        To preserve separation between core and interface logic, the code for this has been moved to
        `LiuXin.interfaces.gui_common.plugin_user_config`.
        Call the method `do_calibre_plugin_config` there with this plugin as the first argument and the parent as the
        second.
        This will invoke the defaut configuration logic for calibre.

        :param parent: The parent window for the widget

        """
        raise NotImplementedError

    def pyqt5_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a PyQt5 environment.

        There should be one of these methods for each of the generic environments which the interfaces support.

        If you just want a method that returns True if the user clicks OK, False otherwise, use the defalt calibre logic
        by calling the method as specified in `do_user_config`

        The changes should be automatically applied.
        Probably.
        But it is really up to you!

        :param parent:
        :return:
        """
        raise NotImplementedError

    def command_line_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a command line environment.

        The changes should be automatically applied.
        But it really is up to you!

        :param parent:
        :return:
        """
        raise NotImplementedError

    # More interface environment methods will be added here as appropriate.

    def load_resources(self, names: list[str]) -> dict[str, bytes]:
        """
        If this plugin comes in a ZIP file (user added plugin), this method will allow you to load resources from the
        ZIP file.

        For example to load an image::

            pixmap = QPixmap()
            pixmap.loadFromData(self.load_resources(['images/icon.png']).itervalues().next())
            icon = QIcon(pixmap)

        Resource will be returned as bytes.

        :param names: List of paths to resources in the zip file using / as separator

        :return: A dictionary of the form ``{name: file_contents}``. Any names
                 that were not found in the zip file will not be present in the
                 dictionary.

        """
        if self.plugin_path is None:
            raise ValueError("This plugin was not loaded from a ZIP file")
        ans = {}
        with zipfile.ZipFile(self.plugin_path, "r") as zf:
            for candidate in zf.namelist():
                if candidate in names:
                    ans[candidate] = zf.read(candidate)
        return ans

    def customization_help(self, gui: bool = False) -> str:
        """
        Return a string giving help on how to customize this plugin.

        By default, raise a :class:`NotImplementedError`, which indicates that the plugin does not require
        customization.

        If you re-implement this method in your subclass, the user will be asked to enter a string as customization for
        this plugin. The customization string will be available as ``self.site_customization``.
        Site customization could be anything, for example, the path to a needed binary on the user's computer.

        :param gui: If True return HTML help, otherwise return plain text help.
        """
        raise NotImplementedError

    @staticmethod
    def temporary_file(suffix: str) -> PersistentTemporaryFile:
        """
        Return a file-like object that is a temporary file on the file system.

        This file will remain available even after being closed and will only
        be removed on interpreter shutdown. Use the ``name`` member of the
        returned object to access the full path to the created temporary file.

        :param suffix: The suffix that the temporary file will have.

        """
        return PersistentTemporaryFile(suffix)

    def is_customizable(self) -> bool:
        """
        Can the plugin be customized?

        :return:
        """
        try:
            self.customization_help()
            return True
        except NotImplementedError:
            return False

    # Todo: *args and **kwargs here pass through to something which does not seem to matter
    # Todo: This also reads as a _real_ bad idea
    def __enter__(self, *args, **kwargs) -> None:
        """
        Add this plugin to the python path so that it's contents become directly importable.

        Useful when bundling large python libraries into the plugin. Use it like this::
            with plugin:
                import something
        Included for legacy compatibility reasons - ideally should never be used.
        """
        if self.plugin_path is not None:

            # Todo: Should this be a with statement?
            zf = LiuXinZipFile(self.plugin_path)

            extensions = set([x.rpartition(".")[-1].lower() for x in zf.namelist()])
            zip_safe = True
            for ext in ("pyd", "so", "dll", "dylib"):
                if ext in extensions:
                    zip_safe = False
                    break

            if zip_safe:
                sys.path.insert(0, self.plugin_path)
                self.sys_insertion_path = self.plugin_path
            else:
                self._sys_insertion_tdir = TemporaryDirectory("plugin_unzip")
                self.sys_insertion_path = self._sys_insertion_tdir.__enter__(*args, **kwargs)
                zf.extractall(self.sys_insertion_path)
                sys.path.insert(0, self.sys_insertion_path)

            zf.close()

    def __exit__(self, *args) -> None:
        """
        Remove the previously added paths.

        :param args:
        :return:
        """
        ip, it = getattr(self, "sys_insertion_path", None), getattr(self, "_sys_insertion_tdir", None)
        if ip in sys.path:
            sys.path.remove(ip)
        if hasattr(it, "__exit__"):
            it.__exit__(*args)

    def cli_main(self, args):
        """
        This method is the main entry point for your plugins command line interface.

        It is called when the user does: calibre-debug -r "Plugin Name".
        Any arguments passed are present in the args variable.
        """
        raise NotImplementedError("The %s plugin has no command line interface" % self.name)


# }}}


class FileTypePlugin(Plugin):  # {{{
    """
    A plugin transforms a particular set of file types.

    Specifically a file plugin_type that preforms a transform on a given file type.
    Each of these plugins can be run at one - or more - of three different times
     - on_import - run before the file is imported into the library
     - on_postimport - run once the file has been added to the file
     - on_preprocess - run just before a conversion
     - on_postprocess - run right after a conversion

    Two methods are presented
     - run -


    """

    #: Set of file types for which this plugin should be run. For example: ``{'lit', 'mobi', 'prc'}``
    file_types: set[str] = set()

    #: If True, this plugin is run when books are added to the database
    on_import = False

    #: If True, this plugin is run after books are added to the database
    on_postimport = False

    #: If True, this plugin is run just before a conversion
    on_preprocess = False

    #: If True, this plugin is run after conversion on the final file produced by the conversion output plugin.
    on_postprocess = False

    plugin_type = _("File plugin_type")

    # Todo: This wants to be an abc
    def run(self, path_to_ebook: str) -> str:
        """
        Run the plugin. Must be implemented in subclasses.

        It should perform whatever modifications are required on the ebook and return the absolute path to the modified
        ebook. If no modifications are needed, it should return the path to the original ebook. If an error is
        encountered it should raise an Exception. The default implementation simply return the path to the original
        ebook.

        The modified ebook file should be created with the :meth:`temporary_file` method.
        :param path_to_ebook: Absolute path to the ebook.
        :return: Absolute path to the modified ebook.
        """
        # Default implementation does nothing
        return path_to_ebook

    def postimport(self, book_id, book_format, db):
        """
        Called post import, i.e., after the book file has been added to the database.

        :param book_id: DatabasePing id of the added book.
        :param book_format: The file plugin_type of the book that was added.
        :param db: Library database.
        """
        pass  # Default implementation does nothing


# }}}

class _MetadataReaderPlugin:
    """
    We want both LiuXin and calibre metadata readers to have a common interface - but not a common class heirachy.
    """

    # Set of file types for which this plugin should be run. For example: ``set(['lit', 'mobi', 'prc'])``
    file_types = frozenset([])

    # Basic measure of run cost
    inplace_run_cost = "high"

    # What platforms does this plugin work on?
    supported_platforms = ["windows", "osx", "linux"]

    # Default numeric version tuple
    version = calibre_numeric_version

    author = "Kovid Goyal"

    # Displayable plugin type
    plugin_type = _("Metadata reader")

    # Todo: Work out the signature from where these are called
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)

        self.quick = False

    # Todo: Pull the calibre metadata object out and gen an API for it
    def get_metadata(self, stream: BinaryIO, ftype: str):
        """
        Return metadata for the file represented by stream (a file like object that supports reading).

        Raise an exception when there is an error with the input data.

        :param ftype: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :return: A :class:`LiuXin.metadata.metadata.MetaData` object
        """
        raise NotImplementedError

    def get_metadata_inplace(self, file_path: Union[pathlib.Path, str], ftype: str):
        """
        Returns metadata for the file represented by the file path.

        Must be a valid path with read access.
        Raises an exception when there is an error with the input data.
        Sometimes avoids having to copy the entire file into memory.
        :param file_path:
        :param ftype: Guaranteed to be one of the entries in :attr:`file_types`.
        :return: A :class:`LiuXin.metadata.metadata.MetaData` object
        """
        # Tries to open the file as a stream and use the get_metadata method on it
        with open(file_path, "rb") as md_file_stream:
            return self.get_metadata(stream=md_file_stream, ftype=ftype)


class MetadataReaderPlugin(Plugin, _MetadataReaderPlugin):  # {{{
    """
    A plugin that implements reading metadata from a set of file types.
    """


class MetadataWriterPlugin(Plugin):
    """
    A plugin that implements writing metadata to files in a certain set of file types.
    """

    #: Set of file types for which this plugin should be run
    #: For example: ``set(['lit', 'mobi', 'prc'])``
    file_types = set([])

    supported_platforms = ["windows", "osx", "linux"]

    version = calibre_numeric_version

    author = "Kovid Goyal"

    plugin_type = _("Metadata writer")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        :param args:
        :param kwargs:
        """
        super().__init__(self, *args, **kwargs)

        self.apply_null = False

    def set_metadata(self, stream: BinaryIO, mi, type: str) -> None:
        """
        Set metadata for the file represented by stream (a file like object that supports reading).

        Raise an exception when there is an error with the input data.
        :param stream: The file to be modified
        :param type: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :param mi: A :class:`calibre.ebooks.metadata.book.Metadata` object
        """
        raise NotImplementedError

    def set_metadata_inplace(self, file_path: Union[str, pathlib.Path], mi, type: str) -> None:
        """
        Set metadata for the file pointed to by the file path.

        Means that the file doesn't have to be copied into memory for updating.
        Raise an exception when there is an error with the input data.

        WARNING: THE ACTUAL FILE ON DISK WILL BE MODIFIED.
        PLEASE CONSIDER TAKING A BACKUP FIRST.

        :param file_path: The file to be modified
        :param type: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :param mi: A :class:`calibre.ebooks.metadata.book.Metadata` object
        """
        raise NotImplementedError


# }}}


class CatalogPlugin(Plugin):  # {{{
    """
    A plugin that implements a catalog generator.

    Catalogs are files containing catalog entries from the database.
    The default plugin only writes out calibre metadata.
    You want a LiuXinCatalogPlugin if you want the full LiuXin metadata.
    """

    resources_path = None

    #: Output file plugin_type this generator can produce
    #: For example: 'epub' or 'xml'
    file_types = set([])

    plugin_type = _("Catalog generator")

    #: CLI parser options specific to this plugin, declared as namedtuple Option::
    #:
    #:  from collections import namedtuple
    #:  Option = namedtuple('Option', 'option, default, dest, help')
    #:  cli_options = [Option('--catalog-title',
    #:                       default = 'My Catalog',
    #:                       dest = 'catalog_title',
    #:                       help = (_('Title of generated catalog. \nDefault:') + " '" +
    #:                       '%default' + "'"))]
    #:  cli_options parsed in library.cli:catalog_option_parser()
    cli_options = []

    # Todo: Make sure that this is taken account of
    def _field_sorter(self, key: str) -> str:
        """
        Custom fields sort after standard fields.
        """
        if key.startswith("#"):
            return "~%s" % key[1:]
        else:
            return key

    def search_sort_db(self, db, opts):
        """
        Generate a catalog off a db search.

        Returns the data as a dict for writing out.
        :param db:
        :param opts:
        :return:
        """

        db.search(opts.search_text)

        if opts.sort_by:
            # 2nd arg = ascending
            db.sort(opts.sort_by, True)
        return db.get_data_as_dict(ids=opts.ids)

    # Todo: Add field maps to the database so that it can emulate calibre
    def get_output_fields(self, db, opts) -> list[str]:
        """
        Returns a list of the requested fields.

        :param db:
        :param opts:
        :return:
        """
        all_std_fields = {
            "author_sort",
            "authors",
            "comments",
            "cover",
            "formats",
            "id",
            "isbn",
            "library_name",
            "ondevice",
            "pubdate",
            "publisher",
            "rating",
            "series_index",
            "series",
            "size",
            "tags",
            "timestamp",
            "title_sort",
            "title",
            "uuid",
            "languages",
            "identifiers",
        }
        all_custom_fields = set(db.custom_field_keys())
        for field in list(all_custom_fields):
            fm = db.field_metadata[field]
            if fm["datatype"] == "series":
                all_custom_fields.add(field + "_index")
        all_fields = all_std_fields.union(all_custom_fields)

        if opts.fields != "all":
            # Make a list from opts.fields
            of = [x.strip() for x in opts.fields.split(",")]
            requested_fields = set(of)

            # Validate requested_fields
            if requested_fields - all_fields:
                from LiuXin_alpha.utils.calibre.library import current_library_name

                invalid_fields = sorted(list(requested_fields - all_fields))
                err_str = "invalid --fields specified: %s" % ", ".join(invalid_fields)
                err_str += "available fields in '%s': %s" % (
                    current_library_name(),
                    ", ".join(sorted(list(all_fields))),
                )
                default_log.error(err_str)
                raise ValueError("unable to generate catalog with specified fields")

            fields = [x for x in of if x in all_fields]
        else:
            fields = sorted(all_fields, key=self._field_sorter)

        if not opts.connected_device["is_device_connected"] and "ondevice" in fields:
            fields.pop(int(fields.index("ondevice")))

        return fields

    def initialize(self) -> None:
        """
        If plugin is not a built-in, copy the plugin's .ui and .py files from the zip file to $TMPDIR.

        Tab will be dynamically generated and added to the Catalog Options dialog in
        calibre.gui2.dialogs.catalog.py:Catalog
        """
        from LiuXin_alpha.customize.builtins import plugins as builtin_plugins
        from LiuXin_alpha.customize.ui import config
        from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory

        if not type(self) in builtin_plugins and self.name not in config["disabled_plugins"]:
            files_to_copy = ["%s.%s" % (self.name.lower(), ext) for ext in ["ui", "py"]]
            resources = zipfile.ZipFile(self.plugin_path, "r")

            if self.resources_path is None:
                self.resources_path = PersistentTemporaryDirectory("_plugin_resources", prefix="")

            for file in files_to_copy:
                try:
                    resources.extract(file, self.resources_path)
                except:
                    print(
                        " customize:__init__.initialize(): %s not found in %s"
                        % (file, os.path.basename(self.plugin_path))
                    )
                    continue
            resources.close()

    def run(self, path_to_output: Union[str, pathlib.Path], opts, db, ids: Iterable[str], notification=None) -> None:
        """
        Run the plugin. Must be implemented in subclasses.
        It should generate the catalog in the format specified in file_types, returning the absolute path to the
        generated catalog file. If an error is encountered it should raise an Exception.

        The generated catalog file should be created with the :meth:`temporary_file` method.

        :param path_to_output: Absolute path to the generated catalog file.
        :param opts: A dictionary of keyword arguments
        :param db: A LibraryDatabase2 object
        :param ids:
        """
        # Default implementation does nothing
        raise NotImplementedError("CatalogPlugin.generate_catalog() default method, should be overridden in subclass")


# }}}


class InterfaceActionBase(Plugin):  # {{{
    """
    Slots into the GUI.

    Probably not going to live here.
    """

    supported_platforms = ["windows", "osx", "linux"]

    author = "Kovid Goyal"

    plugin_type = _("User Interface Action")

    can_be_disabled = False

    actual_plugin = None

    def __init__(self, *args, **kwargs):
        Plugin.__init__(self, *args, **kwargs)
        self.actual_plugin_ = None

    def load_actual_plugin(self, gui):
        """
        This method must return the actual interface action plugin object.

        :param gui:
        :return:
        """
        ac = self.actual_plugin_
        if ac is None:
            mod, cls = self.actual_plugin.split(":")
            ac = getattr(importlib.import_module(mod), cls)(gui, self.site_customization)
            self.actual_plugin_ = ac
        return ac


# }}}


class PreferencesPlugin(Plugin):  # {{{
    """
    A plugin representing a widget displayed in the Preferences dialog.

    This plugin has only one important method :meth:`create_widget`. The various fields of the plugin control how it is
    categorized in the UI.
    """

    supported_platforms = ["windows", "osx", "linux"]
    author = "Kovid Goyal"
    plugin_type = _("Preferences")
    can_be_disabled = False

    #: Import path to module that contains a class named ConfigWidget
    #: which implements the ConfigWidgetInterface. Used by
    #: :meth:`create_widget`.
    config_widget = None

    #: Where in the list of categories the :attr:`category` of this plugin should be.
    category_order = 100

    #: Where in the list of names in a category, the :attr:`gui_name` of this plugin should be
    name_order = 100

    #: The category this plugin should be in
    category = None

    #: The category name displayed to the user for this plugin
    gui_category = None

    #: The name displayed to the user for this plugin
    gui_name = None

    #: The icon for this plugin, should be an absolute path
    icon = None

    #: The description used for tooltips and the like
    description = None

    def create_widget(self, parent=None):
        """
        Create and return the actual Qt widget used for setting this group of preferences. The widget must implement the

        :class:`calibre.gui2.preferences.ConfigWidgetInterface`.

        The default implementation uses :attr:`config_widget` to instantiate the widget.
        :param parent:
        :return:
        """
        base, _, wc = self.config_widget.partition(":")
        if not wc:
            wc = "ConfigWidget"
        base = importlib.import_module(base)
        widget = getattr(base, wc)
        return widget(parent)


class StoreBase(Plugin):  # {{{
    """
    Interface to an ebook store to allow buying books from within calibre.
    """
    # Plugins this store will run for
    supported_platforms = ["windows", "osx", "linux"]

    author = "John Schember"

    plugin_type = _("Store")

    # Information about the store. Should be in the primary language
    # of the store. This should not be translatable when set by
    # a subclass.
    description = _("An ebook store.")

    # Minimum calibre version for the plugin to run
    minimum_calibre_version = (0, 8, 0)

    # Plugin version
    version = (1, 0, 1)

    actual_plugin = None

    # Does the store only distribute ebooks without DRM.
    drm_free_only = False

    # This is the 2 letter country code for the corporate headquarters of the store.
    headquarters = ""

    # All formats the store distributes ebooks in.
    formats = []

    # Is this store on an affiliate program?
    affiliate = False

    def load_actual_plugin(self, gui):
        """
        This method must return the actual interface action plugin object.

        :param gui:
        :return:
        """
        mod, cls = self.actual_plugin.split(":")
        self.actual_plugin_object = getattr(importlib.import_module(mod), cls)(gui, self.name)
        return self.actual_plugin_object

    def customization_help(self, gui: bool = False) -> None:
        """
        Help with customizing the store.

        :param gui:
        :return:
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.customization_help(gui)
        raise NotImplementedError()

    def config_widget(self) -> Any:
        """
        Provides a config widget to config the store plugin.

        At a guess, this returns a QWidget.
        :return:
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.config_widget()
        raise NotImplementedError()

    def save_settings(self, config_widget: Any) -> None:
        """
        Save setting changes made with the config_widget.

        :param config_widget:
        :return:
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.save_settings(config_widget)
        raise NotImplementedError()


# }}}


class ViewerPlugin(Plugin):  # {{{
    """
    These plugins are used to add functionality to the calibre viewer.
    """

    plugin_type = _("Viewer")

    def load_fonts(self) -> None:
        """
        This method is called once at viewer startup. It should load any fonts it wants to make available. For example::

            def load_fonts():
                from PyQt5.Qt import QFontDatabase
                font_data = get_resources(['myfont1.ttf', 'myfont2.ttf'])
                for raw in font_data.itervalues():
                    QFontDatabase.addApplicationFontFromData(raw)
        """
        pass

    def load_javascript(self, evaljs):
        """
        This method is called every time a new HTML document is loaded in the viewer. Use it to load javascript
        libraries into the viewer. For example::

            def load_javascript(self, evaljs):
                js = get_resources('myjavascript.js')
                evaljs(js)
        :param evaljs:
        :return:
        """
        pass

    def run_javascript(self, evaljs):
        """
        This method is called every time a document has finished loading. Use it in the same way as load_javascript().

        :param evaljs:
        :return:
        """
        pass

    def customize_ui(self, ui):
        """
        This method is called once when the viewer is created. Use it to make any customizations you want to the
        viewer's user interface. For example, you can modify the toolbars via ui.tool_bar and ui.tool_bar2.
        :param ui:
        :return:
        """
        pass

    def customize_context_menu(self, menu, event, hit_test_result):
        """
        This method is called every time the context (right-click) menu is shown. You can use it to customize the
        context menu. ``event`` is the context menu event and hit_test_result is the QWebHitTestResult for this event
        in the currently loaded document.
        :param menu:
        :param event:
        :param hit_test_result:
        :return:
        """
        pass


# }}}


class LibraryClosedPlugin(Plugin):  # {{{
    """
    LibraryClosedPlugins are run when a library is closed, either at shutdown, when the library is changed, or when a
    library is used in some other way.
    At the moment these plugins won't be called by the CLI functions.
    """

    plugin_type = _("Library Closed")

    # minimum version 2.54 because that is when support was added
    minimum_calibre_version = (2, 54, 0)

    def run(self, db) -> None:
        """
        The db will be a reference to the new_api (db.cache.py).

        The plugin must run to completion.
        It must not use the GUI, threads, or any signals.
        """
        raise NotImplementedError("LibraryClosedPlugin " "run method must be overridden in subclass")


# }}}


class EditBookToolPlugin(Plugin):  # {{{
    """
    Tool to edit a book.
    """

    plugin_type = _("Edit Book Tool")

    minimum_calibre_version = (1, 46, 0)


# }}}

# ------------------------------------
#
# - LIUXIN SPECIFIC PLUGINS START HERE

class LiuXinPlugin(Plugin):
    """
    Base class for all LiuXin specific plugins.

    The assumption with the rest of the plugins is that they're calibre at base.
    This is the base class for all LiuXin specific plugins.
    As a rule, the above plugins expect objects with a calibre compatible surface.
    These plugins do not (at least by default).
    """

    # Will start incrementing... soon
    minimum_liuxin_version = (1, 0, 0)


class MDInputTransform(LiuXinPlugin):  # {{{
    """
    Base class for the MetaData Input Transformation plugins.

    These plugins are intended to be run every time a set of MetaData is extracted from a file.
    A plugin of this plugin_type takes a collection of MetaData objects, and returns a MetaData object.
    This collection could be a single MetaData object.
    The MetaData object should be loaded with either the files or, preferably, local paths to the files which are being
    examined.
    This allows this plugin to go back and check the metadata again. If needed.

    This is also intended to be the base class for transforming  a single metadata object.
    (for example "I want to ensure the title is in title case").
    In this case it should take and return a single MetaData object.
    (You should _check_ you're only being given one in this case - mistakes happen).
    """

    # There are several different metadata containers floating around
    # As such, these transforms could support all - or none - of them
    target_classes = []

    def transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Takes a collection of MetaData objects. Uses them to preform a transform. Returns the transformed MetaData.

        :param first:
        :param rest:
        :return:
        """
        # Optional runtime guard if you want it strict:
        if any(type(x) is not type(first) for x in rest):
            raise TypeError("All args must be the same concrete class")

        return self._true_transform_metadata(first, *rest)

    def _true_transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Mostly needed for typing.

        :param first:
        :param rest:
        :return:
        """
        raise NotImplementedError("You need to actually work out how to do this.")


class LXMetadataReaderPlugin(LiuXinPlugin, _MetadataReaderPlugin):
    """
    To distinguish the calibre metadata readers from the ones which have been re-written for LiuXin.
    """
    # All file formats this plugin could be used for
    valid_for = None

    # The file formats this plugin SHOULD be used for
    priority_for = None

    # Costs of actually running the
    run_cost = "high"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        :param args:
        :param kwargs:
        """
        super().__init__(self, *args, **kwargs)
        self.quick = False

    @staticmethod
    def standardize_type(file_type: str) -> str:
        """
        Standardizes a plugin_type so that it can be compared against the known types that the plugin can be run for.

        :param file_type:
        :return file_type:
        """
        file_type = deepcopy(file_type)
        if file_type.startswith("."):
            file_type = file_type[1:]
        return file_type.upper()

    def get_metadata(self, stream: Union[BinaryIO, str, pathlib.Path], file_type: str):
        """
        Return metadata for the file represented by stream or path.

        (a file like object that supports reading) or a filepath on the local system).
        Raise an exception when there is an error with the input data.
        :param file_type: The plugin_type of file. Guaranteed to be one of the entries
        in :attr:`file_types`.
        :return: A :class:`calibre.ebooks.metadata.book.Metadata` object
        """
        return None


class Archive(LiuXinPlugin):
    """
    Provides a zipfile like read interface to a compressed file format.

    Options for writing interfaces are also provided - where possible.
    read_formats and write_formats are the formats that this plugin can read/write to -
    stored as the extension without the dot.
    Note - all classes that inherit from this should try and raise only one form of error - ArchiveError from
    LiuXin.errors
    """
    # This plugin can read from these formats
    read_formats: frozenset[str] = frozenset()

    # This plugin can write to these formats
    write_formats: frozenset[str] = frozenset()

    # If the plugin supports multiple write types, which one should be used by default?
    default_write_type: str

    def __init__(self,
                 file_path: Union[pathlib.Path, str],
                 *,
                 mode: Literal["r", "w", "a"],
                 compression_flags=None,
                 write_type: Optional[str] = None,
                 password: str) -> None:
        """
        Initialize an object representing the compressed file.

        :param file_path: Path to the file
        :param mode: Should be the standard python file modes for zipfile (a, r, w e.t.c)
                     Note that there is no such mode as rb e.t.c supported for zip files - archives are opened in
                     bytes mode by default.
                     This should be reflected in all archive implementations.
        :param compression_flags: A flag for the compression method
        :param write_type: If an archive doesn't exist at the given file_path, then it has to be created. For plugins
                           that can write to multiple file types the write_type is the plugin_type of archive you want to write
                            to (e.g. if a plugin can write to both rar and zip, and you want to create a rar archive,
                            the set write_type="rar").
                            If the plugin can only write to one plugin_type of archive this will be ignored.
        :return:
        """
        super().__init__(plugin_path="builtin")

        # Properties of the archive on disk - it's location, size, plugin_type e.t.c
        self.file_path = file_path
        file_ext = os.path.splitext(file_path)[0]
        if file_ext.startswith("."):
            file_ext = file_ext[1:]
        self.file_ext = file_ext
        self.file_name = os.path.splitext(os.path.basename(self.file_path))[0]
        self.mode = mode
        self.compression_flags = compression_flags
        self.write_type = write_type

        if write_type is not None and not self.write_formats or write_type not in self.write_formats:
            err_str = "This class has been called with an invalid write plugin_type - " "valid write types: {}".format(
                self.write_formats
            )
            raise NotImplementedError(err_str)

        # Properties of the files in the archives
        self.compression_type = None
        self.block_count = None
        self.physical_size = None
        self.final_size = None
        self.multivolume = "unknown"
        self.password = password

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO REPRESENT THE CLASS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        """
        Returns a string representation of the object
        :return:
        """
        return self.__unicode__().encode("utf-8")

    def __unicode__(self):
        """
        A string representation of a archive object - for output to the console.
        """
        ans = []

        def format(x, y):
            candidate = None
            try:
                candidate = "%-20s: %s" % (six_unicode(x), six_unicode(y))
                # ans.append(u'%-20s: %s'%(unicode(x), unicode(y)))
            except UnicodeDecodeError:
                # Todo: Use the default encoding here
                candidate = "%-20s: %s" % (
                    six_unicode(x, "utf-8"),
                    six_unicode(y, "utf-8"),
                )
                # ans.append(u'%-20s: %s'%(unicode(x,'utf-8'), unicode(y,'utf-8')))
            finally:
                if candidate is None:
                    ans.append("%-20s: %s" % (six_unicode(x), repr(y)))
                else:
                    ans.append(candidate)

        # Todo: This really needs testing against python 2 and python 3
        def set_format(x, y):
            assert hasattr(y, "__iter__")
            try:
                candidate = "%-20s: %s" % (six_unicode(x), six_unicode(""))
            except UnicodeDecodeError:
                candidate = "%-20s: %s" % (
                    six_unicode(x, "utf-8"),
                    six_unicode("", "utf-8"),
                )
            ans.append(candidate)
            for item in y:
                try:
                    candidate = "%-20s: %s" % (six_unicode(""), six_unicode(item))
                except UnicodeDecodeError:
                    candidate = "%-20s: %s" % (
                        six_unicode("", "utf-8"),
                        six_unicode(item, "utf-8"),
                    )
                ans.append(candidate)

        format("file_name", self.file_name)
        format("file_extension", self.file_ext)
        format("file_path", self.file_path)
        format("compression_type", self.compression_type)
        format("block_count", self.block_count)
        format("physical_size", self.physical_size)
        set_format("files", self.files)

        return "\n".join(ans)

    def printdir(self):
        """
        Prints a contents of the archive to sys.stdout. Mostly useless and will not be implemented.
        Only here for completeness of the api.
        :return:
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GATHER BASIC INFORMATION ABOUT THE FILE
    # ------------------------------------------------------------------------------------------------------------------
    @classmethod
    def is_valid(cls, path):
        """
        Takes a local path - determines if the file can be read by this class (is a valid example of one of the archive
        types that the class can read).
        Equivalent to the is_zipfile method.
        :param path:
        :return:
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - READ METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def getinfo(self, name):
        """
        Return info on an element in the archive.
        Calling this for a name not in the archive results in a KeyError.
        Should return an object with the same API as the ZipInfo object (Note that, in later versions of ZipFile, the
        ZipInfo object for a file can be used in place of the name when specifying an object for extraction - this
        might not be the case here - always use the name to be sure).
        In cases where the plugin doesn't support much data extraction, thwere will always be a file name. That is the
        only garantee which is made.
        :param name:
        :return:
        """
        raise NotImplementedError

    def infolist(self):
        """
        Returns a list containing an info object for every element.  The objects are in the same order as their entries
        in the actual ZIP file on disk if an existing archive was opened.
        It's assumed that the objects that this method returns have the same interface as
        :param name:
        :return:
        """
        raise NotImplementedError

    def namelist(self):
        """
        Returns a list of all members of the archive by name.
        Paths are unix style (/ separated).
        Not a property to more closely match the zipfile interface.
        :return:
        """
        raise NotImplementedError

    @property
    def files(self):
        """
        Returns all the files in the archive. The paths are relative to the root of the file and are unix styles paths
        (separated by /).
        :return:
        """
        raise NotImplementedError

    @property
    def folders(self):
        """
        Returns all the folders in the archive. The paths are relative to the root of the file and are unix style paths
        (separated by /).
        :return:
        """
        raise NotImplementedError

    def extract(self, path, pwd, member):
        """
        Extract a member of the archive.
        Note that this function works like the method of this name from zipfile - if you point the member to a file in
        the archive it'll create the dictionary structure of the archive up to that file, then create that file.
        If you just
        :param path: Where the file should be extracted
        :param pwd: Password for archive
        :param member: Either the name of the object or an info object (preferably a name - as, often, the name will
                       just have to be extracted out of the info object anyways).
        :return normalized_path: A normalized path created to the extracted member of the archive
        """
        raise NotImplementedError

    def extractall(self, path, pwd, members):
        """
        Extract all members from the archive to the current working directory. path specifies a different directory to
        extract to. members is optional and must be a subset of the list returned by namelist(). pwd is the password
        used for encrypted files.
        This works as the zipfile extractall method.
        :param path:
        :param pwd:
        :param members:
        :return:
        """
        raise NotImplementedError

    def get_file(self, path, member, pwd):
        """
        Extract the file and write it out to a pre-prepared file path.
        :param path:
        :param member:
        :param pwd:
        :return:
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - WRITE METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    def write(self, filename, arcname, compress_type):
        """
        Write the file named filename to the archive, giving it the archive name arcname (by default, this will be the
        same as filename, but without a drive letter and with leading path separators removed). If given, compress_type
        overrides the value given for the compression parameter to the constructor for the new entry. The archive must
        be open with mode ’w’ or ’a’ – calling write() on a ZipFile created with mode ’r’ will raise a RuntimeError.
        :param filename:
        :param arcname:
        :param compress_type:
        :return:
        """
        raise NotImplementedError

    def writestr(self, arcname, bytes_str, compress_type):
        """
        Method for writing bytes directly to the archive.
        :param arcname: The name of the file as it will appear in the archive
        :param bytes: The byte string to wrtie
        :param compress_type: Type of compression to use (only supported in some plugins, where the compression plugin_type
                              can be changed)
        :return:
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - HELPER METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def testarc(self):
        """
        Tests the archive to check that it's valid. Ideally reads all the files and checks them. Returns the name of the
        first bad file, or None.
        :return:
        """
        raise NotImplementedError

    def close(self):
        """
        Write anything in memory to file and close up. You must call this method when you've finished working with a
        file to ensure that everything is written and the file can safely be finalized.
        :return:
        """
        raise NotImplementedError

    def __enter__(self):
        """
        Allows use of this class as a context manager. Functions to ensure a call to close at the end of operations.
        :return:
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures that self.close() is called at the end of operations.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()


# ------------------------------------
