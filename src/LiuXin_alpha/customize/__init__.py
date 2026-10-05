# -*- coding: utf-8 -*-

"""
Expose the supported customize compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

from __future__ import with_statement, print_function, annotations

import os
import sys
import zipfile
import importlib
import pathlib
from copy import deepcopy

from typing import Union, Any, BinaryIO, NamedTuple, Iterable, Tuple, ClassVar, Literal, Optional

import LiuXin_alpha.databases.utils
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

from typing import Protocol, runtime_checkable, Optional, Tuple
from datetime import datetime


@runtime_checkable
class ZipInfoLike(Protocol):
    # --- core identity / metadata ---
    """
    Provide the zipinfolike contract for validated ebook processing.

    Example:
        Exercise ZipInfoLike through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    filename: str
    date_time: Tuple[int, int, int, int, int, int]  # (Y, M, D, h, m, s)
    compress_type: int
    comment: bytes
    extra: bytes

    # --- sizes / CRC ---
    file_size: int
    compress_size: int
    CRC: int

    # --- flags / versions ---
    flag_bits: int
    create_system: int
    create_version: int
    extract_version: int
    reserved: int

    # --- perms / attributes ---
    internal_attr: int
    external_attr: int

    # --- offsets ---
    header_offset: int

    # --- extra fields often present on ZipInfo ---
    volume: int

    # --- methods ZipInfo provides ---
    def is_dir(self) -> bool:
        """
        Return whether is dir holds for the supplied ebook data.

        Example:
            Exercise ZipInfoLike.is dir through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: True when the documented condition holds; otherwise False.
        """
        ...
    def FileHeader(self, zip64: Optional[bool] = None) -> bytes:
        """
        Perform the FileHeader operation under explicit file-format and conversion rules.

        Example:
            Exercise ZipInfoLike.FileHeader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param zip64: Value supplied for zip64 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    # Not always needed, but commonly used for display/logging.
    def __repr__(self) -> str:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise ZipInfoLike.  repr   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


class Base:
    """
    Provide the base contract for validated ebook processing.

    Example:
        Exercise Base through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    ...

T = TypeVar("T", bound=Base)


class CatalogCLIOption(NamedTuple):
    """
    Provide the catalogclioption contract for validated ebook processing.

    Example:
        Exercise CatalogCLIOption through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
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

    Example:
        Exercise PluginPreferences through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self):
        """
        Initialize and validate the pluginpreferences state.

        Example:
            Exercise PluginPreferences.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; validated state is stored on the receiving object.
        """
        self.defaults = dict()


class Plugin:  # {{{
    """
    Base class for a calibre plugin.

    Example:
        Exercise Plugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise Plugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param plugin_path: Value supplied for plugin path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.plugin_path = plugin_path
        self.site_customization = None
        self.prefs = PluginPreferences()

    def initialize(self) -> None:
        """
        Called once when calibre plugins are initialized. Plugins are re-initialized every time a new plugin is added.

        Example:
            Exercise Plugin.initialize through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def config_widget(self):
        """
        Implement this method and :meth:`save_settings` in your plugin to use a custom configuration dialog, rather then relying on the simple string based default customization.

        Example:
            Exercise Plugin.config widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def save_settings(self, config_widget):
        """
        Save the settings specified by the user with config_widget.

        Example:
            Exercise Plugin.save settings through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param config_widget: Value supplied for config widget under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def do_user_config(self, parent=None):
        """
        This method shows a configuration dialog for this plugin.

        Example:
            Exercise Plugin.do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def pyqt5_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a PyQt5 environment.

        Example:
            Exercise Plugin.pyqt5 do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def command_line_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a command line environment.

        Example:
            Exercise Plugin.command line do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # More interface environment methods will be added here as appropriate.

    def load_resources(self, names: list[str]) -> dict[str, bytes]:
        """
        If this plugin comes in a ZIP file (user added plugin), this method will allow you to load resources from the ZIP file.

        Example:
            Exercise Plugin.load resources through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param names: Value supplied for names under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise Plugin.customization help through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @staticmethod
    def temporary_file(suffix: str) -> PersistentTemporaryFile:
        """
        Return a file-like object that is a temporary file on the file system.

        Example:
            Exercise Plugin.temporary file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param suffix: Text appended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return PersistentTemporaryFile(suffix)

    def is_customizable(self) -> bool:
        """
        Can the plugin be customized?

        Example:
            Exercise Plugin.is customizable through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: True when the documented condition holds; otherwise False.
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

        Example:
            Exercise Plugin.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise Plugin.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ip, it = getattr(self, "sys_insertion_path", None), getattr(self, "_sys_insertion_tdir", None)
        if ip in sys.path:
            sys.path.remove(ip)
        if hasattr(it, "__exit__"):
            it.__exit__(*args)

    def cli_main(self, args):
        """
        This method is the main entry point for your plugins command line interface.

        Example:
            Exercise Plugin.cli main through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("The %s plugin has no command line interface" % self.name)


# }}}


class FileTypePlugin(Plugin):  # {{{
    """
    A plugin transforms a particular set of file types.

    Example:
        Exercise FileTypePlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise FileTypePlugin.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path_to_ebook: Value supplied for path to ebook under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Default implementation does nothing
        return path_to_ebook

    def postimport(self, book_id, book_format, db):
        """
        Called post import, i.e., after the book file has been added to the database.

        Example:
            Exercise FileTypePlugin.postimport through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param book_format: Value supplied for book format under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass  # Default implementation does nothing


# }}}

class _MetadataReaderPlugin:
    """
    We want both LiuXin and calibre metadata readers to have a common interface - but not a common class heirachy.

    Example:
        Exercise  MetadataReaderPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise  MetadataReaderPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(*args, **kwargs)

        self.quick = False

    # Todo: Pull the calibre metadata object out and gen an API for it
    def get_metadata(self, stream: BinaryIO, ftype: str):
        """
        Return metadata for the file represented by stream (a file like object that supports reading).

        Example:
            Exercise  MetadataReaderPlugin.get metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_metadata_inplace(self, file_path: Union[pathlib.Path, str], ftype: str):
        """
        Returns metadata for the file represented by the file path.

        Example:
            Exercise  MetadataReaderPlugin.get metadata inplace through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Tries to open the file as a stream and use the get_metadata method on it
        with open(file_path, "rb") as md_file_stream:
            return self.get_metadata(stream=md_file_stream, ftype=ftype)


class MetadataReaderPlugin(Plugin, _MetadataReaderPlugin):  # {{{
    """
    A plugin that implements reading metadata from a set of file types.

    Example:
        Exercise MetadataReaderPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self, plugin_path: Union[str, pathlib.Path, None], *args: Any, **kwargs: Any) -> None:
        """
        Initialize shared plugin state and metadata-reader flags.

        Example:
            Exercise MetadataReaderPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param plugin_path: Value supplied for plugin path under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(plugin_path, *args, **kwargs)
        self.quick = False


class MetadataWriterPlugin(Plugin):
    """
    A plugin that implements writing metadata to files in a certain set of file types.

    Example:
        Exercise MetadataWriterPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise MetadataWriterPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(*args, **kwargs)

        self.apply_null = False

    def set_metadata(self, stream: BinaryIO, mi, type: str) -> None:
        """
        Set metadata for the file represented by stream (a file like object that supports reading).

        Example:
            Exercise MetadataWriterPlugin.set metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def set_metadata_inplace(self, file_path: Union[str, pathlib.Path], mi, type: str) -> None:
        """
        Set metadata for the file pointed to by the file path.

        Example:
            Exercise MetadataWriterPlugin.set metadata inplace through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


# }}}


class CatalogPlugin(Plugin):  # {{{
    """
    A plugin that implements a catalog generator.

    Example:
        Exercise CatalogPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise CatalogPlugin. field sorter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if key.startswith("#"):
            return "~%s" % key[1:]
        else:
            return key

    def search_sort_db(self, db, opts):
        """
        Generate a catalog off a db search.

        Example:
            Exercise CatalogPlugin.search sort db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        db.search(opts.search_text)

        if opts.sort_by:
            # 2nd arg = ascending
            db.sort(opts.sort_by, True)
        return LiuXin_alpha.databases.utils.get_data_as_dict(ids=opts.ids)

    # Todo: Add field maps to the database so that it can emulate calibre
    def get_output_fields(self, db, opts) -> list[str]:
        """
        Returns a list of the requested fields.

        Example:
            Exercise CatalogPlugin.get output fields through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise CatalogPlugin.initialize through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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
        Run the plugin. Must be implemented in subclasses. It should generate the catalog in the format specified in file_types, returning the absolute path to the generated catalog file. If an error is encountered it should raise an Exception.

        Example:
            Exercise CatalogPlugin.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path_to_output: Value supplied for path to output under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :param ids: Value supplied for ids under the utility contract.
        :param notification: Value supplied for notification under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Default implementation does nothing
        raise NotImplementedError("CatalogPlugin.generate_catalog() default method, should be overridden in subclass")


# }}}


class InterfaceActionBase(Plugin):  # {{{
    """
    Slots into the GUI.

    Example:
        Exercise InterfaceActionBase through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    supported_platforms = ["windows", "osx", "linux"]

    author = "Kovid Goyal"

    plugin_type = _("User Interface Action")

    can_be_disabled = False

    actual_plugin = None

    def __init__(self, *args, **kwargs):
        """
        Initialize and validate the interfaceactionbase state.

        Example:
            Exercise InterfaceActionBase.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        Plugin.__init__(self, *args, **kwargs)
        self.actual_plugin_ = None

    def load_actual_plugin(self, gui):
        """
        This method must return the actual interface action plugin object.

        Example:
            Exercise InterfaceActionBase.load actual plugin through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    Example:
        Exercise PreferencesPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise PreferencesPlugin.create widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    Example:
        Exercise StoreBase through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise StoreBase.load actual plugin through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mod, cls = self.actual_plugin.split(":")
        self.actual_plugin_object = getattr(importlib.import_module(mod), cls)(gui, self.name)
        return self.actual_plugin_object

    def customization_help(self, gui: bool = False) -> None:
        """
        Help with customizing the store.

        Example:
            Exercise StoreBase.customization help through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.customization_help(gui)
        raise NotImplementedError()

    def config_widget(self) -> Any:
        """
        Provides a config widget to config the store plugin.

        Example:
            Exercise StoreBase.config widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.config_widget()
        raise NotImplementedError()

    def save_settings(self, config_widget: Any) -> None:
        """
        Save setting changes made with the config_widget.

        Example:
            Exercise StoreBase.save settings through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param config_widget: Value supplied for config widget under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if getattr(self, "actual_plugin_object", None) is not None:
            return self.actual_plugin_object.save_settings(config_widget)
        raise NotImplementedError()


# }}}


class ViewerPlugin(Plugin):  # {{{
    """
    These plugins are used to add functionality to the calibre viewer.

    Example:
        Exercise ViewerPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    plugin_type = _("Viewer")

    def load_fonts(self) -> None:
        """
        This method is called once at viewer startup. It should load any fonts it wants to make available. For example::

        Example:
            Exercise ViewerPlugin.load fonts through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def load_javascript(self, evaljs):
        """
        This method is called every time a new HTML document is loaded in the viewer. Use it to load javascript libraries into the viewer. For example::

        Example:
            Exercise ViewerPlugin.load javascript through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param evaljs: Value supplied for evaljs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def run_javascript(self, evaljs):
        """
        This method is called every time a document has finished loading. Use it in the same way as load_javascript().

        Example:
            Exercise ViewerPlugin.run javascript through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param evaljs: Value supplied for evaljs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def customize_ui(self, ui):
        """
        This method is called once when the viewer is created. Use it to make any customizations you want to the viewer's user interface. For example, you can modify the toolbars via ui.tool_bar and ui.tool_bar2.

        Example:
            Exercise ViewerPlugin.customize ui through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param ui: Value supplied for ui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def customize_context_menu(self, menu, event, hit_test_result):
        """
        This method is called every time the context (right-click) menu is shown. You can use it to customize the context menu. ``event`` is the context menu event and hit_test_result is the QWebHitTestResult for this event in the currently loaded document.

        Example:
            Exercise ViewerPlugin.customize context menu through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param menu: Value supplied for menu under the utility contract.
        :param event: Value supplied for event under the utility contract.
        :param hit_test_result: Value supplied for hit test result under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


# }}}


class LibraryClosedPlugin(Plugin):  # {{{
    """
    LibraryClosedPlugins are run when a library is closed, either at shutdown, when the library is changed, or when a library is used in some other way. At the moment these plugins won't be called by the CLI functions.

    Example:
        Exercise LibraryClosedPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    plugin_type = _("Library Closed")

    # minimum version 2.54 because that is when support was added
    minimum_calibre_version = (2, 54, 0)

    def run(self, db) -> None:
        """
        The db will be a reference to the new_api (db.cache.py).

        Example:
            Exercise LibraryClosedPlugin.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("LibraryClosedPlugin " "run method must be overridden in subclass")


# }}}


class EditBookToolPlugin(Plugin):  # {{{
    """
    Tool to edit a book.

    Example:
        Exercise EditBookToolPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

    Example:
        Exercise LiuXinPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    # Will start incrementing... soon
    minimum_liuxin_version = (1, 0, 0)


class MDInputTransform(LiuXinPlugin):  # {{{
    """
    Base class for the MetaData Input Transformation plugins.

    Example:
        Exercise MDInputTransform through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    # There are several different metadata containers floating around
    # As such, these transforms could support all - or none - of them
    target_classes = []

    def transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Takes a collection of MetaData objects. Uses them to preform a transform. Returns the transformed MetaData.

        Example:
            Exercise MDInputTransform.transform metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param first: Value supplied for first under the utility contract.
        :param rest: Value supplied for rest under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Optional runtime guard if you want it strict:
        if any(type(x) is not type(first) for x in rest):
            raise TypeError("All args must be the same concrete class")

        return self._true_transform_metadata(first, *rest)

    def _true_transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Mostly needed for typing.

        Example:
            Exercise MDInputTransform. true transform metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param first: Value supplied for first under the utility contract.
        :param rest: Value supplied for rest under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("You need to actually work out how to do this.")


class LXMetadataReaderPlugin(LiuXinPlugin, _MetadataReaderPlugin):
    """
    To distinguish the calibre metadata readers from the ones which have been re-written for LiuXin.

    Example:
        Exercise LXMetadataReaderPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise LXMetadataReaderPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(self, *args, **kwargs)
        self.quick = False

    @staticmethod
    def standardize_type(file_type: str) -> str:
        """
        Standardizes a plugin_type so that it can be compared against the known types that the plugin can be run for.

        Example:
            Exercise LXMetadataReaderPlugin.standardize type through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_type: Value supplied for file type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        file_type = deepcopy(file_type)
        if file_type.startswith("."):
            file_type = file_type[1:]
        return file_type.upper()

    def get_metadata(self, stream: Union[BinaryIO, str, pathlib.Path], file_type: str):
        """
        Return metadata for the file represented by stream or path.

        Example:
            Exercise LXMetadataReaderPlugin.get metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param file_type: Value supplied for file type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None


class Archive(LiuXinPlugin):
    """
    Provides a zipfile like read interface to a compressed file format.

    Example:
        Exercise Archive through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise Archive.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param compression_flags: Value supplied for compression flags under the utility
            contract.
        :param write_type: Value supplied for write type under the utility contract.
        :param password: Value supplied for password under the utility contract.
        :return: None; validated state is stored on the receiving object.
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
    def __str__(self) -> str:
        """
        Returns a string representation of the object

        Example:
            Exercise Archive.  str   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        ans = []

        def format(x: Any, y: Any) -> None:

            """
            Perform the format operation under explicit file-format and conversion rules.

            Example:
                Exercise Archive.  str  .format through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param x: Value supplied for x under the utility contract.
            :param y: Value supplied for y under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
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
            """
            Set format under the format's safety and compatibility rules.

            Example:
                Exercise Archive.  str  .set format through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param x: Value supplied for x under the utility contract.
            :param y: Value supplied for y under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
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

    def printdir(self) -> None:
        """
        Prints a contents of the archive to sys.stdout.

        Example:
            Exercise Archive.printdir through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GATHER BASIC INFORMATION ABOUT THE FILE
    # ------------------------------------------------------------------------------------------------------------------
    @classmethod
    def is_valid(cls, path: Union[str, pathlib.Path]) -> bool:
        """
        Takes a local path - determines if the file can be read by this class.

        Example:
            Exercise Archive.is valid through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - READ METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def getinfo(self, name: str) -> ZipInfoLike:
        """
        Return info on an element in the archive.

        Example:
            Exercise Archive.getinfo through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def infolist(self) -> list[ZipInfoLike]:
        """
        Returns a list containing an info object for every element.

        Example:
            Exercise Archive.infolist through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def namelist(self) -> list[str]:
        """
        Returns a list of all members of the archive by name.

        Example:
            Exercise Archive.namelist through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def files(self) -> Iterable[str]:
        """
        Returns all the files in the archive.

        Example:
            Exercise Archive.files through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def folders(self) -> Iterable[str]:
        """
        Returns all the folders in the archive.

        Example:
            Exercise Archive.folders through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def extract(self,
                path: Union[str, pathlib.Path],
                pwd: str,
                member: Union[str, ZipInfoLike]) -> pathlib.Path:
        """
        Extract a member of the archive.

        Example:
            Exercise Archive.extract through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param pwd: Value supplied for pwd under the utility contract.
        :param member: Value supplied for member under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def extractall(self, path, pwd, members):
        """
        Extract all members from the archive to the current working directory.

        Example:
            Exercise Archive.extractall through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param pwd: Value supplied for pwd under the utility contract.
        :param members: Value supplied for members under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_file(self, path, member, pwd):
        """
        Extract the file and write it out to a pre-prepared file path.

        Example:
            Exercise Archive.get file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param member: Value supplied for member under the utility contract.
        :param pwd: Value supplied for pwd under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - WRITE METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    def write(self, filename, arcname, compress_type):
        """
        Write the file named filename to the archive, giving it the archive name arcname (by default, this will be the same as filename, but without a drive letter and with leading path separators removed). If given, compress_type overrides the value given for the compression parameter to the constructor for the new entry. The archive must be open with mode ’w’ or ’a’ – calling write() on a ZipFile created with mode ’r’ will raise a RuntimeError.

        Example:
            Exercise Archive.write through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param filename: Filename used for type inference or archive output.
        :param arcname: Value supplied for arcname under the utility contract.
        :param compress_type: Value supplied for compress type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def writestr(self, arcname, bytes_str, compress_type):
        """
        Method for writing bytes directly to the archive.

        Example:
            Exercise Archive.writestr through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param arcname: Value supplied for arcname under the utility contract.
        :param bytes_str: Value supplied for bytes str under the utility contract.
        :param compress_type: Value supplied for compress type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------------------------------------------------------
    # - HELPER METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def testarc(self):
        """
        Tests the archive to check that it's valid. Ideally reads all the files and checks them. Returns the name of the first bad file, or None.

        Example:
            Exercise Archive.testarc through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def close(self):
        """
        Write anything in memory to file and close up. You must call this method when you've finished working with a file to ensure that everything is written and the file can safely be finalized.

        Example:
            Exercise Archive.close through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __enter__(self):
        """
        Allows use of this class as a context manager. Functions to ensure a call to close at the end of operations.

        Example:
            Exercise Archive.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures that self.close() is called at the end of operations.

        Example:
            Exercise Archive.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc_val: Value supplied for exc val under the utility contract.
        :param exc_tb: Value supplied for exc tb under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.close()


# ------------------------------------
