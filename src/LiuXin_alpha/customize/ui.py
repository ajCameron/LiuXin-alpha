"""
User interface for customize - as a rule, you should try and import from here where possible.
"""
# Todo: Make sure this is actually so

from __future__ import with_statement, print_function

import os
import shutil
import traceback
import functools
import sys
import time
from collections import defaultdict

from typing import Union, Optional, Iterator, Iterable, Any, BinaryIO, Callable, Type

from LiuXin_alpha.constants import VERBOSE_DEBUG as DEBUG

from LiuXin_alpha.customize import (
    Archive,
    CatalogPlugin,
    FileTypePlugin,
    PluginNotFound,
    MetadataReaderPlugin,
    MetadataWriterPlugin,
    InterfaceActionBase as InterfaceAction,
    PreferencesPlugin,
    platform,
    InvalidPlugin,
    StoreBase as Store,
    ViewerPlugin,
    EditBookToolPlugin,
    MDInputTransform,
    LibraryClosedPlugin,
    Plugin,
)
from LiuXin_alpha.customize.builtins import plugins as builtin_plugins
from LiuXin_alpha.customize.builtins.standardization import CreatorStandardize
from LiuXin_alpha.customize.builtins.standardization import TitlePhashHandler
from LiuXin_alpha.customize.builtins.standardization import BaseNameGenerator
from LiuXin_alpha.customize.conversion import InputFormatPlugin, OutputFormatPlugin
from LiuXin_alpha.customize.profiles import InputProfile, OutputProfile
from LiuXin_alpha.customize.zipplugin import loader
from LiuXin_alpha.customize.archives import get_compressor_plugins

from LiuXin_alpha.databases.database import Database

try:
    from LiuXin_alpha.devices.interface import DevicePlugin
except ModuleNotFoundError:
    class DevicePlugin(object):
        pass

try:
    from LiuXin_alpha.metadata.metadata import MetaData as MetaInformation
except ModuleNotFoundError:
    from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
        CalibreLikeLiuXinBookMetaData as MetaInformation,
    )

try:
    from LiuXin_alpha.metadata.web_sources.base import Source
except ModuleNotFoundError:
    class Source(object):
        pass

from LiuXin_alpha.utils.config.config_base import make_config_dir, Config, ConfigProxy, plugin_dir
from LiuXin_alpha.utils.config.config_tools import OptionParser
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

try:
    from past.builtins import basestring
except ModuleNotFoundError:
    basestring = str

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

from LiuXin_alpha.databases.caches.utils import run_plugins_on_import

builtin_names = frozenset([p.name for p in builtin_plugins])


class NameConflict(ValueError):
    pass


def _config() -> ConfigProxy:
    """
    Return a ConfigProxy for the config - customized with the plugin information.

    :return:
    """
    c = Config("customize")
    c.add_opt("plugins", default={}, help=_("Installed plugins"))
    c.add_opt("filetype_mapping", default={}, help=_("Mapping for filetype plugins"))
    c.add_opt("plugin_customization", default={}, help=_("Local plugin customization"))
    c.add_opt("disabled_plugins", default=set([]), help=_("Disabled plugins"))
    c.add_opt("enabled_plugins", default=set([]), help=_("Enabled plugins"))

    return ConfigProxy(c)


config = _config()


def find_plugin(name: str) -> Optional[Plugin]:
    """
    Searches the initialized plugins for the plugin by name and returns it if it can be found.

    :param name:
    :return:
    """
    for plugin in _initialized_plugins:
        if plugin.name == name:
            return plugin


def load_plugin(path_to_zip_file: Union[str, os.PathLike]) -> Plugin:  # {{{
    """
    Load plugin from zip file or raise InvalidPlugin error

    :return: A :class:`Plugin` instance.
    """
    return loader.load(path_to_zip_file)


# }}}

# Enable/disable plugins {{{


def disable_plugin(plugin_or_name: Union[Plugin, str]) -> None:
    """
    Pass in the plugin, or it's name, disable it and note it in preferences.

    :param plugin_or_name: Either the plugin or it's name.
    :return:
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    plugin = find_plugin(x)
    if not plugin.can_be_disabled:
        raise ValueError("Plugin %s cannot be disabled" % x)
    dp = config["disabled_plugins"]
    dp.add(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    if x in ep:
        ep.remove(x)
    config["enabled_plugins"] = ep


def enable_plugin(plugin_or_name: Union[Plugin, str]) -> None:
    """
    Pass in the plugin - or it's name - enable it and note it in preferences.

    :param plugin_or_name:
    :return:
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    dp = config["disabled_plugins"]
    if x in dp:
        dp.remove(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    ep.add(x)
    config["enabled_plugins"] = ep


def restore_plugin_state_to_default(plugin_or_name: Union[Plugin, str]) -> None:
    """
    If the plugin defaults enabled - enable it and visa versa.

    :param plugin_or_name:
    :return:
    """
    x = getattr(plugin_or_name, "name", plugin_or_name)
    dp = config["disabled_plugins"]
    if x in dp:
        dp.remove(x)
    config["disabled_plugins"] = dp
    ep = config["enabled_plugins"]
    if x in ep:
        ep.remove(x)
    config["enabled_plugins"] = ep


# Plugins which should start off as disabled
default_disabled_plugins = frozenset(
    [
        "Overdrive",
        "Douban Books",
        "OZON.ru",
        "Edelweiss",
        "Google Images",
        "Big Book Search",
    ]
)


def is_disabled(plugin: Plugin) -> bool:
    """
    Is the given plugin disable?

    :param plugin: Must be a plugin object.
    :return:
    """
    if plugin.name in config["enabled_plugins"]:
        return False
    return plugin.name in config["disabled_plugins"] or plugin.name in default_disabled_plugins


# }}}

# File type plugins {{{

_on_import: dict[str, Plugin] = {}
_on_postimport: dict[str, Plugin] = {}
_on_preprocess: dict[str, Plugin] = {}
_on_postprocess: dict[str, Plugin] = {}


def reread_filetype_plugins() -> None:
    """
    Reload the filetype plugins.

    :return:
    """
    default_log.info("Starting reread_filetype_plugins")

    global _on_import
    global _on_postimport
    global _on_preprocess
    global _on_postprocess
    _on_import = {}
    _on_postimport = {}
    _on_preprocess = {}
    _on_postprocess = {}

    for plugin in _initialized_plugins:
        if isinstance(plugin, FileTypePlugin):
            for ft in plugin.file_types:

                if plugin.on_import:
                    if ft not in _on_import.keys():  # used to use "has_ket"
                        _on_import[ft] = []
                    _on_import[ft].append(plugin)

                if plugin.on_postimport:
                    if ft not in _on_postimport.keys():
                        _on_postimport[ft] = []
                    _on_postimport[ft].append(plugin)

                if plugin.on_preprocess:
                    if ft not in _on_preprocess.keys():
                        _on_preprocess[ft] = []
                    _on_preprocess[ft].append(plugin)

                if plugin.on_postprocess:
                    if ft not in _on_postprocess.keys():
                        _on_postprocess[ft] = []
                    _on_postprocess[ft].append(plugin)

    default_log.info("Finishing reread_filetype_plugins")


def _run_filetype_plugins(path_to_file: str, ft: str = None, occasion: str = "preprocess") -> str:
    """
    INTERNAL USE - runs filetype plugins on a given file.

    Intended for internal use (used the wrapped versions for actually doing stuf)
    :param path_to_file: Path to the file to run the plugins on.
    :param ft: filetype
    :param occasion:  Options are 'import', 'preprocess' and 'postprocess'
    :return:
    """
    occasion_plugins = {
        "import": _on_import,
        "preprocess": _on_preprocess,
        "postprocess": _on_postprocess,
    }[occasion]

    customization = config["plugin_customization"]

    if ft is None:
        ft = os.path.splitext(path_to_file)[-1].lower().replace(".", "")
    nfp = path_to_file
    for plugin in occasion_plugins.get(ft, []):

        if is_disabled(plugin):
            continue

        plugin.site_customization = customization.get(plugin.name, "")
        # Some file type plugins out there override the output streams with buggy implementations
        oo, oe = sys.stdout, sys.stderr

        with plugin:
            try:
                nfp = plugin.run(path_to_file)
                if not nfp:
                    nfp = path_to_file
            except:
                print(
                    "Running file type plugin %s failed with traceback:" % plugin.name,
                    file=oe,
                )
                traceback.print_exc(file=oe)

        sys.stdout, sys.stderr = oo, oe

    x = lambda j: os.path.normpath(os.path.normcase(j))

    if occasion == "postprocess" and x(nfp) != x(path_to_file):
        shutil.copyfile(nfp, path_to_file)
        nfp = path_to_file
    return nfp


# ------------------------------------------------------
# - Public interfaces for the plugins system
# ------------------------------------------------------

# Runs plugins before importing a file
run_plugins_on_import = functools.partial(_run_filetype_plugins, occasion="import")

# Runs plugins before converting? a file
run_plugins_on_preprocess = functools.partial(_run_filetype_plugins, occasion="preprocess")

# Runs plugins after converting? a file
run_plugins_on_postprocess = functools.partial(_run_filetype_plugins, occasion="postprocess")


def run_plugins_on_postimport(db: Database, book_id: int, fmt: str) -> None:
    """
    Runs all the postimport plugins available on the target book.

    This should transform the book in place in the database.
    :param db: The database the book is to be found in
               Needs to be compatible with the db object in plugin.postimport - which it will be passed through to.
    :param book_id:
    :param fmt: The format of the book.
    :return:
    """
    customization = config["plugin_customization"]
    fmt = fmt.lower()
    for plugin in _on_postimport.get(fmt, []):
        if is_disabled(plugin):
            continue
        plugin.site_customization = customization.get(plugin.name, "")
        with plugin:
            try:
                plugin.postimport(book_id, fmt, db)
            except:
                print("Running file type plugin %s failed with traceback:" % plugin.name)
                traceback.print_exc()


# Todo: Check these are actually the same in calibre - there might be a meaningful semantic difference
run_plugins_on_postadd = run_plugins_on_postimport

# }}}


# Plugin customization {{{
def customize_plugin(plugin: Plugin, custom: str) -> None:
    """
    Update config with customization for the given plugin.

    Customization is keyed off the name of the plugin and is valued with the new string.
    While you are limited to a string, it can be a json encoded one.
    :param plugin:
    :param custom:
    :return:
    """
    d = config["plugin_customization"]
    d[plugin.name] = custom.strip()
    config["plugin_customization"] = d


def plugin_customization(plugin: Plugin) -> str:
    """
    Return the customisation string for the given plugin.

    You're responsible for doing any de-pickling or parsing on the returned string.
    You'll just get a string back.
    :param plugin: Plugin to get the customisation string for.
    :return:
    """
    return config["plugin_customization"].get(plugin.name, "")


# }}}


# Input/Output profiles {{{
def input_profiles() -> Iterator[InputProfile]:
    """
    Yield all input profiles.

    :return:
    """

    for plugin in _initialized_plugins:
        if isinstance(plugin, InputProfile):
            yield plugin


def output_profiles() -> Iterator[OutputProfile]:
    """
    Yield all the output profiles.

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, OutputProfile):
            yield plugin


# }}}


# Interface Actions # {{{
def interface_actions() -> Iterator[InterfaceAction]:
    """
    Yields all interface actions.

    :return:
    """
    customization = config["plugin_customization"]
    for plugin in _initialized_plugins:
        if isinstance(plugin, InterfaceAction):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


# }}}

# Preferences Plugins # {{{


def preferences_plugins() -> Iterator[PreferencesPlugin]:
    """
    Yields all preferences plugins.

    :return:
    """
    customization = config["plugin_customization"]
    for plugin in _initialized_plugins:
        if isinstance(plugin, PreferencesPlugin):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


# }}}

# Store Plugins # {{{


def store_plugins() -> Iterator[Store]:
    """
    Yields all store plugins.

    :return:
    """
    customization = config["plugin_customization"]
    for plugin in _initialized_plugins:
        if isinstance(plugin, Store):
            plugin.site_customization = customization.get(plugin.name, "")
            yield plugin


def available_store_plugins() -> Iterator[Store]:
    """
    Yields all store plugins which have not been disabled.

    :return:
    """
    for plugin in store_plugins():
        if not is_disabled(plugin):
            yield plugin


def stores() -> set[Store]:
    """
    Returns a set of all store plugins - including currently disabled ones.

    :return:
    """
    stores = set([])
    for plugin in store_plugins():
        stores.add(plugin.name)
    return stores


def available_stores() -> set[Store]:
    """
    Returns a set of all the available store plugins.

    :return:
    """
    stores = set([])
    for plugin in available_store_plugins():
        stores.add(plugin.name)
    return stores


# }}}

# Metadata read/write {{{
_metadata_readers: dict[str, list[MetadataReaderPlugin, ...]] = {}
_metadata_writers: dict[str, list[MetadataWriterPlugin, ...]] = {}


def reread_metadata_plugins() -> None:
    """
    Read the metadata IO plugins.

    :return:
    """

    default_log.info("About to start reread_metadata_plugins")

    global _metadata_readers
    global _metadata_writers
    _metadata_readers = {}
    for plugin in _initialized_plugins:
        if isinstance(plugin, MetadataReaderPlugin):
            for ft in plugin.file_types:
                if ft not in _metadata_readers.keys():
                    _metadata_readers[ft] = []
                _metadata_readers[ft].append(plugin)
        elif isinstance(plugin, MetadataWriterPlugin):
            for ft in plugin.file_types:
                if ft not in _metadata_writers.keys():
                    _metadata_writers[ft] = []
                _metadata_writers[ft].append(plugin)

    default_log.info("Finishing reread_metadata_plugins")


def metadata_readers() -> set[MetadataReaderPlugin]:
    """
    Return a set of all the metadata reader plugins.

    :return:
    """
    ans = set([])
    for plugins in _metadata_readers.values():
        for plugin in plugins:
            ans.add(plugin)
    return ans


def metadata_writers() -> set[MetadataWriterPlugin]:
    """
    Return a set of the metadata writer plugins.

    :return:
    """
    ans = set([])
    for plugins in _metadata_writers.values():
        for plugin in plugins:
            ans.add(plugin)
    return ans


# Todo: Swallows exceptions
class QuickMetadata:
    """
    Context manager which turns the quick metadata option on and off.
    """

    def __init__(self) -> None:
        self.quick = False

    def __enter__(self) -> None:
        self.quick = True

    def __exit__(self, *args: Any) -> None:
        self.quick = False


quick_metadata = QuickMetadata()


# Todo: Swallows exceptions
class ApplyNullMetadata:
    """
    Context manager which turns the apply null metadata option on and off.
    """

    def __init__(self) -> None:
        self.apply_null = False

    def __enter__(self) -> None:
        self.apply_null = True

    def __exit__(self, *args) -> None:
        self.apply_null = False


apply_null_metadata = ApplyNullMetadata()


class ForceIdentifiers(object):
    """
    Context manager which turns the force_identifiers option on and off.
    """

    def __init__(self) -> None:
        self.force_identifiers = False

    def __enter__(self) -> None:
        self.force_identifiers = True

    def __exit__(self, *args: Any) -> None:
        self.force_identifiers = False


force_identifiers = ForceIdentifiers()


# Todo: Insert MetaData synthesis call here
def get_file_type_metadata(stream: Union[BinaryIO, str, os.PathLike], ftype: str, calibre: bool = False):
    """
    Get metadata from a stream of ftype.

    calibre assumes that there is only one metadata extractor for each file type - this method has the same signature as
    the calibre method and does the same thing - with the MetaData cleaner options run on the object(s) before they are
    returned.
    :param stream: A stream positioned at the beginning of the file
    :param ftype: The type of the file
    :param calibre: True if you want a calibre metadata object back. False if you want a LiuXin metadata object.
    :return:
    """
    # Ensure that the method returns a mi object - come what may.
    mi = MetaInformation(None, None)

    # If the method has been given a string instead of a stream checks to see if that file exists and then opens it as a
    # stream - if it's been fed a stream then proceed
    if isinstance(stream, basestring) or isinstance(stream, os.PathLike):
        if os.path.exists(stream):
            info_str = "get_file_type_metadata passed a file path in place of a stream.\n"
            info_str += "location exists - opening for reading.\n"
            info_str += "stream: " + six_unicode(stream)
            default_log.info(info_str)
            stream = open(stream, "r")
        else:
            info_str = "get_file_type_metadata passed a file path in place of a stream.\n"
            info_str += "location doesn't exist - aborting.\n"
            info_str += "stream: " + six_unicode(stream)
            default_log.info(info_str)
            return mi

    ftype = ftype.lower().strip()
    if ftype in _metadata_readers.keys():
        for plugin in _metadata_readers[ftype]:
            if not is_disabled(plugin):
                with plugin:
                    try:
                        plugin.quick = quick_metadata.quick
                        if hasattr(stream, "seek"):
                            stream.seek(0)
                        mi = plugin.get_metadata(stream, ftype.lower().strip())
                        break
                    except:
                        traceback.print_exc()
                        continue

    if calibre:
        return mi.to_calibre()
    return mi


# Todo: Only feed this method unambiguous metadata - re-institute the calibre_metadata class and feed it in here
def set_file_type_metadata(
    stream: BinaryIO, mi, ftype: str, report_error: Optional[Callable[[Any, str, str], None]] = None
) -> None:
    """
    Write MetaData into the file of the given type.

    :param stream: A stream positioned
    :param mi: The metadata to be written into the file
    :param ftype: The type of file to be written to
    :param report_error: A function which can be used to report on what went wrong with the plugin
    :return:
    """
    ftype = ftype.lower().strip()

    # There should only be one plugin to write metadata to each file type
    if ftype in _metadata_writers.keys():
        for plugin in _metadata_writers[ftype]:
            if not is_disabled(plugin):
                with plugin:
                    try:
                        plugin.apply_null = apply_null_metadata.apply_null
                        plugin.force_identifiers = force_identifiers.force_identifiers
                        plugin.set_metadata(stream, mi, ftype.lower().strip())
                        break
                    except:
                        if report_error is None:
                            from LiuXin_alpha import prints

                            prints(
                                "Failed to set metadata for the",
                                ftype.upper(),
                                "format of:",
                                getattr(mi, "title", ""),
                                file=sys.stderr,
                            )
                            traceback.print_exc()
                        else:
                            report_error(mi, ftype, traceback.format_exc())


def can_set_metadata(ftype: str) -> bool:
    """
    Can metadata be set for this particular file type?.

    :param ftype:
    :return True/False:
    """
    ftype = ftype.lower().strip()

    for plugin in _metadata_writers.get(ftype, ()):
        if not is_disabled(plugin):
            return True

    return False


# }}}

# Add/remove plugins {{{


def add_plugin(path_to_zip_file: Union[str, os.PathLike]) -> Plugin:
    """
    Add a plugin from a zip file.

    :param path_to_zip_file: Path to a zip file containing the plugin
    :return plugin: The initialized plugin
    """
    make_config_dir()

    plugin = load_plugin(path_to_zip_file)

    if plugin.name in builtin_names:
        raise NameConflict("A builtin plugin with the name %r already exists" % plugin.name)

    plugin = initialize_plugin(plugin, path_to_zip_file)
    plugins = config["plugins"]

    # Plugins will be stored in the plugin_dir
    zfp = os.path.join(plugin_dir, plugin.name + ".zip")
    if os.path.exists(zfp):
        os.remove(zfp)
    shutil.copyfile(path_to_zip_file, zfp)

    plugins[plugin.name] = zfp
    config["plugins"] = plugins

    initialize_plugins()

    return plugin


def remove_plugin(plugin_or_name: Union[Plugin, str]) -> bool:
    """
    Takes a plugin or the name of the plugin - removes it from the active plugins.

    :param plugin_or_name:
    :return status: Was the plugin removed?
    """
    name = getattr(plugin_or_name, "name", plugin_or_name)
    plugins = config["plugins"]
    removed = False
    if name in plugins:
        removed = True
        try:
            zfp = os.path.join(plugin_dir, name + ".zip")
            if os.path.exists(zfp):
                os.remove(zfp)
            zfp = plugins[name]
            if os.path.exists(zfp):
                os.remove(zfp)
        except:
            pass
        plugins.pop(name)
    config["plugins"] = plugins
    initialize_plugins()
    return removed


# }}}

# Input/Output format plugins {{{


def input_format_plugins() -> Iterator[InputFormatPlugin]:
    """
    Iterator for the input format plugins

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, InputFormatPlugin):
            yield plugin


def plugin_for_input_format(fmt: str) -> Optional[Plugin]:
    """
    Get the plugin associated with the given input format.

    :param fmt: The format of the return file
    :return:
    """
    customization = config["plugin_customization"]
    for plugin in input_format_plugins():
        if fmt.lower() in plugin.file_types:
            plugin.site_customization = customization.get(plugin.name, None)
            return plugin


def all_input_formats() -> set[str]:
    """
    Returns a set of all the formats' calibre can read - disabled or not.

    :return:
    """
    formats = set([])
    for plugin in input_format_plugins():
        for fmt in plugin.file_types:
            formats.add(fmt)
    return formats


def available_input_formats() -> set[str]:
    """
    Returns a set of the currently available input formats.

    :return:
    """
    formats = set([])
    for plugin in input_format_plugins():
        if not is_disabled(plugin):
            for fmt in plugin.file_types:
                formats.add(fmt)
    formats.add("zip"), formats.add("rar")
    return formats


def output_format_plugins() -> Iterator[OutputFormatPlugin]:
    """
    An iterator over all the output format plugins.
    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, OutputFormatPlugin):
            yield plugin


def plugin_for_output_format(fmt: str) -> Optional[Plugin]:
    """
    Returns a output format plugin for the requested format.

    :param fmt:
    :return:
    """
    customization = config["plugin_customization"]
    for plugin in output_format_plugins():
        if fmt.lower() == plugin.file_type:
            plugin.site_customization = customization.get(plugin.name, None)
            return plugin


def available_output_formats() -> set[str]:
    """
    Returns a set of all available output formats.

    :return:
    """
    formats = set([])
    for plugin in output_format_plugins():
        if not is_disabled(plugin):
            formats.add(plugin.file_type)
    return formats


# }}}

# Catalog plugins {{{


def catalog_plugins() -> Iterator[CatalogPlugin]:
    """
    Iterator which yields all the catalog plugins

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, CatalogPlugin):
            yield plugin


def available_catalog_formats() -> set[str]:
    """
    Which catalog formats are currently available?

    :return:
    """
    formats = set([])
    for plugin in catalog_plugins():
        if not is_disabled(plugin):
            for format in plugin.file_types:
                formats.add(format)
    return formats


def plugin_for_catalog_format(fmt: str) -> Optional[Plugin]:
    """
    Returns the catalog plugin for that format.

    :param fmt:
    :return:
    """
    for plugin in catalog_plugins():
        if fmt.lower() in plugin.file_types:
            return plugin


# }}}

# Device plugins {{{
def device_plugins(include_disabled: bool = False) -> Iterator[DevicePlugin]:
    """
    Returns an iterator over all the devices that the program can talk to, initializing them if required.

    :param include_disabled:
    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, DevicePlugin):
            if include_disabled or not is_disabled(plugin):
                if platform in plugin.supported_platforms:
                    if getattr(plugin, "plugin_needs_delayed_initialization", False):
                        plugin.do_delayed_plugin_initialization()
                    yield plugin


def disabled_device_plugins() -> Iterator[DevicePlugin]:
    """
    Device plugins which are currently disabled.

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, DevicePlugin):
            if is_disabled(plugin):
                if platform in plugin.supported_platforms:
                    yield plugin


# }}}

# Metadata sources2 {{{
def metadata_plugins(capabilities: Iterable[str]) -> Iterator[Plugin]:
    """
    Returns an iterator of all plugins which have a certain capability.

    :param capabilities: A set of the required capabilities
    :return:
    """
    capabilities = frozenset(capabilities)
    for plugin in all_metadata_plugins():
        if plugin.capabilities.intersection(capabilities) and not is_disabled(plugin):
            yield plugin


def all_metadata_plugins() -> Iterator[Source]:
    """
    Yields all metadata source plugins.

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, Source):
            yield plugin


# }}}

# Viewer plugins {{{
# Todo: Where are these? Should probably be moved to GUI
def all_viewer_plugins() -> Iterator[ViewerPlugin]:
    """
    Plugins for the document viewer to enable it to read different file formats
    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, ViewerPlugin):
            yield plugin


# }}}

# Editor plugins {{{
# Todo: Likewise, where are these? Should probably be moved to GUI
def all_edit_book_tool_plugins() -> Iterator[EditBookToolPlugin]:
    """
    Tools to enable editing books from the viewer.
    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, EditBookToolPlugin):
            yield plugin


# }}}


# Library Closed Plugins # {{{
def available_library_closed_plugins() -> Iterator[LibraryClosedPlugin]:
    """
    Get the currently enabled LibraryClosedPlugins.

    :return:
    """
    customization = config["plugin_customization"]
    for plugin in _initialized_plugins:
        if isinstance(plugin, LibraryClosedPlugin):
            if not is_disabled(plugin):
                plugin.site_customization = customization.get(plugin.name, "")
                yield plugin


def has_library_closed_plugins() -> bool:
    """
    Check to see if there are any enabled LibraryClosedPlugin in the system.

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, LibraryClosedPlugin):
            if not is_disabled(plugin):
                return True
    return False


# }}}


# MD transform plugins {{{
def all_metadata_synthesis_plugins() -> Iterator[MDInputTransform]:
    """
    All plugins which are used to transform MetaData.

    :return:
    """
    for plugin in _initialized_plugins:
        if isinstance(plugin, MDInputTransform):
            yield plugin


# }}}


# Initialize plugins {{{
_initialized_plugins = []


# Todo: This seems to be a genuine bug
# Todo: Call it a plugin_class instead of plugin or something?
def initialize_plugin(plugin: Type[Plugin], path_to_zip_file: Union[str, os.PathLike]) -> Optional[Plugin]:
    """
    Initialize a given plugin.

    :param plugin: The plugin to be initialized
    :param path_to_zip_file: Path to the zip file containing the code for the plugin
    :return:
    """
    try:
        p = plugin(path_to_zip_file)
        p.initialize()
        return p
    except Exception:
        print("Failed to initialize plugin:", plugin.name, plugin.version)
        tb = traceback.format_exc()
        raise InvalidPlugin((_("Initialization of plugin %s failed with traceback:") % tb) + "\n" + tb)


def has_external_plugins() -> bool:
    """
    True if there are updatable (zip file based) plugins.

    :return status: True/False
    """
    return bool(config["plugins"])


def initialize_plugins(perf: bool = False) -> None:
    """
    Initialize all plugins.

    :param perf: Print a report on performance..
    :return:
    """
    default_log.info("Starting initialize_plugins")

    global _initialized_plugins
    _initialized_plugins = []
    conflicts = [name for name in config["plugins"] if name in builtin_names]
    for p in conflicts:
        remove_plugin(p)
    external_plugins = config["plugins"]
    ostdout, ostderr = sys.stdout, sys.stderr

    if perf:
        times = defaultdict(lambda: 0)

    for zfp in list(external_plugins) + builtin_plugins:

        default_log.info("About to initialize plugin: {}".format(repr(zfp)))

        try:
            if not isinstance(zfp, type):
                # We have a plugin name
                pname = zfp
                zfp = os.path.join(plugin_dir, zfp + ".zip")
                if not os.path.exists(zfp):
                    zfp = external_plugins[pname]
            try:
                plugin = load_plugin(zfp) if not isinstance(zfp, type) else zfp
            except PluginNotFound:
                continue
            if perf:
                st = time.time()
            plugin = initialize_plugin(plugin, None if isinstance(zfp, type) else zfp)
            if perf:
                times[plugin.name] = time.time() - st
            _initialized_plugins.append(plugin)
        except:
            print("Failed to initialize plugin: {}".format(repr(zfp)))
            if DEBUG:
                traceback.print_exc()

        default_log.info("Plugin has been initialized: {}".format(repr(zfp)))

    default_log.info("Initial initialization finished")

    # Prevent a custom plugin from overriding stdout/stderr as this breaks ipython
    sys.stdout, sys.stderr = ostdout, ostderr
    if perf:
        for x in sorted(times, key=lambda x: times[x]):
            print("%50s: %.3f" % (x, times[x]))
    # _initialized_plugins.sort(
    #     cmp=lambda x, y: cmp(x.priority, y.priority), reverse=True
    # )
    sorted(_initialized_plugins, key=lambda x: x.priority, reverse=True)

    reread_filetype_plugins()
    reread_metadata_plugins()


initialize_plugins()


def initialized_plugins() -> Iterator[Plugin]:
    for plugin in _initialized_plugins:
        yield plugin


# }}}

# ----------------------------------------------------------------------------------------------------------------------
#
# - ADDITIONS FOR LIUXIN ONLY PLUGINS START HERE


def extract_metadata(file_path: Union[str, os.PathLike]):
    """
    Takes a file path or a stream - extract metadata from it using all available plugins.

    Returns one metadata item from every plugin which can read from the source.
    :param file_path:
    :return:
    """
    pass


def synthesize_metadata(metadata):
    """
    Takes a collection of metadata objects (which can be a single object). Feeds them through the plugins and returns.
    :param metadatas:
    :return:
    """
    pass


def creator_standardization_plugin() -> Type[CreatorStandardize]:
    """
    Returns the creator standardization plugin for the system.
    :return:
    """
    return CreatorStandardize


def title_phash_handler() -> Type[TitlePhashHandler]:
    """
    Prepares and returns the title_phash_handler plugin for the system,
    :return:
    """
    # Overwrite the default creator_standardize method with the actual one
    cs_plugin = creator_standardization_plugin()
    TitlePhashHandler.creator_standardize = cs_plugin.standardize_creator

    return TitlePhashHandler


def name_generator() -> Type[BaseNameGenerator]:
    """
    Returns the name generator plugin - which is used to create file and folder names.
    :return:
    """
    return BaseNameGenerator


def get_compressor_plugin(read: bool = True, arc_type: str = "zip") -> Type[Archive]:
    """
    Returns a compressor plugin suitable for handling an archive of a particular type.
    :param read: Is the plugin intended to read the archive or write to it?
    :param arc_type: The type of archive to return
    :return:
    """
    compressor_plugins = get_compressor_plugins()
    if read:
        for plugin in compressor_plugins:
            if arc_type in plugin.read_formats:
                return plugin
    else:
        for plugin in compressor_plugins:
            if arc_type in plugin.write_formats:
                return plugin

    err_str = "Cannot find a plugin to handle the archive of the required type in the required way\n"
    err_str += "read: {}\n".format(read)
    err_str += "arc_type: {}\n".format(arc_type)
    raise NotImplementedError(err_str)


#
# ----------------------------------------------------------------------------------------------------------------------

# CLI {{{


def build_plugin(path: str) -> None:
    """
    Adds a plugin to LiuXin after preforming some basic checks.

    :param path:
    :return:
    """
    from LiuXin_alpha import prints
    from LiuXin_alpha.utils.calibre.ptempfile import PersistentTemporaryFile
    from LiuXin_alpha.utils.calibre_utils.calibre_zipfile import ZipFile, ZIP_STORED

    path = type("")(path)
    names = frozenset(os.listdir(path))
    if "__init__.py" not in names:
        prints(path, " is not a valid plugin")
        raise SystemExit(1)
    t = PersistentTemporaryFile(".zip")
    with ZipFile(t, "w", ZIP_STORED) as zf:
        zf.add_dir(path, simple_filter=lambda x: x in {".git", ".bzr", ".svn", ".hg"})
    t.close()
    plugin = add_plugin(t.name)
    os.remove(t.name)
    prints("Plugin updated:", plugin.name, plugin.version)


def option_parser() -> OptionParser:
    """
    Provides a command line interface to this module.

    :return:
    """
    parser = OptionParser(
        usage=_(
            """\
    %prog options

    Customize calibre by loading external plugins.
    """
        )
    )
    parser.add_option(
        "-a",
        "--add-plugin",
        default=None,
        help=_("Add a plugin by specifying the path to the zip file containing it."),
    )
    parser.add_option(
        "-b",
        "--build-plugin",
        default=None,
        help=_(
            "For plugin developers: Path to the directory where you are"
            " developing the plugin. This command will automatically zip "
            "up the plugin and update it in calibre."
        ),
    )
    parser.add_option(
        "-r",
        "--remove-plugin",
        default=None,
        help=_("Remove a custom plugin by name. Has no effect on builtin plugins"),
    )
    parser.add_option(
        "--customize-plugin",
        default=None,
        help=_("Customize plugin. Specify name of plugin and customization string separated by a comma."),
    )
    parser.add_option(
        "-l",
        "--list-plugins",
        default=False,
        action="store_true",
        help=_("List all installed plugins"),
    )
    parser.add_option("--enable-plugin", default=None, help=_("Enable the named plugin"))
    parser.add_option("--disable-plugin", default=None, help=_("Disable the named plugin"))
    return parser


def main(args=sys.argv) -> int:
    """
    Various options to check the loaded plugins or build ones.

    :param args:
    :return:
    """
    parser = option_parser()
    if len(args) < 2:
        parser.print_help()
        return 1
    opts, args = parser.parse_args(args)
    if opts.add_plugin is not None:
        plugin = add_plugin(opts.add_plugin)
        print("Plugin added:", plugin.name, plugin.version)
    if opts.build_plugin is not None:
        build_plugin(opts.build_plugin)
    if opts.remove_plugin is not None:
        if remove_plugin(opts.remove_plugin):
            print("Plugin removed")
        else:
            print("No custom plugin named", opts.remove_plugin)
    if opts.customize_plugin is not None:
        name, custom = opts.customize_plugin.split(",")
        plugin = find_plugin(name.strip())
        if plugin is None:
            print("No plugin with the name %s exists" % name)
            return 1
        customize_plugin(plugin, custom)
    if opts.enable_plugin is not None:
        enable_plugin(opts.enable_plugin.strip())
    if opts.disable_plugin is not None:
        disable_plugin(opts.disable_plugin.strip())
    if opts.list_plugins:
        fmt = "%-15s%-20s%-15s%-15s%s"
        print(fmt % tuple(("Type|Name|Version|Disabled|Site Customization".split("|"))))
        print()
        for plugin in initialized_plugins():
            print(
                fmt
                % (
                    plugin.plugin_type,
                    plugin.name,
                    plugin.version,
                    is_disabled(plugin),
                    plugin_customization(plugin),
                )
            )
            print("\t", plugin.description)
            if plugin.is_customizable():
                try:
                    print("\t", plugin.customization_help())
                except NotImplementedError:
                    pass
            print()

    return 0


def run_import_plugins(path_or_stream, fmt):
    """
    Run all import plugins on a stream

    :param path_or_stream:
    :param fmt:
    :return:
    """
    from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

    fmt = fmt.lower()
    if hasattr(path_or_stream, "seek"):
        path_or_stream.seek(0)
        pt = PersistentTemporaryFile("_import_plugin." + fmt)
        shutil.copyfileobj(path_or_stream, pt, 1024**2)
        pt.close()
        path = pt.name
    else:
        path = path_or_stream
    return run_plugins_on_import(path)



if __name__ == "__main__":
    sys.exit(main())
# }}}
