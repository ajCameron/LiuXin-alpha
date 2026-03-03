# Manager system for plugins to read metadata from files
# this has been written so it can just use calibre metadata extraction methods
# all the module actually needs is a get_metadata module
# many of the metadata methods use containers and other useful things from over in the file formats dictionary.

from __future__ import print_function

import imp
import os
from copy import deepcopy

from LiuXin_alpha.constants import VERBOSE_DEBUG
from LiuXin_alpha.utils.logging import LiuXin_debug_print
from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS

# used to locate the file on disk so that the plugins can be imported
__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
valid_plugins = []
valid_file_formats = set()


class InvalidMetadataExtractor(Exception):
    def __init__(self, err_str):
        self.err_str = err_str
        print(self.err_str)


def get_metadata(target_object, force_type=False):
    """
    Polls the metadata from file folder to find plugins to handle a specific file type.

    Gets a sorted list of those plugins. Works through them until one works.
    :param target_object: Either a pointer to a stream, or the location of an object on disk.
    :return return_metadata: Metadata extracted from the object, or None if None could be extracted
    """
    target_object = deepcopy(target_object)
    return_metadata = None

    # For the moment assuming the object
    if not os.path.exists(target_object):
        raise IOError("File not found.")

    if not force_type:
        dotted_ext = os.path.splitext(target_object)[1]
        if len(dotted_ext) > 0 and dotted_ext[0] == ".":
            ext = dotted_ext[1:]
        else:
            ext = dotted_ext
    else:
        ext = force_type

    ext = ext.lower()
    if ext in ("html", "htm", "xhtml", "xhtm", "xml"):
        ext = "html"
    elif ext in ("mobi", "prc", "azw"):
        ext = "mobi"
    elif ext in ("odt", "ods", "odp", "odg", "odf"):
        ext = "odt"

    possible_plugins = get_plugins_for_extension(ext)
    for plugin in possible_plugins:
        try:
            return_metadata = plugin.get_metadata(target_object)
        except:
            err_str = "Error while running a metadata extractor plugin.\n"
            err_str += "Plugin name: " + plugin.module_name + "\n"
            err_str += "Plugin path: " + plugin.file_path + "\n"
            raise NotImplementedError(err_str)
        if return_metadata is not None:
            break

    return return_metadata


def get_plugins_for_extension(ext):
    """
    Scans the available plugins. Finds ones which can handle a certain extension. Further sorts by the cost of running
    the plugin. Returns the ordered list.
    :param ext: The extension to be searched for
    :return ordered_plugins: An index of the available plugins sorted by the cost of running the plugin
    """
    ext = deepcopy(ext)
    ext = ext.upper()
    plugins = []
    if not valid_plugins:
        load_plugins()

    for plugin in valid_plugins:
        if ext in [test.upper() for test in plugin.VALID_FOR]:
            plugins.append(plugin)

    return sort_plugins_by_run_cost(plugins)


def sort_plugins_by_run_cost(plugins):
    """
    Takes an index of plugins.
    :param plugins:
    :return ordered_plugins:
    """
    # Cannot deepcopy - it doesn't play well with user defined classes. Shouldn't be needed here
    run_cost_dict = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}
    plugins_cost = []

    # Building an index of the numeric run costs
    for plugin in plugins:

        # Checks for a properly formed module
        try:
            plugin_cost = plugin.RUN_COST[0]
        except KeyError:
            err_str = "Plugin run cost not properly set.\n"
            err_str += "Plugin name: " + plugin.module_name + "\n"
            err_str += "Given RUN_COST: " + repr(plugin.RUN_COST) + "\n"
            raise AssertionError(err_str)
        if plugin_cost not in run_cost_dict:
            err_str = "Unrecognized run cost detected\n"
            err_str += "Plugin name: " + plugin.module_name + "\n"
            raise AssertionError(err_str)

        # Adds the numeric run cost to the run cost index
        plugins_cost.append(plugin_cost)

    assert len(plugins_cost) == len(plugins)

    # Zipping them together and sorting
    sortable_index = zip(plugins, plugins_cost)
    sortable_index.sort(key=lambda x: x[1])
    return [item[0] for item in sortable_index]


def load_plugins():
    """
    loads all valid metadata extractors and adds them to VALID_METADATA_EXTRACTORS
    :return None: Purely internal method
    """
    plugin_sources = os.listdir(__folder__)

    # Filter the plugin sources to remove the things that we really don't want to try loading
    plugin_sources = filter_plugin_sources(plugin_sources)
    plugin_paths = [os.path.join(__folder__, plugin_name) for plugin_name in plugin_sources]
    for path in plugin_paths:
        try:
            new_plugin = MetaDataReaderPlugin(path)
            valid_plugins.append(new_plugin)
            valid_file_formats.union(new_plugin.VALID_FOR)
        except InvalidMetadataExtractor as e:
            if VERBOSE_DEBUG:
                LiuXin_debug_print(e.err_str)
            else:
                pass


def filter_plugin_sources(plugin_sources_names):
    """
    Takes a list of file_names. Removes the ones we probably don't want being compiled as extensions.
    :param plugin_sources_names:
    :return plugin_sources_names: Except now filtered.
    """
    plugin_sources_names = deepcopy(plugin_sources_names)
    plugin_sources_names = [name for name in plugin_sources_names if name != "__init__.py"]
    plugin_sources_names = [name for name in plugin_sources_names if not name.endswith(".pyc")]
    return plugin_sources_names


# Todo: Add functionality to check for .pyc files and load from them instead of from source every time
class MetaDataReaderPlugin(object):
    """
    A class to represent a metadata reader object.
    """

    def __init__(self, file_path):
        """
        Takes the name of a file. Tries to load the pre-defined constants from it and then parses them,
        :param file_path:
        :return:
        """
        self.file_path = file_path
        self.module_name = os.path.basename(os.path.splitext(file_path)[0])
        try:
            self.module = imp.load_source(self.module_name, file_path)
        except IOError:
            err_str = "No such module as: " + self.module_name + "\n"
            err_str += "Located at: " + repr(file_path) + "\n"
            raise InvalidMetadataExtractor(err_str)
        try:
            self.VALID_FOR = self.module.VALID_FOR
            if self.VALID_FOR == ["ALL_EBOOKS"]:
                self.VALID_FOR = [EXT.upper() for EXT in BOOK_EXTENSIONS]
        except AttributeError:
            err_str = "Invalid Metadata Extractor Module detected: " + self.module_name + "\n"
            err_str += "At: " + repr(file_path) + "\n"
            err_str += "Module has no VALID_FOR constant"
            raise InvalidMetadataExtractor(err_str)
        try:
            self.PRIORITY_FOR = self.module.PRIORITY_FOR
        except AttributeError:
            err_str = "Invalid Metadata Extractor Module detected: " + self.module_name + "\n"
            err_str += "At: " + repr(file_path) + "\n"
            err_str += "Module has no PRIORITY_FOR constant"
            raise InvalidMetadataExtractor(err_str)
        try:
            self.RUN_COST = self.module.RUN_COST
        except AttributeError:
            err_str = "Invalid Metadata Extractor Module detected: " + self.module_name + "\n"
            err_str += "At: " + repr(file_path) + "\n"
            err_str += "Module has no RUN_COST constant"
            raise InvalidMetadataExtractor(err_str)
        try:
            self.get_metadata = self.module.get_metadata
        except AttributeError:
            err_str = "Invalid Metadata Extractor Module detected: " + self.module_name + "\n"
            err_str += "At: " + repr(file_path) + "\n"
            err_str += "Module has no get_metadata method"
            raise InvalidMetadataExtractor(err_str)
