from __future__ import print_function

__author__ = "root"

import os
import imp
from copy import deepcopy

from LiuXin.utils.general_ops.io_ops import *

from LiuXin.constants import VERBOSE_DEBUG

# from LiuXin.customize import MDInputTransform

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
md_import_plugins = []


def load_md_import_plugins():
    """
    Load the MetaData transformation plugins - this will be applied to each MetaData object before it is applied to the
    database.
    Plugins are applied by their priority - this should be a number between 0 and 10 (0 being lowest, 10 being highest).
    In the case of priority conflict plugins will be ordered by their names, and then run (not a brilliant solution).
    :return:
    """
    # Any package found in LiuXin.metadata.liuxin_plugins is considered for import
    file_list = get_valid_packages()
    md_import_plugins = []

    # Scanning the files and trying to pick up MDInputTransform plugins.
    # For the moment importing everything, and separating the modules later
    for plugin in file_list:

        # Loading modules out of the given path - see which modules have been picked up - check them for the right type
        # of class and, if present, loads them.
        plugin_path = os.path.join(__folder__, plugin)
        cand_module = imp.load_source(plugin_path)

    print("After import - ")
    print("locals().values() " + repr(locals().values()))


def get_valid_packages():
    """
    Produces a list of files in this folder which might contain drivers.
    All Python files will be loaded and checked for valid plugins
    :return:
    """
    # Todo: Add more validation to check that these are actual python packages
    files_and_folders = os.listdir(__folder__)
    files = [item for item in files_and_folders if os.path.isfile(os.path.join(__folder__, item))]
    return files


# Tests to make sure that the modules load properly
if __name__ == "__main__":

    VERBOSE_DEBUG = True
    load_md_import_plugins()
