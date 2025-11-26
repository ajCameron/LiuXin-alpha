
"""
Paths to various resources.
"""


from __future__ import print_function

import os

from copy import deepcopy


def rebuild_file_path(split_file_path):
    """
    Used when a path has been split down into a tuple or an index.
    Calls os.path.join repeatably with every element of the iterable.
    :param split_file_path:
    :return:
    """

    split_file_path = deepcopy(split_file_path)

    if len(split_file_path) == 0:
        return False

    current_path = split_file_path[0] + os.sep

    for part in split_file_path[1:]:
        current_path = os.path.join(current_path, part)

    return current_path


# Calculating the path to the folder LiuXin is executing in
LiuXin_path = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
LiuXin_path_split = LiuXin_path.split(os.sep)
LiuXin_base_folder_split = LiuXin_path_split[:-2]
LiuXin_base_folder = rebuild_file_path(LiuXin_base_folder_split)

# paths to the other random folder that LiuXin likes for operational purposes
LiuXin_prefs_folder = os.path.join(LiuXin_base_folder, "LiuXin_prefs")
LiuXin_calibre_prefs_folder = os.path.join(LiuXin_prefs_folder, "calibre_prefs")
LiuXin_calibre_caches = os.path.join(LiuXin_calibre_prefs_folder, "caches")
LiuXin_calibre_config_folder = os.path.join(LiuXin_prefs_folder, "calibre_config")
config_dir = LiuXin_calibre_config_folder
CONFIG_DIR_MODE = 0o0700

LiuXin_debug_folder = os.path.join(LiuXin_base_folder, "LiuXin_debug")

LiuXin_scratch_folder = os.path.join(LiuXin_base_folder, "LiuXin_scratch")

# paths to locations within the _data folder are here
LiuXin_data_folder = os.path.join(LiuXin_base_folder, "LiuXin_data")
LiuXin_calibre_resources_folder = os.path.join(LiuXin_data_folder, "calibre_resources")

# path to the calibre_resources folder
LiuXin_calibre_resources = os.path.join(LiuXin_data_folder, "calibre_resources")

LiuXin_database_folder = os.path.join(LiuXin_data_folder, "databases")
LiuXin_default_database = os.path.join(LiuXin_database_folder, "LX_default_database.db")
LiuXin_local_covers_path = os.path.join(LiuXin_data_folder, "covers")
LiuXin_data_sources = os.path.join(LiuXin_data_folder, "data_sources")

LiuXin_program_folder = os.path.join(LiuXin_base_folder, "LiuXin_programs")

# A common folder at the root of LiuXin folder is designated the import_cache. Here files can be stored while they're
# being processed before being written out to the folder stores
LiuXin_import_cache = os.path.join(LiuXin_base_folder, "LiuXin_ic")
LiuXin_ic_new_books = os.path.join(LiuXin_import_cache, "new_books")
LiuXin_ic_compressed_files = os.path.join(LiuXin_import_cache, "compressed_files")


LiuXin_path = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
LiuXin_path_split = LiuXin_path.split(os.sep)
LiuXin_base_folder_split = LiuXin_path_split[:-1]
LiuXin_base_folder = rebuild_file_path(LiuXin_base_folder_split)

LiuXin_prefs_folder = os.path.join(LiuXin_base_folder, "LiuXin_prefs")
LiuXin_calibre_prefs_folder = os.path.join(LiuXin_prefs_folder, "calibre_prefs")
LiuXin_calibre_caches = os.path.join(LiuXin_calibre_prefs_folder, "caches")
LiuXin_calibre_config_folder = os.path.join(LiuXin_prefs_folder, "calibre_config")

config_dir = LiuXin_calibre_config_folder
CONFIG_DIR_MODE = 0o700

LiuXin_debug_folder = os.path.join(LiuXin_base_folder, "LiuXin_debug")
LiuXin_scratch_folder = os.path.join(LiuXin_base_folder, "LiuXin_scratch")
LiuXin_data_folder = os.path.join(LiuXin_base_folder, "LiuXin_data")
LiuXin_calibre_resources = os.path.join(LiuXin_data_folder, "calibre_resources")
LiuXin_calibre_resources_folder = os.path.join(LiuXin_data_folder, "calibre_resources")
LiuXin_database_folder = os.path.join(LiuXin_data_folder, "databases")
LiuXin_default_database = os.path.join(LiuXin_database_folder, "LX_default_database.db")
LiuXin_local_covers_path = os.path.join(LiuXin_data_folder, "covers")
LiuXin_data_sources = os.path.join(LiuXin_data_folder, "data_sources")
LiuXin_program_folder = os.path.join(LiuXin_base_folder, "LiuXin_programs")
LiuXin_import_cache = os.path.join(LiuXin_base_folder, "LiuXin_ic")
LiuXin_ic_new_books = os.path.join(LiuXin_import_cache, "new_books")
LiuXin_ic_compressed_files = os.path.join(LiuXin_import_cache, "compressed_files")


# calculating the current path to the LiuXin files and the path to the other random folders

# Calculating the path to the folder LiuXin is executing in

# paths to the other random folder that LiuXin likes for operational purposes
if not os.path.exists(LiuXin_prefs_folder):
    os.mkdir(LiuXin_prefs_folder)
if not os.path.exists(LiuXin_calibre_config_folder):
    os.mkdir(LiuXin_calibre_config_folder)

# paths to locations within the _data folder are here

# path to the calibre_resources folder


# Todo: THESE ARE ALL ACTUALLY PREFERENCES - NOT CONSTANTS
