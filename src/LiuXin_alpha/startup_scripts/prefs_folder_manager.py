from __future__ import unicode_literals, print_function

__author__ = "Cameron"

import json
import os
from copy import deepcopy

# TODO: Check for the existence of prefs - add them if they are needed
# checks that a LiuXin_prefs folder exists and is reachable. If it isn't creates it.
from LiuXin_alpha.constants.paths import (
    LiuXin_base_folder,
    LiuXin_prefs_folder,
    LiuXin_calibre_prefs_folder,
    LiuXin_debug_folder,
    LiuXin_scratch_folder,
    LiuXin_program_folder,
)

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
__base_folder__ = deepcopy(LiuXin_base_folder)


def ensure_prefs_folder():
    """
    Ensures that a preferences folder exists. Creates it and issues a warning if it doesn't.
    """
    if not check_for_prefs_folder():
        create_prefs_folder()
        print("No preference folder detected. Re-creating from defaults.")
    else:
        print("Preferences folder found.")
    if not check_for_calibre_prefs():
        create_calibre_prefs()


# checks to see if the preference folder exists
def check_for_prefs_folder():
    """
    Returns true if the preferences folder exists in the right place, and false if it doesn't
    """
    folder_status = os.path.isdir(LiuXin_prefs_folder)
    return folder_status


# ensures that a preferences folder exists
def create_prefs_folder(mode=None):
    """
    Creates a preferences folder if it doesn't exist.
    :param mode:
    :return:
    """
    if mode is None:
        os.mkdir(LiuXin_prefs_folder)
    elif mode is not None:
        os.mkdir(LiuXin_prefs_folder, mode)


def check_for_calibre_prefs():
    """
    Checks to see if the preference scratch folder used by the embedded, butchered, install of calibre exists.
    """
    folder_status = os.path.isdir(LiuXin_calibre_prefs_folder)
    return folder_status


def create_calibre_prefs(CONFIG_DIR_MODE=0o700):
    """
    Creates a calibre prefs folder.
    """
    os.makedirs(LiuXin_calibre_prefs_folder, CONFIG_DIR_MODE)
    os.makedirs(os.path.join(LiuXin_calibre_prefs_folder, "caches"))


def ensure_debug_folder():
    """
    Ensures that the LiuXin_debug folder exists. Creating it if necessary.
    """
    if not check_for_debug_folder():
        create_debug_folder()
        print("Debug folder not found. Created.")
    else:
        LiuXin_print("Debug folder found.")


def check_for_debug_folder():
    """
    Checks to see if the debug folder exists.
    """
    folder_status = os.path.isdir(LiuXin_debug_folder)
    return folder_status


# ensures that a debug folder exists
def create_debug_folder(mode=None):
    """
    Creates a preferences folder if it doesn't exist.
    """
    if mode is None:
        os.mkdir(LiuXin_debug_folder)
    elif mode is not None:
        os.mkdir(LiuXin_debug_folder, mode)


def ensure_scratch_folder():
    """
    Ensures that the LiuXin_debug folder exists. Creating it if necessary.
    """
    if not check_for_scratch_folder():
        create_scratch_folder()
        print("Scratch folder not found. Created.")
    else:
        print("Scratch folder found.")


def check_for_scratch_folder():
    """
    Checks to see if the debug folder exists.
    """
    folder_status = os.path.isdir(LiuXin_scratch_folder)
    return folder_status


# ensures that a debug folder exists
def create_scratch_folder(mode=None):
    """
    Creates a preferences folder if it doesn't exist.
    """
    if mode is None:
        if not os.path.exists(LiuXin_scratch_folder):
            os.mkdir(LiuXin_scratch_folder)
    # Todo: Need to check the mode matches and recreate the folder with the right mode if it doesn't
    elif mode is not None:
        if not os.path.exists(LiuXin_scratch_folder):
            os.mkdir(LiuXin_scratch_folder, mode)


def ensure_program_folder():
    """
    Ensures that the LiuXin_debug folder exists. Creating it if necessary.
    """
    if not check_for_program_folder():
        create_program_folder()
        print("Program folder not found. Created.")
    else:
        print("Program folder found.")


def check_for_program_folder():
    """
    Checks to see if the debug folder exists.
    """
    folder_status = os.path.isdir(LiuXin_program_folder)
    return folder_status


# ensures that a debug folder exists
def create_program_folder(mode=None):
    """
    Creates a preferences folder if it doesn't exist.
    """
    if mode is None:
        os.mkdir(LiuXin_program_folder)
    elif mode is not None:
        os.mkdir(LiuXin_program_folder, mode)


# ----------------------------------------------------------------------------------------------------------------------
#
# - METHOD TO GENERALLY ENSURE REQUESTED FOLDERS ARE LOADED START HERE


def load_ensured_folders():
    """
    Loads the LX_folders.txt file. Then parses it to pull out the indexes specifying the folders that should be created.
    :return folders_index: An index of indexes.
    """

    cand_json_path = os.path.join(__folder__, "lx_folders.json")
    with open(cand_json_path, "r") as input_json_file:
        return json.load(input_json_file)

    # Read the file in as a list of string
    target_fp = os.path.join(__folder__, "LX_folders")
    with open(target_fp, "r") as target_file:
        target_file_lines = target_file.readlines()

    # Processing the strings into lists - return the list of lists of strings
    from LiuXin_alpha.utils.general_ops.io_ops import safe_parse_string_list

    rtn_list = []
    for line in target_file_lines:
        rtn_list.append(safe_parse_string_list(line))

    return rtn_list


# Todo: Add checking that the folders are r/w accessible
# Todo: Tidy this module
def ensure_folders(mode=None):
    """
    Makes sure that all the folders requested in the LX_folders file exist.
    :param mode: What mode should the file be cre8ated with? Default None
    :return True/False: True if all files where created, False if they weren't for some reason
    """
    err_count = 0

    # Loading the requested folder lists
    req_folders = load_ensured_folders()

    # Sorting the requested folder lists by length (should ensure that the lower level folders are created first)
    req_folders = sorted(req_folders, key=len)
    for folder in req_folders:

        # Trying to create the requested folder
        rb_folder_path = deepcopy(__base_folder__)
        for folder_name in folder:
            rb_folder_path = os.path.join(rb_folder_path, folder_name)
        if mode is None:
            if not os.path.exists(rb_folder_path):
                os.mkdir(rb_folder_path)
            else:
                pass
        elif mode is not None:
            if not os.path.exists(rb_folder_path):
                os.mkdir(rb_folder_path, mode)
            else:
                # Todo: Add mode checking and changing.
                print(rb_folder_path, " already exists")
        # Checking to see if the requested folders now exist
        if not os.path.exists(rb_folder_path):
            err_count += 1

    if err_count == 0:
        return True
    else:
        return False


#
# ----------------------------------------------------------------------------------------------------------------------
