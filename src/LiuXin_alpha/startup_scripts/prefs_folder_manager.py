"""
Create legacy preference, cache, debug, scratch, and manifest-listed startup folders.

Paths are imported from the shared constants module. Creation helpers generally
expect existing parents and propagate filesystem errors; the manifest loader does
not validate or confine its path components. These routines are startup utilities,
not an access-permission check or a safe importer for untrusted manifests.
"""

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
    Ensure the preferences root and then its Calibre configuration directory exist.

    Print whether the root was found or created. A newly created Calibre directory
    also gets a ``caches`` child; an existing one is not checked for that child.
    Errors propagate, and folders already created are not rolled back.

    Example:
        >>> ensure_prefs_folder()  # doctest: +SKIP


    :return: ``None``; missing directories are created and a root-status message is printed.
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
    Check whether the configured LiuXin preferences path currently names a directory.

    This does not test write access or the presence of configuration files.

    Example:
        >>> exists = check_for_prefs_folder()  # doctest: +SKIP


    :return: Result of ``os.path.isdir`` for ``LiuXin_prefs_folder``.
    """
    folder_status = os.path.isdir(LiuXin_prefs_folder)
    return folder_status


# ensures that a preferences folder exists
def create_prefs_folder(mode=None):
    """
    Create the configured preferences root with one ``os.mkdir`` call.

    No existence check or parent-directory creation is performed. Explicit modes
    are passed to the operating system and remain subject to its permissions rules.

    Example:
        >>> create_prefs_folder(mode=0o700)  # doctest: +SKIP


    :param mode: Optional permission mode; ``None`` uses the default of ``os.mkdir``.
    :return: ``None`` after directory creation.
    :raises OSError: If the path already exists or directory creation otherwise fails.
    """
    if mode is None:
        os.mkdir(LiuXin_prefs_folder)
    elif mode is not None:
        os.mkdir(LiuXin_prefs_folder, mode)


def check_for_calibre_prefs():
    """
    Check whether the configured Calibre preferences path names a directory.

    Its contents and cache subdirectory are not inspected.

    Example:
        >>> exists = check_for_calibre_prefs()  # doctest: +SKIP


    :return: Result of ``os.path.isdir`` for ``LiuXin_calibre_prefs_folder``.
    """
    folder_status = os.path.isdir(LiuXin_calibre_prefs_folder)
    return folder_status


def create_calibre_prefs(CONFIG_DIR_MODE=0o700):
    """
    Recursively create the Calibre preferences directory, then its ``caches`` child.

    The supplied mode applies to the preferences-directory call; the cache call
    uses default permissions. Existing directories raise rather than being accepted.
    Failure of the second call leaves the first directory in place.

    Example:
        >>> create_calibre_prefs(CONFIG_DIR_MODE=0o700)  # doctest: +SKIP


    :param CONFIG_DIR_MODE: Permission mode passed when creating the preferences directory.
    :return: ``None`` after both directory-creation calls succeed.
    :raises OSError: If either directory already exists or cannot be created.
    """
    os.makedirs(LiuXin_calibre_prefs_folder, CONFIG_DIR_MODE)
    os.makedirs(os.path.join(LiuXin_calibre_prefs_folder, "caches"))


def ensure_debug_folder():
    """
    Create the configured debug directory when absent and report its status.

    The existing-directory branch calls a legacy ``LiuXin_print`` global which
    this module does not define; an embedding environment must supply that name.

    Example:
        >>> ensure_debug_folder()  # doctest: +SKIP


    :return: ``None`` after checking/creating the directory and reporting its status.
    :raises NameError: If the directory exists but the legacy print name is unavailable.
    """
    if not check_for_debug_folder():
        create_debug_folder()
        print("Debug folder not found. Created.")
    else:
        LiuXin_print("Debug folder found.")


def check_for_debug_folder():
    """
    Check whether the configured debug path names a directory, without testing access.

    Example:
        >>> exists = check_for_debug_folder()  # doctest: +SKIP


    :return: Result of ``os.path.isdir`` for ``LiuXin_debug_folder``.
    """
    folder_status = os.path.isdir(LiuXin_debug_folder)
    return folder_status


# ensures that a debug folder exists
def create_debug_folder(mode=None):
    """
    Create the configured debug directory without checking existence or creating parents.

    Example:
        >>> create_debug_folder(mode=0o700)  # doctest: +SKIP


    :param mode: Optional permissions passed to ``os.mkdir``; ``None`` uses its default.
    :return: ``None`` after successful creation.
    :raises OSError: If the path already exists or directory creation otherwise fails.
    """
    if mode is None:
        os.mkdir(LiuXin_debug_folder)
    elif mode is not None:
        os.mkdir(LiuXin_debug_folder, mode)


def ensure_scratch_folder():
    """
    Check the scratch directory, create it if absent, and print the selected status message.

    The creation helper accepts any existing path without validating its type;
    this routine does not recheck that a directory exists after that helper returns.

    Example:
        >>> ensure_scratch_folder()  # doctest: +SKIP


    :return: ``None``; a found/created message is printed unless an error interrupts the call.
    """
    if not check_for_scratch_folder():
        create_scratch_folder()
        print("Scratch folder not found. Created.")
    else:
        print("Scratch folder found.")


def check_for_scratch_folder():
    """
    Check whether the configured scratch path names a directory, without testing access.

    Example:
        >>> exists = check_for_scratch_folder()  # doctest: +SKIP


    :return: Result of ``os.path.isdir`` for ``LiuXin_scratch_folder``.
    """
    folder_status = os.path.isdir(LiuXin_scratch_folder)
    return folder_status


# ensures that a debug folder exists
def create_scratch_folder(mode=None):
    """
    Create the scratch path only when ``os.path.exists`` reports it absent.

    Existing files as well as directories are left untouched; their permissions
    and type are not verified. Missing parent directories are not created.

    Example:
        >>> create_scratch_folder(mode=0o700)  # doctest: +SKIP


    :param mode: Optional permissions for a new directory; ``None`` uses the default.
    :return: ``None`` after creating the directory or accepting an existing path.
    :raises OSError: If a required creation fails, including an existence-check race.
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
    Ensure the configured program directory exists and print whether it was found or created.

    Example:
        >>> ensure_program_folder()  # doctest: +SKIP


    :return: ``None`` after checking/creating the program directory and printing its status.
    """
    if not check_for_program_folder():
        create_program_folder()
        print("Program folder not found. Created.")
    else:
        print("Program folder found.")


def check_for_program_folder():
    """
    Check whether the configured program path names a directory, without testing access.

    Example:
        >>> exists = check_for_program_folder()  # doctest: +SKIP


    :return: Result of ``os.path.isdir`` for ``LiuXin_program_folder``.
    """
    folder_status = os.path.isdir(LiuXin_program_folder)
    return folder_status


# ensures that a debug folder exists
def create_program_folder(mode=None):
    """
    Create the configured program directory with a single ``os.mkdir`` call.

    Neither existing paths nor missing parents are handled specially.

    Example:
        >>> create_program_folder(mode=0o700)  # doctest: +SKIP


    :param mode: Optional permissions passed to ``os.mkdir``; ``None`` uses its default.
    :return: ``None`` after directory creation.
    :raises OSError: If the path already exists or directory creation otherwise fails.
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
    Parse the adjacent ``lx_folders.json`` manifest of startup-directory path components.

    Return the JSON value without validating its shape or components. The older
    ``LX_folders`` text-parsing code below the return is unreachable, not a fallback.

    Example:
        >>> ["LiuXin_data"] in load_ensured_folders()
        True


    :return: Decoded JSON value, expected by callers to be lists of path-component lists.
    :raises OSError: If the manifest cannot be opened or read.
    :raises json.JSONDecodeError: If the manifest is not valid JSON.
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
    Create missing paths listed in the JSON manifest, processing shallower entries first.

    Join each component list to the captured base folder, then call ``os.mkdir``
    only when that path does not exist. The manifest must include required parents;
    components are neither validated nor confined to the base folder. Existing
    paths, including files, count as present and keep their permissions.

    Filesystem and manifest errors propagate immediately without rollback. ``False``
    covers only paths still absent at the final per-entry existence check, not
    arbitrary creation errors. With an explicit mode, existing paths are printed.

    Example:
        >>> all_present = ensure_folders(mode=0o700)  # doctest: +SKIP


    :param mode: Optional permissions for newly created paths; ``None`` uses the default.
    :return: Whether every manifest path existed when its post-creation check ran.
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
