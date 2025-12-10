__author__ = "Cameron"
# Loads LiuXin's databases of names

import os
import csv
from copy import deepcopy

from LiuXin_alpha.constants.paths import LiuXin_data_folder
from LiuXin_alpha.utils.logging import default_log

# Calculating some of the file locations at runtime
LIUXIN_NAMES_LIST_FOLDERS = os.path.join(LiuXin_data_folder, "names_lists")
FIRST_NAMES_PATH = os.path.join(LIUXIN_NAMES_LIST_FOLDERS, "First_Names.csv")
LAST_NAMES_PATH = os.path.join(LIUXIN_NAMES_LIST_FOLDERS, "Last_Names.csv")

# Todo: Consider using https://github.com/philipperemy/name-dataset/tree/master

FIRST_NAMES: set[str] = {"tim", "alan", "ethyl"}
LAST_NAMES: set[str] = {"mariner", "cameron", "reynaulds"}


# Todo: Replace lower with icu_lower
def load_names(lower_case: bool = True) -> tuple[set[str], set[str]]:
    """
    Loads the first name and last name CSV files into memory.

    :return first_name_set, last_name_set: Sets of all the first and last names present in the csv files.
    """
    global FIRST_NAMES
    global LAST_NAMES

    if not os.path.exists(LIUXIN_NAMES_LIST_FOLDERS):
        default_log.info(f"Cannot load names file from {LIUXIN_NAMES_LIST_FOLDERS = }")
        return FIRST_NAMES, LAST_NAMES

    with open(FIRST_NAMES_PATH, "rU") as first_names_csv:
        first_reader = csv.reader(first_names_csv, dialect=csv.excel_tab)
        for row in first_reader:
            for item in row:
                if lower_case:
                    FIRST_NAMES.add(item.lower())
                else:
                    FIRST_NAMES.add(item)

    with open(LAST_NAMES_PATH, "rU") as last_names_csv:
        last_reader = csv.reader(last_names_csv, dialect=csv.excel_tab)
        for row in last_reader:
            for item in row:
                if lower_case:
                    LAST_NAMES.add(item.lower())
                else:
                    LAST_NAMES.add(item)

    return FIRST_NAMES, LAST_NAMES


def add_name(name: str, first_name: bool = False, last_name: bool = False) -> bool:
    """
    Add a name to the namelist csv files.

    :param name:
    :param first_name:
    :param last_name:
    :return:
    """
    name = deepcopy(name)
    assert first_name or last_name, "Please specify if the name is first, last or both"
    if first_name:
        with open(FIRST_NAMES_PATH, "a") as first_name_csv:
            first_writer = csv.writer(first_name_csv, delimiter=" ")
            first_writer.writerow([name])

    elif last_name:
        with open(LAST_NAMES_PATH, "a") as last_name_csv:
            last_writer = csv.writer(last_name_csv, delimiter=" ")
            last_writer.writerow([name])

    elif first_name and last_name:
        with open(FIRST_NAMES_PATH, "a") as first_name_csv:
            with open(LAST_NAMES_PATH, "a") as last_name_csv:
                first_writer = csv.writer(first_name_csv, delimiter=" ")
                first_writer.writerow([name])
                last_writer = csv.writer(last_name_csv, delimiter=" ")
                last_writer.writerow([name])
    else:
        raise AssertionError("")

    # check that the name has been successfully inserted
    first_names, last_names = load_names(lower_case=False)
    if first_name:
        if name in first_names:
            return True
        else:
            return False

    elif last_name:
        if name in last_names:
            return True
        else:
            return False

    elif last_name and first_name:
        if name in last_names and name in first_names:
            return True
        else:
            return False

    else:
        raise AssertionError("Logical error in add_name check stage.")
