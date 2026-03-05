__author__ = "Cameron"
# Loads LiuXin's database of names

import os
import csv
from copy import deepcopy

from LiuXin.paths import LiuXin_data_folder

# Calculating some of the file locations at runtime
LiuXin_names_lists_folder = os.path.join(LiuXin_data_folder, "names_lists")
First_Names = os.path.join(LiuXin_names_lists_folder, "First_Names.csv")
Last_Names = os.path.join(LiuXin_names_lists_folder, "Last_Names.csv")


# Todo: Replace lower with icu_lower
def load_names(lower_case=True):
    """
    Loads the first name and last name CSV files into memory.
    :return first_name_set, last_name_set: Sets of all the first and last names present in the csv files.
    """
    first_names = set()
    last_names = set()
    with open(First_Names, "rU") as first_names_csv:
        first_reader = csv.reader(first_names_csv, dialect=csv.excel_tab)
        for row in first_reader:
            for item in row:
                if lower_case:
                    first_names.add(item.lower())
                else:
                    first_names.add(item)
    with open(Last_Names, "rU") as last_names_csv:
        last_reader = csv.reader(last_names_csv, dialect=csv.excel_tab)
        for row in last_reader:
            for item in row:
                if lower_case:
                    last_names.add(item.lower())
                else:
                    last_names.add(item)

    return first_names, last_names


def add_name(name, first_name=False, last_name=False):
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
        with open(First_Names, "a") as first_name_csv:
            first_writer = csv.writer(first_name_csv, delimiter=" ")
            first_writer.writerow([name])
    elif last_name:
        with open(Last_Names, "a") as last_name_csv:
            last_writer = csv.writer(last_name_csv, delimiter=" ")
            last_writer.writerow([name])
    elif first_name and last_name:
        with open(First_Names, "a") as first_name_csv:
            with open(Last_Names, "a") as last_name_csv:
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
