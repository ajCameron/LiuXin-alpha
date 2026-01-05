
"""
Tools to generate and manipulate LiuXin style names and file names.
"""

import uuid
import re
import string
import random
import os
from copy import deepcopy

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.constants import VERBOSE_DEBUG

# from LiuXin.databases.row_collection import RowCollection
#
# from LiuXin.utils.calibre import relpath, guess_type, remove_bracketed_text
# from LiuXin import prints
# from LiuXin.utils.localization import _
#
# from LiuXin.utils.calibre.tweaks import tweaks

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

# Todo: This all needs to be re-written - can't be arsed right now


def smart_truncate(file_name_ext, length_limit=20):
    """
    Truncate the given file_name down to a more suitable form.

    :param file_name_ext:
    :param length_limit:
    :return new_file_name/False: False if the truncation cannot be accomplished.
    """
    if length_limit < 10:
        return False

    from LiuXin.utils.file_ops.file_properties import get_file_name

    file_name = get_file_name(file_name_ext)
    file_ext = os.path.splitext(file_name_ext)[1]

    if len(file_ext) >= length_limit:
        return False

    new_file_name = file_name[: (length_limit - len(file_ext))] + file_ext
    return new_file_name


# The anatomy of an LX folder name should be as follows
# 1) LX - to let the user know what the deal with the funny file ending is
# 2) _0_ - The resource type.
#        - 0 - Not set - For every other type of folder.
#        - 1 - File - This should never turn up in a Folder of any type.
#        - 2 - Title - A base folder which should contain different versions of the same File
#        - 3 - Series - A folder dedicated to a series
#        - 4 - Author - A folder linked to an author (well - a Creator of type Author)
#        - 5 - Folder - Somehow I forgot it
# 3) A string of the ids associated with this folder (or an random alpha-num string)
# 4) __folder_id
def make_folder_name(folder_row):
    """
    Takes a folder_row - extracts the actual folder dict. Seeds a RowCollection object with it. Iterates through the
    allowable linked row types until it finds one which actually has some content.
    Builds the appropriate name for that folder and returns it.
    :param folder_row:
    :return folder_name:
    """
    if folder_row["folder_use_original_name"] == "1":
        original_name = folder_row["folder_original_name"]
        folder_id = folder_row.get_row_id("folders")
        alpha_string = get_random_alpha_string(5)
        return original_name + " - LX_0_" + alpha_string + "_" + folder_id

    folder_row_dict = folder_row.get_row("folders")
    folder_row_collection = RowCollection(seed_row=folder_row_dict, target_database=folder_row.db)
    folder_row_collection.sort_all_row_indices()
    folder_row_id = folder_row.get_row_id("folders")

    if folder_row_collection["creators"]:
        creator_rows = folder_row_collection["creators"]
        creator_names = []
        creator_ids = []
        for row in creator_rows:
            creator_ids.append(row.get_row_id("creators"))
            creator_names.append(row["creator"])
        creator_names_string = " & ".join(creator_names)
        creator_ids_string = "_".join(creator_ids)
        alpha_string = get_random_alpha_string(5)

        return creator_names_string + " - LX_4_" + creator_ids_string + "_" + alpha_string + "_" + folder_row_id

    elif folder_row_collection["series"]:
        series_row = folder_row_collection.get_first_linked_row("series")
        series = series_row["series"]
        series_id = series_row.get_row_id("series")
        alpha_string = get_random_alpha_string(5)

        return series + " - LX_3_" + series_id + "_" + alpha_string + "_" + folder_row_id

    elif folder_row_collection["titles"]:
        title_row = folder_row_collection.get_first_linked_row("titles")
        title = title_row["title"]
        title_id = title_row.get_row_id("titles")
        alpha_string = get_random_alpha_string(5)

        return title + " - LX_2_" + title_id + "_" + alpha_string + "_" + folder_row_id

    elif folder_row_collection["files"]:
        # If there are just files associated with this object, but nothing else, fall back on default behavior
        folder_row_id = folder_row["folder_id"]
        folder_row_o_name = folder_row["folder_original_name"]
        alpha_string = get_random_alpha_string(5)

        return folder_row_o_name + " - LX_0_" + alpha_string + "_" + folder_row_id

    else:

        folder_row_id = folder_row["folder_id"]
        folder_row_o_name = folder_row["folder_original_name"]
        alpha_string = get_random_alpha_string(5)

        return folder_row_o_name + " - LX_0_" + alpha_string + "_" + folder_row_id


# An LX file name should have the following structure
# [author - LX_4_id][series - series number - LX_3_id][title - LX_2_id] - LX_1_[alpha string]_file_id
# except for the ones where we're using the original name
# These have the structure original_name - original_name - LX_0_gjwtl_file_id.extension
def make_file_name(file_row):
    """
    ...writing this method made me simply give up and write the Row_Collection object.
    Takes a file_row. Extracts the file_row_dict from it.
    :param file_row:
    :return:
    """
    if file_row["file_use_original_name"] == "1":
        original_name = file_row["file_original_name"]
        file_id = file_row.get_row_id("folders")
        alpha_string = get_random_alpha_string(5)
        extension = file_row["file_extension"]
        return sanitize_object_names(original_name + " - LX_0_" + alpha_string + "_" + file_id + extension)

    file_row_dict = file_row.get_row("files")
    database = file_row.db
    file_row_collection = RowCollection(seed_row=file_row_dict, target_database=database)

    series_row = file_row_collection.get_first_linked_row("series")
    creator_row = file_row_collection.get_first_linked_row("creators")
    title_row = file_row_collection.get_first_linked_row("")
    file_extension = file_row["file_extension"]

    creator_token = ""
    if creator_row is not None:
        creator_row_id = creator_row["creator_id"]
        creator_name = creator_row["creator"]
        creator_token += "[" + creator_name + " - LX_4_" + creator_row_id + "]"

    series_token = ""
    if series_row is not None:
        series_row_id = series_row["series_id"]
        series_number = series_row["series_number"]
        series = series_row["series"]
        series_token += "[" + series + " - " + series_number + " - LX_3_" + series_row_id + "]"

    title_token = ""
    if title_row is not None:
        title_row_id = title_row["title_id"]
        title = title_row["title"]
        title_token += "[" + title + " - LX_2_" + title_row_id + "]"

    file_token = " - LX_1_" + get_random_alpha_string(5) + file_row["file_id"]
    return sanitize_object_names(creator_token + series_token + title_token + file_token + file_extension)


def make_cover_name(cover_row):
    """
    Takes a cover Row - builds an appropriate name for it out of data in it.  Returns the name.
    The returned name won't have an extension
    :param cover_row:
    :return cover_name:
    """
    cover_id = cover_row["cover_id"]
    cover_tag = make_lx_tag(cover_id, "covers")
    cover_original_name = cover_row["cover_original_name"]
    if cover_original_name != "":
        return cover_original_name + cover_tag
    else:
        return get_random_alpha_num_string(10) + cover_tag


def get_random_alpha_string(string_len):
    """
    Returns a random alphabetical string of length string len.
    :param string_len:
    :return alpha_string:
    """
    return "".join(random.choice(string.ascii_lowercase) for _ in range(string_len))


def get_random_alpha_num_string(string_len):
    """
    Produces a random alpha_numeric string of the requested length.
    Provided the requested length is not greater than that of a standard uuid with all the full stops stripped.
    :param string_len:
    :return:
    """
    start_string = six_unicode(uuid.uuid4())
    start_string = re.sub(r"\.", "", start_string)
    return start_string[:string_len]


# needs testing
def check_for_LX_ending(test_string):
    """
    Checks the given test string to see if it ends with a structure like an LX tag.
    :param test_string:
    :return True/False:
    """
    # LX endings have an LX tag form (see below)
    test_string = deepcopy(test_string)
    test_string = six_unicode(test_string)

    # This pattern should match the LX tag ending of any LX object name
    LX_tag_pattern = r" - LX_[0-3]_[0-9]+_[0-9a-z]+$"
    if re.search(LX_tag_pattern, test_string) is None:
        return False
    else:
        return True


# testing please
def strip_LX_ending(object_name):
    """
    Takes an object name - uses a regex to strip the LX tag from the end.
    :param object_name:
    :return:
    """
    if not check_for_LX_ending(object_name):
        if VERBOSE_DEBUG:
            err_str = "strip_LX_ending has been supplied with a string without an LX ending"
            err_str += "object_name: " + repr(object_name) + "\n"
            raise InputIntegrityError(err_str)
        else:
            raise InputIntegrityError

    object_name = deepcopy(object_name)
    object_name = six_unicode(object_name)
    object_name_tokenized = object_name.split("(")
    object_name_tokenized = object_name_tokenized[:-1]

    rebuilt_name = "-".join(object_name_tokenized)
    # dropping the last " "
    if rebuilt_name.endswith(" "):
        rebuilt_name = re.sub(r"( )+$", "", rebuilt_name)
    # killing any trailing underscore
    if rebuilt_name.endswith("_"):
        rebuilt_name = re.sub(r"_+$", "", rebuilt_name)

    return rebuilt_name


# Assuming the file started with a name like "LiuXin User Manual.pdf". Assuming the Row has been properly filled out, the
# return will be something like "LiuXin User Manual - LX_0_247_9f3c4.pdf" with " - LX_0_247_9f3c4" being the LiuXin tag
# See before an explanation of LiuXin tags
# Todo: This should be customizable in preferences
def make_initial_folder_name(row, separate_tag=False, folder_id=None):
    """
    Takes a Row. Builds a nice name for an object associated with that Row.
    This method is only defined for certain types of Row.
    It will throw an error if anything except a simple row is fed to it.
    It should be preference dependant - but that feature hasn't been coded yet.
    It only works for certain types of Row.
    :param row:
    :return:
    """
    if folder_id is None:
        raise InputIntegrityError

    # Different algorithms are used depending on the type of row fed. Eventually this will controllable via preferences
    # It should be possible to reconstruct the id and the type of resource from the ending

    # Load the row as far as possible with information before feeding it into this method - if data such as the file_name
    # is not present in the Row it will be ignored when the name is generated.
    if row.primary_row_table_name == "files":

        row.ensure_rows_have_ids()
        file_id = row["file_id"]
        file_name = row["file_name"]
        file_extension = row["file_extension"]
        if not file_extension.startswith("."):
            file_extension = "." + file_extension
        lx_tag = make_lx_tag(folder_id, "files", file_id)
        if not separate_tag:
            return file_name + lx_tag + file_extension
        else:
            return file_name + lx_tag + file_extension, lx_tag

    elif row.primary_row_table_name == "titles":

        row.ensure_rows_have_ids()
        title_id = row["title_id"]
        title_name = row["title"]
        lx_tag = make_lx_tag(title_id, "titles", folder_id)
        if not separate_tag:
            return sanitize_object_names(title_name) + lx_tag
        else:
            return sanitize_object_names(title_name + lx_tag), lx_tag

    elif row.primary_row_table_name == "series":

        row.ensure_rows_have_ids()
        series_id = row["series_id"]
        series = row["series"]
        lx_tag = make_lx_tag(series_id, "series", folder_id)
        if not separate_tag:
            return sanitize_object_names(series + lx_tag)
        else:
            return sanitize_object_names(series + lx_tag), lx_tag

    elif row.primary_row_table_name == "creators":

        row.ensure_rows_have_ids()
        creator_id = row["creator_id"]
        creator = row["creator"]
        lx_tag = make_lx_tag(creator_id, "authors", folder_id)
        if not separate_tag:
            return sanitize_object_names(creator + lx_tag)
        else:
            return sanitize_object_names(creator + lx_tag), lx_tag

    elif row.primary_row_table_name == "folders":

        row.ensure_rows_have_ids()
        o_folder_id = row["folder_id"]
        o_folder_name = row["folder_name"]
        lx_tag = make_lx_tag(o_folder_id, None, folder_id)
        if not separate_tag:
            return sanitize_object_names(o_folder_name + lx_tag)
        else:
            return sanitize_object_names(o_folder_name + lx_tag), lx_tag

    else:

        err_str = "Row is of unrecognized type.\n"
        err_str += "row: " + six_unicode(row) + "\n"
        raise NotImplementedError(err_str)


# The tag should be a unique thing which is maintained with the file throughout it's lifespan
# Takes what it needs to make the tag - returns the tag itself
# LX tags have the following structure.
# 1) LX - to let the user know what the deal with the funny file ending is
# 2) _0_ - The resource type.
#        - 0 - Not set - For every other type of folder.
#        - 1 - File - This should never turn up in a Folder of any type.
#        - 2 - Title - A base folder which should contain different versions of the same File
#        - 3 - Series - A folder dedicated to a series
#        - 4 - Author - A folder linked to an author (well - a Creator of type Author)
#        - 5 - Cover - A cover - which can be linked to ... anything
def make_lx_tag(row_id, row_type, folder_id):
    """
    Makes a tag - largely a lookup table, which takes some information and sticks it together.
    :param row_id:
    :param row_type: Allowable types and File, Title, Series, Author
    :return tag:
    """
    if row_type is None:
        tag = " - LX_{}_(0-".format(row_id) + six_unicode(folder_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    elif row_type.lower() == "files":
        tag = " - LX_{}_(1-".format(folder_id) + six_unicode(row_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    elif row_type.lower() == "titles":
        tag = " - LX_{}_(2-".format(folder_id) + six_unicode(row_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    elif row_type.lower() == "series":
        tag = " - LX_{}_(3-".format(folder_id) + six_unicode(row_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    elif row_type.lower() == "authors":
        tag = " - LX_{}_(4-".format(folder_id) + six_unicode(row_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    elif row_type.lower() == "covers":
        tag = " - LX_{}_(5-".format(folder_id) + six_unicode(row_id) + ")_" + six_unicode(uuid.uuid4())[:5]
        return sanitize_object_names(tag)
    else:
        err_str = "Attempt to run make_LX_tag failed. Unrecognized row_type.\n"
        err_str += "row_type: " + repr(row_type) + "\n"
        err_str += "row_id: " + repr(row_id) + "\n"
        raise NotImplementedError(err_str)


def make_folder_tag(folder_id):
    """
    Makes a bare tag which just contains the folder_id - used when a folder is being assimilated and it hasn't yet been
    linked to anything else
    :param folder_id:
    :return:
    """
    return sanitize_object_names(" - LX_{}".format(folder_id) + six_unicode(uuid.uuid4())[:5])


# Todo: Centralize and standardize the creator_sort setting functions
def make_creators_folder_name(creator_list, folder_id):
    """
    Takes a list of creator rows - creates a nice name for them which can be easily parsed by the system.
    Assumes the list is sorted in order of name priority - i.e. first name first.
    :param creator_list:
    :return:
    """
    if len(creator_list) == 0:
        err_str = "make_creators_folder_name has been passed a blank index.\n"
        raise InputIntegrityError(err_str)

    creator_sort_list = []
    for creator in creator_list:
        if creator["creator_sort"] == "None" or creator["creator_sort"] is None:
            creator["creator_sort"] = author_to_author_sort(creator["creator"])
            creator.sync()
            creator_sort_list.append(creator["creator_sort"])
        else:
            creator_sort_list.append(creator["creator_sort"])
    creator_id_list = ["4_" + six_unicode(creator["creator_id"]) for creator in creator_list]

    creator_sort_str = " & ".join(creator_sort_list)
    # # Dots in folder names can sometimes cause problems
    # creator_sort_str = creator_sort_str.replace(".", "")

    creator_ids_str = "-".join(creator_id_list)
    folder_tag = " - LX_{}_(".format(folder_id) + creator_ids_str + ")_" + six_unicode(uuid.uuid4())[:5]
    return sanitize_object_names(creator_sort_str), folder_tag


def make_series_folder_name(series_row, folder_id):
    """
    Make a name suitable for a folder linked to a series_row
    :param series_row:
    :param folder_id:
    :return:
    """
    series_id = six_unicode(series_row["series_id"])
    series_name = series_row["series"]
    series_tag = " - LX_{}_(".format(folder_id) + "3_" + series_id + ")_" + six_unicode(uuid.uuid4())[:5]
    return sanitize_object_names(series_name), series_tag


def make_book_folder_name(book_row, folder_id, title_string=None, series_position=None):
    """
    Make a name suitable for writing into the name of a book folder.
    DatabasePing access is required if you want to add the position of the book in the series it's
    :param book_row:
    :param folder_id:
    :param title_string:
    :param series_position:
    :return:
    """
    book_id = six_unicode(book_row["book_id"])
    if title_string is not None:
        book_name = six_unicode(title_string)
    else:
        book_name = _("Unknown")
    if series_position is not None:
        book_name = six_unicode(series_position) + " - " + book_name

    book_tag = " - LX_{}_(".format(folder_id) + "2_" + book_id + ")_" + six_unicode(uuid.uuid4())[:5]

    return sanitize_object_names(book_name), book_tag


# Todeo: Review all uses of exec. This is madness!
def authors_str_to_sort_str(authors_str):
    """
    Takes a string of authors. Makes an author sort string out of them.
    :param authors_str:
    :return:
    """
    if not authors_str:
        return None

    if isinstance(authors_str, list):
        return author_list_to_sort_str(authors_str)

    authors_code = "author_list = [" + deepcopy(authors_str) + "]"
    exec(authors_code)
    return author_list_to_sort_str(author_list)


def author_list_to_sort_str(author_list):
    """
    Takes a list of authors - converts it into a sort string and returns it.
    Assumes that the authors are in the right order.
    :param author_list:
    :return:
    """
    author_list = deepcopy(author_list)
    rtn_str = " & ".join(author_list)
    return rtn_str


def author_to_author_sort(author, method=None):
    """
    Converts an author name string into an author sort string.
    :param author:
    :param method:
    :return:
    """
    if not author:
        return ""
    sauthor = remove_bracketed_text(author).strip()
    tokens = sauthor.split()
    if len(tokens) < 2:
        return author
    if method is None:
        method = tweaks["author_sort_copy_method"]

    ltoks = frozenset(x.lower() for x in tokens)
    copy_words = frozenset(x.lower() for x in tweaks["author_name_copywords"])
    if ltoks.intersection(copy_words):
        method = "copy"

    if method == "copy":
        return author

    prefixes = set([y.lower() for y in tweaks["author_name_prefixes"]])
    prefixes |= set([y + "." for y in prefixes])
    while True:
        if not tokens:
            return author
        tok = tokens[0].lower()
        if tok in prefixes:
            tokens = tokens[1:]
        else:
            break

    suffixes = set([y.lower() for y in tweaks["author_name_suffixes"]])
    suffixes |= set([y + "." for y in suffixes])

    suffix = ""
    while True:
        if not tokens:
            return author
        last = tokens[-1].lower()
        if last in suffixes:
            suffix = tokens[-1] + " " + suffix
            tokens = tokens[:-1]
        else:
            break
    suffix = suffix.strip()

    if method == "comma" and "," in "".join(tokens):
        return author

    atokens = tokens[-1:] + tokens[:-1]
    num_toks = len(atokens)
    if suffix:
        atokens.append(suffix)

    if method != "nocomma" and num_toks > 1:
        atokens[0] += ","

    return " ".join(atokens)


# ----------------------------------------------------------------------------------------------------------------------
# - METHODS TO CREATE AND PROCESS TAGS START HERE
# ----------------------------------------------------------------------------------------------------------------------


def split_file_tag(target_file_name_ext):
    """
    Takes a file name (including extension) splits the tag from out of that name and returns it.
    :param target_file_name:
    :return:
    """
    target_file_name_ext = deepcopy(target_file_name_ext)
    target_file_name = os.path.splitext(target_file_name_ext)[0]
    return split_folder_tag(target_file_name)


def split_file_id(target_file_name_ext):
    """
    Takes a file name - extracts the file_id from it and returns it.
    :param target_file_name_ext:
    :return:
    """
    file_tag = split_file_tag(target_file_name_ext)
    return split_folder_id(file_tag)


def split_folder_tag(target_folder_name):
    """
    Takes a folder name. Splits the tag out of the folder name and returns it.
    :param target_folder_name:
    :return folder_tag:
    """
    target_folder_name = deepcopy(target_folder_name)

    tag_re = r"^.*( - LX_.*)$"
    tag_pat = re.compile(tag_re, re.I)
    tag_match = tag_pat.match(target_folder_name)

    if tag_match is None:
        return None
    else:
        return tag_match.group(1)


def split_folder_id(target_folder_name):
    """
    Takes a folder name. Splits the id out of the name and returns it.
    :param target_folder_name:
    :return folder_id:
    """
    target_folder_name = deepcopy(target_folder_name)

    tag_re = r"^.* - LX_([0-9]+)_.*$"
    tag_pat = re.compile(tag_re, re.I)
    tag_match = tag_pat.match(target_folder_name)

    if tag_match is None:
        return None
    else:
        return tag_match.group(1)


# Todo: The maintennce bot should be on the look out for these all the time
# Todo: Somewhere there is a method to make safe file names - use it here
def sanitize_object_names(target_name):
    """
    Makes a name safe for actually writing to all operating systems.
    :param target_name:
    :return:
    """
    target_name = deepcopy(target_name)
    return target_name.replace(":", "-")


# def make_creator_folder_name(creator_rows, folder_row):
#     """
#     Make a name suitable for a creator folder at the root of a folder store.
#     :param creator_rows
#     :param folder_row:
#     """
#     ids_list = [r["creator_id"] for r in creator_rows]
#     creator_sort = [r["creator_sort"] for r in creator_rows]
#     folder_id = folder_row["folder_id"]
#
#     lx_tag = u' - LX_' + "c_".join(ids_list) + "f_{0}".format(folder_id) + unicode(uuid.uuid4()).replace("-","")[:6]
#     creators_string = '_'.join(creator_sort)
#     return creators_string + lx_tag


# @staticmethod
# def split_file_tag(file_name_ext):
#     """
#     Splits the file tag out of the file name and returns it.
#     :param file_name_ext: File full name including the extension
#     :return:
#     """
#     file_name_ext = deepcopy(file_name_ext)
#     file_name = os.path.splitext(file_name_ext)[0]
#     file_tag_re = r'^.*(_LX_1_[0-9]+_[a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9])$'
#     file_tag_pat = re.compile(file_tag_re, re.I)
#     file_tag_match = file_tag_pat.match(file_name)
#
#     # If the match is none trying to return it's group will throw an error
#     if file_tag_match is not None:
#         return file_tag_match.group(1)
#     else:
#         return None
#
#
# def split_file_id(self, file_name_ext):
#     """
#     Extracts the file_id from the tag in the file name and returns it.
#     :param file_name_ext:
#     :return:
#     """
#     file_tag = self.split_file_tag(file_name_ext)
#     file_id_re = r'^_LX_1_([0-9]+)_[a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9]$'
#     file_id_pat = re.compile(file_id_re, re.I)
#     file_id_match = file_id_pat.match(file_tag)
#
#     if file_id_match is not None:
#         return file_id_match.group(1)
#     else:
#         return None
#
#
# # Structure of a folder_tag - [folder_name]_LX_[folder_id]_[6 a/n characters]
# @staticmethod
# def split_folder_id(target_folder_name):
#     """
#     Splits the tag off the end of the folder and returns the folder_id.
#     :param target_folder_name:
#     :return folder_id/None: None if the folder_is can't be extracted
#     """
#     target_folder_name = deepcopy(target_folder_name)
#
#     id_re = r'^.*_LX_5_([0-9]+)_[a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9]$'
#     id_pat = re.compile(id_re, re.I)
#     id_match = id_pat.match(target_folder_name)
#
#     if id_match is None:
#         return None
#     else:
#         return id_match.group(1)
#
#
# @staticmethod
# def make_folder_tag(folder_id):
#     """
#     Makes the tag which will be appended to the end of the folder_name.
#     :param folder_id:
#     :return folder_tag:
#     """
#     folder_id = unicode(folder_id)
#     an_string = unicode(uuid.uuid4())[:6]
#     lx_folder_tag = "_LX_5_{}_{}".format(folder_id, an_string)
#     return lx_folder_tag
#
#
# @staticmethod
# def make_file_tag(file_id):
#     """
#     Makes the tag which will be appended to the end of the file_name.
#     :param file_id:
#     :return:
#     """
#     file_id = unicode(file_id)
#     an_string = unicode(uuid.uuid4())[:6]
#     lx_file_tag = "_LX_1_{}_{}".format(file_id, an_string)
#     return lx_file_tag
