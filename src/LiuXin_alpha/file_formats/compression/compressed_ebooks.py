from __future__ import print_function

import os
from copy import deepcopy

import zipfile

import LiuXin_alpha.utils.file_ops as file_ops

from LiuXin_alpha.constants import BOOK_EXTENSIONS_DOTTED
from LiuXin_alpha.constants import RAR_BOOK_FILE_CONTENTS
from LiuXin_alpha.constants import RAR_BOOK_FILE_CONTENTS_DOTTED

from LiuXin_alpha.utils.file_ops.file_properties import get_file_ext

from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_unicode

__author__ = "Cameron"


# Takes a list of files. Tries to work out if it's a compressed book, or a compressed collection of other files
def is_ebook(list_of_names):
    """
    Takes the list of names which has been copied out of a file.
    Tries to work out if it corresponds to a compressed book or a compressed collection of other files.
    :param list_of_names:
    :return: True/False
    """
    list_of_names = deepcopy(list_of_names)
    if is_comic(list_of_names):
        return True
    else:
        for name in list_of_names:
            name, ext = os.path.splitext(name)
            if len(ext) > 0:
                ext = ext[1:]
                if ext not in RAR_BOOK_FILE_CONTENTS:
                    return False
        else:
            return True


# Imported from calibre.ebooks.metadata.archive import is_comic
def is_comic(list_of_names):
    """
    Takes a list of file names. Determines if they are all images.
    If all files in a directory are images, it's probably a comic.
    :param list_of_names: List of names in a directory
    :return: True/False
    """
    extensions = set(
        [
            x.rpartition(".")[-1].lower()
            for x in list_of_names
            if "." in x and x.lower().rpartition("/")[-1] != "thumbs.db"
        ]
    )
    comic_extensions = {"jpg", "jpeg", "png"}
    return len(extensions - comic_extensions) == 0


# Analyses (where possible) a compressed file and returns if it's a book (i.e. a compressed collection of html and other
#  resources)
def is_file_book(file_path):
    """
    Examines the file and tries to determine if it's a single book or not.
    :param file_path:
    :return:
    """

    ext = get_file_ext(file_path)

    if ext in BOOK_EXTENSIONS_DOTTED:
        return True

    if (ext == ".zip") and (is_zip_archive_book(file_path)):
        return True
    elif (ext == ".rar") and (is_zip_archive_book(file_path)):
        return True
    else:
        return False


def is_zip_archive_book(file_path):
    """
    Takes a file_path to a zip file. Guesses as to if it's a single book or if it needs unpacking.
    :param file_path:
    :return:
    """
    file_path_local = file_path  # making a local copy, just in case

    with zipfile.ZipFile(file_path, "r") as myzip:
        files = myzip.namelist()

    extensions = file_ops.count_file_types(files)

    for extension in iter(extensions):
        if extension not in RAR_BOOK_FILE_CONTENTS_DOTTED:
            return False

    return True


def is_rar_archive_book(file_path):
    """
    Takes a file_path to a rar file. Guesses as to if it's a single book or if it needs unpacking.
    :param file_path:
    :return:
    """
    files = []  # creating a index to store the files

    try:
        myrar = rarfile.RarFile(file_path)

        for item in myrar.infolist():
            files.append(item.filename)

        extensions = file_ops.count_file_types(files)

        for extension in iter(extensions):
            if extension not in RAR_BOOK_FILE_CONTENTS_DOTTED:
                return False

        return True
    except:
        error_message = "Error parsing file. " + six_unicode(file_path)
        print(error_message)
        return False
