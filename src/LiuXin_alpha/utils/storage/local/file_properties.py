
"""
Macros to get various properties of files more conveniently.
"""


import os
import hashlib
from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


def get_file_name(file_path: str) -> str:
    """
    Returns the raw name of a file as a string.

    :param file_path:
    :return:
    """

    file_path_local = six_unicode(file_path)

    # splitting the path down into section
    file_path_split = file_path_local.split(os.sep)

    name = file_path_split[-1]

    # splitting at . - though there are examples where there are more than one
    file_name_split = name.split(".")

    file_name_split = file_name_split[:-1]

    file_name = ""

    for part in file_name_split:
        file_name += part + "."

    file_name = file_name[:-1]  # removing the trailing .

    return file_name


def get_file_ext(file_in: str) -> str:

    file_in = six_unicode(file_in)

    _, file_extension = os.path.splitext(file_in)

    return file_extension


def get_file_name_and_ext(
    file_path: str,
) -> str:
    """
    Takes the file path and works back to give the file name and extension

    :param file_path:
    :return:
    """

    file_path_local = six_unicode(file_path)

    current_letter = None
    position = len(file_path_local)

    while current_letter != os.sep:
        position -= 1
        current_letter = file_path_local[position - 1]

    return file_path_local[position:]


def get_file_hash(file_path: str, blocksize: int = 64) -> str:
    """
    Receives a file path. Returns a hash for that file.

    Now with additional length, due to an observed collision in sha-512.
    :param file_path: A path to the file in question
    :param blocksize:
    :return:
    """
    hasher = hashlib.sha512()  # Declaring this as a default causes hash return to be non-deterministic.

    size = get_file_size(file_path)
    file_in_pointer = open(file_path, "r")

    buf = file_in_pointer.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = file_in_pointer.read(blocksize)

    file_in_pointer.close()

    return hasher.hexdigest() + six_unicode(
        size
    )  # Honestly can't believe this is needed - but I've seen a hash collision, and so it is
    # Still don't actually believe it


def get_file_size(file_in: str) -> int:
    """
    Calculates the file size in bits and returns an integer (not a long).

    :param file_in:
    :return file_size: In bytes
    """
    return int(os.path.getsize(file_in))


def ext_equality(ext1: str, ext2: str) -> bool:
    """
    Tells you if both extensions belong to the same sub-type.

    :param ext1:
    :param ext2:
    :return:
    """
    # .rar files ... what can you do?

    if ext1 == ext2:
        return True
    elif is_ext_rar(ext1) and is_ext_rar(ext2):
        return True
    else:
        return False


def is_ext_rar(ext) -> bool:
    """
    Returns True if an extension is .rar

    :param ext:
    :return:
    """
    ext = six_unicode(ext).lower().strip()
    if ext == ".rar":
        return True

    if ext[:2] == ".r":
        try:
            test = int(ext[2:])
        except KeyError:
            return False
        else:
            return True

    return False


# Todo: Make this a dataclass.
def get_all_file_properties(path):
    """
    Takes a path to a file. Returns a dictionary of useful values.

    :param path: Path to the file you want the information from
    :return:
    """
    # Useful when adding books to get all the values that the new books database requires
    # This method is only for files which are of on_disk type - should be merged into the on_disk folder_store_driver
    properties = dict()  # loading a dictionary with the values to be lead into the table
    file_path = os.path.abspath(path)  # getting a python safe path

    properties["path"] = file_path
    properties["name"] = get_file_name(file_path)
    properties["extension"] = get_file_ext(file_path)
    properties["hash_1"] = get_file_hash(file_path)
    properties["hash_2"] = get_file_hash(file_path)
    properties["size"] = get_file_size(file_path)
    properties["effective_size"] = deepcopy(properties["size"])

    return properties
