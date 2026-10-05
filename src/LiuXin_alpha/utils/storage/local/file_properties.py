
"""
Read normalized local file size, timestamp and identity properties.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise file properties through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
"""


import os
import hashlib
from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


def get_file_name(file_path: str) -> str:
    """
    Returns the raw name of a file as a string.

    Example:
        Exercise get file name through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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

    """
    Return file ext under the documented compatibility and safety rules.

    Example:
        Exercise get file ext through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param file_in: Value supplied for file in under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    file_in = six_unicode(file_in)

    _, file_extension = os.path.splitext(file_in)

    return file_extension


def get_file_name_and_ext(
    file_path: str,
) -> str:
    """
    Takes the file path and works back to give the file name and extension

    Example:
        Exercise get file name and ext through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    file_path_local = six_unicode(file_path)

    current_letter = None
    position = len(file_path_local)

    while current_letter != os.sep:
        position -= 1
        current_letter = file_path_local[position - 1]

    return file_path_local[position:]


def get_file_hash(file_path: str, blocksize: int = 64 * 1024) -> str:
    """
    Receives a file path. Returns a hash for that file.

    Example:
        Exercise get file hash through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param file_path: Value supplied for file path under the utility contract.
    :param blocksize: Value supplied for blocksize under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    # Declaring this as a default causes hash return to be non-deterministic.
    hasher = hashlib.sha512()

    # NOTE: This must be binary mode.
    # On Windows, opening a SQLite DB (or any binary file) in text mode will try to decode
    # bytes using the active codepage (e.g. cp1252) and can explode with UnicodeDecodeError.
    size = get_file_size(file_path)
    with open(file_path, "rb") as file_in_pointer:
        while True:
            buf = file_in_pointer.read(blocksize)
            if not buf:
                break
            hasher.update(buf)

    # Honestly can't believe this is needed - but I've seen a hash collision? I think?
    return hasher.hexdigest() + six_unicode(size)
    # Still don't actually believe it


def get_file_size(file_in: str) -> int:
    """
    Calculates the file size in bits and returns an integer (not a long).

    Example:
        Exercise get file size through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param file_in: Value supplied for file in under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return int(os.path.getsize(file_in))


def ext_equality(ext1: str, ext2: str) -> bool:
    """
    Tells you if both extensions belong to the same sub-type.

    Example:
        Exercise ext equality through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param ext1: Value supplied for ext1 under the utility contract.
    :param ext2: Value supplied for ext2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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

    Example:
        Exercise is ext rar through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param ext: Value supplied for ext under the utility contract.
    :return: True when the documented condition holds; otherwise False.
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

    Example:
        Exercise get all file properties through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
