

"""
Path manip tools.
"""

import os

relpath = os.path.relpath

# probably safe


def splitext(path):
    """
    In LiuXin extensions are without the leading "."
    :param path:
    :return:
    """
    key, ext = os.path.splitext(path)
    return key, ext[1:].lower()


def formats_ok(formats):
    return len(formats) > 0


def path_ok(path):
    return not os.path.isdir(path) and os.access(path, os.R_OK)


_metadata_extensions = None


def metadata_extensions():
    """
    Set of all known book extensions + OPF (the OPF is used to read metadata, but not actually added) - thus set of all
    file extensions from which metadata should be read.
    :return:
    """

    global _metadata_extensions
    if _metadata_extensions is None:
        _metadata_extensions = (
            frozenset(map(six_unicode, BOOK_EXTENSIONS))
            | {"opf"}
            | frozenset(map(six_unicode, COMPRESSED_FILE_EXTENSIONS))
        )
    return _metadata_extensions


def listdir(root):
    for path in os.listdir(root):
        yield os.path.abspath(os.path.join(root, path))


