#!/usr/bin/env  python

"""
Read ZIP structures and inflate entries through the bundled compatibility implementation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise libunzip through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
from __future__ import print_function

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

import re

try:
    import zipfile
except ImportError:
    from LiuXin_alpha.utils import calibre_zipfile as zipfile


def update(
    pathtozip,
    patterns,
    filepaths,
    names,
    compression=zipfile.ZIP_DEFLATED,
    verbose=True,
):
    """
    Update files in the zip file at `pathtozip` matching the given `patterns` with the given `filepaths`. If more than one file matches, all of the files are replaced.

    Example:
        Exercise update through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param pathtozip: Value supplied for pathtozip under the utility contract.
    :param patterns: Value supplied for patterns under the utility contract.
    :param filepaths: Value supplied for filepaths under the utility contract.
    :param names: Value supplied for names under the utility contract.
    :param compression: Value supplied for compression under the utility contract.
    :param verbose: Value supplied for verbose under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert len(patterns) == len(filepaths) == len(names)
    z = zipfile.ZipFile(pathtozip, mode="a")
    for name in z.namelist():
        for pat, fname, new_name in zip(patterns, filepaths, names):
            if pat.search(name):
                if verbose:
                    print("Updating %s with %s" % (name, fname))
                if new_name is None:
                    z.replace(fname, arcname=name, compress_type=compression)
                else:
                    z.delete(name)
                    z.write(fname, new_name, compress_type=compression)
                break
    z.close()


def extract(filename, dir):
    """
    Extract archive C{filename} into directory C{dir}

    Example:
        Exercise extract through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param filename: Filename used for type inference or archive output.
    :param dir: Value supplied for dir under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zf = zipfile.ZipFile(filename)
    zf.extractall(dir)


def extract_member(
    filename,
    match=re.compile(r"\.(jpg|jpeg|gif|png)\s*$", re.I),
    sort_alphabetically=False,
):
    """
    Perform the extract member utility operation under explicit compatibility rules.

    Example:
        Exercise extract member through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param filename: Filename used for type inference or archive output.
    :param match: Value supplied for match under the utility contract.
    :param sort_alphabetically: Value supplied for sort alphabetically under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    zf = zipfile.ZipFile(filename)
    names = list(zf.namelist())
    if sort_alphabetically:
        names.sort()
    for name in names:
        if match.search(name):
            return name, zf.read(name)
