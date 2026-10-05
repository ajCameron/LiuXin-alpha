# -*- coding: utf-8 -*-

"""
Expose the supported ereader compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class EreaderError(Exception):
    """
    Report a ereadererror encountered while processing an ebook format.

    Example:
        Exercise EreaderError through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    pass


def image_name(name: _typing.Any, taken_names: tuple[_typing.Any, ...] = ()) -> _typing.Any:
    """
    Perform the image name operation under explicit file-format and conversion rules.

    Example:
        Exercise image name through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param taken_names: Value supplied for taken names under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(name, bytes):
        name = name.decode("ascii", "ignore")
    elif not isinstance(name, six_string_types):
        name = str(name)

    name = name.replace("\x00", "").replace("\\", "/").strip()
    name = os.path.basename(name)
    if name in ("", ".", ".."):
        name = "image.png"

    if len(name) > 32:
        cut = len(name) - 32
        names = name[:10]
        namee = name[10 + cut :]
        name = "%s%s.png" % (names, namee)

    taken = set()
    for item in taken_names:
        if isinstance(item, bytes):
            item = item.decode("ascii", "ignore")
        elif not isinstance(item, six_string_types):
            item = str(item)
        taken.add(item.replace("\x00", ""))

    base = name
    root, ext = os.path.splitext(base)
    suffix = 1
    while name in taken:
        marker = str(suffix)
        max_root = max(1, 32 - len(ext) - len(marker))
        name = "%s%s%s" % (root[:max_root], marker, ext)
        suffix += 1

    name = name.ljust(32, "\x00")[:32]

    return name
