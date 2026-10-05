#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Coordinate MOBI diagnostic extraction and report generation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise main through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import shutil
import sys

from LiuXin_alpha.file_formats.mobi.debug.headers import MOBIFile
from LiuXin_alpha.file_formats.mobi.debug.mobi6 import inspect_mobi as inspect_mobi6
from LiuXin_alpha.file_formats.mobi.debug.mobi8 import inspect_mobi as inspect_mobi8

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def inspect_mobi(path_or_stream: _typing.Any, ddir: _typing.Any = None) -> None:  # {{{
    """
    Perform the inspect mobi operation under explicit file-format and conversion rules.

    Example:
        Exercise inspect mobi through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param path_or_stream: Value supplied for path or stream under the utility contract.
    :param ddir: Value supplied for ddir under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    stream = path_or_stream if hasattr(path_or_stream, "read") else open(path_or_stream, "rb")
    f = MOBIFile(stream)
    if ddir is None:
        ddir = "decompiled_" + os.path.splitext(os.path.basename(stream.name))[0]
    try:
        shutil.rmtree(ddir)
    except Exception as e:
        print("Couldn't removed ddir - error message - {}".format(e))
        pass
    os.makedirs(ddir)
    if f.kf8_type is None:
        inspect_mobi6(f, ddir)
    elif f.kf8_type == "joint":
        p6 = os.path.join(ddir, "mobi6")
        os.mkdir(p6)
        inspect_mobi6(f, p6)
        p8 = os.path.join(ddir, "mobi8")
        os.mkdir(p8)
        inspect_mobi8(f, p8)
    else:
        inspect_mobi8(f, ddir)

    print("Debug data saved to:", ddir)


# }}}


def main() -> None:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    inspect_mobi(sys.argv[1])


if __name__ == "__main__":
    main()
