#!/usr/bin/env python2
# vim:fileencoding=utf-8
"""
Render DOCX package structures into inspectable diagnostic output.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise dump through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import shutil
import sys

from lxml import etree

from LiuXin_alpha.utils.calibre import walk
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def pretty_all_xml_in_dir(path: _typing.Any) -> None:
    """
    Perform the pretty all xml in dir operation under explicit file-format and conversion rules.

    Example:
        Exercise pretty all xml in dir through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for f in walk(path):
        if f.endswith(".xml") or f.endswith(".rels"):
            with open(f, "r+b") as stream:
                raw = stream.read()
                if raw:
                    root = etree.fromstring(raw)
                    stream.seek(0)
                    stream.truncate()
                    stream.write(
                        etree.tostring(
                            root,
                            pretty_print=True,
                            encoding="utf-8",
                            xml_declaration=True,
                        )
                    )


def do_dump(path: _typing.Any, dest: _typing.Any) -> None:
    """
    Perform the do dump operation under explicit file-format and conversion rules.

    Example:
        Exercise do dump through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param dest: Value supplied for dest under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if os.path.exists(dest):
        shutil.rmtree(dest)
    with ZipFile(path) as zf:
        zf.extractall(dest)
    pretty_all_xml_in_dir(dest)


def dump(path: _typing.Any) -> None:

    """
    Perform the dump operation under explicit file-format and conversion rules.

    Example:
        Exercise dump through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dest = os.path.splitext(os.path.basename(path))[0]
    dest += "-dumped"
    do_dump(path, dest)

    print(path, "dumped to", dest)


if __name__ == "__main__":
    dump(sys.argv[-1])
