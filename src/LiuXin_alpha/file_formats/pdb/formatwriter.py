# -*- coding: utf-8 -*-

"""
Define the shared Palm database format-writer interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise formatwriter through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class FormatWriter(object):
    """
    Provide the formatwriter contract for validated ebook processing.

    Example:
        Exercise FormatWriter through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Initialize and validate the formatwriter state.

        Example:
            Exercise FormatWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        raise NotImplementedError()

    def write_content(self: _typing.Self, oeb_book: _typing.Any, output_stream: _typing.Any, metadata: _typing.Any = None) -> None:
        """
        Write content under the format's safety and compatibility rules.

        Example:
            Exercise FormatWriter.write content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_stream: Value supplied for output stream under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()
