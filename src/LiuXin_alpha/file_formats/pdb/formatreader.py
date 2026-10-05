# -*- coding: utf-8 -*-

"""
Define the shared Palm database format-reader interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise formatreader through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

from abc import ABC, abstractmethod
from os import PathLike
from typing import BinaryIO

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class FormatReader(ABC):
    """
    Parse formatreader data into normalized ebook structures.

    Example:
        Exercise FormatReader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    @abstractmethod
    def __init__(
        self: _typing.Self,
        header: object,
        stream: BinaryIO,
        log: object,
        options: object,
    ) -> None:
        """
        Initialize and validate the formatreader state.

        Example:
            Exercise FormatReader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param header: Value supplied for header under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param log: Value supplied for log under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        ...

    @abstractmethod
    def extract_content(
        self: _typing.Self,
        output_dir: str | PathLike[str],
    ) -> object:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise FormatReader.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...
