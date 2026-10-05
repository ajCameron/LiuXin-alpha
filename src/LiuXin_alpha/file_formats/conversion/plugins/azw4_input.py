# -*- coding: utf-8 -*-

"""
Convert AZW4 containers into normalized conversion input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise azw4 input through a consuming regression::

        python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
"""
from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class AZW4Input(InputFormatPlugin):
    """
    AZW4 files are Amazon's print replica ebook format.

    Example:
        Exercise AZW4Input through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
    """

    name = "AZW4 Input"
    author = "John Schember"
    description = "Convert AZW4 to HTML"
    file_types = {"azw4"}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise AZW4Input.convert through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.azw4.reader import Reader

        # AZW4 handling is byte-pattern based and does not need the PDB header.
        reader = Reader(None, stream, log, options)
        return reader.extract_content(choose_conversion_workdir("_azw4_input"))
