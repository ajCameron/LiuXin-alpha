# -*- coding: utf-8 -*-

"""
Normalize and classify line breaks in plain-text input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise newlines through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing
import os

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class TxtNewlines(object):

    """
    Provide the txtnewlines contract for validated ebook processing.

    Example:
        Exercise TxtNewlines through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    NEWLINE_TYPES = {
        "system": os.linesep,
        "unix": "\n",
        "old_mac": "\r",
        "windows": "\r\n",
    }

    def __init__(self: _typing.Self, newline_type: _typing.Any) -> None:
        """
        Initialize and validate the txtnewlines state.

        Example:
            Exercise TxtNewlines.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param newline_type: Value supplied for newline type under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.newline = self.NEWLINE_TYPES.get(newline_type.lower(), os.linesep)


def specified_newlines(newline: _typing.Any, text: _typing.Any) -> _typing.Any:
    # Convert all newlines to \n
    """
    Perform the specified newlines operation under explicit file-format and conversion rules.

    Example:
        Exercise specified newlines through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :param newline: Value supplied for newline under the utility contract.
    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = text.replace("\r\n", "\n")
    text = text.replace("\r", "\n")

    if newline == "\n":
        return text

    return text.replace("\n", newline)
