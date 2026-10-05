#!/usr/bin/env python


"""
Detect and validate the character encoding used by RTF input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise check encoding through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import sys


class CheckEncoding:
    """
    Provide the checkencoding contract for validated ebook processing.

    Example:
        Exercise CheckEncoding through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, bug_handler: _typing.Any) -> None:
        """
        Initialize and validate the checkencoding state.

        Example:
            Exercise CheckEncoding.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param bug_handler: Value supplied for bug handler under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__bug_handler = bug_handler

    def __get_position_error(self: _typing.Self, line: _typing.Any, encoding: _typing.Any, line_num: _typing.Any) -> None:
        """
        Perform the get position error operation under explicit file-format and conversion rules.

        Example:
            Exercise CheckEncoding.  get position error through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param line_num: Value supplied for line num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        char_position = 0
        for char in line:
            char_position += 1
            try:
                char.decode(encoding)
            except ValueError as msg:
                sys.stderr.write("line: %s char: %s\n%s\n" % (line_num, char_position, str(msg)))

    def check_encoding(self: _typing.Self, path: _typing.Any, encoding: str = "us-ascii", verbose: bool = True) -> bool:
        """
        Perform the check encoding operation under explicit file-format and conversion rules.

        Example:
            Exercise CheckEncoding.check encoding through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param encoding: Value supplied for encoding under the utility contract.
        :param verbose: Value supplied for verbose under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        line_num = 0
        with open(path, "rb") as read_obj:
            for line in read_obj:
                line_num += 1
                try:
                    line.decode(encoding)
                except ValueError:
                    if verbose:
                        if len(line) < 1000:
                            self.__get_position_error(line, encoding, line_num)
                        else:
                            sys.stderr.write("line: %d has bad encoding\n" % line_num)
                    return True
        return False


if __name__ == "__main__":
    check_encoding_obj = CheckEncoding()
    check_encoding_obj.check_encoding(sys.argv[1])
