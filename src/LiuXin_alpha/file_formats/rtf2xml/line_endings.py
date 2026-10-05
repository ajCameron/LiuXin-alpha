#########################################################################
#                                                                       #
#                                                                       #
#   copyright 2002 Paul Henry Tremblay                                  #
#                                                                       #
#   This program is distributed in the hope that it will be useful,     #
#   but WITHOUT ANY WARRANTY; without even the implied warranty of      #
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU    #
#   General Public License for more details.                            #
#                                                                       #
#                                                                       #
#########################################################################
"""
Normalize line-ending tokens in retained RTF intermediates.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise line endings through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.file_formats.rtf2xml import copy
from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars
from LiuXin_alpha.utils.ptempfiles import better_mktemp


class FixLineEndings:
    """
    Fix line endings

    Example:
        Exercise FixLineEndings through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
    """

    def __init__(
        self: _typing.Self,
        bug_handler: _typing.Any,
        in_file: _typing.Any = None,
        copy: _typing.Any = None,
        run_level: int = 1,
        replace_illegals: int = 1,
    ) -> None:
        """
        Initialize and validate the fixlineendings state.

        Example:
            Exercise FixLineEndings.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param in_file: Value supplied for in file under the utility contract.
        :param copy: Value supplied for copy under the utility contract.
        :param run_level: Value supplied for run level under the utility contract.
        :param replace_illegals: Value supplied for replace illegals under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = in_file
        self.__bug_handler = bug_handler
        self.__copy = copy
        self.__run_level = run_level
        self.__write_to = better_mktemp()
        self.__replace_illegals = replace_illegals

    def fix_endings(self: _typing.Self) -> None:
        # read
        """
        Perform the fix endings operation under explicit file-format and conversion rules.

        Example:
            Exercise FixLineEndings.fix endings through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with open(self.__file, "rb") as read_obj:
            input_file = read_obj.read()
        # calibre go from win and mac to unix
        input_file = input_file.replace(b"\r\n", b"\n")
        input_file = input_file.replace(b"\r", b"\n")
        # remove ASCII invalid chars : 0 to 8 and 11-14 to 24-26-27
        if self.__replace_illegals:
            # clean_ascii_chars returns text in the ported cleantext module.
            input_file = clean_ascii_chars(input_file, encoding="latin-1", errors="replace").encode(
                "latin-1", "replace"
            )
        # write
        with open(self.__write_to, "wb") as write_obj:
            write_obj.write(input_file)
        # copy
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "line_endings.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
