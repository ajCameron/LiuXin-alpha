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
Process retained RTF preamble declarations after core tables.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise preamble rest through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
"""
from __future__ import annotations

import typing as _typing
import sys, os

from LiuXin_alpha.file_formats.rtf2xml import copy
from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class Preamble:
    """
    Fix the reamaing parts of the preamble. This module does very little. It makes sure that no text gets put in the revision of list table. In the future, when I understand how to interpret the revision table and list table, I will make these methods more functional.

    Example:
        Exercise Preamble through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
    """

    def __init__(
        self: _typing.Self,
        file: _typing.Any,
        bug_handler: _typing.Any,
        platform: _typing.Any,
        default_font: _typing.Any,
        code_page: _typing.Any,
        copy: _typing.Any = None,
        temp_dir: _typing.Any = None,
    ) -> None:
        """
        Required: file--file to parse platform --Windows or Macintosh default_font -- the default font code_page --the code page (ansi1252, for example) Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) Returns: nothing

        Example:
            Exercise Preamble.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param file: Value supplied for file under the utility contract.
        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param platform: Value supplied for platform under the utility contract.
        :param default_font: Value supplied for default font under the utility contract.
        :param code_page: Value supplied for code page under the utility contract.
        :param copy: Value supplied for copy under the utility contract.
        :param temp_dir: Value supplied for temp dir under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = file
        self.__bug_handler = bug_handler
        self.__copy = copy
        self.__default_font = default_font
        self.__code_page = code_page
        self.__platform = platform
        if temp_dir:
            self.__write_to = os.path.join(temp_dir, "info_table_info.data")
        else:
            self.__write_to = "info_table_info.data"

    def __initiate_values(self: _typing.Self) -> None:
        """
        Initiate all values.

        Example:
            Exercise Preamble.  initiate values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "default"
        self.__text_string = ""
        self.__state_dict = {
            "default": self.__default_func,
            "revision": self.__revision_table_func,
            "list_table": self.__list_table_func,
            "body": self.__body_func,
        }
        self.__default_dict = {
            "mi<mk<rtfhed-beg": self.__found_rtf_head_func,
            "mi<mk<listabbeg_": self.__found_list_table_func,
            "mi<mk<revtbl-beg": self.__found_revision_table_func,
            "mi<mk<body-open_": self.__found_body_func,
        }

    def __default_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the default func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  default func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__default_dict.get(self.__token_info)
        if action:
            action(line)
        else:
            self.__write_obj.write(line)

    def __found_rtf_head_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line -- the line to parse Returns: nothing. Logic: Write to the output file the default font info, the code page info, and the platform info.

        Example:
            Exercise Preamble.  found rtf head func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write(
            "mi<tg<empty-att_<rtf-definition"
            "<default-font>%s<code-page>%s"
            "<platform>%s\n" % (self.__default_font, self.__code_page, self.__platform)
        )

    def __found_list_table_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the found list table func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  found list table func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "list_table"

    def __list_table_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the list table func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  list table func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<listabend_":
            self.__state = "default"
        elif line[0:2] == "tx":
            pass
        else:
            self.__write_obj.write(line)

    def __found_revision_table_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the found revision table func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  found revision table func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "revision"

    def __revision_table_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the revision table func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  revision table func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<revtbl-end":
            self.__state = "default"
        elif line[0:2] == "tx":
            pass
        else:
            self.__write_obj.write(line)

    def __found_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the found body func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  found body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "body"
        self.__write_obj.write(line)

    def __body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the body func operation under explicit file-format and conversion rules.

        Example:
            Exercise Preamble.  body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write(line)

    def fix_preamble(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: nothing (changes the original file) Logic: Read one line in at a time. Determine what action to take based on the state. The state can either be default, the revision table, or the list table.

        Example:
            Exercise Preamble.fix preamble through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__initiate_values()
        with open_for_read(self.__file) as read_obj:
            with open_for_write(self.__write_to) as self.__write_obj:
                for line in read_obj:
                    self.__token_info = line[:16]
                    action = self.__state_dict.get(self.__state)
                    if action is None:
                        sys.stderr.write("no matching state in module preamble_rest.py\n" + self.__state + "\n")
                    action(line)
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "preamble_div.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
