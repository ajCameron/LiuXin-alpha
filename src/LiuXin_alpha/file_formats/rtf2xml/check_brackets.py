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
Validate balanced structural markers in tokenized RTF input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise check brackets through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
from LiuXin_alpha.file_formats.rtf2xml import open_for_read


class CheckBrackets:
    """
    Check that brackets match up

    Example:
        Exercise CheckBrackets through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """

    def __init__(self: _typing.Self, bug_handler: _typing.Any = None, file: _typing.Any = None) -> None:
        """
        Initialize and validate the checkbrackets state.

        Example:
            Exercise CheckBrackets.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param file: Value supplied for file under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = file
        self.__bug_handler = bug_handler
        self.__bracket_count = 0
        self.__ob_count = 0
        self.__cb_count = 0
        self.__open_bracket_num = []

    def open_brack(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the open brack operation under explicit file-format and conversion rules.

        Example:
            Exercise CheckBrackets.open brack through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        num = line[-5:-1]
        self.__open_bracket_num.append(num)
        self.__bracket_count += 1

    def close_brack(self: _typing.Self, line: _typing.Any) -> bool:
        """
        Perform the close brack operation under explicit file-format and conversion rules.

        Example:
            Exercise CheckBrackets.close brack through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        num = line[-5:-1]
        try:
            last_num = self.__open_bracket_num.pop()
        except:
            return False
        if num != last_num:
            return False
        self.__bracket_count -= 1
        return True

    def check_brackets(self: _typing.Self) -> tuple[_typing.Any, ...]:
        """
        Perform the check brackets operation under explicit file-format and conversion rules.

        Example:
            Exercise CheckBrackets.check brackets through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        line_count = 0
        with open_for_read(self.__file) as read_obj:
            for line in read_obj:
                line_count += 1
                self.__token_info = line[:16]
                if self.__token_info == "ob<nu<open-brack":
                    self.open_brack(line)
                if self.__token_info == "cb<nu<clos-brack":
                    if not self.close_brack(line):
                        return (False, "closed bracket doesn't match, line %s" % line_count)

        if self.__bracket_count != 0:
            msg = (
                "At end of file open and closed brackets don't match\n" "total number of brackets is %s"
            ) % self.__bracket_count
            return (False, msg)
        return (True, "Brackets match!")
