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
Group RTF tokens into normalized paragraph structures.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise paragraphs through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
"""
from __future__ import annotations

import typing as _typing
import sys, os

from LiuXin_alpha.file_formats.rtf2xml import copy
from LiuXin_alpha.utils.ptempfiles import better_mktemp
from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class Paragraphs:
    """
    Provide the paragraphs contract for validated ebook processing.

    Example:
        Exercise Paragraphs through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
    """

    def __init__(
        self: _typing.Self,
        in_file: _typing.Any,
        bug_handler: _typing.Any,
        copy: _typing.Any = None,
        write_empty_para: int = 1,
        run_level: int = 1,
    ) -> None:
        """
        Required: 'file'--file to parse Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) Returns: nothing

        Example:
            Exercise Paragraphs.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param in_file: Value supplied for in file under the utility contract.
        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param copy: Value supplied for copy under the utility contract.
        :param write_empty_para: Value supplied for write empty para under the utility
            contract.
        :param run_level: Value supplied for run level under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = in_file
        self.__bug_handler = bug_handler
        self.__copy = copy
        self.__write_empty_para = write_empty_para
        self.__run_level = run_level
        self.__write_to = better_mktemp()

    def __initiate_values(self: _typing.Self) -> None:
        """
        Initiate all values.

        Example:
            Exercise Paragraphs.  initiate values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "before_body"
        self.__start_marker = "mi<mk<para-start\n"  # outside para tags
        self.__start2_marker = "mi<mk<par-start_\n"  # inside para tags
        self.__end2_marker = "mi<mk<par-end___\n"  # inside para tags
        self.__end_marker = "mi<mk<para-end__\n"  # outside para tags
        self.__state_dict = {
            "before_body": self.__before_body_func,
            "not_paragraph": self.__not_paragraph_func,
            "paragraph": self.__paragraph_func,
        }
        self.__paragraph_dict = {
            "cw<pf<par-end___": self.__close_para_func,  # end of paragraph
            "mi<mk<headi_-end": self.__close_para_func,  # end of header or footer
            # 'cw<pf<par-def___'      : self.__close_para_func,   # paragraph definition
            # 'mi<mk<fld-bk-end'      : self.__close_para_func,   # end of field-block
            "mi<mk<fldbk-end_": self.__close_para_func,  # end of field-block
            "mi<mk<body-close": self.__close_para_func,  # end of body
            "mi<mk<sect-close": self.__close_para_func,  # end of body
            "mi<mk<sect-start": self.__close_para_func,  # start of section
            "mi<mk<foot___clo": self.__close_para_func,  # end of footnote
            "cw<tb<cell______": self.__close_para_func,  # end of cell
            "mi<mk<par-in-fld": self.__close_para_func,  # start of block field
            "cw<pf<par-def___": self.__bogus_para__def_func,  # paragraph definition
        }
        self.__not_paragraph_dict = {
            "tx<nu<__________": self.__start_para_func,
            "tx<hx<__________": self.__start_para_func,
            "tx<ut<__________": self.__start_para_func,
            "tx<mc<__________": self.__start_para_func,
            "mi<mk<inline-fld": self.__start_para_func,
            "mi<mk<para-beg__": self.__start_para_func,
            "cw<pf<par-end___": self.__empty_para_func,
            "mi<mk<pict-start": self.__start_para_func,
            "cw<pf<page-break": self.__empty_pgbk_func,  # page break
        }

    def __before_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: This function handles all the lines before the start of the body. Once the body starts, the state is switched to 'not_paragraph'

        Example:
            Exercise Paragraphs.  before body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<body-open_":
            self.__state = "not_paragraph"
        self.__write_obj.write(line)

    def __not_paragraph_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line to parse Returns: nothing Logic: This function handles all lines that are outside of the paragraph. It looks for clues that start a paragraph, and when found, switches states and writes the start tags.

        Example:
            Exercise Paragraphs.  not paragraph func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__not_paragraph_dict.get(self.__token_info)
        if action:
            action(line)
        self.__write_obj.write(line)

    def __paragraph_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line to parse Returns: nothing Logic: This function handles all the lines that are in the paragraph. It looks for clues to the end of the paragraph. When a clue is found, it calls on another method to write the end of the tag and change the state.

        Example:
            Exercise Paragraphs.  paragraph func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__paragraph_dict.get(self.__token_info)
        if action:
            action(line)
        else:
            self.__write_obj.write(line)

    def __start_para_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: This function writes the beginning tags for a paragraph and changes the state to paragraph.

        Example:
            Exercise Paragraphs.  start para func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write(self.__start_marker)  # marker for later parsing
        self.__write_obj.write("mi<tg<open______<para\n")
        self.__write_obj.write(self.__start2_marker)
        self.__state = "paragraph"

    def __empty_para_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: This function writes the empty tags for a paragraph. It does not do anything if self.__write_empty_para is 0.

        Example:
            Exercise Paragraphs.  empty para func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__write_empty_para:
            self.__write_obj.write(self.__start_marker)  # marker for later parsing
            self.__write_obj.write("mi<tg<empty_____<para\n")
            self.__write_obj.write(self.__end_marker)  # marker for later parsing

    def __empty_pgbk_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: This function writes the empty tags for a page break.

        Example:
            Exercise Paragraphs.  empty pgbk func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write("mi<tg<empty_____<page-break\n")

    def __close_para_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: This function writes the end tags for a paragraph and changes the state to not_paragraph.

        Example:
            Exercise Paragraphs.  close para func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write(self.__end2_marker)  # marker for later parser
        self.__write_obj.write("mi<tg<close_____<para\n")
        self.__write_obj.write(self.__end_marker)  # marker for later parser
        self.__write_obj.write(line)
        self.__state = "not_paragraph"

    def __bogus_para__def_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the bogus para def func operation under explicit file-format and conversion rules.

        Example:
            Exercise Paragraphs.  bogus para  def func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write("mi<mk<bogus-pard\n")

    def make_paragraphs(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: nothing (changes the original file) Logic: Read one line in at a time. Determine what action to take based on the state. If the state is before the body, look for the beginning of the body. When the body is found, change the state to 'not_paragraph'. The only other state is 'paragraph'.

        Example:
            Exercise Paragraphs.make paragraphs through a consuming regression::

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
                        try:
                            sys.stderr.write("no matching state in module paragraphs.py\n")
                            sys.stderr.write(self.__state + "\n")
                        except:
                            pass
                    action(line)
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "paragraphs.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
