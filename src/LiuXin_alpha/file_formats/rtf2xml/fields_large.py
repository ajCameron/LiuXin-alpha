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
Process multi-token RTF fields and their nested results.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fields large through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import sys
import os
from LiuXin_alpha.file_formats.rtf2xml import field_strings, copy
from LiuXin_alpha.utils.ptempfiles import better_mktemp
from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class FieldsLarge:
    """
    Provide the fieldslarge contract for validated ebook processing.

    Example:
        Exercise FieldsLarge through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """

    def __init__(
        self: _typing.Self,
        in_file: _typing.Any,
        bug_handler: _typing.Any,
        copy: _typing.Any = None,
        run_level: int = 1,
    ) -> None:
        """
        Required: 'file'--file to parse Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) Returns: nothing

        Example:
            Exercise FieldsLarge.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param in_file: Value supplied for in file under the utility contract.
        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param copy: Value supplied for copy under the utility contract.
        :param run_level: Value supplied for run level under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = in_file
        self.__bug_handler = bug_handler
        self.__copy = copy
        self.__run_level = run_level
        self.__write_to = better_mktemp()

    def __initiate_values(self: _typing.Self) -> None:
        """
        Initiate all values.

        Example:
            Exercise FieldsLarge.  initiate values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__text_string = ""
        self.__field_instruction_string = ""
        self.__marker = "mi<mk<inline-fld\n"
        self.__state = "before_body"
        self.__string_obj = field_strings.FieldStrings(
            run_level=self.__run_level,
            bug_handler=self.__bug_handler,
        )
        self.__state_dict = {
            "before_body": self.__before_body_func,
            "in_body": self.__in_body_func,
            "field": self.__in_field_func,
            "field_instruction": self.__field_instruction_func,
        }
        self.__in_body_dict = {
            "cw<fd<field_____": self.__found_field_func,
        }
        self.__field_dict = {
            "cw<fd<field-inst": self.__found_field_instruction_func,
            "cw<fd<field_____": self.__found_field_func,
            "cw<pf<par-end___": self.__par_in_field_func,
            "cw<sc<section___": self.__sec_in_field_func,
        }
        self.__field_count = []  # keep track of the brackets
        self.__field_instruction = []  # field instruction strings
        self.__symbol = 0  # whether or not the field is really UTF-8
        # (these fields cannot be nested.)
        self.__field_instruction_string = ""  # string that collects field instruction
        self.__par_in_field = []  # paragraphs in field?
        self.__sec_in_field = []  # sections in field?
        self.__field_string = []  # list of field strings

    def __before_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line ro parse Returns: nothing (changes an instant and writes a line) Logic: Check for the beginninf of the body. If found, changed the state. Always write out the line.

        Example:
            Exercise FieldsLarge.  before body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<body-open_":
            self.__state = "in_body"
        self.__write_obj.write(line)

    def __in_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line to parse Returns: nothing. (Writes a line to the output file, or performs other actions.) Logic: Check of the beginning of a field. Always output the line.

        Example:
            Exercise FieldsLarge.  in body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__in_body_dict.get(self.__token_info)
        if action:
            action(line)
        self.__write_obj.write(line)

    def __found_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Set the values for parsing the field. Four lists have to have items appended to them.

        Example:
            Exercise FieldsLarge.  found field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "field"
        self.__cb_count = 0
        ob_count = self.__ob_count
        self.__field_string.append("")
        self.__field_count.append(ob_count)
        self.__sec_in_field.append(0)
        self.__par_in_field.append(0)

    def __in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing. Logic: Check for the end of the field; a paragraph break; a section break; the beginning of another field; or the beginning of the field instruction.

        Example:
            Exercise FieldsLarge.  in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__cb_count == self.__field_count[-1]:
            self.__field_string[-1] += line
            self.__end_field_func()
        else:
            action = self.__field_dict.get(self.__token_info)
            if action:
                action(line)
            else:
                self.__field_string[-1] += line

    def __par_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Write the line to the output file and set the last item in the paragraph in field list to true.

        Example:
            Exercise FieldsLarge.  par in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__field_string[-1] += line
        self.__par_in_field[-1] = 1

    def __sec_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Write the line to the output file and set the last item in the section in field list to true.

        Example:
            Exercise FieldsLarge.  sec in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__field_string[-1] += line
        self.__sec_in_field[-1] = 1

    def __found_field_instruction_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line -- line to parse Returns: nothing Change the state to field instruction. Set the open bracket count of the beginning of this field so you know when it ends. Set the closed bracket count to 0 so you don't prematureley exit this state.

        Example:
            Exercise FieldsLarge.  found field instruction func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "field_instruction"
        self.__field_instruction_count = self.__ob_count
        self.__cb_count = 0

    def __field_instruction_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Collect all the lines until the end of the field is reached. Process these lines with the module rtr.field_strings. Check if the field instruction is 'Symbol' (really UTF-8).

        Example:
            Exercise FieldsLarge.  field instruction func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__cb_count == self.__field_instruction_count:
            # The closing bracket should be written, since the opening bracket
            # was written
            self.__field_string[-1] += line
            my_list = self.__string_obj.process_string(self.__field_instruction_string, "field_instruction")
            instruction = my_list[2]
            self.__field_instruction.append(instruction)
            if my_list[0] == "Symbol":
                self.__symbol = 1
            self.__state = "field"
            self.__field_instruction_string = ""
        else:
            self.__field_instruction_string += line

    def __end_field_func(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: Nothing Logic: Pop the last values in the instructions list, the fields list, the paragraph list, and the section list. If the field is a symbol, do not write the tags <field></field>, since this field is really just UTF-8. If the field contains paragraph or section breaks, it is a field-block rather than just a field. Write the paragraph or section markers for later parsing of the file. If the filed list contains more strings, add the latest (processed) string to the last string in the list. Otherwise, write the string to the output file.

        Example:
            Exercise FieldsLarge.  end field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        last_bracket = self.__field_count.pop()
        instruction = self.__field_instruction.pop()
        inner_field_string = self.__field_string.pop()
        sec_in_field = self.__sec_in_field.pop()
        par_in_field = self.__par_in_field.pop()
        # add a closing bracket, since the closing bracket is not included in
        # the field string
        if self.__symbol:
            inner_field_string = "%scb<nu<clos-brack<%s\n" % (instruction, last_bracket)
        elif sec_in_field or par_in_field:
            inner_field_string = (
                "mi<mk<fldbkstart\n"
                "mi<tg<open-att__<field-block<type>%s\n%s"
                "mi<mk<fldbk-end_\n"
                "mi<tg<close_____<field-block\n"
                "mi<mk<fld-bk-end\n" % (instruction, inner_field_string)
            )
        # write a marker to show an inline field for later parsing
        else:
            inner_field_string = (
                "%s"
                "mi<tg<open-att__<field<type>%s\n%s"
                "mi<tg<close_____<field\n" % (self.__marker, instruction, inner_field_string)
            )
        if sec_in_field:
            inner_field_string = "mi<mk<sec-fd-beg\n" + inner_field_string + "mi<mk<sec-fd-end\n"
        if par_in_field:
            inner_field_string = "mi<mk<par-in-fld\n" + inner_field_string
        if len(self.__field_string) == 0:
            self.__write_field_string(inner_field_string)
        else:
            self.__field_string[-1] += inner_field_string
        self.__symbol = 0

    def __write_field_string(self: _typing.Self, the_string: _typing.Any) -> None:
        """
        Perform the write field string operation under explicit file-format and conversion rules.

        Example:
            Exercise FieldsLarge.  write field string through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param the_string: Value supplied for the string under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "in_body"
        self.__write_obj.write(the_string)

    def fix_fields(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: nothing (changes the original file) Logic: Read one line in at a time. Determine what action to take based on the state. If the state is before the body, look for the beginning of the body. If the state is body, send the line to the body method.

        Example:
            Exercise FieldsLarge.fix fields through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__initiate_values()
        read_obj = open_for_read(self.__file)
        self.__write_obj = open_for_write(self.__write_to)
        line_to_read = 1
        while line_to_read:
            line_to_read = read_obj.readline()
            line = line_to_read
            self.__token_info = line[:16]
            if self.__token_info == "ob<nu<open-brack":
                self.__ob_count = line[-5:-1]
            if self.__token_info == "cb<nu<clos-brack":
                self.__cb_count = line[-5:-1]
            action = self.__state_dict.get(self.__state)
            if action is None:
                sys.stderr.write("no no matching state in module styles.py\n")
                sys.stderr.write(self.__state + "\n")
            action(line)
        read_obj.close()
        self.__write_obj.close()
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "fields_large.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
