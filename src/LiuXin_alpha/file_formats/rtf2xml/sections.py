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
Build document sections from RTF section-control tokens.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sections through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
"""
from __future__ import annotations

import typing as _typing
import sys, os

from LiuXin_alpha.file_formats.rtf2xml import copy
from LiuXin_alpha.utils.ptempfiles import better_mktemp

from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class Sections:
    """
    Provide the sections contract for validated ebook processing.

    Example:
        Exercise Sections through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
    """

    def __init__(self: _typing.Self, in_file: _typing.Any, bug_handler: _typing.Any, copy: _typing.Any = None, run_level: int = 1) -> None:
        """
        Required: 'file'--file to parse Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) Returns: nothing

        Example:
            Exercise Sections.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


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
            Exercise Sections.  initiate values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__mark_start = "mi<mk<sect-start\n"
        self.__mark_end = "mi<mk<sect-end__\n"
        self.__in_field = 0
        self.__section_values = {}
        self.__list_of_sec_values = []
        self.__field_num = []
        self.__section_num = 0
        self.__state = "before_body"
        self.__found_first_sec = 0
        self.__text_string = ""
        self.__field_instruction_string = ""
        self.__state_dict = {
            "before_body": self.__before_body_func,
            "body": self.__body_func,
            "before_first_sec": self.__before_first_sec_func,
            "section": self.__section_func,
            "section_def": self.__section_def_func,
            "sec_in_field": self.__sec_in_field_func,
        }
        # cw<sc<sect-defin<nu<true
        self.__body_dict = {
            "cw<sc<section___": self.__found_section_func,
            "mi<mk<sec-fd-beg": self.__found_sec_in_field_func,
            "cw<sc<sect-defin": self.__found_section_def_bef_sec_func,
        }
        self.__section_def_dict = {
            "cw<pf<par-def___": (self.__end_sec_def_func, None),
            "mi<mk<body-open_": (self.__end_sec_def_func, None),
            "cw<tb<columns___": (self.__attribute_func, "columns"),
            "cw<pa<margin-lef": (self.__attribute_func, "margin-left"),
            "cw<pa<margin-rig": (self.__attribute_func, "margin-right"),
            "mi<mk<header-ind": (self.__end_sec_def_func, None),
            # premature endings
            # __end_sec_premature_func
            "tx<nu<__________": (self.__end_sec_premature_func, None),
            "cw<ci<font-style": (self.__end_sec_premature_func, None),
            "cw<ci<font-size_": (self.__end_sec_premature_func, None),
        }
        self.__sec_in_field_dict = {
            "mi<mk<sec-fd-end": self.__end_sec_in_field_func,
            # changed this 2004-04-26
            # two lines
            # 'cw<sc<section___'      : self.__found_section_in_field_func,
            # 'cw<sc<sect-defin'      : self.__found_section_def_in_field_func,
        }

    def __found_section_def_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- the line to parse Returns: nothing Logic: I have found a section definition. Change the state to setion_def (so subsequent lines will be processesed as part of the section definition), and clear the section_values dictionary.

        Example:
            Exercise Sections.  found section def func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "section_def"
        self.__section_values.clear()

    def __attribute_func(self: _typing.Self, line: _typing.Any, name: _typing.Any) -> None:
        """
        Required: line -- the line to be parsed name -- the changed, readable name (as opposed to the abbreviated one) Returns: nothing Logic: I need to add the right data to the section values dictionary so I can retrieve it later. The attribute (or key) is the name; the value is the last part of the text string. ex: cw<tb<columns___<nu<2

        Example:
            Exercise Sections.  attribute func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        attribute = name
        value = line[20:-1]
        self.__section_values[attribute] = value

    def __found_section_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line -- the line to parse Returns: nothing Logic: I have found the beginning of a section, so change the state accordingly. Also add one to the section counter.

        Example:
            Exercise Sections.  found section func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "section"
        self.__write_obj.write(line)
        self.__section_num += 1

    def __found_section_def_bef_sec_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line -- the line to parse Returns: nothing Logic: I have found the beginning of a section, so change the state accordingly. Also add one to the section counter.

        Example:
            Exercise Sections.  found section def bef sec func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__section_num += 1
        self.__found_section_def_func(line)
        self.__write_obj.write(line)

    def __section_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --the line to parse Returns: nothing Logic:

        Example:
            Exercise Sections.  section func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "cw<sc<sect-defin":
            self.__found_section_def_func(line)
        self.__write_obj.write(line)

    def __section_def_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line to parse Returns: nothing Logic: I have found a section definition. Check if the line is the end of the defnition (a paragraph definition), or if it contains info that should be added to the values dictionary. If neither of these cases are true, output the line to a file.

        Example:
            Exercise Sections.  section def func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action, name = self.__section_def_dict.get(self.__token_info, (None, None))
        if action:
            action(line, name)
            if self.__in_field:
                self.__sec_in_field_string += line
            else:
                self.__write_obj.write(line)
        else:
            self.__write_obj.write(line)

    def __end_sec_def_func(self: _typing.Self, line: _typing.Any, name: _typing.Any) -> None:
        """
        Requires: line --the line to parse name --changed, readable name Returns: nothing Logic: The end of the section definition has been found. Reset the state. Call on the write_section method.

        Example:
            Exercise Sections.  end sec def func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.__in_field:
            self.__state = "body"
        else:
            self.__state = "sec_in_field"
        self.__write_section(line)

    def __end_sec_premature_func(self: _typing.Self, line: _typing.Any, name: _typing.Any) -> None:
        """
        Perform the end sec premature func operation under explicit file-format and conversion rules.

        Example:
            Exercise Sections.  end sec premature func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.__in_field:
            self.__state = "body"
        else:
            self.__state = "sec_in_field"
        self.__write_section(line)
        self.__write_obj.write("cw<pf<par-def___<nu<true\n")
        self.__write_obj.write("ob<nu<open-brack<0000\n")
        self.__write_obj.write("cb<nu<clos-brack<0000\n")

    def __write_section(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: nothing Returns: nothing Logic: Form a string of attributes and values. If you are not in a field block, write this string to the output file. Otherwise, call on the handle_sec_def method to handle this string.

        Example:
            Exercise Sections.  write section through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        my_string = self.__mark_start
        if self.__found_first_sec:
            my_string += "mi<tg<close_____<section\n"
        else:
            self.__found_first_sec = 1
        my_string += "mi<tg<open-att__<section<num>%s" % str(self.__section_num)
        my_string += "<num-in-level>%s" % str(self.__section_num)
        my_string += "<type>rtf-native"
        my_string += "<level>0"
        keys = self.__section_values.keys()
        if len(keys) > 0:
            for key in keys:
                my_string += f"<{key}>{self.__section_values[key]}"
        my_string += "\n"
        my_string += self.__mark_end
        # # my_string += line
        if self.__state == "body":
            self.__write_obj.write(my_string)
        elif self.__state == "sec_in_field":
            self.__handle_sec_def(my_string)
        elif self.__run_level > 3:
            msg = "missed a flag\n"
            raise self.__bug_handler(msg)

    def __handle_sec_def(self: _typing.Self, my_string: _typing.Any) -> None:
        """
        Requires: my_string -- the string of attributes and values. (Do I need this?) Returns: nothing Logic: I need to append the dictionary of attributes and values to list so I can use it later when I reach the end of the field-block.

        Example:
            Exercise Sections.  handle sec def through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param my_string: Value supplied for my string under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        values_dict = self.__section_values
        self.__list_of_sec_values.append(values_dict)

    def __body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --the line to parse Returns: nothing Logic: Look for the beginning of a section. Otherwise, print the line to the output file.

        Example:
            Exercise Sections.  body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__body_dict.get(self.__token_info)
        if action:
            action(line)
        else:
            self.__write_obj.write(line)

    def __before_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Look for the beginning of the body. Always print out the line.

        Example:
            Exercise Sections.  before body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<body-open_":
            self.__state = "before_first_sec"
        self.__write_obj.write(line)

    def __before_first_sec_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the before first sec func operation under explicit file-format and conversion rules.

        Example:
            Exercise Sections.  before first sec func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "cw<sc<sect-defin":
            self.__state = "section_def"
            self.__section_num += 1
            self.__section_values.clear()
        elif self.__token_info == "cw<pf<par-def___":
            self.__state = "body"
            self.__section_num += 1
            self.__write_obj.write(
                "mi<tg<open-att__<section<num>%s"
                "<num-in-level>%s"
                "<type>rtf-native"
                "<level>0\n" % (str(self.__section_num), str(self.__section_num))
            )
            self.__found_first_sec = 1
        elif self.__token_info == "tx<nu<__________":
            self.__state = "body"
            self.__section_num += 1
            self.__write_obj.write(
                "mi<tg<open-att__<section<num>%s"
                "<num-in-level>%s"
                "<type>rtf-native"
                "<level>0\n" % (str(self.__section_num), str(self.__section_num))
            )
            self.__write_obj.write("cw<pf<par-def___<true\n")
            self.__found_first_sec = 1
        self.__write_obj.write(line)

    def __found_sec_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: I have found the beginning of a field that has a section (or really, two) inside of it. Change the state, and start adding to one long string.

        Example:
            Exercise Sections.  found sec in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "sec_in_field"
        self.__sec_in_field_string = line
        self.__in_field = 1

    def __sec_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --the line to parse Returns: nothing Logic: Check for the end of the field, or the beginning of a section definition. CHANGED! Just print out each line. Ignore any sections or section definition info.

        Example:
            Exercise Sections.  sec in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__sec_in_field_dict.get(self.__token_info)
        if action:
            action(line)
        else:
            # change this 2004-04-26
            # self.__sec_in_field_string += line
            self.__write_obj.write(line)

    def __end_sec_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: Add the last line to the field string. Call on the method print_field_sec_attributes to write the close and beginning of a section tag. Print out the field string. Call on the same method to again write the close and beginning of a section tag. Change the state.

        Example:
            Exercise Sections.  end sec in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # change this 2004-04-26
        # Don't do anything
        """
        self.__sec_in_field_string += line
        self.__print_field_sec_attributes()
        self.__write_obj.write(self.__sec_in_field_string)
        self.__print_field_sec_attributes()
        """
        self.__state = "body"
        self.__in_field = 0
        # this is changed too
        self.__write_obj.write(line)

    def __print_field_sec_attributes(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: nothing Logic: Get the number and dictionary of values from the lists. The number and dictionary will be the first item of each list. Write the close tag. Write the start tag. Write the attribute and values in the dictionary. Get rid of the first item in each list. keys = self.__section_values.keys() if len(keys) > 0: my_string += 'mi<tg<open-att__<section-definition' for key in keys: my_string += '<%s>%s' % (key, self.__section_values[key]) my_string += ' ' else: my_string += 'mi<tg<open______<section-definition '

        Example:
            Exercise Sections.  print field sec attributes through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        num = self.__field_num[0]
        self.__field_num = self.__field_num[1:]
        self.__write_obj.write("mi<tg<close_____<section\n" "mi<tg<open-att__<section<num>%s" % str(num))
        if self.__list_of_sec_values:
            keys = self.__list_of_sec_values[0].keys()
            for key in keys:
                self.__write_obj.write(f"<{key}>{self.__list_of_sec_values[0][key]}\n")
            self.__list_of_sec_values = self.__list_of_sec_values[1:]
        self.__write_obj.write("<level>0")
        self.__write_obj.write("<type>rtf-native")
        self.__write_obj.write("<num-in-level>%s" % str(self.__section_num))
        self.__write_obj.write("\n")
        # Look here

    def __found_section_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: I have found a section in a field block. Add one to section counter, and append this number to a list.

        Example:
            Exercise Sections.  found section in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__section_num += 1
        self.__field_num.append(self.__section_num)
        self.__sec_in_field_string += line

    def __found_section_def_in_field_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Requires: line --line to parse Returns: nothing Logic: I have found a section definition in a filed block. Change the state and clear the values dictionary.

        Example:
            Exercise Sections.  found section def in field func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "section_def"
        self.__section_values.clear()

    def make_sections(self: _typing.Self) -> None:
        """
        Requires: nothing Returns: nothing (changes the original file) Logic: Read one line in at a time. Determine what action to take based on the state. If the state is before the body, look for the beginning of the body. If the state is body, send the line to the body method.

        Example:
            Exercise Sections.make sections through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


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
            action = self.__state_dict.get(self.__state)
            if action is None:
                sys.stderr.write("no matching state in module sections.py\n")
                sys.stderr.write(self.__state + "\n")
            action(line)
        read_obj.close()
        self.__write_obj.close()
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "sections.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
