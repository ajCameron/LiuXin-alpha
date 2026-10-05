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
Decode RTF hexadecimal escapes into normalized Unicode text.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise hex 2 utf8 through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
"""
from __future__ import annotations

import typing as _typing
import sys, os, io

from LiuXin_alpha.file_formats.rtf2xml import get_char_map, copy
from LiuXin_alpha.file_formats.rtf2xml.char_set import char_set
from LiuXin_alpha.utils.ptempfiles import better_mktemp

from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class Hex2Utf8:
    """
    Convert Microsoft hexadecimal numbers to utf-8

    Example:
        Exercise Hex2Utf8 through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py
    """

    def __init__(
        self: _typing.Self,
        in_file: _typing.Any,
        area_to_convert: _typing.Any,
        char_file: _typing.Any,
        default_char_map: _typing.Any,
        bug_handler: _typing.Any,
        invalid_rtf_handler: _typing.Any,
        copy: _typing.Any = None,
        temp_dir: _typing.Any = None,
        symbol: _typing.Any = None,
        wingdings: _typing.Any = None,
        caps: _typing.Any = None,
        convert_caps: _typing.Any = None,
        dingbats: _typing.Any = None,
        run_level: int = 1,
    ) -> None:
        """
        Required: 'file' 'area_to_convert'--the area of file to convert 'char_file'--the file containing the character mappings 'default_char_map'--name of default character map Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) 'symbol'--whether to load the symbol character map 'winddings'--whether to load the wingdings character map 'caps'--whether to load the caps character map 'convert_to_caps'--wether to convert caps to utf-8 Returns: nothing

        Example:
            Exercise Hex2Utf8.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param in_file: Value supplied for in file under the utility contract.
        :param area_to_convert: Value supplied for area to convert under the utility
            contract.
        :param char_file: Value supplied for char file under the utility contract.
        :param default_char_map: Value supplied for default char map under the utility
            contract.
        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param invalid_rtf_handler: Value supplied for invalid rtf handler under the utility
            contract.
        :param copy: Value supplied for copy under the utility contract.
        :param temp_dir: Value supplied for temp dir under the utility contract.
        :param symbol: Value supplied for symbol under the utility contract.
        :param wingdings: Value supplied for wingdings under the utility contract.
        :param caps: Value supplied for caps under the utility contract.
        :param convert_caps: Value supplied for convert caps under the utility contract.
        :param dingbats: Value supplied for dingbats under the utility contract.
        :param run_level: Value supplied for run level under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = in_file
        self.__copy = copy
        if area_to_convert not in ("preamble", "body"):
            msg = (
                "Developer error! Wrong flag.\n"
                'in module "hex_2_utf8.py\n'
                '"area_to_convert" must be "body" or "preamble"\n'
            )
            raise self.__bug_handler(msg)
        self.__char_file = char_file
        self.__area_to_convert = area_to_convert
        self.__default_char_map = default_char_map
        self.__symbol = symbol
        self.__wingdings = wingdings
        self.__dingbats = dingbats
        self.__caps = caps
        self.__convert_caps = 0
        self.__convert_symbol = 0
        self.__convert_wingdings = 0
        self.__convert_zapf = 0
        self.__run_level = run_level
        self.__write_to = better_mktemp()
        self.__bug_handler = bug_handler
        self.__invalid_rtf_handler = invalid_rtf_handler

    def update_values(
        self: _typing.Self,
        file: _typing.Any,
        area_to_convert: _typing.Any,
        char_file: _typing.Any,
        convert_caps: _typing.Any,
        convert_symbol: _typing.Any,
        convert_wingdings: _typing.Any,
        convert_zapf: _typing.Any,
        copy: _typing.Any = None,
        temp_dir: _typing.Any = None,
        symbol: _typing.Any = None,
        wingdings: _typing.Any = None,
        caps: _typing.Any = None,
        dingbats: _typing.Any = None,
    ) -> None:
        """
        Required: 'file' 'area_to_convert'--the area of file to convert 'char_file'--the file containing the character mappings Optional: 'copy'-- whether to make a copy of result for debugging 'temp_dir' --where to output temporary results (default is directory from which the script is run.) 'symbol'--whether to load the symbol character map 'winddings'--whether to load the wingdings character map 'caps'--whether to load the caps character map 'convert_to_caps'--wether to convert caps to utf-8 Returns: nothing

        Example:
            Exercise Hex2Utf8.update values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param file: Value supplied for file under the utility contract.
        :param area_to_convert: Value supplied for area to convert under the utility
            contract.
        :param char_file: Value supplied for char file under the utility contract.
        :param convert_caps: Value supplied for convert caps under the utility contract.
        :param convert_symbol: Value supplied for convert symbol under the utility contract.
        :param convert_wingdings: Value supplied for convert wingdings under the utility
            contract.
        :param convert_zapf: Value supplied for convert zapf under the utility contract.
        :param copy: Value supplied for copy under the utility contract.
        :param temp_dir: Value supplied for temp dir under the utility contract.
        :param symbol: Value supplied for symbol under the utility contract.
        :param wingdings: Value supplied for wingdings under the utility contract.
        :param caps: Value supplied for caps under the utility contract.
        :param dingbats: Value supplied for dingbats under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__file = file
        self.__copy = copy
        if area_to_convert not in ("preamble", "body"):
            msg = 'in module "hex_2_utf8.py\n' '"area_to_convert" must be "body" or "preamble"\n'
            raise self.__bug_handler(msg)
        self.__area_to_convert = area_to_convert
        self.__symbol = symbol
        self.__wingdings = wingdings
        self.__dingbats = dingbats
        self.__caps = caps
        self.__convert_caps = convert_caps
        self.__convert_symbol = convert_symbol
        self.__convert_wingdings = convert_wingdings
        self.__convert_zapf = convert_zapf
        # new!
        # no longer try to convert these
        # self.__convert_symbol = 0
        # self.__convert_wingdings = 0
        # self.__convert_zapf = 0

    def __initiate_values(self: _typing.Self) -> None:
        """
        Required: Nothing Set values, including those for the dictionaries. The file that contains the maps is broken down into many different sets. For example, for the Symbol font, there is the standard part for hexadecimal numbers, and the part for Microsoft characters. Read each part in, and then combine them.

        Example:
            Exercise Hex2Utf8.  initiate values through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # the default encoding system, the lower map for characters 0 through
        # 128, and the encoding system for Microsoft characters.
        # New on 2004-05-8: the self.__char_map is not in directory with other
        # modules
        self.__char_file = io.StringIO(char_set)
        char_map_obj = get_char_map.GetCharMap(
            char_file=self.__char_file,
            bug_handler=self.__bug_handler,
        )
        up_128_dict = char_map_obj.get_char_map(map=self.__default_char_map)
        bt_128_dict = char_map_obj.get_char_map(map="bottom_128")
        ms_standard_dict = char_map_obj.get_char_map(map="ms_standard")
        self.__def_dict = {}
        self.__def_dict.update(up_128_dict)
        self.__def_dict.update(bt_128_dict)
        self.__def_dict.update(ms_standard_dict)
        self.__current_dict = self.__def_dict
        self.__current_dict_name = "default"
        self.__in_caps = 0
        self.__special_fonts_found = 0
        if self.__symbol:
            symbol_base_dict = char_map_obj.get_char_map(map="SYMBOL")
            ms_symbol_dict = char_map_obj.get_char_map(map="ms_symbol")
            self.__symbol_dict = {}
            self.__symbol_dict.update(symbol_base_dict)
            self.__symbol_dict.update(ms_symbol_dict)
        if self.__wingdings:
            wingdings_base_dict = char_map_obj.get_char_map(map="wingdings")
            ms_wingdings_dict = char_map_obj.get_char_map(map="ms_wingdings")
            self.__wingdings_dict = {}
            self.__wingdings_dict.update(wingdings_base_dict)
            self.__wingdings_dict.update(ms_wingdings_dict)
        if self.__dingbats:
            dingbats_base_dict = char_map_obj.get_char_map(map="dingbats")
            ms_dingbats_dict = char_map_obj.get_char_map(map="ms_dingbats")
            self.__dingbats_dict = {}
            self.__dingbats_dict.update(dingbats_base_dict)
            self.__dingbats_dict.update(ms_dingbats_dict)
        # load dictionary for caps, and make a string for the replacement
        self.__caps_uni_dict = char_map_obj.get_char_map(map="caps_uni")
        # # print self.__caps_uni_dict
        # don't think I'll need this
        # keys = self.__caps_uni_dict.keys()
        # self.__caps_uni_replace = '|'.join(keys)
        self.__preamble_state_dict = {
            "preamble": self.__preamble_func,
            "body": self.__body_func,
            "mi<mk<body-open_": self.__found_body_func,
            "tx<hx<__________": self.__hex_text_func,
        }
        self.__body_state_dict = {
            "preamble": self.__preamble_for_body_func,
            "body": self.__body_for_body_func,
        }
        self.__in_body_dict = {
            "mi<mk<body-open_": self.__found_body_func,
            "tx<ut<__________": self.__utf_to_caps_func,
            "tx<hx<__________": self.__hex_text_func,
            "tx<mc<__________": self.__hex_text_func,
            "tx<nu<__________": self.__text_func,
            "mi<mk<font______": self.__start_font_func,
            "mi<mk<caps______": self.__start_caps_func,
            "mi<mk<font-end__": self.__end_font_func,
            "mi<mk<caps-end__": self.__end_caps_func,
        }
        self.__caps_list = ["false"]
        self.__font_list = ["not-defined"]

    def __hex_text_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: 'line' -- the line Logic: get the hex_num and look it up in the default dictionary. If the token is in the dictionary, then check if the value starts with a "&". If it does, then tag the result as utf text. Otherwise, tag it as normal text. If the hex_num is not in the dictionary, then a mistake has been made.

        Example:
            Exercise Hex2Utf8.  hex text func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        hex_num = line[17:-1]
        converted = self.__current_dict.get(hex_num)
        if converted is not None:
            # tag as utf-8
            if converted[0:1] == "&":
                font = self.__current_dict_name
                if (
                    self.__convert_caps
                    and self.__caps_list[-1] == "true"
                    and font not in ("Symbol", "Wingdings", "Zapf Dingbats")
                ):
                    converted = self.__utf_token_to_caps_func(converted)
                self.__write_obj.write("tx<ut<__________<%s\n" % converted)
            # tag as normal text
            else:
                font = self.__current_dict_name
                if (
                    self.__convert_caps
                    and self.__caps_list[-1] == "true"
                    and font not in ("Symbol", "Wingdings", "Zapf Dingbats")
                ):
                    converted = converted.upper()
                self.__write_obj.write("tx<nu<__________<%s\n" % converted)
        # error
        else:
            token = hex_num.replace("'", "")
            the_num = 0
            if token:
                the_num = int(token, 16)
            if the_num > 10:
                self.__write_obj.write("mi<tg<empty-att_<udef_symbol<num>%s<description>not-in-table\n" % hex_num)
                if self.__run_level > 4:
                    # msg = 'no dictionary entry for %s\n'
                    # msg += 'the hexadecimal num is "%s"\n' % (hex_num)
                    # msg += 'dictionary is %s\n' % self.__current_dict_name
                    msg = 'Character "&#x%s;" does not appear to be valid (or is a control character)\n' % token
                    raise self.__bug_handler(msg)

    def __found_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the found body func operation under explicit file-format and conversion rules.

        Example:
            Exercise Hex2Utf8.  found body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "body"
        self.__write_obj.write(line)

    def __body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        When parsing preamble

        Example:
            Exercise Hex2Utf8.  body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__write_obj.write(line)

    def __preamble_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Perform the preamble func operation under explicit file-format and conversion rules.

        Example:
            Exercise Hex2Utf8.  preamble func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__preamble_state_dict.get(self.__token_info)
        if action is not None:
            action(line)
        else:
            self.__write_obj.write(line)

    def __convert_preamble(self: _typing.Self) -> None:
        """
        Perform the convert preamble operation under explicit file-format and conversion rules.

        Example:
            Exercise Hex2Utf8.  convert preamble through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "preamble"
        with open_for_write(self.__write_to) as self.__write_obj:
            with open_for_read(self.__file) as read_obj:
                for line in read_obj:
                    self.__token_info = line[:16]
                    action = self.__preamble_state_dict.get(self.__state)
                    if action is None:
                        sys.stderr.write("error no state found in hex_2_utf8", self.__state)
                    action(line)
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "preamble_utf_convert.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)

    def __preamble_for_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: Used when parsing the body.

        Example:
            Exercise Hex2Utf8.  preamble for body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.__token_info == "mi<mk<body-open_":
            self.__found_body_func(line)
        self.__write_obj.write(line)

    def __body_for_body_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: Used when parsing the body.

        Example:
            Exercise Hex2Utf8.  body for body func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        action = self.__in_body_dict.get(self.__token_info)
        if action is not None:
            action(line)
        else:
            self.__write_obj.write(line)

    def __start_font_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: add font face to font_list

        Example:
            Exercise Hex2Utf8.  start font func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        face = line[17:-1]
        self.__font_list.append(face)
        if face == "Symbol" and self.__convert_symbol:
            self.__current_dict_name = "Symbol"
            self.__current_dict = self.__symbol_dict
        elif face == "Wingdings" and self.__convert_wingdings:
            self.__current_dict_name = "Wingdings"
            self.__current_dict = self.__wingdings_dict
        elif face == "Zapf Dingbats" and self.__convert_zapf:
            self.__current_dict_name = "Zapf Dingbats"
            self.__current_dict = self.__dingbats_dict
        else:
            self.__current_dict_name = "default"
            self.__current_dict = self.__def_dict

    def __end_font_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: pop font_list

        Example:
            Exercise Hex2Utf8.  end font func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.__font_list) > 1:
            self.__font_list.pop()
        else:
            sys.stderr.write("module is hex_2_utf8\n")
            sys.stderr.write("method is end_font_func\n")
            sys.stderr.write("self.__font_list should be greater than one?\n")
        face = self.__font_list[-1]
        if face == "Symbol" and self.__convert_symbol:
            self.__current_dict_name = "Symbol"
            self.__current_dict = self.__symbol_dict
        elif face == "Wingdings" and self.__convert_wingdings:
            self.__current_dict_name = "Wingdings"
            self.__current_dict = self.__wingdings_dict
        elif face == "Zapf Dingbats" and self.__convert_zapf:
            self.__current_dict_name = "Zapf Dingbats"
            self.__current_dict = self.__dingbats_dict
        else:
            self.__current_dict_name = "default"
            self.__current_dict = self.__def_dict

    def __start_special_font_func_old(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line Returns; nothing Logic: change the dictionary to use in conversion

        Example:
            Exercise Hex2Utf8.  start special font func old through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # for error checking
        if self.__token_info == "mi<mk<font-symbo":
            self.__current_dict.append(self.__symbol_dict)
            self.__special_fonts_found += 1
            self.__current_dict_name = "Symbol"
        elif self.__token_info == "mi<mk<font-wingd":
            self.__special_fonts_found += 1
            self.__current_dict.append(self.__wingdings_dict)
            self.__current_dict_name = "Wingdings"
        elif self.__token_info == "mi<mk<font-dingb":
            self.__current_dict.append(self.__dingbats_dict)
            self.__special_fonts_found += 1
            self.__current_dict_name = "Zapf Dingbats"

    def __end_special_font_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line --line to parse Returns: nothing Logic: pop the last dictionary, which should be a special font

        Example:
            Exercise Hex2Utf8.  end special font func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.__current_dict) < 2:
            sys.stderr.write("module is hex_2_utf 8\n")
            sys.stderr.write("method is __end_special_font_func\n")
            sys.stderr.write("less than two dictionaries --can't pop\n")
            self.__special_fonts_found -= 1
        else:
            self.__current_dict.pop()
            self.__special_fonts_found -= 1
            self.__dict_name = "default"

    def __start_caps_func_old(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: A marker that marks the start of caps has been found. Set self.__in_caps to 1

        Example:
            Exercise Hex2Utf8.  start caps func old through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__in_caps = 1

    def __start_caps_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: A marker that marks the start of caps has been found. Set self.__in_caps to 1

        Example:
            Exercise Hex2Utf8.  start caps func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__in_caps = 1
        value = line[17:-1]
        self.__caps_list.append(value)

    def __end_caps_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: A marker that marks the end of caps has been found. set self.__in_caps to 0

        Example:
            Exercise Hex2Utf8.  end caps func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.__caps_list) > 1:
            self.__caps_list.pop()
        else:
            sys.stderr.write(
                "Module is hex_2_utf8\n" "method is __end_caps_func\n" "caps list should be more than one?\n"
            )  # self.__in_caps not set

    def __text_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse Returns: nothing Logic: if in caps, convert. Otherwise, print out.

        Example:
            Exercise Hex2Utf8.  text func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        text = line[17:-1]
        # print line
        if self.__current_dict_name in ("Symbol", "Wingdings", "Zapf Dingbats"):
            the_string = ""
            for letter in text:
                hex_num = hex(ord(letter))
                hex_num = str(hex_num)
                hex_num = hex_num.upper()
                hex_num = hex_num[2:]
                hex_num = "'%s" % hex_num
                converted = self.__current_dict.get(hex_num)
                if converted is None:
                    sys.stderr.write("module is hex_2_ut8\nmethod is __text_func\n")
                    sys.stderr.write('no hex value for "%s"\n' % hex_num)
                else:
                    the_string += converted
            self.__write_obj.write("tx<nu<__________<%s\n" % the_string)
            # print the_string
        else:
            if (
                self.__caps_list[-1] == "true"
                and self.__convert_caps
                and self.__current_dict_name not in ("Symbol", "Wingdings", "Zapf Dingbats")
            ):
                text = text.upper()
            self.__write_obj.write("tx<nu<__________<%s\n" % text)

    def __utf_to_caps_func(self: _typing.Self, line: _typing.Any) -> None:
        """
        Required: line -- line to parse returns nothing Logic Get the text, and use another method to convert

        Example:
            Exercise Hex2Utf8.  utf to caps func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        utf_text = line[17:-1]
        if self.__caps_list[-1] == "true" and self.__convert_caps:
            # utf_text = utf_text.upper()
            utf_text = self.__utf_token_to_caps_func(utf_text)
        self.__write_obj.write("tx<ut<__________<%s\n" % utf_text)

    def __utf_token_to_caps_func(self: _typing.Self, char_entity: _typing.Any) -> _typing.Any:
        """
        Required: utf_text -- such as &xxx; Returns: token converted to the capital equivalent Logic: RTF often stores text in the improper values. For example, a capital umlaut o (?), is stores as ?. This function swaps the case by looking up the value in a dictionary.

        Example:
            Exercise Hex2Utf8.  utf token to caps func through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :param char_entity: Value supplied for char entity under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        hex_num = char_entity[3:]
        length = len(hex_num)
        if length == 3:
            hex_num = "00%s" % hex_num
        elif length == 4:
            hex_num = "0%s" % hex_num
        new_char_entity = "&#x%s" % hex_num
        converted = self.__caps_uni_dict.get(new_char_entity)
        if not converted:
            # bullets and other entities don't have capital equivalents
            return char_entity
        else:
            return converted

    def __convert_body(self: _typing.Self) -> None:
        """
        Perform the convert body operation under explicit file-format and conversion rules.

        Example:
            Exercise Hex2Utf8.  convert body through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__state = "body"
        with open_for_read(self.__file) as read_obj:
            with open_for_write(self.__write_to) as self.__write_obj:
                for line in read_obj:
                    self.__token_info = line[:16]
                    action = self.__body_state_dict.get(self.__state)
                    if action is None:
                        sys.stderr.write("error no state found in hex_2_utf8", self.__state)
                    action(line)
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "body_utf_convert.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)

    def convert_hex_2_utf8(self: _typing.Self) -> None:
        """
        Convert hex 2 utf8 under the format's safety and compatibility rules.

        Example:
            Exercise Hex2Utf8.convert hex 2 utf8 through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf2xml_regressions.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__initiate_values()
        if self.__area_to_convert == "preamble":
            self.__convert_preamble()
        else:
            self.__convert_body()


"""
how to swap case for non-capitals
my_string.swapcase()
An example of how to use a hash for the caps function
(but I shouldn't need this, since utf text is separate
 from regular text?)
sub_dict = {
    "&#x0430;"   : "some other value"
    }
def my_sub_func(matchobj):
    info =  matchobj.group(0)
    value = sub_dict.get(info)
    return value
    return "f"
line = "&#x0430; more text"
reg_exp = re.compile(r'(?P<name>&#x0430;|&#x0431;)')
line2 = re.sub(reg_exp, my_sub_func, line)
print line2
"""
