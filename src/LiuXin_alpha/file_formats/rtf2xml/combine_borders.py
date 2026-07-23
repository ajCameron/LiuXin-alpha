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
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.file_formats.rtf2xml import copy
from LiuXin_alpha.utils.ptempfiles import better_mktemp
from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write


class CombineBorders:
    """Combine borders in RTF tokens to make later processing easier"""

    def __init__(
        self: _typing.Self,
        in_file: _typing.Any,
        bug_handler: _typing.Any,
        copy: _typing.Any = None,
        run_level: int = 1,
    ) -> None:
        self.__file = in_file
        self.__bug_handler = bug_handler
        self.__copy = copy
        self.__write_to = better_mktemp()
        self.__state = "default"
        self.__bord_pos = "default"
        self.__bord_att = []

    def found_bd(self: _typing.Self, line: _typing.Any) -> None:
        # cw<bd<bor-t-r-vi
        self.__state = "border"
        self.__bord_pos = line[6:16]

    def __default_func(self: _typing.Self, line: _typing.Any) -> _typing.Any:
        # cw<bd<bor-t-r-vi
        if self.__first_five == "cw<bd":
            self.found_bd(line)
            return ""
        return line

    def end_border(self: _typing.Self, line: _typing.Any, write_obj: _typing.Any) -> None:
        border_string = "|".join(self.__bord_att)
        self.__bord_att = []
        write_obj.write("cw<bd<{}<nu<{}\n".format(self.__bord_pos, border_string))
        self.__state = "default"
        self.__bord_string = ""
        if self.__first_five == "cw<bd":
            self.found_bd(line)
        else:
            write_obj.write(line)

    def add_to_border_desc(self: _typing.Self, line: _typing.Any) -> None:
        # cw<bt<bdr-hair__<nu<true
        # cw<bt<bdr-linew<nu<0.50
        # tx<__________<some text
        border_desc = line[6:16]
        num = line[20:-1]
        if num == "true":
            num = ""
        else:
            num = ":" + num
        self.__bord_att.append(border_desc + num)

    def __border_func(self: _typing.Self, line: _typing.Any, write_obj: _typing.Any) -> None:
        if self.__first_five != "cw<bt":
            self.end_border(line, write_obj)
        else:
            self.add_to_border_desc(line)

    def combine_borders(self: _typing.Self) -> None:
        with open_for_read(self.__file) as read_obj:
            with open_for_write(self.__write_to) as write_obj:
                for line in read_obj:
                    self.__first_five = line[0:5]
                    if self.__state == "border":
                        self.__border_func(line, write_obj)
                    else:
                        write_obj.write(self.__default_func(line))
        copy_obj = copy.Copy(bug_handler=self.__bug_handler)
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "combine_borders.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
