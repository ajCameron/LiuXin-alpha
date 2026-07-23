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
from LiuXin_alpha.file_formats.rtf2xml import open_for_read, open_for_write

from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars
from LiuXin_alpha.utils.ptempfiles import better_mktemp


class ReplaceIllegals:
    """
    reaplace illegal lower ascii characters
    """

    def __init__(
        self: _typing.Self,
        in_file: _typing.Any,
        copy: _typing.Any = None,
        run_level: int = 1,
    ) -> None:
        self.__file = in_file
        self.__copy = copy
        self.__run_level = run_level
        self.__write_to = better_mktemp()

    def replace_illegals(self: _typing.Self) -> None:
        """ """
        with open_for_read(self.__file) as read_obj:
            with open_for_write(self.__write_to) as write_obj:
                for line in read_obj:
                    write_obj.write(clean_ascii_chars(line))
        copy_obj = copy.Copy()
        if self.__copy:
            copy_obj.copy_file(self.__write_to, "replace_illegals.data")
        copy_obj.rename(self.__write_to, self.__file)
        os.remove(self.__write_to)
