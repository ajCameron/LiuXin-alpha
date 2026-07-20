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
import shutil


class Copy:
    """Copy each changed file to a directory for debugging purposes"""

    __dir = ""

    def __init__(
        self: _typing.Self,
        bug_handler: _typing.Any,
        file: _typing.Any = None,
        deb_dir: _typing.Any = None,
    ) -> None:
        self.__file = file
        self.__bug_handler = bug_handler

    def set_dir(self: _typing.Self, deb_dir: _typing.Any) -> None:
        """Set the temporary directory to write files to"""
        if deb_dir is None:
            message = "No directory has been provided to write to in the copy.py"
            raise self.__bug_handler(message)
        check = os.path.isdir(deb_dir)
        if not check:
            message = "%(deb_dir)s is not a directory" % vars()
            raise self.__bug_handler(message)
        Copy.__dir = deb_dir

    def remove_files(self: _typing.Self) -> None:
        """Remove files from directory"""
        self.__remove_the_files(Copy.__dir)

    def __remove_the_files(self: _typing.Self, the_dir: _typing.Any) -> None:
        """Remove files from directory"""
        list_of_files = os.listdir(the_dir)
        for file in list_of_files:
            rem_file = os.path.join(Copy.__dir, file)
            if os.path.isdir(rem_file):
                self.__remove_the_files(rem_file)
            else:
                try:
                    os.remove(rem_file)
                except OSError:
                    pass

    def copy_file(self: _typing.Self, file: _typing.Any, new_file: _typing.Any) -> None:
        """
        Copy the file to a new name
        If the platform is linux, use the faster linux command
        of cp. Otherwise, use a safe python method.
        """
        write_file = os.path.join(Copy.__dir, new_file)
        shutil.copyfile(file, write_file)

    def rename(self: _typing.Self, source: _typing.Any, dest: _typing.Any) -> None:
        shutil.copyfile(source, dest)
