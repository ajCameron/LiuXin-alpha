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
Copy retained RTF conversion intermediates for diagnostics.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise copy through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import os
import shutil


class Copy:
    """
    Copy each changed file to a directory for debugging purposes

    Example:
        Exercise Copy through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """

    __dir = ""

    def __init__(
        self: _typing.Self,
        bug_handler: _typing.Any,
        file: _typing.Any = None,
        deb_dir: _typing.Any = None,
    ) -> None:
        """
        Initialize and validate the copy state.

        Example:
            Exercise Copy.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param file: Value supplied for file under the utility contract.
        :param deb_dir: Value supplied for deb dir under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = file
        self.__bug_handler = bug_handler

    def set_dir(self: _typing.Self, deb_dir: _typing.Any) -> None:
        """
        Set the temporary directory to write files to

        Example:
            Exercise Copy.set dir through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param deb_dir: Value supplied for deb dir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if deb_dir is None:
            message = "No directory has been provided to write to in the copy.py"
            raise self.__bug_handler(message)
        check = os.path.isdir(deb_dir)
        if not check:
            message = "%(deb_dir)s is not a directory" % vars()
            raise self.__bug_handler(message)
        Copy.__dir = deb_dir

    def remove_files(self: _typing.Self) -> None:
        """
        Remove files from directory

        Example:
            Exercise Copy.remove files through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__remove_the_files(Copy.__dir)

    def __remove_the_files(self: _typing.Self, the_dir: _typing.Any) -> None:
        """
        Remove files from directory

        Example:
            Exercise Copy.  remove the files through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param the_dir: Value supplied for the dir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        Copy the file to a new name If the platform is linux, use the faster linux command of cp. Otherwise, use a safe python method.

        Example:
            Exercise Copy.copy file through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param file: Value supplied for file under the utility contract.
        :param new_file: Value supplied for new file under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        write_file = os.path.join(Copy.__dir, new_file)
        shutil.copyfile(file, write_file)

    def rename(self: _typing.Self, source: _typing.Any, dest: _typing.Any) -> None:
        """
        Perform the rename operation under explicit file-format and conversion rules.

        Example:
            Exercise Copy.rename through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param source: Value supplied for source under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        shutil.copyfile(source, dest)
