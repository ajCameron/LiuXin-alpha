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
Load character mappings used by the RTF decoder.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise get char map through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
class GetCharMap:
    """
    Return the character map for the given value

    Example:
        Exercise GetCharMap through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """

    def __init__(self: _typing.Self, bug_handler: _typing.Any, char_file: _typing.Any) -> None:
        """
        Required:

        Example:
            Exercise GetCharMap.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param char_file: Value supplied for char file under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__char_file = char_file
        self.__bug_handler = bug_handler

    def get_char_map(self: _typing.Self, map: _typing.Any) -> _typing.Any:
        # if map == 'ansicpg10000':
        #   map = 'mac_roman'
        """
        Return char map under the format's safety and compatibility rules.

        Example:
            Exercise GetCharMap.get char map through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param map: Value supplied for map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        found_map = False
        map_dict = {}
        self.__char_file.seek(0)
        for line in self.__char_file:
            if not line.strip():
                continue
            begin_element = "<%s>" % map
            end_element = "</%s>" % map
            if not found_map:
                if begin_element in line:
                    found_map = True
            else:
                if end_element in line:
                    break
                fields = line.split(":")
                fields[1].replace("\\colon", ":")
                map_dict[fields[1]] = fields[3]

        if not found_map:
            msg = 'no map found\nmap is "%s"\n' % (map,)
            raise self.__bug_handler(msg)
        return map_dict
