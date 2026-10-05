#########################################################################
#                                                                       #
#   copyright 2002 Paul Henry Tremblay                                  #
#                                                                       #
#########################################################################

"""
Select the fallback encoding for underspecified RTF input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise default encoding through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import re
from LiuXin_alpha.file_formats.rtf2xml import open_for_read


class DefaultEncoding:
    """
    Find the default encoding for the doc

    Example:
        Exercise DefaultEncoding through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """

    # Note: not all those encoding are really supported by rtf2xml
    # See http://msdn.microsoft.com/en-us/library/windows/desktop/dd317756%28v=vs.85%29.aspx
    # and src\calibre\gui2\widgets.py for the input list in calibre
    ENCODINGS = {
        # Special cases
        "cp1252": "1252",
        "utf-8": "1252",
        "ascii": "1252",
        # Normal cases
        "big5": "950",
        "cp1250": "1250",
        "cp1251": "1251",
        "cp1253": "1253",
        "cp1254": "1254",
        "cp1255": "1255",
        "cp1256": "1256",
        "shift_jis": "932",
        "gb2312": "936",
        # Not in RTF 1.9.1 codepage specification
        "hz": "52936",
        "iso8859_5": "28595",
        "iso2022_jp": "50222",
        "iso2022_kr": "50225",
        "euc_jp": "51932",
        "euc_kr": "51949",
        "gb18030": "54936",
    }

    def __init__(self: _typing.Self, in_file: _typing.Any, bug_handler: _typing.Any, default_encoding: _typing.Any, run_level: int = 1, check_raw: bool = False) -> None:
        """
        Initialize and validate the defaultencoding state.

        Example:
            Exercise DefaultEncoding.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param in_file: Value supplied for in file under the utility contract.
        :param bug_handler: Value supplied for bug handler under the utility contract.
        :param default_encoding: Value supplied for default encoding under the utility
            contract.
        :param run_level: Value supplied for run level under the utility contract.
        :param check_raw: Value supplied for check raw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__file = in_file
        self.__bug_handler = bug_handler
        self.__platform = "Windows"
        self.__default_num = "not-defined"
        self.__code_page = self.ENCODINGS.get(default_encoding, "1252")
        self.__datafetched = False
        self.__fetchraw = check_raw

    def find_default_encoding(self: _typing.Self) -> tuple[_typing.Any, ...]:
        """
        Find default encoding under the format's safety and compatibility rules.

        Example:
            Exercise DefaultEncoding.find default encoding through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.__datafetched:
            self._encoding()
            self.__datafetched = True
            code_page = "ansicpg" + self.__code_page
            # if self.__code_page == '10000':
            # self.__code_page = 'mac_roman'
        return self.__platform, code_page, self.__default_num

    def get_codepage(self: _typing.Self) -> _typing.Any:
        """
        Return codepage under the format's safety and compatibility rules.

        Example:
            Exercise DefaultEncoding.get codepage through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.__datafetched:
            self._encoding()
            self.__datafetched = True
            # if self.__code_page == '10000':
            # self.__code_page = 'mac_roman'
        return self.__code_page

    def get_platform(self: _typing.Self) -> _typing.Any:
        """
        Return platform under the format's safety and compatibility rules.

        Example:
            Exercise DefaultEncoding.get platform through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.__datafetched:
            self._encoding()
            self.__datafetched = True
        return self.__platform

    def _encoding(self: _typing.Self) -> None:
        """
        Perform the encoding operation under explicit file-format and conversion rules.

        Example:
            Exercise DefaultEncoding. encoding through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with open_for_read(self.__file) as read_obj:
            cpfound = False
            if not self.__fetchraw:
                for line in read_obj:
                    self.__token_info = line[:16]
                    if self.__token_info == "mi<mk<rtfhed-end":
                        break
                    if self.__token_info == "cw<ri<macintosh_":
                        self.__platform = "Macintosh"
                    elif self.__token_info == "cw<ri<pc________":
                        self.__platform = "IBMPC"
                    elif self.__token_info == "cw<ri<pca_______":
                        self.__platform = "OS/2"
                    if self.__token_info == "cw<ri<ansi-codpg" and int(line[20:-1]):
                        self.__code_page = line[20:-1]
                    if self.__token_info == "cw<ri<deflt-font":
                        self.__default_num = line[20:-1]
                        cpfound = True
                        # cw<ri<deflt-font<nu<0
                if self.__platform != "Windows" and not cpfound:
                    if self.__platform == "Macintosh":
                        self.__code_page = "10000"
                    elif self.__platform == "IBMPC":
                        self.__code_page = "437"
                    elif self.__platform == "OS/2":
                        self.__code_page = "850"
            else:
                fenc = re.compile(r"\\(mac|pc|ansi|pca)[\\ \{\}\t\n]+")
                fenccp = re.compile(r"\\ansicpg(\d+)[\\ \{\}\t\n]+")

                for line in read_obj:
                    if fenc.search(line):
                        enc = fenc.search(line).group(1)
                    if fenccp.search(line):
                        cp = fenccp.search(line).group(1)
                        if not int(cp):
                            self.__code_page = cp
                        cpfound = True
                        break
                if self.__platform != "Windows" and not cpfound:
                    if enc == "mac":
                        self.__code_page = "10000"
                    elif enc == "pc":
                        self.__code_page = "437"
                    elif enc == "pca":
                        self.__code_page = "850"


if __name__ == "__main__":
    import sys

    encode_obj = DefaultEncoding(
        in_file=sys.argv[1],
        default_encoding=sys.argv[2],
        bug_handler=Exception,
        check_raw=True,
    )
    print(encode_obj.get_codepage())
