#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Normalize RTF control words and escaped data before parsing.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise preprocess through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
RTF tokenizer and token parser. v.1.0 (1/17/2010)
Author: Gerendi Sandor Attila

At this point this will tokenize a RTF file then rebuild it from the tokens.
In the process the UTF8 tokens are altered to be supported by the RTF2XML and also remain RTF specification compilant.
"""

__license__ = "GPL v3"
__copyright__ = "2010, Gerendi Sandor Attila"
__docformat__ = "restructuredtext en"


class tokenDelimitatorStart:
    """
    Provide the tokendelimitatorstart contract for validated ebook processing.

    Example:
        Exercise tokenDelimitatorStart through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the tokendelimitatorstart state.

        Example:
            Exercise tokenDelimitatorStart.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        pass

    def toRTF(self: _typing.Self) -> str:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenDelimitatorStart.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "{"

    def __repr__(self: _typing.Self) -> str:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenDelimitatorStart.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "{"


class tokenDelimitatorEnd:
    """
    Provide the tokendelimitatorend contract for validated ebook processing.

    Example:
        Exercise tokenDelimitatorEnd through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the tokendelimitatorend state.

        Example:
            Exercise tokenDelimitatorEnd.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        pass

    def toRTF(self: _typing.Self) -> str:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenDelimitatorEnd.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "}"

    def __repr__(self: _typing.Self) -> str:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenDelimitatorEnd.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "}"


class tokenControlWord:
    """
    Provide the tokencontrolword contract for validated ebook processing.

    Example:
        Exercise tokenControlWord through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, name: _typing.Any, separator: str = "") -> None:
        """
        Initialize and validate the tokencontrolword state.

        Example:
            Exercise tokenControlWord.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param separator: Delimiter used to split or join list values.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlWord.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name + self.separator

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlWord.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name + self.separator


class tokenControlWordWithNumericArgument:
    """
    Provide the tokencontrolwordwithnumericargument contract for validated ebook processing.

    Example:
        Exercise tokenControlWordWithNumericArgument through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, name: _typing.Any, argument: _typing.Any, separator: str = "") -> None:
        """
        Initialize and validate the tokencontrolwordwithnumericargument state.

        Example:
            Exercise tokenControlWordWithNumericArgument.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param argument: Value supplied for argument under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.argument = argument
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlWordWithNumericArgument.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name + repr(self.argument) + self.separator

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlWordWithNumericArgument.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name + repr(self.argument) + self.separator


class tokenControlSymbol:
    """
    Provide the tokencontrolsymbol contract for validated ebook processing.

    Example:
        Exercise tokenControlSymbol through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the tokencontrolsymbol state.

        Example:
            Exercise tokenControlSymbol.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlSymbol.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenControlSymbol.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name


class tokenData:
    """
    Provide the tokendata contract for validated ebook processing.

    Example:
        Exercise tokenData through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        """
        Initialize and validate the tokendata state.

        Example:
            Exercise tokenData.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = data

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenData.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenData.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data


class tokenBinN:
    """
    Provide the tokenbinn contract for validated ebook processing.

    Example:
        Exercise tokenBinN through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any, separator: str = "") -> None:
        """
        Initialize and validate the tokenbinn state.

        Example:
            Exercise tokenBinN.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = data
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenBinN.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\\bin" + repr(len(self.data)) + self.separator + self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenBinN.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\\bin" + repr(len(self.data)) + self.separator + self.data


class token8bitChar:
    """
    Provide the token8bitchar contract for validated ebook processing.

    Example:
        Exercise token8bitChar through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        """
        Initialize and validate the token8bitchar state.

        Example:
            Exercise token8bitChar.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = data

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise token8bitChar.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\\'" + self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise token8bitChar.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\\'" + self.data


class tokenUnicode:
    """
    Provide the tokenunicode contract for validated ebook processing.

    Example:
        Exercise tokenUnicode through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any, separator: str = "", current_ucn: int = 1, eqList: _typing.Any = None) -> None:
        """
        Initialize and validate the tokenunicode state.

        Example:
            Exercise tokenUnicode.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :param current_ucn: Value supplied for current ucn under the utility contract.
        :param eqList: Value supplied for eqList under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = data
        self.separator = separator
        self.current_ucn = current_ucn
        self.eqList = [] if eqList is None else eqList

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenUnicode.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        result = "\\u" + repr(self.data) + " "
        ucn = self.current_ucn
        if len(self.eqList) < ucn:
            ucn = len(self.eqList)
            result = tokenControlWordWithNumericArgument("\\uc", ucn).toRTF() + result
        i = 0
        for eq in self.eqList:
            if i >= ucn:
                break
            result = result + eq.toRTF()
        return result

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise tokenUnicode.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "\\u" + repr(self.data)


def isAsciiLetter(value: _typing.Any) -> bool:
    """
    Perform the isAsciiLetter operation under explicit file-format and conversion rules.

    Example:
        Exercise isAsciiLetter through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ((value >= "a") and (value <= "z")) or ((value >= "A") and (value <= "Z"))


def isDigit(value: _typing.Any) -> bool:
    """
    Perform the isDigit operation under explicit file-format and conversion rules.

    Example:
        Exercise isDigit through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (value >= "0") and (value <= "9")


def isChar(value: _typing.Any, char: _typing.Any) -> bool:
    """
    Perform the isChar operation under explicit file-format and conversion rules.

    Example:
        Exercise isChar through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :param char: Value supplied for char under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return value == char


def isString(buffer: _typing.Any, string: _typing.Any) -> bool:
    """
    Perform the isString operation under explicit file-format and conversion rules.

    Example:
        Exercise isString through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param buffer: Value supplied for buffer under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return buffer == string


class RtfTokenParser:
    """
    Parse rtftokenparser data into normalized ebook structures.

    Example:
        Exercise RtfTokenParser through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, tokens: _typing.Any) -> None:
        """
        Initialize and validate the rtftokenparser state.

        Example:
            Exercise RtfTokenParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param tokens: Value supplied for tokens under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tokens = tokens
        self.process()
        self.processUnicode()

    def process(self: _typing.Self) -> None:
        """
        Perform the process operation under explicit file-format and conversion rules.

        Example:
            Exercise RtfTokenParser.process through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        i = 0
        new_tokens = []
        while i < len(self.tokens):
            if isinstance(self.tokens[i], tokenControlSymbol):
                if isString(self.tokens[i].name, "\\'"):
                    i += 1
                    if not isinstance(self.tokens[i], tokenData):
                        raise Exception("Error: token8bitChar without data.")
                    if len(self.tokens[i].data) < 2:
                        raise Exception("Error: token8bitChar without data.")
                    new_tokens.append(token8bitChar(self.tokens[i].data[0:2]))
                    if len(self.tokens[i].data) > 2:
                        new_tokens.append(tokenData(self.tokens[i].data[2:]))
                    i += 1
                    continue

            new_tokens.append(self.tokens[i])
            i += 1

        self.tokens = list(new_tokens)

    def processUnicode(self: _typing.Self) -> None:
        """
        Perform the processUnicode operation under explicit file-format and conversion rules.

        Example:
            Exercise RtfTokenParser.processUnicode through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        i = 0
        new_tokens = []
        uc_nb_stack = [1]
        while i < len(self.tokens):
            if isinstance(self.tokens[i], tokenDelimitatorStart):
                uc_nb_stack.append(uc_nb_stack[len(uc_nb_stack) - 1])
                new_tokens.append(self.tokens[i])
                i += 1
                continue
            if isinstance(self.tokens[i], tokenDelimitatorEnd):
                uc_nb_stack.pop()
                new_tokens.append(self.tokens[i])
                i += 1
                continue
            if isinstance(self.tokens[i], tokenControlWordWithNumericArgument):
                if isString(self.tokens[i].name, "\\uc"):
                    uc_nb_stack[len(uc_nb_stack) - 1] = self.tokens[i].argument
                    new_tokens.append(self.tokens[i])
                    i += 1
                    continue
                if isString(self.tokens[i].name, "\\u"):
                    x = i
                    j = 0
                    i += 1
                    replace = []
                    partialData = None
                    ucn = uc_nb_stack[len(uc_nb_stack) - 1]
                    while (i < len(self.tokens)) and (j < ucn):
                        if isinstance(self.tokens[i], tokenDelimitatorStart):
                            break
                        if isinstance(self.tokens[i], tokenDelimitatorEnd):
                            break
                        if isinstance(self.tokens[i], tokenData):
                            if len(self.tokens[i].data) >= ucn - j:
                                replace.append(tokenData(self.tokens[i].data[0 : ucn - j]))
                                if len(self.tokens[i].data) > ucn - j:
                                    partialData = tokenData(self.tokens[i].data[ucn - j :])
                                i += 1
                                break
                            else:
                                replace.append(self.tokens[i])
                                j += len(self.tokens[i].data)
                                i += 1
                                continue
                        if isinstance(self.tokens[i], token8bitChar) or isinstance(self.tokens[i], tokenBinN):
                            replace.append(self.tokens[i])
                            i += 1
                            j += 1
                            continue
                        raise Exception("Error: incorect utf replacement.")

                    # calibre rtf2xml does not support utfreplace
                    replace = []

                    new_tokens.append(
                        tokenUnicode(
                            self.tokens[x].argument,
                            self.tokens[x].separator,
                            uc_nb_stack[len(uc_nb_stack) - 1],
                            replace,
                        )
                    )
                    if partialData is not None:
                        new_tokens.append(partialData)
                    continue

            new_tokens.append(self.tokens[i])
            i += 1

        self.tokens = list(new_tokens)

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise RtfTokenParser.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        result = []
        for token in self.tokens:
            result.append(token.toRTF())
        return "".join(result)


class RtfTokenizer:
    """
    Provide the rtftokenizer contract for validated ebook processing.

    Example:
        Exercise RtfTokenizer through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    def __init__(self: _typing.Self, rtfData: _typing.Any) -> None:
        """
        Initialize and validate the rtftokenizer state.

        Example:
            Exercise RtfTokenizer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param rtfData: Value supplied for rtfData under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.rtfData = []
        self.tokens = []
        if isinstance(rtfData, bytes):
            rtfData = rtfData.decode("latin-1", "replace")
        self.rtfData = rtfData
        self.tokenize()

    def tokenize(self: _typing.Self) -> None:
        """
        Perform the tokenize operation under explicit file-format and conversion rules.

        Example:
            Exercise RtfTokenizer.tokenize through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        i = 0
        lastDataStart = -1
        while i < len(self.rtfData):

            if isChar(self.rtfData[i], "{"):
                if lastDataStart > -1:
                    self.tokens.append(tokenData(self.rtfData[lastDataStart:i]))
                    lastDataStart = -1
                self.tokens.append(tokenDelimitatorStart())
                i += 1
                continue

            if isChar(self.rtfData[i], "}"):
                if lastDataStart > -1:
                    self.tokens.append(tokenData(self.rtfData[lastDataStart:i]))
                    lastDataStart = -1
                self.tokens.append(tokenDelimitatorEnd())
                i += 1
                continue

            if isChar(self.rtfData[i], "\\"):
                if i + 1 >= len(self.rtfData):
                    raise Exception("Error: Control character found at the end of the document.")

                if lastDataStart > -1:
                    self.tokens.append(tokenData(self.rtfData[lastDataStart:i]))
                    lastDataStart = -1

                tokenStart = i
                i = i + 1

                # Control Words
                if isAsciiLetter(self.rtfData[i]):
                    # consume <ASCII Letter Sequence>
                    consumed = False
                    while i < len(self.rtfData):
                        if not isAsciiLetter(self.rtfData[i]):
                            tokenEnd = i
                            consumed = True
                            break
                        i += 1

                    if not consumed:
                        raise Exception("Error (at:%d): Control Word without end." % (tokenStart))

                    # we have numeric argument before delimiter
                    if isChar(self.rtfData[i], "-") or isDigit(self.rtfData[i]):
                        # consume the optional sign and numeric argument
                        if isChar(self.rtfData[i], "-"):
                            i += 1
                        l = 0
                        while i < len(self.rtfData) and isDigit(self.rtfData[i]):
                            l += 1
                            i += 1
                            if l > 10:
                                raise Exception(
                                    "Error (at:%d): Too many digits in control word numeric argument." % tokenStart
                                )

                        if l == 0:
                            raise Exception("Error (at:%d): Control Word without numeric argument digits." % tokenStart)
                        if i >= len(self.rtfData):
                            raise Exception("Error (at:%d): Control Word without numeric argument end." % tokenStart)

                    separator = ""
                    if isChar(self.rtfData[i], " "):
                        separator = " "

                    controlWord = self.rtfData[tokenStart:tokenEnd]
                    if tokenEnd < i:
                        value = int(self.rtfData[tokenEnd:i])
                        if isString(controlWord, "\\bin"):
                            i = i + value
                            self.tokens.append(tokenBinN(self.rtfData[tokenStart:i], separator))
                        else:
                            self.tokens.append(tokenControlWordWithNumericArgument(controlWord, value, separator))
                    else:
                        self.tokens.append(tokenControlWord(controlWord, separator))
                    # space delimiter, we should discard it
                    if self.rtfData[i] == " ":
                        i += 1

                # Control Symbol
                else:
                    self.tokens.append(tokenControlSymbol(self.rtfData[tokenStart : i + 1]))
                    i += 1
                continue

            if lastDataStart < 0:
                lastDataStart = i
            i += 1

    def toRTF(self: _typing.Self) -> _typing.Any:
        """
        Perform the toRTF operation under explicit file-format and conversion rules.

        Example:
            Exercise RtfTokenizer.toRTF through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        result = []
        for token in self.tokens:
            result.append(token.toRTF())
        return "".join(result)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage %prog rtfFileToConvert")
        sys.exit()
    f = open(sys.argv[1], "rb")
    local_data = f.read()
    f.close()

    tokenizer = RtfTokenizer(local_data)
    parsedTokens = RtfTokenParser(tokenizer.tokens)

    local_data = parsedTokens.toRTF()

    f = open(sys.argv[1], "w")
    f.write(local_data)
    f.close()
