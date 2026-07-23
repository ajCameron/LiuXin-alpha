#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

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
    def __init__(self: _typing.Self) -> None:
        pass

    def toRTF(self: _typing.Self) -> str:
        return "{"

    def __repr__(self: _typing.Self) -> str:
        return "{"


class tokenDelimitatorEnd:
    def __init__(self: _typing.Self) -> None:
        pass

    def toRTF(self: _typing.Self) -> str:
        return "}"

    def __repr__(self: _typing.Self) -> str:
        return "}"


class tokenControlWord:
    def __init__(self: _typing.Self, name: _typing.Any, separator: str = "") -> None:
        self.name = name
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        return self.name + self.separator

    def __repr__(self: _typing.Self) -> _typing.Any:
        return self.name + self.separator


class tokenControlWordWithNumericArgument:
    def __init__(self: _typing.Self, name: _typing.Any, argument: _typing.Any, separator: str = "") -> None:
        self.name = name
        self.argument = argument
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        return self.name + repr(self.argument) + self.separator

    def __repr__(self: _typing.Self) -> _typing.Any:
        return self.name + repr(self.argument) + self.separator


class tokenControlSymbol:
    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        self.name = name

    def toRTF(self: _typing.Self) -> _typing.Any:
        return self.name

    def __repr__(self: _typing.Self) -> _typing.Any:
        return self.name


class tokenData:
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        self.data = data

    def toRTF(self: _typing.Self) -> _typing.Any:
        return self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        return self.data


class tokenBinN:
    def __init__(self: _typing.Self, data: _typing.Any, separator: str = "") -> None:
        self.data = data
        self.separator = separator

    def toRTF(self: _typing.Self) -> _typing.Any:
        return "\\bin" + repr(len(self.data)) + self.separator + self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        return "\\bin" + repr(len(self.data)) + self.separator + self.data


class token8bitChar:
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        self.data = data

    def toRTF(self: _typing.Self) -> _typing.Any:
        return "\\'" + self.data

    def __repr__(self: _typing.Self) -> _typing.Any:
        return "\\'" + self.data


class tokenUnicode:
    def __init__(self: _typing.Self, data: _typing.Any, separator: str = "", current_ucn: int = 1, eqList: _typing.Any = None) -> None:
        self.data = data
        self.separator = separator
        self.current_ucn = current_ucn
        self.eqList = [] if eqList is None else eqList

    def toRTF(self: _typing.Self) -> _typing.Any:
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
        return "\\u" + repr(self.data)


def isAsciiLetter(value: _typing.Any) -> bool:
    return ((value >= "a") and (value <= "z")) or ((value >= "A") and (value <= "Z"))


def isDigit(value: _typing.Any) -> bool:
    return (value >= "0") and (value <= "9")


def isChar(value: _typing.Any, char: _typing.Any) -> bool:
    return value == char


def isString(buffer: _typing.Any, string: _typing.Any) -> bool:
    return buffer == string


class RtfTokenParser:
    def __init__(self: _typing.Self, tokens: _typing.Any) -> None:
        self.tokens = tokens
        self.process()
        self.processUnicode()

    def process(self: _typing.Self) -> None:
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
        result = []
        for token in self.tokens:
            result.append(token.toRTF())
        return "".join(result)


class RtfTokenizer:
    def __init__(self: _typing.Self, rtfData: _typing.Any) -> None:
        self.rtfData = []
        self.tokens = []
        if isinstance(rtfData, bytes):
            rtfData = rtfData.decode("latin-1", "replace")
        self.rtfData = rtfData
        self.tokenize()

    def tokenize(self: _typing.Self) -> None:
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
