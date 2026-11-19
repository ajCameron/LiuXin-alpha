"""
Split PDF bytes into tokens and objects, and decode PDF text strings.

The lexer works on a window of the file. A window that does not reach the end of the
file is incomplete, and a token that runs to its end raises :class:`NeedMore`, so the
caller can read a larger window and try again. Every pattern is anchored or bounded,
so lexing is linear in the window's size.

Example:
    Parse an array with a reference, a name, a string and a dictionary::

        >>> lexer = Lexer(b"[1 0 R /N (s) << /K true >>]")
        >>> parse_value(lexer, *lexer.token(), 0, 64)
        [PdfRef(number=1, generation=0), 'N', b's', {'K': True}]
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, NamedTuple, Self

from LiuXin_alpha.file_formats.pdf.errors import PdfReadError

__all__ = ["Lexer", "NeedMore", "PdfName", "PdfRef", "PdfStream", "WHITESPACE", "decode_text", "parse_value"]

#: The bytes PDF treats as whitespace.
WHITESPACE = b"\x00\t\n\x0c\r "
_SKIP = re.compile(rb"(?:[\x00\t\n\x0c\r ]|%[^\r\n]*)*")
_REGULAR = re.compile(rb"[^\x00\t\n\x0c\r ()<>\[\]{}/%]*")
_STRING_SPECIAL = re.compile(rb"[()\\]")
_NOT_HEX = re.compile(rb"[^0-9A-Fa-f]")
_NAME_ESCAPE = re.compile(rb"#([0-9A-Fa-f]{2})")
_INTEGER = re.compile(rb"[+-]?\d{1,20}")
_REAL = re.compile(rb"[+-]?(?:\d{1,20}\.\d{0,20}|\.\d{1,20})")
_ESCAPES = {ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}
_LANGUAGE_ESCAPE = re.compile("\x1b[^\x1b]{0,8}\x1b")

#: PDFDocEncoding where it differs from Latin-1 (PDF 32000-1:2008, Annex D.2).
_PDFDOC = {
    0x18: "\u02d8", 0x19: "\u02c7", 0x1A: "\u02c6", 0x1B: "\u02d9", 0x1C: "\u02dd", 0x1D: "\u02db",
    0x1E: "\u02da", 0x1F: "\u02dc", 0x80: "\u2022", 0x81: "\u2020", 0x82: "\u2021", 0x83: "\u2026",
    0x84: "\u2014", 0x85: "\u2013", 0x86: "\u0192", 0x87: "\u2044", 0x88: "\u2039", 0x89: "\u203a",
    0x8A: "\u2212", 0x8B: "\u2030", 0x8C: "\u201e", 0x8D: "\u201c", 0x8E: "\u201d", 0x8F: "\u2018",
    0x90: "\u2019", 0x91: "\u201a", 0x92: "\u2122", 0x93: "\ufb01", 0x94: "\ufb02", 0x95: "\u0141",
    0x96: "\u0152", 0x97: "\u0160", 0x98: "\u0178", 0x99: "\u017d", 0x9A: "\u0131", 0x9B: "\u0142",
    0x9C: "\u0153", 0x9D: "\u0161", 0x9E: "\u017e", 0xA0: "\u20ac",
}  # fmt: skip


class PdfName(str):
    """
    A PDF name, without its slash, as distinct from a text string.

    Example:
        >>> PdfName("Title") == "Title"
        True
    """

    __slots__ = ()


class PdfRef(NamedTuple):
    """
    An indirect reference: an object number and a generation number.

    Example:
        >>> PdfRef(12, 0)
        PdfRef(number=12, generation=0)
    """

    number: int
    generation: int


@dataclass(frozen=True)
class PdfStream:
    """
    A stream object: its dictionary, where its data starts in the file, and which object it is.

    Example:
        >>> PdfStream({"Length": 3}, 100, PdfRef(4, 0)).ref.number
        4
    """

    dictionary: dict[str, Any]
    offset: int
    ref: PdfRef


class NeedMore(Exception):
    """
    Signal that a token runs past the end of a window that does not reach the end of the file.

    Example:
        >>> issubclass(NeedMore, Exception)
        True
    """


def decode_text(raw: bytes) -> str:
    """
    Decode a PDF text string: UTF-16 or UTF-8 with a byte order mark, else UTF-8 if valid, else PDFDocEncoding.

    The PDF specification allows only UTF-16BE with a byte order mark, UTF-8 with one
    (PDF 2.0) and PDFDocEncoding. Many tools write raw UTF-8 regardless; bytes that are
    valid UTF-8 and not ASCII are rarely meant as PDFDocEncoding, so they are read as UTF-8.

    Example:
        >>> decode_text(b"\\xfe\\xff\\x00T\\x00\\xe9")
        'Té'
        >>> decode_text(b"Caf\\xe9 \\x84 \\x93x")
        'Café — ﬁx'
        >>> decode_text("東京".encode())
        '東京'


    :param raw: The string's bytes.
    :return: The text, without trailing NUL characters.
    """
    if raw.startswith(b"\xfe\xff"):
        text = _LANGUAGE_ESCAPE.sub("", raw[2:].decode("utf-16-be", "replace"))
    elif raw.startswith(b"\xff\xfe"):
        text = raw[2:].decode("utf-16-le", "replace")
    elif raw.startswith(b"\xef\xbb\xbf"):
        text = raw[3:].decode("utf-8", "replace")
    else:
        utf8: str | None = None
        if any(byte >= 0x80 for byte in raw):
            try:
                utf8 = raw.decode("utf-8")
            except UnicodeDecodeError:
                utf8 = None
        text = utf8 if utf8 is not None else raw.decode("latin-1").translate(_PDFDOC)
    return text.rstrip("\x00")


class Lexer:
    """
    Split a window of a PDF into tokens.

    A window that does not reach the end of the file is incomplete: a token that runs
    to its end raises :class:`NeedMore`, so the caller can read a larger window.

    Example:
        >>> lexer = Lexer(b"/Name (text) 12 -3.5 <4142> [")
        >>> [lexer.token() for _ in range(6)]  # doctest: +NORMALIZE_WHITESPACE
        [('name', 'Name'), ('string', b'text'), ('int', 12), ('real', -3.5), ('string', b'AB'),
         ('array_open', None)]
    """

    __slots__ = ("complete", "data", "pos")

    def __init__(self: Self, data: bytes, pos: int = 0, complete: bool = True) -> None:
        """
        Start lexing at a position.

        Example:
            >>> Lexer(b"1 0 R").pos
            0


        :param data: The window's bytes.
        :param pos: Where to start.
        :param complete: Whether the window reaches the end of the file.
        :return: None.
        """
        self.data = data
        self.pos = pos
        self.complete = complete

    def skip(self: Self) -> None:
        """
        Move past whitespace and comments.

        Example:
            >>> lexer = Lexer(b"  % comment\\n x")
            >>> lexer.skip()
            >>> lexer.pos
            13


        :return: None.
        """
        match = _SKIP.match(self.data, self.pos)
        self.pos = match.end() if match else self.pos

    def _regular(self: Self, start: int) -> bytes:
        """
        Read the run of regular characters from a position.

        Example:
            >>> lexer = Lexer(b"abc def")
            >>> lexer._regular(0), lexer.pos
            (b'abc', 3)


        :param start: Where the run starts.
        :return: The run.
        :raises NeedMore: The run reaches the end of an incomplete window.
        """
        match = _REGULAR.match(self.data, start)
        end = match.end() if match else start
        if end == len(self.data) and not self.complete:
            raise NeedMore
        self.pos = end
        return self.data[start:end]

    def token(self: Self) -> tuple[str, Any]:
        """
        Read the next token.

        Example:
            >>> Lexer(b"  << /A 1 >>").token()
            ('dict_open', None)


        :return: The token's kind and value: ``int``, ``real``, ``name``, ``string``,
            ``dict_open``, ``dict_close``, ``array_open``, ``array_close``, ``keyword``
            (bytes) or ``eof``.
        :raises NeedMore: The token runs past the end of an incomplete window.
        """
        self.skip()
        data, pos = self.data, self.pos
        if pos >= len(data):
            if not self.complete:
                raise NeedMore
            return "eof", None
        char = data[pos]
        if char == 0x2F:  # /
            raw = self._regular(pos + 1)
            raw = _NAME_ESCAPE.sub(lambda match: bytes([int(match.group(1), 16)]), raw)
            try:
                return "name", PdfName(raw.decode("utf-8"))
            except UnicodeDecodeError:
                return "name", PdfName(raw.decode("latin-1"))
        if char == 0x28:  # (
            return "string", self._literal()
        if char == 0x3C:  # <
            if pos + 1 >= len(data) and not self.complete:
                raise NeedMore
            if data[pos + 1 : pos + 2] == b"<":
                self.pos = pos + 2
                return "dict_open", None
            return "string", self._hex()
        if char == 0x3E:  # >
            if pos + 1 >= len(data) and not self.complete:
                raise NeedMore
            if data[pos + 1 : pos + 2] == b">":
                self.pos = pos + 2
                return "dict_close", None
            self.pos = pos + 1
            return "keyword", b">"
        if char == 0x5B:  # [
            self.pos = pos + 1
            return "array_open", None
        if char == 0x5D:  # ]
            self.pos = pos + 1
            return "array_close", None
        if char in b"{})":
            self.pos = pos + 1
            return "keyword", bytes([char])
        raw = self._regular(pos)
        if _INTEGER.fullmatch(raw):
            return "int", int(raw)
        if _REAL.fullmatch(raw):
            return "real", float(raw)
        return "keyword", raw

    def _literal(self: Self) -> bytes:
        """
        Read a literal string, with its escapes and balanced parentheses.

        Example:
            >>> Lexer(rb"(a (b) \\(c\\) \\101\\nd)")._literal()
            b'a (b) (c) A\\nd'


        :return: The string's bytes.
        :raises NeedMore: The string is unterminated in an incomplete window.
        """
        data = self.data
        size = len(data)
        i = self.pos + 1
        depth = 1
        out = bytearray()
        while True:
            match = _STRING_SPECIAL.search(data, i)
            if match is None:
                if not self.complete:
                    raise NeedMore
                out += data[i:]
                self.pos = size
                return bytes(out)
            j = match.start()
            out += data[i:j]
            char = data[j]
            if char == 0x5C:  # backslash
                if j + 2 > size or (j + 4 > size and not self.complete):
                    if not self.complete:
                        raise NeedMore
                    self.pos = size
                    return bytes(out)
                escape = data[j + 1]
                if escape in _ESCAPES:
                    out.append(_ESCAPES[escape])
                    i = j + 2
                elif 0x30 <= escape <= 0x37:
                    k = j + 1
                    while k < j + 4 and k < size and 0x30 <= data[k] <= 0x37:
                        k += 1
                    out.append(int(data[j + 1 : k], 8) & 0xFF)
                    i = k
                elif escape == 0x0D:
                    i = j + 3 if data[j + 2 : j + 3] == b"\n" else j + 2
                elif escape == 0x0A:
                    i = j + 2
                else:
                    out.append(escape)
                    i = j + 2
            elif char == 0x28:
                depth += 1
                out.append(char)
                i = j + 1
            else:
                depth -= 1
                i = j + 1
                if depth == 0:
                    self.pos = i
                    return bytes(out)
                out.append(char)

    def _hex(self: Self) -> bytes:
        """
        Read a hexadecimal string, ignoring whitespace and padding an odd last digit with 0.

        Example:
            >>> Lexer(b"<41 42 4>")._hex()
            b'AB@'


        :return: The string's bytes.
        :raises NeedMore: The string is unterminated in an incomplete window.
        """
        end = self.data.find(b">", self.pos + 1)
        if end < 0:
            if not self.complete:
                raise NeedMore
            end = len(self.data)
        digits = _NOT_HEX.sub(b"", self.data[self.pos + 1 : end])
        if len(digits) % 2:
            digits += b"0"
        self.pos = end + 1
        return bytes.fromhex(digits.decode("ascii"))


def parse_value(lexer: Lexer, kind: str, value: Any, depth: int, limit: int) -> Any:
    """
    Build an object from its first token and the tokens after it.

    Example:
        >>> lexer = Lexer(b"[1 0 R /N (s) << /K true >>] 7")
        >>> parse_value(lexer, *lexer.token(), 0, 64)
        [PdfRef(number=1, generation=0), 'N', b's', {'K': True}]


    :param lexer: The lexer, after the first token.
    :param kind: The first token's kind.
    :param value: The first token's value.
    :param depth: How deeply this object is nested.
    :param limit: The deepest nesting allowed.
    :return: The object: an int, float, bytes (string), :class:`PdfName`, bool, ``None``,
        :class:`PdfRef`, list or dict.
    :raises PdfReadError: The tokens do not form an object, or nest too deeply.
    """
    if kind == "int":
        saved = lexer.pos
        second = lexer.token()
        if second[0] == "int" and value >= 0 and second[1] >= 0:
            third = lexer.token()
            if third == ("keyword", b"R"):
                return PdfRef(value, second[1])
        lexer.pos = saved
        return value
    if kind in ("real", "string", "name"):
        return value
    if kind in ("dict_open", "array_open"):
        if depth >= limit:
            raise PdfReadError("objects nest too deeply")
        if kind == "array_open":
            items: list[Any] = []
            while True:
                kind, value = lexer.token()
                if kind == "array_close":
                    return items
                if kind in ("eof", "dict_close"):
                    raise PdfReadError("unterminated array")
                items.append(parse_value(lexer, kind, value, depth + 1, limit))
        entries: dict[str, Any] = {}
        while True:
            kind, value = lexer.token()
            if kind == "dict_close":
                return entries
            if kind in ("eof", "array_close"):
                raise PdfReadError("unterminated dictionary")
            if kind != "name":
                # A key that is not a name: skip it, as viewers do.
                parse_value(lexer, kind, value, depth + 1, limit)
                continue
            next_kind, next_value = lexer.token()
            if next_kind == "dict_close":
                entries[value] = None
                return entries
            entries[value] = parse_value(lexer, next_kind, next_value, depth + 1, limit)
    if kind == "keyword":
        if value == b"true":
            return True
        if value == b"false":
            return False
        if value == b"null":
            return None
    raise PdfReadError(f"unexpected {kind} token {value!r}")
