# Derived from calibre (https://calibre-ebook.com) 9.18: src/calibre/utils/speedup.c.
# License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
Python ports of the C functions in calibre's ``speedup`` extension that the
ported metadata code calls.

calibre compiles these in C; LiuXin has no C extensions, so they are ported
here with the same rules. Each was checked against calibre's compiled C on
random input.

Example:
    >>> parse_iso8601("2001-02-03T04:05Z")
    (datetime.datetime(2001, 2, 3, 4, 5), True, 0)
    >>> clean_xml_chars("a\\x00b")
    'ab'
"""

from __future__ import annotations

import re
from datetime import datetime

__all__ = ["clean_xml_chars", "parse_iso8601"]

_DIGITS = b"0123456789"
_LEADING_SPACE = b" \n\r\t\v\f"

# The characters XML 1.0 allows, as speedup.c's clean_xml_chars tests them.
_XML_DISALLOWED = re.compile("[^\t\n\r\x20-\x7e\x85\xa0-퟿-﷏ﷰ-�\U00010000-\U0010ffff]")


def clean_xml_chars(text: str) -> str:
    """
    Remove the code points XML 1.0 does not allow, as calibre's C does.

    Example:
        >>> clean_xml_chars("ok\\ufffe\\x0b\\U0001f600")
        'ok\\U0001f600'


    :param text: The text.
    :return: The text without disallowed code points.
    :raises TypeError: For anything but text, as the C function does.
    """
    if not isinstance(text, str):
        raise TypeError("A unicode string is required")
    if not text:
        return text
    return _XML_DISALLOWED.sub("", text)


class _Reader:
    """
    Walk a date string's bytes the way speedup.c's pointer does.

    Example:
        >>> _Reader(b"12").number(4)
        (12, 2)
    """

    def __init__(self, raw: bytes) -> None:
        """
        Start at the first byte.

        Example:
            >>> _Reader(b"x").position
            0


        :param raw: The string's UTF-8 bytes, up to any NUL.
        :return: None.
        """
        self.raw = raw
        self.position = 0

    def peek(self) -> int:
        """
        Return the current byte, or 0 at the end, as a C string reads.

        Example:
            >>> _Reader(b"").peek()
            0


        :return: The byte's value.
        """
        return self.raw[self.position] if self.position < len(self.raw) else 0

    def skip(self, character: bytes) -> None:
        """
        Step over one optional separator.

        Example:
            >>> reader = _Reader(b"-1")
            >>> reader.skip(b"-")
            >>> reader.position
            1


        :param character: The separator.
        :return: None.
        """
        if self.peek() == character[0]:
            self.position += 1

    def number(self, max_digits: int) -> tuple[int, int]:
        """
        Read up to ``max_digits`` decimal digits.

        Example:
            >>> _Reader(b"2001-").number(4)
            (2001, 4)


        :param max_digits: The most digits to read.
        :return: The value and how many digits were read.
        """
        value = count = 0
        while count < max_digits:
            byte = self.peek()
            if byte and byte in _DIGITS:
                value = 10 * value + byte - 48
                self.position += 1
                count += 1
            else:
                break
        return value, count


def _invalid(raw: bytes, message: str) -> ValueError:
    """
    Build the C's error for a date it cannot parse.

    Example:
        >>> str(_invalid(b"x", "No year specified"))
        'x is not a valid ISO 8601 datestring: No year specified'


    :param raw: The date's bytes.
    :param message: What is wrong.
    :return: The error.
    """
    return ValueError(
        f"{raw.decode('utf-8')} is not a valid ISO 8601 datestring: {message}"
    )


def parse_iso8601(date_string: str) -> tuple[datetime, bool, int]:
    """
    Parse an ISO 8601 date as calibre's C ``speedup.parse_iso8601`` does.

    The rules are the C's, quirks included: a four-digit year is required;
    month, day, time and time zone are optional; a month of ``00`` means the
    string held only a year; text after the time zone is ignored.

    Example:
        >>> parse_iso8601(" 2001-02-03 04:05:06,5+05:30")
        (datetime.datetime(2001, 2, 3, 4, 5, 6, 500000), True, 19800)
        >>> parse_iso8601("2001")
        (datetime.datetime(2001, 1, 1, 0, 0), False, 0)


    :param date_string: The date.
    :return: The naive date and time, whether a time zone was given, and its
        offset in seconds.
    :raises ValueError: When there is no year or hour, or a field is out of range.
    """
    raw = date_string.encode("utf-8").partition(b"\0")[0]
    reader = _Reader(raw)

    while reader.peek() and reader.peek() in _LEADING_SPACE:
        reader.position += 1
    year, count = reader.number(4)
    if count < 4:
        raise _invalid(raw, "No year specified")
    reader.skip(b"-")
    month, _ = reader.number(2)
    day = 0
    if month == 0:
        month = 1
    else:
        reader.skip(b"-")
        day, _ = reader.number(2)
    if day == 0:
        day = 1
    if month > 12:
        raise _invalid(raw, "month greater than 12")
    hour = minute = second = microsecond = 0
    if reader.peek() in (ord("T"), ord(" ")):
        reader.position += 1
        hour, count = reader.number(2)
        if count < 2:
            raise _invalid(raw, "No hour specified")
        reader.skip(b":")
        minute, _ = reader.number(2)
        reader.skip(b":")
        second, _ = reader.number(2)
        if reader.peek() in (ord("."), ord(",")):
            reader.position += 1
            microsecond, count = reader.number(6)
            while reader.peek() and reader.peek() in _DIGITS:
                reader.position += 1
            microsecond *= 10 ** (6 - count)
    tz_hour, tz_minute, tz_sign = 1000, 0, 0
    marker = reader.peek()
    if marker == ord("Z"):
        tz_hour = 0
        reader.position += 1
    elif marker in (ord("+"), ord("-")):
        tz_sign = 1 if marker == ord("+") else -1
        reader.position += 1
    if tz_sign:
        tz_hour, _ = reader.number(2)
        reader.skip(b":")
        tz_minute, _ = reader.number(2)
    moment = datetime(year, month, day, hour, minute, second, microsecond)
    return moment, tz_hour != 1000, tz_sign * 60 * (tz_hour * 60 + tz_minute)
