"""
Provide shared HTML escaping, text abbreviation, option parsing, and column lookup without importing applications.

Missing columns have an explicit fallback; unexpected row-access failures
remain visible to callers instead of being rendered as absent metadata.
Integer option parsing has its own broader fallback boundary. Text widths count
Python string characters, not terminal cells or rendered HTML display width.
"""

from __future__ import annotations

import html
from typing import Protocol


class RowLookup(Protocol):
    """
    Describe string-column subscription shared by mappings and Core row projections.

    A missing column is signalled with KeyError. No iteration, mutation, or
    concrete row class is required by consumers of this structural protocol.

    Example:
        >>> row: RowLookup = {"title": "雪"}
        >>> row["title"]
        '雪'
    """

    def __getitem__(self, column: str, /) -> object:
        """
        Return a stored column value or raise KeyError to indicate that the column is absent.

        Other access failures remain distinct from a missing column.

        Example:
            >>> row = {"title": None}
            >>> row["title"] is None
            True


        :param column: Positional-only column key used for subscription.
        :return: Stored value, including an explicit None when the column exists.
        :raises KeyError: If the column is not present in the row.
        """
        ...


def escape(value: object) -> str:
    """
    Stringify a value and HTML-escape ampersands, angle brackets, and both quote characters.

    None becomes empty text. Existing entity text is escaped again; this does not
    validate URLs or make values safe for JavaScript, CSS, or unquoted attributes.

    Example:
        >>> escape('<雪 & "猫">')
        '&lt;雪 &amp; &quot;猫&quot;&gt;'


    :param value: Display value converted with str unless it is None.
    :return: HTML-escaped text suitable for text content or a quoted attribute value.
    """
    return html.escape("" if value is None else str(value), quote=True)


def short_text(value: object, *, width: int = 120) -> str:
    """
    Normalize CRLF/CR line endings to LF and abbreviate overlong text with three literal dots.

    Newlines and surrounding whitespace otherwise remain intact. Width counts
    string characters rather than display cells. Truncation keeps at most
    max(0, width - 3) characters, so the dots can exceed a width below three;
    even empty text is abbreviated when width is negative. Width is not coerced.

    Example:
        >>> short_text("abcdef", width=5)
        'ab...'
        >>> short_text("abcdef", width=0)
        '...'


    :param value: Value converted to text, with None treated as empty.
    :param width: Maximum unabridged character count, also used to compute the truncation prefix.
    :return: Normalized full text when it fits, otherwise a prefix followed by three dots.
    """
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def coerce_int(
    raw: str | None,
    *,
    default: int,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    """
    Parse stripped stringified input as an integer, fall back on Exception, then clamp minimum before maximum.

    The broad catch covers only initial stringification/parsing. Conversion errors
    from default or bounds still propagate, as do BaseException subclasses.
    Bounds are not checked for consistency: a maximum below the minimum wins
    because the upper clamp is applied last.

    Example:
        >>> coerce_int(" bad ", default=12, minimum=0, maximum=10)
        10
        >>> coerce_int("1", default=0, minimum=5, maximum=3)
        3


    :param raw: Option text or None; parsed via int(str(raw).strip()).
    :param default: Integer-convertible fallback used when initial parsing raises Exception.
    :param minimum: Lower bound converted to int and applied to the parsed or fallback value.
    :param maximum: Optional upper bound converted to int and applied after the lower bound.
    :return: Parsed/fallback integer after ordered clamping, not necessarily within inconsistent bounds.
    """
    try:
        value = int(str(raw).strip())
    except Exception:
        value = int(default)
    value = max(int(minimum), int(value))
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def row_value(row: RowLookup, column: str) -> object:
    """
    Subscribe to one column, replacing only KeyError with None.

    Stored None and an absent key have the same return value. Other lookup errors
    propagate without retry, normalization, attribute lookup, or fallback to get.

    Example:
        >>> row_value({"title": "雪"}, "title")
        '雪'
        >>> row_value({}, "title") is None
        True


    :param row: Object supporting string-key subscription according to RowLookup.
    :param column: Column key forwarded unchanged to the subscription operation.
    :return: Stored object, or None if subscription raises KeyError.
    """
    try:
        return row[column]
    except KeyError:
        return None
