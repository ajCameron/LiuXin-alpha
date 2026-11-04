"""
Convert shared scalar and sequence values without bibliographic field selection.

Keep the existing text, number, boolean, date, UUID and sequence conversion
contracts here. Callers explicitly select the converter they need; these helpers
do not know about title fallbacks, language/identifier policy or custom fields.
Those rules belong to metadata.field_adaptors, which consumes these primitives.

Some converters intentionally have different missing-value and coercion rules.
Their names and docstrings describe the compatibility contract; sharing a module
does not make them interchangeable.
"""
from __future__ import annotations, division, absolute_import, print_function, unicode_literals

import json
from datetime import datetime
from typing import Any, Optional, AnyStr, Union
from uuid import UUID

from LiuXin_alpha.constants import preferred_encoding
from LiuXin_alpha.utils.date import isoformat, UNDEFINED_DATE, parse_date, is_date_undefined, parse_only_date
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode, six_unicode


def _boolish_to_bool(val: Any) -> Optional[bool]:
    """
    Convert typical DB "bool-ish" values to Python bool.

    Example:
        Exercise  boolish to bool through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        if val == 0:
            return False
        if val == 1:
            return True
        return None
    if isinstance(val, str):
        if val == "0":
            return False
        if val == "1":
            return True
        return None
    return None


def _bool_to_int_or_none(val: Optional[bool]) -> Optional[int]:
    """
    Normalize bool to int or none into the compatibility boolean representation.

    Example:
        Exercise  bool to int or none through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if val is None:
        return None
    return 1 if val else 0


def optional_text(value: Any) -> str | None:
    """
    Stringify and strip a supplied value, treating None or resulting blank text as absent.

    False and zero become nonempty strings; conversion errors propagate.

    Example:
        Exercise  optional text through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    """
    Attempt integer conversion, treating None, empty text, TypeError, and ValueError as absent. Boolean and truncatable numeric values are accepted; positivity is not checked. Other failures such as OverflowError propagate.

    Example:
        Exercise  to int through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _boolish(value: Any, *, default: bool) -> bool:
    """
    Interpret legacy boolean columns with an explicit fallback for unknown values. Preserve booleans, use numeric truthiness, and recognize stripped case-insensitive yes/no, y/n, on/off, true/false, and 1/0 text. Empty text is false; None, unrecognized strings, and other object types return default without using their general truthiness.

    Example:
        Exercise  boolish through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param value: Value normalized, stored, formatted or returned.
    :param default: Value supplied for default under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off", ""}:
            return False
    return default


def _parse_tags(value: Any) -> tuple[str, ...]:
    """
    Convert legacy tag text, JSON, collections, or scalars to a tuple of tag strings. Strings are recursively JSON-decoded when possible; failed decoding/conversion falls back to the original stripped string. Lists, tuples, and sets contribute stripped nonblank item spellings without recursive flattening or deduplication. Set order is not stabilized, and JSON scalar text can change spelling during conversion. None and empty input yield no tags.

    Example:
        Exercise  parse tags through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None or value == "" or value == ():
        return ()
    if isinstance(value, str):
        try:
            return _parse_tags(json.loads(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return (value.strip(),) if value.strip() else ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(
            text for item in value if (text := optional_text(item)) is not None
        )
    text = optional_text(value)
    return () if text is None else (text,)


def _optional_uuid(value: Any) -> UUID | None:
    """
    Strip optional text and parse a nonblank UUID. Blank values become None; other malformed values raise through UUID construction.

    Example:
        Exercise  optional uuid through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = optional_text(value)
    return None if text is None else UUID(text)


def string_to_datetime(x: str) -> datetime:
    """
    Format datetime instances as UTC text and preserve other inputs.

    The formatter treats a naive datetime as local time and converts to UTC.
    Non-datetime values, including None and strings, pass through despite the narrow
    annotations.

    Example:
        >>> string_to_datetime("2024-01-01 00:00:00")
        '2024-01-01 00:00:00'
        >>> string_to_datetime(None) is None
        True


    :param x: Value to prepare; datetime instances use the shared ISO formatter.
    :return: ISO text with a space separator for datetimes, otherwise the original
        object.
    """
    return isoformat(x, sep=" ") if isinstance(x, datetime) else x


def single_text(x: Optional[AnyStr]) -> Optional[str]:
    """
    Decode or stringify one value, strip its edges and collapse empty text to None.

    Non-text inputs first try decode(preferred_encoding, "replace"); AttributeError
    falls back to six_unicode. Internal whitespace is preserved. Other
    decoding/stringification errors propagate.

    Example:
        >>> single_text("  Example  ")
        'Example'
        >>> single_text("   ") is None
        True


    :param x: Text, bytes, another printable value, or None.
    :return: Stripped text, or None for None/empty/whitespace-only input.
    """
    if x is None:
        return x
    if not isinstance(x, unicode):
        try:
            x = x.decode(preferred_encoding, "replace")
        except AttributeError:
            x = six_unicode(x)
    x = x.strip()
    return x if x else None


def multiple_text(sep: str, ui_sep: str, x: AnyStr) -> tuple[str, ...]:
    """
    Split or iterate text values, normalize spacing and replace embedded display separators.

    False input returns an empty tuple before separator access. Byte elements in an
    iterable decode with replacement. A top-level byte string instead uses the
    misspelled error handler "replce", which can raise LookupError when decoding needs
    that handler. Replace the stripped ui_sep with a comma when it is ";", otherwise
    with a semicolon, then collapse whitespace within each token. Empty separators are
    not specially validated.

    Example:
        >>> multiple_text(",", ", ", "  alpha  , beta ,, ")
        ('alpha', 'beta')


    :param sep: Separator used only when splitting a single text input.
    :param ui_sep: Display separator, stripped before replacement within each token.
    :param x: Text to split, an iterable of text/byte values, or a false value.
    :return: Tuple of nonempty normalized strings in input order; duplicates remain.
    :raises LookupError: Decoding a top-level byte string needs the unregistered
        "replce" error handler.
    :raises ValueError: A text input is split with an empty separator.
    """
    if not x:
        return ()

    if isinstance(x, bytes):
        x = x.decode(preferred_encoding, "replce")

    if isinstance(x, unicode):
        x = x.split(sep)

    else:
        x = (y.decode(preferred_encoding, "replace") if isinstance(y, bytes) else y for y in x)

    ui_sep = ui_sep.strip()
    repsep = "," if ui_sep == ";" else ";"
    x = (y.strip().replace(ui_sep, repsep) for y in x if y.strip())
    return tuple(" ".join(y.split()) for y in x if y)


def adapt_datetime(x: Union[AnyStr, datetime]) -> datetime:
    """
    Parse text as local time and normalize undefined or non-date values.

    Strings use parse_date with assume_utc=False and as_utc=False. If undefined-date
    inspection raises AttributeError, replace the value with UNDEFINED_DATE. Truthy
    values identified as undefined also become that sentinel; None remains None. Parsing
    errors are not suppressed.

    Example:
        >>> adapt_datetime(None) is None
        True
        >>> adapt_datetime(123) == UNDEFINED_DATE
        True


    :param x: Date/time text or bytes, a date-like value, or None.
    :return: Parsed local-time datetime, an accepted original value, UNDEFINED_DATE, or
        None.
    """
    if isinstance(x, (unicode, bytes)):
        x = parse_date(x, assume_utc=False, as_utc=False)

    try:
        x_is_date_undefined = is_date_undefined(x)
    except AttributeError:
        x_is_date_undefined = True
        x = UNDEFINED_DATE

    if x and x_is_date_undefined:
        x = UNDEFINED_DATE

    return x


def adapt_date(x: Optional[Union[AnyStr, datetime]]) -> datetime:
    """
    Parse a date-only value with the shared timezone-safe date convention.

    Text uses parse_only_date with its UTC defaults and day-boundary adjustment, so the
    represented day can move near month boundaries. Other values are checked without the
    AttributeError fallback used by adapt_datetime.

    Example:
        >>> adapt_date(None) == UNDEFINED_DATE
        True


    :param x: Date-only text/bytes, a date-like value, or None.
    :return: Parsed or original date-like value, with None/undefined values replaced by
        UNDEFINED_DATE.
    :raises AttributeError: A non-null, non-string value lacks attributes required by
        undefined-date inspection.
    """
    if isinstance(x, (unicode, bytes)):
        x = parse_only_date(x)
    if x is None or is_date_undefined(x):
        x = UNDEFINED_DATE
    return x


def adapt_number(typ: Union[int, float], x: AnyStr) -> Optional[Union[int, float]]:
    """
    Convert a non-null value with the requested numeric callable.

    No general whitespace stripping or boolean rejection is added. Nonempty bytes are
    not decoded before comparison with "none", so b"none" reaches the numeric converter
    and fails. Conversion exceptions propagate.

    Example:
        >>> adapt_number(int, "42")
        42
        >>> adapt_number(float, "NONE") is None
        True


    :param typ: Conversion callable, normally int or float.
    :param x: Value to convert; None, empty text/bytes and case-insensitive text "none"
        mean unset.
    :return: None for recognized unset values, otherwise typ(x).
    :raises ValueError: The requested numeric converter cannot parse the value.
    :raises TypeError: The callable cannot convert the supplied type.
    """

    if x is None:
        return None

    if isinstance(x, (unicode, bytes)):
        if not x or x.lower() == "none":
            return None

    return typ(x)


def adapt_bool(x: AnyStr) -> Optional[bool]:
    """
    Convert legacy boolean text or numeric values to True, False or None.

    Lowercase string inputs first. Text "true" and "false" are recognized directly;
    other string/byte inputs go through int then bool. Byte words are not decoded and do
    not match the text sentinels. Non-string values use bool, so numeric zero is False.

    Example:
        >>> adapt_bool("FALSE")
        False
        >>> adapt_bool("none") is None
        True


    :param x: Text/bytes or a value with normal Python truth semantics.
    :return: None for None or text "none"/empty text; otherwise a boolean.
    :raises ValueError: A string/byte value is neither a recognized text word nor
        integer-convertible.
    """
    if isinstance(x, (unicode, bytes)):
        x = x.lower()
        if x == "true":
            x = True
        elif x == "false":
            x = False
        elif x == "none" or x == "":
            x = None
        else:
            x = bool(int(x))
    return x if x is None else bool(x)
