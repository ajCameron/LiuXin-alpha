"""
Adapt legacy booleans, integers, identifiers and tag values into stable utility representations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise adaptors through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
from __future__ import annotations

import json
from typing import Any, Optional
from uuid import UUID


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


def _optional_text(value: Any) -> str | None:
    """
    Stringify and strip a supplied value, treating None or resulting blank text as absent. False and zero become nonempty strings; conversion errors propagate.

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
            text for item in value if (text := _optional_text(item)) is not None
        )
    text = _optional_text(value)
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
    text = _optional_text(value)
    return None if text is None else UUID(text)
