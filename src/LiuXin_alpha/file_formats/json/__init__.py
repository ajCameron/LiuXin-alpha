"""
Expose the supported json compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/json/test_json_modernized.py
"""

from __future__ import annotations

import base64
import datetime

from LiuXin_alpha.utils.date import isoformat, parse_date

__author__ = "Cameron"


def _parse_datetime_value(raw: str | bytes) -> datetime.datetime:
    """
    Parse datetime value under the format's safety and compatibility rules.

    Example:
        Exercise  parse datetime value through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return parse_date(raw, assume_utc=True)
    except Exception:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        text = raw[:-1] + "+00:00" if isinstance(raw, str) and raw.endswith("Z") else raw
        dt = datetime.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)


def to_json(obj: object) -> dict[str, str]:
    """
    Perform the to json operation under explicit file-format and conversion rules.

    Example:
        Exercise to json through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raw = bytes(obj)
        return {
            "__class__": "bytearray",
            "__value__": base64.standard_b64encode(raw).decode("ascii"),
        }
    if isinstance(obj, datetime.datetime):
        return {
            "__class__": "datetime.datetime",
            "__value__": isoformat(obj, as_utc=True),
        }
    raise TypeError(repr(obj) + " is not JSON serializable")


def from_json(obj: object) -> object:
    """
    Perform the from json operation under explicit file-format and conversion rules.

    Example:
        Exercise from json through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not isinstance(obj, dict):
        return obj
    cls = obj.get("__class__")
    if cls == "bytearray":
        return bytearray(base64.standard_b64decode(obj["__value__"]))
    if cls == "datetime.datetime":
        return _parse_datetime_value(obj["__value__"])
    return obj
