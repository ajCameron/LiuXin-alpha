"""
Helpers for serializing/deserializing a small set of non-JSON native types.
"""

from __future__ import annotations

import base64
import datetime

from LiuXin_alpha.utils.date import isoformat, parse_date

__author__ = "Cameron"


def _parse_datetime_value(raw: str | bytes) -> datetime.datetime:
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
    if not isinstance(obj, dict):
        return obj
    cls = obj.get("__class__")
    if cls == "bytearray":
        return bytearray(base64.standard_b64decode(obj["__value__"]))
    if cls == "datetime.datetime":
        return _parse_datetime_value(obj["__value__"])
    return obj
