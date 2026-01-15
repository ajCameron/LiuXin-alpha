# Tools to handle JSON files and to parse them for metadata
# This file will probably only contain a few methods, as more detailed methods to handle incoming data return in this
# format will probably be stored with the access methods for the sites that produce them

import base64
import datetime

from LiuXin.utils.date import parse_date
from LiuXin.utils.date import isoformat

__author__ = "Cameron"


def to_json(obj):
    if isinstance(obj, bytearray):
        return {
            "__class__": "bytearray",
            "__value__": base64.standard_b64encode(bytes(obj)),
        }
    if isinstance(obj, datetime.datetime):
        return {
            "__class__": "datetime.datetime",
            "__value__": isoformat(obj, as_utc=True),
        }
    raise TypeError(repr(obj) + " is not JSON serializable")


def from_json(obj):
    if "__class__" in obj:
        if obj["__class__"] == "bytearray":
            return bytearray(base64.standard_b64decode(obj["__value__"]))
        if obj["__class__"] == "datetime.datetime":
            return parse_date(obj["__value__"], assume_utc=True)
    return obj
