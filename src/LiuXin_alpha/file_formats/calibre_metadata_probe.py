"""
Read one file's metadata with an installed calibre, on behalf of LiuXin.

This script runs inside calibre's own Python, started by
``calibre-debug -e <this file> <request.json>``. It loads one of calibre's
metadata reader plugins by name, reads one file with it, and writes calibre's
own serialisation of the result (``metadata_as_dict``) to a JSON result file.
LiuXin never imports calibre; it only reads the result.

The request file is a JSON object with these keys:

- ``plugin``: the reader plugin's name, for example ``"Read EPUB metadata"``;
- ``source``: the absolute path of the file to read;
- ``format``: the file's format, for example ``"epub"``;
- ``result``: the absolute path to write the result to.

The result is a JSON object: ``calibre_version``, ``metadata`` (field to value;
dates as ISO 8601 text), ``cover`` (``[format, base64 bytes]`` or ``null``),
``text_fields`` (fields whose values had no JSON form and were written as
text) and ``error`` (a traceback, or ``null``).

The script must stay runnable on the Python versions calibre has bundled since
calibre 6, so it uses no syntax newer than Python 3.8, and it imports calibre
only inside :func:`main`, so LiuXin can import this module safely. LiuXin
copies it to an empty folder before running it, because ``calibre-debug``
puts the script's folder first on ``sys.path``.
"""

from __future__ import annotations

import datetime
import json
import math
import sys
import traceback
from typing import Any


def jsonable(value: Any, where: str, text_fields: list[str]) -> Any:
    """
    Turn a value from ``metadata_as_dict`` into something JSON can hold.

    Dates become ISO 8601 text, sets become sorted lists, and dictionary keys
    become text. Anything else without a JSON form is written as text, and its
    place is appended to ``text_fields`` so the reader can report it.

    Example:
        >>> found = []
        >>> jsonable({"pubdate": datetime.date(2026, 11, 17), "tags": {"b", "a"}}, "x", found)
        {'pubdate': '2026-11-17', 'tags': ['a', 'b']}
        >>> jsonable(float("nan"), "rating", found), found
        ('nan', ['rating'])


    :param value: The value to convert.
    :param where: The field and position, recorded if text is substituted.
    :param text_fields: A list to append substituted places to.
    :return: A value made only of None, bool, int, float, str, list and dict.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        text_fields.append(where)
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        text_fields.append(where)
        return value.decode("utf-8", "replace")
    if isinstance(value, (list, tuple)):
        return [
            jsonable(item, f"{where}[{index}]", text_fields)
            for index, item in enumerate(value)
        ]
    if isinstance(value, (set, frozenset)):
        items = [jsonable(item, where, text_fields) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, dict):
        return {
            str(key): jsonable(item, f"{where}.{key}", text_fields)
            for key, item in value.items()
        }
    text_fields.append(where)
    return str(value)


def read_with_calibre(request: dict[str, Any]) -> dict[str, Any]:
    """
    Read one file with one of calibre's metadata reader plugins.

    The plugin runs as calibre's ``get_file_type_metadata`` runs it, except
    that it is chosen by name rather than by format, and covers are always
    read.

    Example:
        Called by :func:`main` inside calibre::

            result = read_with_calibre({"plugin": "Read EPUB metadata", ...})


    :param request: The request described in the module docstring.
    :return: The result described in the module docstring, without writing it.
    """
    result: dict[str, Any] = {
        "calibre_version": None,
        "metadata": None,
        "cover": None,
        "text_fields": [],
        "error": None,
    }
    from calibre.constants import __version__
    from calibre.customize.ui import find_plugin
    from calibre.ebooks.metadata.book.serialize import metadata_as_dict

    result["calibre_version"] = str(__version__)
    plugin = find_plugin(request["plugin"])
    if plugin is None:
        result["error"] = (
            f"calibre {__version__} has no plugin named {request['plugin']!r}"
        )
        return result
    with plugin:
        plugin.quick = False
        with open(request["source"], "rb") as stream:
            metadata = plugin.get_metadata(stream, request["format"])
    if metadata is None:
        result["metadata"] = {}
        return result
    serialised = metadata_as_dict(metadata, encode_cover_data=True)
    cover = serialised.pop("cover_data", None)
    text_fields: list[str] = []
    result["metadata"] = {
        str(name): jsonable(value, str(name), text_fields)
        for name, value in serialised.items()
    }
    result["text_fields"] = text_fields
    if cover and len(cover) == 2 and cover[1]:
        result["cover"] = [str(cover[0] or ""), str(cover[1])]
    return result


def main(argv: list[str]) -> int:
    """
    Answer the request named on the command line, writing the result file.

    Any failure is written into the result as a traceback, so LiuXin can show
    what went wrong inside calibre.

    Example:
        Run by calibre's debug launcher::

            calibre-debug -e calibre_metadata_probe.py request.json


    :param argv: ``sys.argv``; the request file's path is the second entry.
    :return: 0 once a result file has been written.
    """
    with open(argv[1], encoding="utf-8") as handle:
        request = json.load(handle)
    try:
        result = read_with_calibre(request)
    except Exception:
        result = {
            "calibre_version": None,
            "metadata": None,
            "cover": None,
            "text_fields": [],
            "error": traceback.format_exc(),
        }
    with open(request["result"], "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
