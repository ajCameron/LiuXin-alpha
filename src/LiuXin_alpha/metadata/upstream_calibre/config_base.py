#!/usr/bin/env python
# License: GPLv3 Copyright: 2011, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/utils/config_base.py
# Changes from upstream: only the tweaks and the JSON helpers are ported: tweaks are
# read from calibre's default_tweaks.py, with no user tweaks file.
# SPDX-License-Identifier: GPL-3.0-only
"""
calibre's tweaks and the JSON helpers its metadata code uses.

Example:
    >>> tweaks["author_sort_copy_method"]
    'comma'
"""

from pathlib import Path
from typing import Any


def parse_python_tweaks(text: str) -> dict[str, Any]:
    """
    Read the literal assignments of a tweaks file, as calibre does.

    Example:
        >>> parse_python_tweaks("a = 1\\nb = f()\\n")
        {'a': 1}


    :param text: The file's Python source.
    :return: Each assigned name and its literal value.
    """
    import ast

    ans: dict[str, Any] = {}
    tree = ast.parse(text, filename="<string>")
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except Exception:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    ans[target.id] = value
    return ans


# calibre's defaults; LiuXin has no user tweaks file.
tweaks: dict[str, Any] = parse_python_tweaks(
    Path(__file__).with_name("default_tweaks.py").read_text(encoding="utf-8")
)


def to_json(obj: object) -> dict[str, object] | str | int:
    """
    Encode what JSON cannot hold, as calibre's ``to_json`` does.

    Example:
        >>> to_json({1, 2}), to_json(bytearray(b"x"))
        ({'__class__': 'set', '__value__': (1, 2)}, {'__class__': 'bytearray', '__value__': 'eA=='})


    :param obj: A set, date, byte array or bytes.
    :return: A JSON-friendly stand-in.
    :raises TypeError: For anything else.
    """
    import datetime

    if isinstance(obj, bytearray):
        from base64 import standard_b64encode

        return {
            "__class__": "bytearray",
            "__value__": standard_b64encode(bytes(obj)).decode("ascii"),
        }
    if isinstance(obj, datetime.datetime):
        from LiuXin_alpha.metadata.upstream_calibre.date import isoformat

        return {
            "__class__": "datetime.datetime",
            "__value__": isoformat(obj, as_utc=True),
        }
    if isinstance(obj, (set, frozenset)):
        return {"__class__": "set", "__value__": tuple(obj)}
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    if hasattr(obj, "toBase64"):
        return {
            "__class__": "bytearray",
            "__value__": bytes(obj.toBase64()).decode("ascii"),
        }  # type: ignore[attr-defined]
    v = getattr(obj, "value", None)
    if isinstance(v, int):
        return v
    raise TypeError(repr(obj) + " is not JSON serializable")


def from_json(obj: dict[str, Any]) -> object:
    """
    Decode what ``to_json`` encoded, as calibre's ``from_json`` does.

    Example:
        >>> from_json({"__class__": "set", "__value__": [1]})
        {1}


    :param obj: A decoded JSON object.
    :return: The value it stands for, or ``obj`` itself.
    """
    custom = obj.get("__class__")
    if custom is not None:
        if custom == "bytearray":
            from base64 import standard_b64decode

            return bytearray(standard_b64decode(obj["__value__"].encode("ascii")))
        if custom == "datetime.datetime":
            from LiuXin_alpha.metadata.upstream_calibre.iso8601 import parse_iso8601

            return parse_iso8601(obj["__value__"], assume_utc=True)
        if custom == "set":
            return set(obj["__value__"])
    return obj


class Tweak:
    """
    Change one tweak for a ``with`` block, as calibre's tests do.

    Example:
        >>> with Tweak("title_series_sorting", "strictly_alphabetic"):
        ...     tweaks["title_series_sorting"]
        'strictly_alphabetic'
        >>> tweaks["title_series_sorting"]
        'library_order'
    """

    def __init__(self, name: str, value: object) -> None:
        """
        Remember the tweak and its new value.

        Example:
            >>> Tweak("x", 1).value
            1


        :param name: The tweak.
        :param value: Its value inside the block.
        :return: None.
        """
        self.name, self.value = name, value

    def __enter__(self) -> None:
        """
        Set the new value.

        Example:
            Called on entering the ``with`` block.


        :return: None.
        """
        self.origval = tweaks[self.name]
        tweaks[self.name] = self.value

    def __exit__(self, *args: object) -> None:
        """
        Restore the old value.

        Example:
            Called on leaving the ``with`` block.


        :param args: The exception details, if any.
        :return: None.
        """
        tweaks[self.name] = self.origval
