# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
import os

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class TxtNewlines(object):

    NEWLINE_TYPES = {
        "system": os.linesep,
        "unix": "\n",
        "old_mac": "\r",
        "windows": "\r\n",
    }

    def __init__(self: _typing.Self, newline_type: _typing.Any) -> None:
        self.newline = self.NEWLINE_TYPES.get(newline_type.lower(), os.linesep)


def specified_newlines(newline: _typing.Any, text: _typing.Any) -> _typing.Any:
    # Convert all newlines to \n
    text = text.replace("\r\n", "\n")
    text = text.replace("\r", "\n")

    if newline == "\n":
        return text

    return text.replace("\n", newline)
