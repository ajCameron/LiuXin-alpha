# -*- coding: utf-8 -*-

"""
Interface defining the necessary public functions for a pdb format writer.
"""
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class FormatWriter(object):
    def __init__(self: _typing.Self, opts: _typing.Any, log: _typing.Any) -> None:
        raise NotImplementedError()

    def write_content(self: _typing.Self, oeb_book: _typing.Any, output_stream: _typing.Any, metadata: _typing.Any = None) -> None:
        raise NotImplementedError()
