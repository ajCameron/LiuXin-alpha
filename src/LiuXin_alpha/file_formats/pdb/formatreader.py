# -*- coding: utf-8 -*-

"""
Interface defining the necessary public functions for a pdb format reader.
"""
from __future__ import annotations

import typing as _typing

from abc import ABC, abstractmethod
from os import PathLike
from typing import BinaryIO

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class FormatReader(ABC):
    @abstractmethod
    def __init__(
        self: _typing.Self,
        header: object,
        stream: BinaryIO,
        log: object,
        options: object,
    ) -> None: ...

    @abstractmethod
    def extract_content(
        self: _typing.Self,
        output_dir: str | PathLike[str],
    ) -> object: ...
