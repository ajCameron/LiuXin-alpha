# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``html`` extension (calibre internal, not stdlib html).

The compiled module in calibre implements HTML parsing helpers and a small spell-check bridge.
This fallback is minimal: init() is a no-op; check_spelling() returns an empty list.

Note: This module name shadows stdlib `html`; ensure your imports are explicit and controlled.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List


def init(*args, **kwargs) -> None:
    return None


def check_spelling(*args, **kwargs) -> List[Any]:
    return []


@dataclass
class Tag:
    name: str = ""

    def copy(self):
        return Tag(self.name)


@dataclass
class State:
    data: Any = None

    def copy(self):
        return State(self.data)
