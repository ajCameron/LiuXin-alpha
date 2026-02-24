"""Macros API contract."""

from __future__ import annotations

import abc


class MacrosAPI(abc.ABC):
    """
    Macros are chained statements, even as a single piece of code or a function.
    """
