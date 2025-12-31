# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``usbobserver`` extension.

The compiled extension is platform-specific (macOS). This fallback provides a minimal API.
"""

from __future__ import annotations


def date_format() -> str:
    # A reasonable default date format.
    return "%Y-%m-%d"
