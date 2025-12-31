# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``monotonic`` extension.
"""

from __future__ import annotations

import time


def monotonic() -> float:
    return time.monotonic()
