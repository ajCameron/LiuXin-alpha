# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``matcher`` extension.

The compiled module provides fast string matching. This fallback uses difflib.
"""

from __future__ import annotations

import difflib
from typing import List, Sequence, Tuple


def ratio(a: str, b: str) -> float:
    return difflib.SequenceMatcher(a=a, b=b).ratio()
