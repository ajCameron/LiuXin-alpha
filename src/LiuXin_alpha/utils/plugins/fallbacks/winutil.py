# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``winutil`` extension.

The C extension provides Windows-specific helpers. For portability, this fallback
implements only what is commonly used: strftime().
"""

from __future__ import annotations

import time
from typing import Optional, Tuple


def strftime(fmt: str, t: Optional[Tuple[int, ...]] = None) -> str:
    if t is None:
        return time.strftime(fmt)
    return time.strftime(fmt, t)  # type: ignore[arg-type]
