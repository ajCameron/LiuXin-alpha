# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``progress_indicator`` plugin.

The compiled plugin is typically used for GUI progress feedback. This fallback provides
a no-op ``ProgressIndicator`` with the expected methods so imports succeed.
"""

from __future__ import annotations


class ProgressIndicator:
    def __init__(self, *args, **kwargs) -> None:
        self.fraction = 0.0

    def set_fraction(self, frac: float) -> None:
        try:
            self.fraction = float(frac)
        except Exception:
            self.fraction = 0.0

    def reset(self) -> None:
        self.fraction = 0.0

    def update(self, *args, **kwargs) -> None:
        return None
