# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``wpd`` extension (Windows Portable Devices).

This is a stub to keep imports working on non-Windows systems or when the extension
is not compiled.
"""

from __future__ import annotations


class WpdError(Exception):
    pass
