#!/usr/bin/env python

"""
Expose the supported comic compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/comic/test_comic_modernized.py
"""
from __future__ import annotations

from .input import extract_comic, find_pages, process_pages

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

__all__ = ["extract_comic", "find_pages", "process_pages"]
