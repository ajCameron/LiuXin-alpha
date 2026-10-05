#!/usr/bin/env  python

"""
Expose the supported txt compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2008, John Schember john@nachtimwald.com"
__docformat__ = "restructuredtext en"

"""
Used for txt output
"""
