# -*- coding: utf-8 -*-

"""
Expose the supported compression compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
"""
from __future__ import annotations

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

__all__ = ["compressed_ebooks", "palmdoc", "tcr"]
