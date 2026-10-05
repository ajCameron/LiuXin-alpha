#!/usr/bin/env  python
"""
Expose the supported snb compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"
__docformat__ = "restructuredtext en"

"""
Used for snb output
"""
