#!/usr/bin/env  python

"""
Expose the supported djvu compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2011, Anthon van der Neut <anthon@mnt.org>"
__docformat__ = "restructuredtext en"

"""
Used for DJVU input
"""
