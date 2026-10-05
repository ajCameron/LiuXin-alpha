#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Expose the supported tests compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
