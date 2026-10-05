#!/usr/bin/env  python

"""
Expose the supported odt compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/odt/test_odt_container_framework.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

"""
Handle the Open Document Format
"""
