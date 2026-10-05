#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>

"""
Expose property-list compatibility helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise plistlib through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from plistlib import loads, dumps  # noqa

loads_binary_or_xml = loads

__all__ = ["loads_binary_or_xml", "loads", "dumps"]
