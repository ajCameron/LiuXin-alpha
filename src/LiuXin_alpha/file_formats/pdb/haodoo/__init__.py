#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported haodoo compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"
