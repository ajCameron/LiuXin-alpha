#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Resolve retained HTML entity names into normalized Unicode text.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise html entities through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

# Ported from calibre.
# The LiuXin-alpha tree vendors the html5lib constants and a small compat layer.
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import entities

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


html5_entities = {k.replace(";", ""): v for k, v in entities.items()}
