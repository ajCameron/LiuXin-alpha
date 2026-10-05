#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>

"""
Expose retained functools compatibility helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise functools through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from functools import lru_cache

__all__ = ["lru_cache"]
