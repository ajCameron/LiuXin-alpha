#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Eli Schwartz <eschwartz@archlinux.org>

"""
Expose normalized HTML entity tables under the polyglot namespace.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise html entities through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from html.entities import name2codepoint

__all__ = ["name2codepoint"]
