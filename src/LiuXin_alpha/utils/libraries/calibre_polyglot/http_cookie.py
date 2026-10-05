#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Eli Schwartz <eschwartz@archlinux.org>

"""
Expose HTTP cookie compatibility aliases.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise http cookie through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from http.cookies import SimpleCookie  # noqa
from http.cookiejar import CookieJar, Cookie  # noqa

__all__ = ["SimpleCookie", "CookieJar", "Cookie"]
