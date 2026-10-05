#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2018, Kovid Goyal <kovid at kovidgoyal.net>

"""
Expose HTTP server compatibility aliases.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise http server through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from http.server import HTTPServer, SimpleHTTPRequestHandler  # noqa

__all__ = ["HTTPServer", "SimpleHTTPRequestHandler"]
