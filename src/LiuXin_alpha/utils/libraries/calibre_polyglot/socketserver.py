#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Eli Schwartz <eschwartz@archlinux.org>

"""
Provide socketserver utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise socketserver through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from socketserver import TCPServer, ThreadingMixIn  # noqa

__all__ = ["TCPServer", "ThreadingMixIn"]
