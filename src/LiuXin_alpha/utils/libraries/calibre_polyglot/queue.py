#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Eli Schwartz <eschwartz@archlinux.org>

"""
Provide queue utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise queue through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from queue import Queue, Empty, Full, PriorityQueue, LifoQueue  # noqa

__all__ = ["Queue", "Empty", "Full", "PriorityQueue", "LifoQueue"]
