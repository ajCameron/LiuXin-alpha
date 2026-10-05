#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Exercise coordinated OEB polishing operations and failure handling.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise main through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


try:
    import init_calibre  # noqa
except ImportError:
    pass

import os
import unittest


def find_tests() -> _typing.Any:
    """
    Find tests under the format's safety and compatibility rules.

    Example:
        Exercise find tests through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return unittest.defaultTestLoader.discover(os.path.dirname(os.path.abspath(__file__)), pattern="*.py")


# Todo: Fix
if __name__ == "__main__":
    suite = find_tests()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
