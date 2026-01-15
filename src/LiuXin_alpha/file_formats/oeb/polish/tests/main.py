#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


try:
    import init_calibre  # noqa
except ImportError:
    pass

import os
import unittest


def find_tests():
    return unittest.defaultTestLoader.discover(os.path.dirname(os.path.abspath(__file__)), pattern="*.py")


# Todo: Fix
if __name__ == "__main__":
    from LiuXin.utils.calibre.db.tests.main import run_tests

    run_tests(find_tests=find_tests)
