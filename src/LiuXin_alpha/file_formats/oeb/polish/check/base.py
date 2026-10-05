#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Define common EPUB/OEB validation problem and checker contracts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from contextlib import closing
from functools import partial
from multiprocessing.pool import ThreadPool
import os

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

DEBUG, INFO, WARN, ERROR, CRITICAL = range(5)


def cpu_count() -> bool:
    """
    Perform the cpu count operation under explicit file-format and conversion rules.

    Example:
        Exercise cpu count through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return os.cpu_count() or 1


class BaseError(object):

    """
    Report a baseerror encountered while processing an ebook format.

    Example:
        Exercise BaseError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = ""
    INDIVIDUAL_FIX = ""
    level = ERROR
    has_multiple_locations = False

    def __init__(self: _typing.Self, msg: _typing.Any, name: _typing.Any, line: _typing.Any = None, col: _typing.Any = None) -> None:
        """
        Initialize and validate the baseerror state.

        Example:
            Exercise BaseError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param msg: Value supplied for msg under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param line: Value supplied for line under the utility contract.
        :param col: Value supplied for col under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.msg, self.line, self.col = msg, line, col
        self.name = name
        # A list with entries of the form: (name, lnum, col)
        self.all_locations = None

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise BaseError.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "%s:%s (%s, %s):%s" % (
            self.__class__.__name__,
            self.name,
            self.line,
            self.col,
            self.msg,
        )

    __repr__ = __str__


def worker(func: _typing.Any, args: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the worker operation under explicit file-format and conversion rules.

    Example:
        Exercise worker through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param func: Value supplied for func under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        result = func(*args)
        tb = None
    except:
        result = None
        import traceback

        tb = traceback.format_exc()
    return result, tb


def run_checkers(func: _typing.Any, args_list: _typing.Any) -> _typing.Any:
    """
    Perform the run checkers operation under explicit file-format and conversion rules.

    Example:
        Exercise run checkers through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param func: Value supplied for func under the utility contract.
    :param args_list: Value supplied for args list under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    num = cpu_count()
    pool = ThreadPool(num)
    ans = []
    with closing(pool):
        for result, tb in pool.map(partial(worker, func), args_list):
            if tb is not None:
                raise Exception("Failed to run worker: \n%s" % tb)
            ans.extend(result)
    return ans
