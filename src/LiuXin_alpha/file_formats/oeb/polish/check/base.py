#!/usr/bin/env python
# vim:fileencoding=utf-8

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
    return os.cpu_count() or 1


class BaseError(object):

    HELP = ""
    INDIVIDUAL_FIX = ""
    level = ERROR
    has_multiple_locations = False

    def __init__(self: _typing.Self, msg: _typing.Any, name: _typing.Any, line: _typing.Any = None, col: _typing.Any = None) -> None:
        self.msg, self.line, self.col = msg, line, col
        self.name = name
        # A list with entries of the form: (name, lnum, col)
        self.all_locations = None

    def __str__(self: _typing.Self) -> _typing.Any:
        return "%s:%s (%s, %s):%s" % (
            self.__class__.__name__,
            self.name,
            self.line,
            self.col,
            self.msg,
        )

    __repr__ = __str__


def worker(func: _typing.Any, args: _typing.Any) -> tuple[_typing.Any, ...]:
    try:
        result = func(*args)
        tb = None
    except:
        result = None
        import traceback

        tb = traceback.format_exc()
    return result, tb


def run_checkers(func: _typing.Any, args_list: _typing.Any) -> _typing.Any:
    num = cpu_count()
    pool = ThreadPool(num)
    ans = []
    with closing(pool):
        for result, tb in pool.map(partial(worker, func), args_list):
            if tb is not None:
                raise Exception("Failed to run worker: \n%s" % tb)
            ans.extend(result)
    return ans
