# The parts of six (https://github.com/benjaminp/six) that python-dateutil uses,
# for Python 3 only. add_metaclass is copied from six 1.17.0.
# Copyright (c) 2010-2024 Benjamin Peterson. License: MIT (below).
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
Stand in for six in the vendored python-dateutil, on Python 3.

LiuXin does not depend on six; the vendoring script points the copy's imports
of it here. Only the names python-dateutil uses are provided.

Example:
    >>> text_type is str, integer_types, PY2
    (True, (<class 'int'>,), False)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

PY2 = False
PY3 = True
text_type = str
binary_type = bytes
string_types = (str,)
integer_types = (int,)
advance_iterator = next


def raise_from(value: BaseException, from_value: BaseException | None) -> None:
    """
    Raise ``value`` chained to ``from_value``, as six's ``raise_from`` does.

    Example:
        >>> try:
        ...     raise_from(ValueError("outer"), KeyError("inner"))
        ... except ValueError as error:
        ...     type(error.__cause__).__name__
        'KeyError'


    :param value: The exception to raise.
    :param from_value: Its cause, or ``None`` to suppress the context.
    :return: Never.
    :raises BaseException: ``value``, always.
    """
    raise value from from_value


def add_metaclass(metaclass: type) -> Callable[[type], Any]:
    """
    Make a class decorator that rebuilds a class with ``metaclass``, as six's does.

    Example:
        >>> class Meta(type):
        ...     pass
        >>> @add_metaclass(Meta)
        ... class Thing:
        ...     __slots__ = ("x",)
        >>> type(Thing).__name__
        'Meta'


    :param metaclass: The metaclass.
    :return: The decorator.
    """

    def wrapper(cls: type) -> Any:
        """
        Rebuild ``cls`` with the metaclass.

        Example:
            See ``add_metaclass``.


        :param cls: The class.
        :return: The rebuilt class.
        """
        orig_vars = cls.__dict__.copy()
        slots = orig_vars.get("__slots__")
        if slots is not None:
            if isinstance(slots, str):
                slots = [slots]
            for slots_var in slots:
                orig_vars.pop(slots_var)
        orig_vars.pop("__dict__", None)
        orig_vars.pop("__weakref__", None)
        if hasattr(cls, "__qualname__"):
            orig_vars["__qualname__"] = cls.__qualname__
        return metaclass(cls.__name__, cls.__bases__, orig_vars)

    return wrapper
