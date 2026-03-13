"""Small subset of ``future``/``past`` builtins used by the codebase."""

from __future__ import annotations

import builtins as _builtins

basestring = (str, bytes)
unicode = str
str = _builtins.str


def cmp(a, b):
    return (a > b) - (a < b)


__all__ = ["basestring", "cmp", "str", "unicode"]
