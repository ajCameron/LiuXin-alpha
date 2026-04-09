"""Thin compatibility wrapper for ``clint.textui``.

Use the real library when available. Otherwise provide a minimal
``puts`` + ``colored`` surface that degrades to plain text output.
"""

from __future__ import annotations

import sys


def _coalesce_message(args):
    if not args:
        return ""
    if len(args) == 1:
        return args[0]

    first = args[0]
    if isinstance(first, bytes):
        parts = []
        for arg in args:
            if isinstance(arg, bytes):
                parts.append(arg)
            else:
                parts.append(str(arg).encode("utf-8", errors="replace"))
        return b" ".join(parts)

    return " ".join(str(arg) for arg in args)


try:
    from clint.textui import puts as puts  # type: ignore
    from clint.textui import colored as colored  # type: ignore
except ModuleNotFoundError:
    class _ColoredFallback(object):
        def __getattr__(self, _name):
            def _passthrough(*args, **_kwargs):
                return _coalesce_message(args)

            return _passthrough

    colored = _ColoredFallback()

    def puts(*args, **kwargs):
        message = _coalesce_message(args)
        stream = kwargs.get("stream", sys.stdout)
        newline = kwargs.get("newline", True)
        end = "\n" if newline else ""
        print(message, file=stream, end=end)


__all__ = ["colored", "puts"]
