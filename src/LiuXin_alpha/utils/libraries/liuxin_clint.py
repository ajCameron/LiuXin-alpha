"""
Render compact command-line progress and colour output without an external dependency.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise liuxin clint through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

import sys


def _coalesce_message(args):
    """
    Perform the coalesce message utility operation under explicit compatibility rules.

    Example:
        Exercise  coalesce message through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param args: Positional values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
        """
        Provide the ColoredFallback utility contract with explicit state and cleanup behavior.

        Example:
            Exercise  ColoredFallback through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py
        """
        def __getattr__(self, _name):
            """
            Perform the getattr utility operation under explicit compatibility rules.

            Example:
                Exercise  ColoredFallback.  getattr   through a consuming regression::

                    python -m pytest -q tests/scripts/test_docstring_migration.py


            :param _name: Value supplied for name under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            def _passthrough(*args, **_kwargs):
                """
                Perform the passthrough utility operation under explicit compatibility rules.

                Example:
                    Exercise  ColoredFallback.  getattr  . passthrough through a consuming regression::

                        python -m pytest -q tests/scripts/test_docstring_migration.py


                :param args: Positional values forwarded to the compatibility implementation.
                :param _kwargs: Value supplied for kwargs under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return _coalesce_message(args)

            return _passthrough

    colored = _ColoredFallback()

    def puts(*args, **kwargs):
        """
        Perform the puts utility operation under explicit compatibility rules.

        Example:
            Exercise puts through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        message = _coalesce_message(args)
        stream = kwargs.get("stream", sys.stdout)
        newline = kwargs.get("newline", True)
        end = "\n" if newline else ""
        print(message, file=stream, end=end)


__all__ = ["colored", "puts"]
