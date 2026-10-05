"""
Expose retained Python 2 compatible builtins.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise builtins through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""

from __future__ import annotations

import builtins as _builtins

basestring = (str, bytes)
unicode = str
str = _builtins.str


def cmp(a, b):
    """
    Perform the cmp operation under explicit file-format and conversion rules.

    Example:
        Exercise cmp through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


    :param a: Value supplied for a under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (a > b) - (a < b)


__all__ = ["basestring", "cmp", "str", "unicode"]
