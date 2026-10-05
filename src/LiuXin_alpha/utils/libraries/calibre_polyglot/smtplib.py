#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>

"""
Provide smtplib utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise smtplib through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
import sys
from functools import partial


class FilteredLog:

    """
    Hide AUTH credentials from the log

    Example:
        Exercise FilteredLog through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """

    def __init__(self, debug_to=None):
        """
        Initialize and validate the FilteredLog state.

        Example:
            Exercise FilteredLog.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param debug_to: Value supplied for debug to under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.debug_to = debug_to or partial(print, file=sys.stderr)

    def __call__(self, *a):
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise FilteredLog.  call   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if a and len(a) == 2 and a[0] == "send:":
            a = list(a)
            raw = a[1]
            if len(raw) > 100:
                raw = raw[:100] + (b"..." if isinstance(raw, bytes) else "...")
            q = b"AUTH " if isinstance(raw, bytes) else "AUTH "
            if q in raw:
                raw = "AUTH <censored>"
            a[1] = raw
        self.debug_to(*a)


import smtplib


class SMTP(smtplib.SMTP):
    """
    Provide the SMTP utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SMTP through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, *a, **kw):
        """
        Initialize and validate the SMTP state.

        Example:
            Exercise SMTP.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :param kw: Value supplied for kw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.debug_to = FilteredLog(kw.pop("debug_to", None))
        super().__init__(*a, **kw)

    def _print_debug(self, *a):
        """
        Perform the print debug utility operation under explicit compatibility rules.

        Example:
            Exercise SMTP. print debug through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.debug_to is not None:
            self.debug_to(*a)
        else:
            super()._print_debug(*a)


class SMTP_SSL(smtplib.SMTP_SSL):
    """
    Provide the SMTP SSL utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SMTP SSL through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, *a, **kw):
        """
        Initialize and validate the SMTP SSL state.

        Example:
            Exercise SMTP SSL.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :param kw: Value supplied for kw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.debug_to = FilteredLog(kw.pop("debug_to", None))
        super().__init__(*a, **kw)

    def _print_debug(self, *a):
        """
        Perform the print debug utility operation under explicit compatibility rules.

        Example:
            Exercise SMTP SSL. print debug through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.debug_to is not None:
            self.debug_to(*a)
        else:
            super()._print_debug(*a)
