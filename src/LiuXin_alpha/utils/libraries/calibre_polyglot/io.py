#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>


"""
Provide io utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise io through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from io import StringIO, BytesIO


class PolyglotStringIO(StringIO):
    """
    Provide the PolyglotStringIO utility contract with explicit state and cleanup behavior.

    Example:
        Exercise PolyglotStringIO through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, initial_data=None, encoding="utf-8", errors="strict"):
        """
        Initialize and validate the PolyglotStringIO state.

        Example:
            Exercise PolyglotStringIO.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param initial_data: Value supplied for initial data under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param errors: Value supplied for errors under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        StringIO.__init__(self)
        self._encoding_for_bytes = encoding
        self._errors = errors
        if initial_data is not None:
            self.write(initial_data)

    def write(self, x):
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise PolyglotStringIO.write through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param x: Value supplied for x under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(x, bytes):
            x = x.decode(self._encoding_for_bytes, errors=self._errors)
        StringIO.write(self, x)


class PolyglotBytesIO(BytesIO):
    """
    Provide the PolyglotBytesIO utility contract with explicit state and cleanup behavior.

    Example:
        Exercise PolyglotBytesIO through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, initial_data=None, encoding="utf-8", errors="strict"):
        """
        Initialize and validate the PolyglotBytesIO state.

        Example:
            Exercise PolyglotBytesIO.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param initial_data: Value supplied for initial data under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param errors: Value supplied for errors under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BytesIO.__init__(self)
        self._encoding_for_bytes = encoding
        self._errors = errors
        if initial_data is not None:
            self.write(initial_data)

    def write(self, x):
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise PolyglotBytesIO.write through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param x: Value supplied for x under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(x, bytes):
            x = x.encode(self._encoding_for_bytes, errors=self._errors)
        BytesIO.write(self, x)
