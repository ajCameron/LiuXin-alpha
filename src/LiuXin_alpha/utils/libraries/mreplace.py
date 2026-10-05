"""
Apply multiple literal replacements in a single compiled pass.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise mreplace through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

import re
from collections import UserDict

__license__ = "GPL v3"
__copyright__ = "2010, sengian <sengian1 @ gmail.com>"
__docformat__ = "restructuredtext en"


class MReplace(UserDict):
    """
    Provide the MReplace utility contract with explicit state and cleanup behavior.

    Example:
        Exercise MReplace through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """
    def __init__(self, data=None, case_sensitive=True):
        """
        Initialize and validate the MReplace state.

        Example:
            Exercise MReplace.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param data: Value supplied for data under the utility contract.
        :param case_sensitive: Value supplied for case sensitive under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(data or {})
        self.re = None
        self.regex = None
        self.case_sensitive = case_sensitive
        self.compile_regex()

    def compile_regex(self):
        """
        Perform the compile regex utility operation under explicit compatibility rules.

        Example:
            Exercise MReplace.compile regex through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.data) > 0:
            keys = sorted(self.data.keys(), key=len, reverse=True)
            tmp = "(%s)" % "|".join(map(re.escape, keys))
            if self.re != tmp:
                self.re = tmp
                if self.case_sensitive:
                    self.regex = re.compile(self.re)
                else:
                    self.regex = re.compile(self.re, re.I)

    def __call__(self, mo):
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise MReplace.  call   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param mo: Value supplied for mo under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self[mo.string[mo.start() : mo.end()]]

    def mreplace(self, text):
        """
        Perform the mreplace utility operation under explicit compatibility rules.

        Example:
            Exercise MReplace.mreplace through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(self.data) < 1 or self.re is None or self.regex is None:
            return text
        return self.regex.sub(self, text)

