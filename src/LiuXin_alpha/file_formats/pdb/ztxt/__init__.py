# -*- coding: utf-8 -*-

"""
Expose the supported ztxt compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class zTXTError(Exception):
    """
    Report a ztxterror encountered while processing an ebook format.

    Example:
        Exercise zTXTError through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    pass
