"""
Expose the supported lit compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
"""
from __future__ import annotations
__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"


class LitError(Exception):
    """
    Report a literror encountered while processing an ebook format.

    Example:
        Exercise LitError through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
    """
    pass
