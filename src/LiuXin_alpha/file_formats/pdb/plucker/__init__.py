"""
Expose the supported plucker compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations
class PluckerError(Exception):
    """
    Report a pluckererror encountered while processing an ebook format.

    Example:
        Exercise PluckerError through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    pass
