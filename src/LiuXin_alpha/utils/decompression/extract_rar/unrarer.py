# serves as a unified front end for the various modules here contained

"""
Parse RAR headers and extract supported entries through the retained pure-Python reader.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise unrarer through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""


class RarFile:
    """
    Provide the RarFile utility contract with explicit state and cleanup behavior.

    Example:
        Exercise RarFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    ...
