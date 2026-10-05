
"""
Provide NumPy-backed cache table adapters.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tables through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""


from LiuXin_alpha.customize.cache.base_tables import BaseTable


class NumpyTable(BaseTable):
    """
    Implementation of the table concept with everything stored (on the back end) in a numpy array.

    Example:
        Exercise NumpyTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
