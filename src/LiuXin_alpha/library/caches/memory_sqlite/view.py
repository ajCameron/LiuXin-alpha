"""
Expose views over in-memory SQLite cache records.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise view through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
