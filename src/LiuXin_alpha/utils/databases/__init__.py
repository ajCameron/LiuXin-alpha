"""
Expose the supported databases compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py
"""
