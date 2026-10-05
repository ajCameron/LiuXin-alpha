
"""
Dump and reload fixture test data.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise dump reload test data through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""

if __name__ == "__main__":

    # Makes the test data and dumps it to file
    from LiuXin_tests.test_databases import make_test_data

    make_test_data()

    # Blank the database and then reload using the data that just got written
    from LiuXin_alpha.databases.database import Database

    Database(create=True)

    from LiuXin_tests.test_databases import load_data

    load_data(overwrite_db=True)
