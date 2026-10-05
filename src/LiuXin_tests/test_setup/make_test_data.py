
"""
Create retained fixture data.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise make test data through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""



if __name__ == "__main__":

    from LiuXin_tests.test_databases import make_test_data

    make_test_data()
