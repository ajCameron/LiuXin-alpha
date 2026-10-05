# Build this test database in a scratch folder - just check that the database builds properly

"""
Run the retained fixture-database utility.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   main   through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""
import os

from LiuXin_tests.test_data.test_db_1 import build_test_db

from LiuXin_alpha.utils.ptempfiles import get_scratch_folder

scratch_db_path = os.path.join(get_scratch_folder(), "test_scratch_db.db")

build_test_db(dst_file_path=scratch_db_path, dump=False)
