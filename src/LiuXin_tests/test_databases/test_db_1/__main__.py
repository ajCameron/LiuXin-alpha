# Build this test database in a scratch folder - just check that the database builds properly

import os

from LiuXin_tests.test_data.test_db_1 import build_test_db

from LiuXin.utils.ptempfiles import get_scratch_folder

scratch_db_path = os.path.join(get_scratch_folder(), "test_scratch_db.db")

build_test_db(dst_file_path=scratch_db_path, dump=False)
