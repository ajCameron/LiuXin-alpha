
import os
import tempfile


class TestDatabaseTmpDirInit:
    """
    We're going to try and init the database in a temporary dir.
    """
    def test_init_database_in_tmp_file(self) -> None:
        """
        Tries to start up a pure in memory database.

        :return:
        """
        from LiuXin_alpha.databases.database import Database

        with tempfile.TemporaryDirectory() as tmpdir:

            test_db_path = os.path.join(tmpdir, "test.db")

            metadata = dict()
            metadata["database_path"] = test_db_path

            test_db = Database(metadata=metadata)

            assert test_db is not None
