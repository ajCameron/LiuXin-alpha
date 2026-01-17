
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

            with Database(metadata=metadata) as test_db:

                assert test_db is not None

    def test_init_database_in_tmp_file_main_tables(self) -> None:
        """
        Tries to start up a pure in memory database.

        :return:
        """
        from LiuXin_alpha.databases.database import Database

        with tempfile.TemporaryDirectory() as tmpdir:

            test_db_path = os.path.join(tmpdir, "test.db")

            metadata = dict()
            metadata["database_path"] = test_db_path

            with Database(metadata=metadata) as test_db:

                assert test_db is not None

                new_title_row = test_db.get_blank_row("titles")

                new_title_row["title"] = "This is a test of the titles table"
                new_title_row.sync()
