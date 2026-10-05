
"""
Check on-disk Database initialization and work-to-title projection inside temporary directories.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
"""
import os
import tempfile


class TestDatabaseTmpDirInit:
    """
    Create file-backed databases with contexts that close the database before removing the temporary directory.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """
    def test_init_database_in_tmp_file(self) -> None:
        """
        Create test.db in a temporary directory and check the opened Database is non-None.

        Both the database and temporary directory use cleanup contexts; this is a
        file-backed database, not an in-memory connection.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; failed expectations raise AssertionError.
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
        Create and sync a work, then check its ID resolves to one matching title in the compatibility view.

        Close the file-backed Database before the temporary directory is removed.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.database import Database

        with tempfile.TemporaryDirectory() as tmpdir:

            test_db_path = os.path.join(tmpdir, "test.db")

            metadata = dict()
            metadata["database_path"] = test_db_path

            with Database(metadata=metadata) as test_db:

                assert test_db is not None

                new_work_row = test_db.get_blank_row("works")

                new_work_row["work_title"] = "This is a test of the works table"
                new_work_row.sync()

                # titles is a read-only compatibility view projected from works
                matches = test_db.search("titles", "title_id", int(new_work_row.row_id))
                assert len(matches) == 1
                assert matches[0]["title"] == "This is a test of the works table"
