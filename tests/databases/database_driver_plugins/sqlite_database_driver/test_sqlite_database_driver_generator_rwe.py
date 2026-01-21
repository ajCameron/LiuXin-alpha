
import sqlite3
import tempfile

from LiuXin_alpha.databases.database_driver_plugins.SQLite.database_generator import create_new_database


class TestBasicGeneration:
    """
    Basic generator tests.
    """
    def test_create_new_database(self) -> None:
        """
        Tests the create_new_database function rwe.

        :return:
        """
        with tempfile.TemporaryDirectory() as tempdir:

            sqlite_connection = sqlite3.connect(":memory:")

            create_new_database(sqlite_connection)
