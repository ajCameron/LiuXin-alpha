
"""
Smoke-test FRBR schema generation through a sqlite3 in-memory connection.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_generator_rwe.py
"""
import sqlite3
import tempfile

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    create_new_database,
)


class TestBasicGeneration:
    """
    Group the minimal FRBR database generation smoke check.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_generator_rwe.py
    """
    def test_create_new_database(self) -> None:
        """
        Call the FRBR generator on an in-memory connection and require it to complete without raising.

        The temporary directory is unused, no generated schema values are asserted, and the
        connection is not explicitly closed.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_generator_rwe.py


        :return: None; failed expectations raise AssertionError.
        """
        with tempfile.TemporaryDirectory() as tempdir:

            sqlite_connection = sqlite3.connect(":memory:")

            create_new_database(sqlite_connection)
