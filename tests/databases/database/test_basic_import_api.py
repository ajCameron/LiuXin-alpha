


class TestBasicTopLevelSurfaceImports:
    """
    Very basic tests that we can import the most basic things.
    """
    def test_import_of_basic_top_level_things(self) -> None:
        """
        Just tests that we can import the most basic things from the top of the module.

        :return:
        """
        from LiuXin_alpha.databases.database import Database

        assert Database is not None

    def test_database_sqlite_driver_import(self) -> None:
        """
        Attempts to import the database sqlite driver plugin.

        :return:
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import DatabaseDriver

