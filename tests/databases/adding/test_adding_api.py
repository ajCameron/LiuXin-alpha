

class TestAddingAPI:
    """
    Testing the API for the adding submodule under the database.
    """
    def test_imports(self) -> None:
        """
        Tests imports from adding.

        :return:
        """
        from LiuXin_alpha.databases.adding import listdir
        assert listdir is not None
