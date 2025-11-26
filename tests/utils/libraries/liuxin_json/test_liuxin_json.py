
"""
Runs tests on the liuxin_json module - which is an enhanced json module.
"""

class TestLiuXinJson:
    """
    Tests the liuxin_json module - an enhanced json module.
    """
    def test_liuxin_json_api(self) -> None:
        """
        Tests the liuxin_json module presents some kind of API.
        :return:
        """
        from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

        assert LiuXinJSON is not None




