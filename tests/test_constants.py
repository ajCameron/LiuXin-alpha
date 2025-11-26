
"""
Constant tests.
"""




class TestLiuXinAlphaConstants:
    """
    Tests the LiuXin constants.
    """
    def test_constants_api(self) -> None:
        """
        Testing the constants for the API.

        :return:
        """
        from LiuXin_alpha.constants import ALLOWED_DOC_TYPES

        assert ALLOWED_DOC_TYPES is not None