
"""
Runs very basic tests on the standardize methods.
"""


class TestStandardizeAPI:
    """
    Tests the basic standardize methods.
    """
    def test_standardize_tags(self) -> None:
        """
        Tests the basic standardize methods exist.

        :return:
        """

        from LiuXin_alpha.metadata.standardize import standardize_tag

        test_string = "some meeeessss"

        assert standardize_tag(test_string) == 'some meeeessss'



