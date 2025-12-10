
"""
Runs very basic tests on the standardize methods.
"""


class TestStandardizeAuthors:
    """
    Tests the basic standardize methods.
    """
    def test_standardize_string_to_authors(self) -> None:
        """
        Tests the basic standardize methods exist.

        :return:
        """

        from LiuXin_alpha.metadata.standardize import string_to_authors

        test_string = "some meeeessss"

        assert string_to_authors(test_string) == ['Some Meeeessss']



