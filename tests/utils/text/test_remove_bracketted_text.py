
"""
Tests removing bracketted text
"""




class TestRemovingBrackettedText:
    """
    Testing removing text with brackets.
    """
    def test_import_and_simple_run(self) -> None:
        """
        Tests we can import the function and run it a bit.

        :return:
        """
        from LiuXin_alpha.utils.text import remove_bracketed_text

        test_text = "This (is) not the (end)"

        assert remove_bracketed_text(test_text) == 'This  not the '
