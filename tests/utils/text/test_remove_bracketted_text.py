
"""
Provide test remove bracketted text utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test remove bracketted text through a consuming regression::

        python -m pytest -q tests/utils/text/test_remove_bracketted_text.py
"""




class TestRemovingBrackettedText:
    """
    Testing removing text with brackets.

    Example:
        Exercise TestRemovingBrackettedText through a consuming regression::

            python -m pytest -q tests/utils/text/test_remove_bracketted_text.py
    """
    def test_import_and_simple_run(self) -> None:
        """
        Tests we can import the function and run it a bit.

        Example:
            Exercise TestRemovingBrackettedText.test import and simple run through a consuming regression::

                python -m pytest -q tests/utils/text/test_remove_bracketted_text.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.utils.text import remove_bracketed_text

        test_text = "This (is) not the (end)"

        assert remove_bracketed_text(test_text) == 'This  not the '
