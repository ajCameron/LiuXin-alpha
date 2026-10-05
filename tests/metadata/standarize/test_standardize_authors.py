
"""
Verify author-name normalization and ordering.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test standardize authors through its owning regression module::

        python -m pytest -q tests/metadata/standarize/test_standardize_authors.py
"""


class TestStandardizeAuthors:
    """
    Tests the basic standardize methods.

    Example:
        Exercise TestStandardizeAuthors through its owning regression module::

            python -m pytest -q tests/metadata/standarize/test_standardize_authors.py
    """
    def test_standardize_string_to_authors(self) -> None:
        """
        Tests the basic standardize methods exist.

        Example:
            Exercise TestStandardizeAuthors.test standardize string to authors through its owning regression module::

                python -m pytest -q tests/metadata/standarize/test_standardize_authors.py


        :return: None; the function records state or raises through its assertions.
        """

        from LiuXin_alpha.metadata.standardize import string_to_authors

        test_string = "some meeeessss"

        assert string_to_authors(test_string) == ['Some Meeeessss']



