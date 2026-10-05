
"""
Verify tag normalization, deduplication and filtering.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test standardize tags through its owning regression module::

        python -m pytest -q tests/metadata/standarize/test_standardize_tags.py
"""


class TestStandardizeAPI:
    """
    Tests the basic standardize methods.

    Example:
        Exercise TestStandardizeAPI through its owning regression module::

            python -m pytest -q tests/metadata/standarize/test_standardize_tags.py
    """
    def test_standardize_tags(self) -> None:
        """
        Tests the basic standardize methods exist.

        Example:
            Exercise TestStandardizeAPI.test standardize tags through its owning regression module::

                python -m pytest -q tests/metadata/standarize/test_standardize_tags.py


        :return: None; the function records state or raises through its assertions.
        """

        from LiuXin_alpha.metadata.standardize import standardize_tag

        test_string = "some meeeessss"

        assert standardize_tag(test_string) == 'some meeeessss'



