
"""
Verify the public standardization facade.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test standardize api through its owning regression module::

        python -m pytest -q tests/metadata/standarize/test_standardize_api.py
"""


class TestStandardizeAPI:
    """
    Tests the basic standardize methods.

    Example:
        Exercise TestStandardizeAPI through its owning regression module::

            python -m pytest -q tests/metadata/standarize/test_standardize_api.py
    """
    def test_standardize_api(self) -> None:
        """
        Tests the basic standardize methods exist.

        Example:
            Exercise TestStandardizeAPI.test standardize api through its owning regression module::

                python -m pytest -q tests/metadata/standarize/test_standardize_api.py


        :return: None; the function records state or raises through its assertions.
        """

        from LiuXin_alpha.metadata.standardize import standardize_tag



