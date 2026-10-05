
"""
Provide test liuxin json utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test liuxin json through a consuming regression::

        python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py
"""

class TestLiuXinJson:
    """
    Tests the liuxin_json module - an enhanced json module.

    Example:
        Exercise TestLiuXinJson through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py
    """
    def test_liuxin_json_api(self) -> None:
        """
        Tests the liuxin_json module presents some kind of API.

        Example:
            Exercise TestLiuXinJson.test liuxin json api through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

        assert LiuXinJSON is not None

    def test_dump_with_liuxin_json(self) -> None:
        """
        Tests dumping an object to string with LiuXin json.

        Example:
            Exercise TestLiuXinJson.test dump with liuxin json through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

        test_case = LiuXinJSON()
        assert test_case.dumps("This is a very simple test.") == '"VGhpcyBpcyBhIHZlcnkgc2ltcGxlIHRlc3Qu"'
