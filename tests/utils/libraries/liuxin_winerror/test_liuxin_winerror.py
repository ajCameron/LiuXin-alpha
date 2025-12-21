
from LiuXin_alpha.utils.libraries.liuxin_winerror import winerror


class TestWinError:
    """
    Preform the most basic tests on the LiuXin winerror backupo class.
    """

    def test_winerror_to_code(self) -> None:
        """
        Tests the most basic of winerror function.

        :return:
        """
        assert winerror.get_name(code=0) == 'ERROR_SUCCESS'

