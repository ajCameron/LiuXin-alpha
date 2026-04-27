
"""
Preform basic tests on the WorkIdentity class.
"""

import pytest

from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity


class TestWorkIdentity:
    """
    Preform basic tests on the WorkIdentity class.
    """
    def test_work_identity_init(self) -> None:
        """
        Tests we can init the WorkIdentity class.

        :return:
        """
        test_class = WorkIdentity()
        assert test_class is not None

        test_class_2 = WorkIdentity(word_id=5)
        assert test_class_2 is not None
        assert test_class_2.work_id == 5

        with pytest.raises(AttributeError):
            test_class_2.work_id = 10
