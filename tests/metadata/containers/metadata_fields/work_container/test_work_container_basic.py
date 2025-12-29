
"""
Preform basic tests on the WorkContainer class.
"""

import pytest

from LiuXin_alpha.metadata.containers.metadata_containers.work_container import WorkContainer


class TestWorkContainer:
    """
    Preform basic tests on the WorkContainer class.
    """
    def test_work_container_init(self) -> None:
        """
        Tests we can init the WorkContainer class.

        :return:
        """
        test_class = WorkContainer()
        assert test_class is not None

        test_class_2 = WorkContainer(word_id=5)
        assert test_class_2 is not None
        assert test_class_2.work_id == 5

        with pytest.raises(AttributeError):
            test_class_2.work_id = 10

