
"""
Verify the compatibility work-field container's basic mapping behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test work container basic through its owning regression module::

        python -m pytest -q tests/metadata/containers/metadata_fields/work_container/test_work_container_basic.py
"""

import pytest

from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity


class TestWorkIdentity:
    """
    Preform basic tests on the WorkIdentity class.

    Example:
        Exercise TestWorkIdentity through its owning regression module::

            python -m pytest -q tests/metadata/containers/metadata_fields/work_container/test_work_container_basic.py
    """
    def test_work_identity_init(self) -> None:
        """
        Tests we can init the WorkIdentity class.

        Example:
            Exercise TestWorkIdentity.test work identity init through its owning regression module::

                python -m pytest -q tests/metadata/containers/metadata_fields/work_container/test_work_container_basic.py


        :return: None; the function records state or raises through its assertions.
        """
        test_class = WorkIdentity()
        assert test_class is not None

        test_class_2 = WorkIdentity(word_id=5)
        assert test_class_2 is not None
        assert test_class_2.work_id == 5

        with pytest.raises(AttributeError):
            test_class_2.work_id = 10
