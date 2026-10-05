
"""
Provide test local store smoke test utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test local store smoke test through a consuming regression::

        python -m pytest -q tests/utils/storage/local/local_store_smoke_test/test_local_store_smoke_test.py
"""

import tempfile

from LiuXin_alpha.utils.storage.local.local_store_smoke_test import StorageIOSmokeTest


class TestSmokeTest:
    """
    Preforms basic tests of the storage test system.

    Example:
        Exercise TestSmokeTest through a consuming regression::

            python -m pytest -q tests/utils/storage/local/local_store_smoke_test/test_local_store_smoke_test.py
    """
    def test_storage_smoke_test(self) -> None:
        """
        Tests the storage smoke test class.

        Example:
            Exercise TestSmokeTest.test storage smoke test through a consuming regression::

                python -m pytest -q tests/utils/storage/local/local_store_smoke_test/test_local_store_smoke_test.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_smoke = StorageIOSmokeTest(root=temp_dir)

            report = test_smoke.run()

        assert isinstance(report, dict)
        assert report["ok"] == True

