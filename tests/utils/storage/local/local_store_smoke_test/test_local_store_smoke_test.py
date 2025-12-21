
"""
Tests the store smoke test class - which preforms basic tests of the storage system.
"""

import tempfile

from LiuXin_alpha.utils.storage.local.local_store_smoke_test import StorageIOSmokeTest


class TestSmokeTest:
    """
    Preforms basic tests of the storage test system.
    """
    def test_storage_smoke_test(self) -> None:
        """
        Tests the storage smoke test class.

        :return:
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_smoke = StorageIOSmokeTest(root=temp_dir)

            report = test_smoke.run()

        assert isinstance(report, dict)
        assert report["ok"] == True

