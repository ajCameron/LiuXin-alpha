
"""
Tests for the on disk unmanaged drive.
"""

import tempfile

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import OnDiskUnmanagedStorageBackend



class TestOnDiskUnmanagedDrive:
    """
    Run basic tests on the class.
    """
    def test_on_disk_unmanaged_drive_init(self) -> None:
        """
        Tests that we can, at least, init the storage backend.

        :return:
        """

        with tempfile.TemporaryDirectory() as tmp_dir:

            test_storage_backend = OnDiskUnmanagedStorageBackend(url=tmp_dir)
            assert test_storage_backend.url == tmp_dir

    def test_on_disk_unmanaged_drive_file_exists(self) -> None:
        """
        Tests that we can check for a file exists.

        :return:
        """
        with tempfile.TemporaryDirectory() as tmp_dir:

            test_storage_backend = OnDiskUnmanagedStorageBackend(url=tmp_dir)
            assert test_storage_backend.url == tmp_dir

            assert test_storage_backend.file_exists("this file is not real") is False







