
"""
Tests the OnDiskUnmanagedLocation class.
"""

import tempfile

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import OnDiskUnmanagedStorageBackend


class TestOnDiskUnmanagedLocation:
    """
    Tests the OnDiskUnmanagedLocation class.
    """
    def test_basic_api(self) -> None:
        """
        Tests the basic API.

        :return:
        """
        from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import OnDiskUnmanagedStoreLocation

        with tempfile.TemporaryDirectory() as tmp_dir:

            test_storage_backend = OnDiskUnmanagedStorageBackend(url=tmp_dir)
            assert test_storage_backend.url == tmp_dir

            assert test_storage_backend.file_exists("this file is not real") is False

            test_loc = OnDiskUnmanagedStoreLocation(store=test_storage_backend)

            assert test_loc.store == test_storage_backend