
"""
Tests for the on disk unmanaged drive.
"""

import tempfile

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import OnDiskUnmanagedStorageBackend



class TestOnDiskUnmanagedDrive:
    """
    Run basic tests on the class.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:

        test_storage_backend = OnDiskUnmanagedStorageBackend(url=tmp_dir)



