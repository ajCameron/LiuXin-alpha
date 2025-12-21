
"""
Contains the storage backend for an unmanaged drive.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Union


from LiuXin_alpha.storage.api.storage_api import StorageBackendAPI, StorageBackendStatus
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name
from LiuXin_alpha.utils.storage.local.local_store_smoke_test import StorageIOSmokeTest
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes

from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.storage.api.storage_api import StorageBackendCheckStatus


class OnDiskUnmanagedStorageBackend(StorageBackendAPI):
    """
    Represents an unmanaged drive.

    If you want to use LiuXin to index your hard drive... you can.
    It might have problems if you move stuff around, but that can be fixed.
    """
    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        """
        Initialize the store.

        :param url:
        """
        super().__init__(
            url=url,
            name=name,
            uuid=uuid
        )

    def startup(self) -> StorageBackendStatus:
        """
        Preform store startup - including store checks.

        :return:
        """
        return self.self_test()

    def url_to_name(self, url: str) -> str:
        """
        Takes a URL of the path sort and makes a safeish string from it.

        :param url:
        :return:
        """
        return safe_path_to_name(url)

    def self_test(self) -> StorageBackendStatus:
        """
        Preform store self checks.

        :return:
        """
        # Hopefully the URL is a path...
        os.path.exists(self.url)

        storage_smoke_tester = StorageIOSmokeTest(root=self.url)
        report = storage_smoke_tester.run()

        return StorageBackendStatus(
            name=self.name,
            url=self.url,
            file_count=None,
            store_free_space=get_free_bytes(self.url),
            check_status=StorageBackendCheckStatus(
                store_marker_file=True, read=True, write=True, sundry=True
            ),
            good=report["ok"],
            uuid=self.uuid,
            event_log=DefaultEventLog(),
            checked=True
        )

    def status(self) -> StorageBackendStatus:
        """
        Return the status of the store - at the moment just from running the self checks again.

        :return:
        """
        return self.self_test()









