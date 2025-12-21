
"""
Flat stores every file very simply - in a single folder with its name the LiuXin file id.

Mostly for testing purposes.
"""

from LiuXin_alpha.storage.api.storage_api import StorageBackendAPI


class OnDiskFlatStorageBackend(StorageBackendAPI):
    """
    A flat file store - just stores every file it's given in a single folder.
    """
    def __init__(self, url: str) -> None:
        """
        Startup an on disc flat file store.

        :param url:
        """
        super().__init__(url=url)
