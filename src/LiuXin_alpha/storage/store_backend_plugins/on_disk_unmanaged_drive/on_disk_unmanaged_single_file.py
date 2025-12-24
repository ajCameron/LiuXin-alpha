
"""
Represents a single file in an unmanaged on disk folder store.
"""


from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


class OnDiskUnmanagedSingleFile(SingleFileAPI):
    """
    Represents a single file on disk.

    Said disk need NOT be managed by LiuXin.
    People will (definitely) use this. But they probably shouldn't.
    It's here for some backup and archive tools and to unify file imports.
    The plan is you mount ANY SOURCE YOU WANT as a storage backend - then let LiuXin transfer the files to another
    backend to actually keep them.
    """
    def __init__(self, file_url: str, file_status: SingleFileStatus) -> None:
        """
        Startup the file.

        :param file_url:
        """
        super().__init__(file_url=file_url, file_status=file_status)

    def recheck_status(self) -> SingleFileStatus:
        """
        Recheck the status of the file.

        :return:
        """
        self.file_status.recheck_status()
        return self.file_status

    def as_string(self) -> str:
        """
        Return the file as a string - this can be a memory and time intensive operation.

        :return:
        """

    def as_bytes(self) -> bytes:
        """
        Return the file as bytes.

        :return:
        """
