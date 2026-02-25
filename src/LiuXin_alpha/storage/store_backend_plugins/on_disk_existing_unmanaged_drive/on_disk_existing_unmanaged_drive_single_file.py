
"""
Represents a single file in an unmanaged on disk folder store.
"""

from __future__ import annotations

import os

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash


class OnDiskUnmanagedSingleFile(SingleFileAPI):
    """
    Represents a single file on disk.

    Said disk need NOT be managed by LiuXin.
    People will (definitely) use this. But they probably shouldn't.
    It's here for some backup and archive tools and to unify file imports.
    The plan is you mount ANY SOURCE YOU WANT as a storage backend - then let LiuXin transfer the files to another
    backend to actually keep them.
    """
    def __init__(self, file_url: str, file_status: SingleFileStatus | None = None) -> None:
        """
        Startup the file.

        :param file_url:
        """
        if file_status is None:
            def _exists(url: str) -> bool:
                return os.path.exists(url)

            def _size(url: str) -> int:
                if not _exists(url):
                    return 0
                return int(os.path.getsize(url))

            def _hash(url: str) -> str:
                if not _exists(url):
                    return ""
                return get_file_hash(url)

            file_status = SingleFileStatus(
                url=file_url,
                check_exists_function=_exists,
                check_size_function=_size,
                check_hash_function=_hash,
            )

        super().__init__(file_url=file_url, file_status=file_status)

    def recheck_status(self) -> SingleFileStatus:
        """
        Recheck the status of the file.

        :return:
        """
        if self.file_status is None:
            raise AttributeError("Cannot recheck file status without a status object.")
        self.file_status.recheck_self(all=True)
        return self.file_status

    def as_string(self) -> str:
        """
        Return the file as a string - this can be a memory and time intensive operation.

        :return:
        """
        with self.open(self.file_url, mode="r", encoding="utf-8") as f:
            return f.read()

    def as_bytes(self) -> bytes:
        """
        Return the file as bytes.

        :return:
        """
        with self.open(self.file_url, mode="rb") as f:
            return f.read()
