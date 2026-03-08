from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend import (
        SquashfsReadOnlyStorageBackend,
    )


class SquashfsReadOnlySingleFile(SingleFileAPI):
    """
    One file inside a SquashFS archive store.
    """

    def __init__(
        self,
        *,
        file_url: str,
        backend: "SquashfsReadOnlyStorageBackend",
        file_status: Optional[SingleFileStatus] = None,
    ) -> None:
        super().__init__(file_url=file_url, file_status=file_status)
        self._backend = backend

    def recheck_status(self) -> SingleFileStatus:
        if self.file_status is None:
            self.file_status = self._backend.get_file_status(self.file_url)
        else:
            self.file_status.recheck_self(all=True)
        return self.file_status

    def as_string(self) -> str:
        return self.as_bytes().decode("utf-8", "replace")

    def as_bytes(self) -> bytes:
        return self._backend.read_file_bytes(self.file_url)
