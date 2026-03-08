"""Single-file wrapper for payloads stored in the single-file SQLite backend."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite.single_file_sqlite_storage_backend import (
        SingleFileSqliteStorageBackend,
    )


class SingleFileSqliteSingleFile(SingleFileAPI):
    """
    One file payload stored inside the single-file SQLite store.
    """

    def __init__(
        self,
        *,
        file_url: str,
        backend: "SingleFileSqliteStorageBackend",
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
