from __future__ import annotations

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

from .rclone_utils import run_rclone_json


class RcloneHttpReadOnlySingleFile(SingleFileAPI):
    """Minimal read-only SingleFile wrapper.

    LiuXin-alpha's storage/file API is still evolving; this class is intentionally
    small and focuses on making `open()` and metadata retrieval possible.
    """

    def __init__(self, file_url: str, store: object | None = None) -> None:
        super().__init__(file_url)
        self._store = store

    def get_status(self) -> SingleFileStatus:
        return SingleFileStatus.GOOD

    def as_string(self) -> str:
        with self.open(self.file_url, "r", encoding="utf-8") as f:
            return f.read()

    def as_bytes(self) -> bytes:
        with self.open(self.file_url, "rb") as f:
            return f.read()

    # Note: SingleFileAPI inherits a rich typed `open` from file_api.py; we rely on that.
