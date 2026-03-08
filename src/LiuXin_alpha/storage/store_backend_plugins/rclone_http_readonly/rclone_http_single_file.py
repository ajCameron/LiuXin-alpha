"""Read-only single-file wrapper backed by `rclone cat` and `lsjson`."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

from .rclone_utils import run_rclone, run_rclone_json


class RcloneHttpReadOnlySingleFile(SingleFileAPI):
    """Minimal read-only SingleFile wrapper.

    LiuXin-alpha's storage/file API is still evolving; this class is intentionally
    small and focuses on making `open()` and metadata retrieval possible.
    """

    def __init__(self, file_url: str, store: object | None = None) -> None:
        super().__init__(file_url=file_url, file_status=None)
        self._store = store

    def _store_options(self) -> tuple[str, tuple[str, ...], dict[str, str] | None, float | None]:
        opts = getattr(self._store, "options", None)
        rclone_exe = getattr(opts, "rclone_exe", "rclone")
        rclone_args = tuple(getattr(opts, "rclone_args", ()))
        env = getattr(opts, "env", None)
        timeout_s = getattr(opts, "timeout_s", 60.0)
        return rclone_exe, rclone_args, env, timeout_s

    def _stat_blob(self) -> dict[str, Any] | None:
        rclone_exe, rclone_args, env, timeout_s = self._store_options()
        blob = run_rclone_json(
            ["lsjson", "--stat", self.file_url],
            rclone_exe=rclone_exe,
            extra_args=rclone_args,
            env=env,
            timeout_s=timeout_s,
            check=False,
        )
        return blob if isinstance(blob, dict) else None

    def recheck_status(self) -> SingleFileStatus:
        def _exists(url: str) -> bool:
            blob = self._stat_blob()
            return bool(blob is not None)

        def _size(url: str) -> int:
            blob = self._stat_blob()
            if blob is None:
                return 0
            return int(blob.get("Size") or 0)

        def _hash(url: str) -> str:
            blob = self._stat_blob()
            if blob is None:
                return ""
            hashes = blob.get("Hashes") or {}
            if isinstance(hashes, dict):
                # Pick a stable preference order and return first available.
                for key in ("sha256", "sha1", "md5", "crc32"):
                    if key in hashes and hashes[key]:
                        return str(hashes[key])
                for value in hashes.values():
                    if value:
                        return str(value)
            return ""

        if self.file_status is None:
            self.file_status = SingleFileStatus(
                url=self.file_url,
                check_exists_function=_exists,
                check_size_function=_size,
                check_hash_function=_hash,
            )
        else:
            self.file_status.update_check_exists_function(_exists)
            self.file_status.update_check_size_function(_size)
            self.file_status.update_check_hash_function(_hash)
            self.file_status.recheck_self(all=True)

        return self.file_status

    def as_string(self) -> str:
        rclone_exe, rclone_args, env, timeout_s = self._store_options()
        result = run_rclone(
            ["cat", self.file_url],
            rclone_exe=rclone_exe,
            extra_args=rclone_args,
            env=env,
            timeout_s=timeout_s,
            check=True,
        )
        return result.stdout

    def as_bytes(self) -> bytes:
        return self.as_string().encode("utf-8")

    # Note: SingleFileAPI inherits a rich typed `open` from file_api.py; we rely on that.
