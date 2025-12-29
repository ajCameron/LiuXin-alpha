from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Sequence

from LiuXin_alpha.storage.api.storage_api import StorageBackendAPI, StorageBackendStatus, StorageBackendCheckStatus
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog

from .rclone_utils import run_rclone_json, run_rclone
from .rclone_http_single_file import RcloneHttpReadOnlySingleFile


@dataclass
class RcloneBackendOptions:
    rclone_exe: str = "rclone"
    rclone_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 60.0


class RcloneHttpReadOnlyStorageBackend(StorageBackendAPI):
    """Read-only StorageBackend powered by `rclone`'s HTTP remote.

    `url` is an rclone filesystem (fs) string, e.g.

    - Config-based:   ``remote:`` or ``remote:some/base/path``
    - Config-less:    ``:http,url=https://example.com:``  (note: no shell quotes)

    This backend is intentionally read-only: add/delete operations raise.
    """

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: RcloneBackendOptions | None = None,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self.options = options or RcloneBackendOptions()
        self._event_log = InMemoryEventLog()

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> None:
        # Validate rclone exists and is runnable.
        run_rclone(
            ["version"],
            rclone_exe=self.options.rclone_exe,
            extra_args=self.options.rclone_args,
            env=self.options.env,
            timeout_s=self.options.timeout_s,
        )

    def self_test(self) -> StorageBackendStatus:
        cs = StorageBackendCheckStatus()
        cs.store_marker_file = True
        cs.read = False
        cs.write = False
        cs.sundry = False

        good = "unknown"
        try:
            # List root (non-recursive) to prove we can read.
            run_rclone_json(
                ["lsjson", "--max-depth", "1", self.url],
                rclone_exe=self.options.rclone_exe,
                extra_args=self.options.rclone_args,
                env=self.options.env,
                timeout_s=self.options.timeout_s,
                check=True,
            )
            cs.read = True
            cs.sundry = True
            good = "ok (read-only)"
        except Exception as e:
            self._event_log.put(f"self_test failed: {e!r}")
            cs.read = False
            good = "unhealthy"

        # No robust free-space for HTTP remotes (rclone about unsupported).
        return StorageBackendStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            file_count=None,
            store_free_space=0,
            check_status=cs,
            checked=bool(cs.read),
            url=self.url,
            good=good,
            event_log=self._event_log,
        )

    def status(self) -> StorageBackendStatus:
        return self.self_test()

    def file_exists(self, file_url: str) -> bool:
        try:
            run_rclone_json(
                ["lsjson", "--stat", file_url],
                rclone_exe=self.options.rclone_exe,
                extra_args=self.options.rclone_args,
                env=self.options.env,
                timeout_s=self.options.timeout_s,
                check=True,
            )
            return True
        except Exception as e:
            # Treat "not found" as missing; other failures surface as False here for now.
            msg = str(e).lower()
            if "not found" in msg or "doesn't exist" in msg or "couldn't find" in msg or "error 404" in msg:
                return False
            return False

    def get_file(self, file_url: str) -> RcloneHttpReadOnlySingleFile:
        return RcloneHttpReadOnlySingleFile(file_url=file_url, store=self)

    def add_storage_backend(self, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("HTTP backend is read-only")

    def add_file(self, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("HTTP backend is read-only")

    def retrieve_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Use Location or rclone directly for transfer operations")

    def delete_file(self, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("HTTP backend is read-only")

    def iter(self) -> Iterator[RcloneHttpReadOnlySingleFile]:
        # Iterate all files in the store.
        items = run_rclone_json(
            ["lsjson", "-R", "--files-only", self.url],
            rclone_exe=self.options.rclone_exe,
            extra_args=self.options.rclone_args,
            env=self.options.env,
            timeout_s=self.options.timeout_s,
            check=True,
        ) or []
        for it in items:
            p = it.get("Path") or it.get("Name")
            if not p:
                continue
            # Join to a full file url.
            if self.url.endswith(":"):
                full = f"{self.url}{p}"
            else:
                full = f"{self.url.rstrip('/')}/{p}"
            yield self.get_file(full)
