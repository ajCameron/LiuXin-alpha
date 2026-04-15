"""
Read-only SquashFS archive storage backend.

The store root (`url`) is one `.sqfs`/`.squashfs` file. Files are addressed by
their relative internal archive path.
"""

from __future__ import annotations

import hashlib
import pathlib
import re
import shutil
import subprocess
import time

from typing import Dict, Iterator, Optional, Type

from LiuXin_alpha.storage.api import StorePluginAPI, StoreCheckStatus, StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_location import (
    SquashfsReadOnlyStoreLocation,
)
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class SquashfsReadOnlyStorageBackend(StorePluginAPI):
    """
    Read-only archive backend over `unsquashfs`.
    """

    location_cls: Type[SquashfsReadOnlyStoreLocation] = SquashfsReadOnlyStoreLocation
    _line_re = re.compile(
        r"^\S+\s+\S+\s+(?P<size>\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(?P<path>.+)$"
    )
    _root_prefix = "squashfs-root/"

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        *,
        unsquashfs_exe: str = "unsquashfs",
        timeout_s: float = 60.0,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._archive_path = pathlib.Path(self.url).expanduser().resolve()
        if not self._archive_path.exists() or not self._archive_path.is_file():
            raise FileNotFoundError("SquashFS archive not found: {!r}".format(url))
        self.set_url(str(self._archive_path))
        self._event_log = DefaultEventLog()
        self._cached_status: Optional[StoreStatus] = None
        self._unsquashfs_exe = unsquashfs_exe
        self._timeout_s = float(timeout_s)
        self._file_index: Dict[str, int] = {}
        self._indexed_archive_mtime_ns: Optional[int] = None

    @property
    def archive_path(self) -> pathlib.Path:
        return self._archive_path

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        self._cached_status = self.self_test()
        return self._cached_status

    def _run_unsquashfs(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[bytes]:
        exe = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        proc = subprocess.run(
            [exe, *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self._timeout_s,
            check=False,
        )
        if check and proc.returncode != 0:
            raise RuntimeError(
                "unsquashfs failed (rc={}): {}".format(
                    proc.returncode,
                    proc.stderr.decode("utf-8", "replace").strip(),
                )
            )
        return proc

    def _iter_unsquashfs_cat_chunks(
        self,
        internal_path: str,
        *,
        chunk_size: int = 1024 * 1024,
    ):
        exe = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        proc = subprocess.Popen(
            [exe, "-cat", str(self._archive_path), internal_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        start = time.monotonic()
        try:
            if proc.stdout is None:
                raise RuntimeError("unsquashfs stdout pipe not available")
            while True:
                chunk = proc.stdout.read(chunk_size)
                if not chunk:
                    break
                yield chunk
                if (time.monotonic() - start) > self._timeout_s:
                    raise TimeoutError(
                        "unsquashfs streaming timed out after {:.1f}s".format(self._timeout_s)
                    )
            try:
                _, stderr = proc.communicate(timeout=max(0.1, self._timeout_s))
            except subprocess.TimeoutExpired as exc:
                proc.kill()
                raise TimeoutError(
                    "unsquashfs streaming timed out after {:.1f}s".format(self._timeout_s)
                ) from exc
            if proc.returncode != 0:
                raise RuntimeError(
                    "unsquashfs failed (rc={}): {}".format(
                        proc.returncode,
                        (stderr or b"").decode("utf-8", "replace").strip(),
                    )
                )
        finally:
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

    def _normalize_internal_path(self, file_url: str) -> Optional[str]:
        if file_url is None:
            return None
        text = str(file_url).strip()
        prefix = self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            text = text[len(prefix) :]
        text = text.replace("\\", "/")
        text = text.lstrip("/")
        if not text:
            return None
        parts: list[str] = []
        for part in text.split("/"):
            if part in {"", "."}:
                continue
            if part == "..":
                return None
            parts.append(part)
        if not parts:
            return None
        return "/".join(parts)

    def _build_index(self) -> Dict[str, int]:
        out = self._run_unsquashfs(["-llc", str(self._archive_path)], check=True).stdout.decode("utf-8", "replace")
        index: Dict[str, int] = {}
        for raw_line in out.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = self._line_re.match(line)
            if match is None:
                continue
            path = match.group("path")
            if not path.startswith(self._root_prefix):
                continue
            rel = path[len(self._root_prefix) :]
            rel = rel.lstrip("/")
            if not rel:
                continue
            index[rel] = int(match.group("size"))
        return index

    def _get_index(self, *, force: bool = False) -> Dict[str, int]:
        mtime_ns = int(self._archive_path.stat().st_mtime_ns)
        if force or self._indexed_archive_mtime_ns != mtime_ns:
            self._file_index = self._build_index()
            self._indexed_archive_mtime_ns = mtime_ns
        return self._file_index

    def self_test(self) -> StoreStatus:
        marker_ok = self._archive_path.exists() and self._archive_path.is_file()
        read_ok = False
        write_ok = False
        sundry_ok = False
        file_count: Optional[int] = None

        try:
            idx = self._get_index(force=True)
            file_count = len(idx)
            read_ok = True
            sundry_ok = True
        except Exception as exc:
            self._event_log.put("self_test failed: {!r}".format(exc))

        try:
            free_space = int(get_free_bytes(str(self._archive_path.parent)))
        except Exception:
            free_space = None

        check_status = StoreCheckStatus(
            store_marker_file=marker_ok,
            read=read_ok,
            write=write_ok,
            sundry=sundry_ok,
        )
        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=self.url,
            file_count=file_count,
            store_free_space=free_space,
            check_status=check_status,
            checked=bool(marker_ok and read_ok and sundry_ok),
            good=bool(marker_ok and read_ok and sundry_ok),
            event_log=self._event_log,
            details={"mode": "read_only", "container": "squashfs_archive"},
        )
        self._cached_status = status
        return status

    @property
    def db_path(self) -> pathlib.Path:
        return self._archive_path

    @property
    def root_path(self) -> pathlib.Path:
        return self._archive_path

    def location(self, *tokens: str) -> SquashfsReadOnlyStoreLocation:
        return self.location_cls(*tokens, store=self)

    def _location_from_url(self, file_url: str | StoreLocationMixinAPI) -> SquashfsReadOnlyStoreLocation:
        if isinstance(file_url, StoreLocationMixinAPI):
            if file_url.store is self:
                return file_url
            file_url = file_url.file_url
        internal = self._normalize_internal_path(str(file_url))
        if internal is None:
            raise ValueError("Malformed file URL/path for squashfs store: {!r}".format(file_url))
        return self.location(*internal.split("/"))

    def status(self) -> StoreStatus:
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def _file_hash(self, internal_path: str) -> str:
        hasher = hashlib.sha256()
        for chunk in self._iter_unsquashfs_cat_chunks(internal_path):
            hasher.update(chunk)
        return hasher.hexdigest()

    def file_exists(self, file_url: str | StoreLocationMixinAPI) -> bool:
        internal = self._normalize_internal_path(str(file_url.file_url if isinstance(file_url, StoreLocationMixinAPI) else file_url))
        if internal is None:
            return False
        return internal in self._get_index()

    def file_size(self, file_url: str | StoreLocationMixinAPI) -> Optional[int]:
        internal = self._normalize_internal_path(str(file_url.file_url if isinstance(file_url, StoreLocationMixinAPI) else file_url))
        if internal is None:
            return None
        return int(self._get_index().get(internal, 0))

    def get_file_status(self, file_url: str | StoreLocationMixinAPI) -> SingleFileStatus:
        internal = self._normalize_internal_path(str(file_url.file_url if isinstance(file_url, StoreLocationMixinAPI) else file_url))
        if internal is None:
            raise ValueError("Malformed file URL/path for squashfs store: {!r}".format(file_url))
        canonical_url = "{}/{}".format(self.url.rstrip("/"), internal)

        def _exists(url: str) -> bool:
            p = self._normalize_internal_path(url)
            return bool(p and p in self._get_index())

        def _size(url: str) -> int:
            p = self._normalize_internal_path(url)
            if p is None:
                return 0
            return int(self._get_index().get(p, 0))

        def _hash(url: str) -> str:
            p = self._normalize_internal_path(url)
            if p is None:
                return ""
            if p not in self._get_index():
                return ""
            return self._file_hash(p)

        return SingleFileStatus(
            url=canonical_url,
            size=int(self._get_index().get(internal, 0)),
            file_hash="",
            check_exists_function=_exists,
            check_size_function=_size,
            check_hash_function=_hash,
        )

    def get_file(self, file_url: str | StoreLocationMixinAPI) -> SquashfsReadOnlyStoreLocation:
        location = self._location_from_url(file_url)
        if not location.is_file():
            raise FileNotFoundError(location.file_url)
        return location

    def read_file_bytes(self, file_url: str | StoreLocationMixinAPI) -> bytes:
        internal = self._normalize_internal_path(str(file_url.file_url if isinstance(file_url, StoreLocationMixinAPI) else file_url))
        if internal is None:
            raise ValueError("Malformed file URL/path for squashfs store: {!r}".format(file_url))
        if internal not in self._get_index():
            raise FileNotFoundError("Path not found in squashfs archive: {!r}".format(internal))
        return self._run_unsquashfs(["-cat", str(self._archive_path), internal], check=True).stdout

    def true_files(self) -> Iterator[SquashfsReadOnlyStoreLocation]:
        for internal in sorted(self._get_index().keys()):
            yield self.get_file("{}/{}".format(self.url.rstrip("/"), internal))

    def add_file(self, file_bytes: bytes, *, metadata=None) -> SquashfsReadOnlyStoreLocation:
        raise PermissionError("SquashfsReadOnlyStorageBackend is read-only.")

    def delete_file(self, file_url: str | StoreLocationMixinAPI) -> bool:
        raise PermissionError("SquashfsReadOnlyStorageBackend is read-only.")
