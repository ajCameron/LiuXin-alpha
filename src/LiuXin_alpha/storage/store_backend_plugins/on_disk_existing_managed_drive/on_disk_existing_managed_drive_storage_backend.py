"""
On-disk existing-managed store backend.

This backend treats a local directory as a managed store boundary and supports
read/write operations for files ingested by LiuXin.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import uuid

from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.api.storage_api import StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_single_file import (
    OnDiskExistingManagedSingleFile,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.storage.local.local_store_smoke_test import StorageIOSmokeTest
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class OnDiskExistingManagedStorageBackend(StoreAPI):
    """
    Represents a managed local directory store.

    The root directory is treated as the store boundary: relative paths are
    resolved under it and absolute paths must stay inside it.
    """

    location_cls: Type[OnDiskExistingManagedStoreLocation] = OnDiskExistingManagedStoreLocation
    single_file_cls: Type[OnDiskExistingManagedSingleFile] = OnDiskExistingManagedSingleFile

    _root_path: pathlib.Path

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._root_path = pathlib.Path(self.url).expanduser().resolve()
        self._root_path.mkdir(parents=True, exist_ok=True)
        self._event_log = DefaultEventLog()
        self._cached_status: Optional[StoreStatus] = None

    @property
    def root_path(self) -> pathlib.Path:
        return self._root_path

    def startup(self) -> StoreStatus:
        self._cached_status = self.self_test()
        return self._cached_status

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def _resolve_file_path(self, file_url: str, *, require_inside_store: bool = True) -> pathlib.Path:
        candidate = pathlib.Path(file_url).expanduser()
        if not candidate.is_absolute():
            candidate = self._root_path.joinpath(candidate)

        resolved = candidate.resolve(strict=False)
        if require_inside_store and not resolved.is_relative_to(self._root_path):
            raise ValueError("File path escapes store root: {!r}".format(str(candidate)))
        return resolved

    def _make_status_functions(self):
        def _exists(url: str) -> bool:
            try:
                return self._resolve_file_path(url).is_file()
            except ValueError:
                return False

        def _size(url: str) -> int:
            try:
                path = self._resolve_file_path(url)
            except ValueError:
                return 0
            if not path.is_file():
                return 0
            return int(path.stat().st_size)

        def _hash(url: str) -> str:
            try:
                path = self._resolve_file_path(url)
            except ValueError:
                return ""
            if not path.is_file():
                return ""
            return get_file_hash(str(path))

        return _exists, _size, _hash

    def self_test(self) -> StoreStatus:
        root_ok = self._root_path.exists() and self._root_path.is_dir()
        read_ok = bool(root_ok and os.access(self._root_path, os.R_OK))
        write_ok = bool(root_ok and os.access(self._root_path, os.W_OK))
        smoke_ok = False

        if root_ok:
            try:
                smoke_report = StorageIOSmokeTest(
                    root=self._root_path,
                    strict=False,
                    cleanup=True,
                    big_file_mb=1,
                    concurrent_files=2,
                    concurrent_file_mb=1,
                    random_access_mb=1,
                    chunk_size=64 * 1024,
                ).run()
                smoke_ok = bool(smoke_report.get("ok"))
            except Exception as exc:
                self._event_log.put("self_test smoketest failed: {!r}".format(exc))

        check_status = StoreCheckStatus(
            store_marker_file=root_ok,
            read=read_ok and smoke_ok,
            write=write_ok and smoke_ok,
            sundry=smoke_ok,
        )

        try:
            free_space = int(get_free_bytes(str(self._root_path)))
        except Exception:
            free_space = None

        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=str(self._root_path),
            file_count=None,
            store_free_space=free_space,
            check_status=check_status,
            checked=check_status.all_ok,
            good=check_status.all_ok,
            event_log=self._event_log,
            details={"mode": "read_write"},
        )
        self._cached_status = status
        return status

    def status(self) -> StoreStatus:
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def location(self, *tokens: str) -> OnDiskExistingManagedStoreLocation:
        return self.location_cls(*tokens, store=self)

    def file_exists(self, file_url: str) -> bool:
        try:
            return self._resolve_file_path(file_url).is_file()
        except ValueError:
            return False

    def file_size(self, file_url: str) -> Optional[int]:
        path = self._resolve_file_path(file_url)
        if not path.is_file():
            return None
        return int(path.stat().st_size)

    def get_file_status(self, file_url: str) -> SingleFileStatus:
        path = self._resolve_file_path(file_url)
        exists_fn, size_fn, hash_fn = self._make_status_functions()
        return SingleFileStatus(
            url=str(path),
            check_exists_function=exists_fn,
            check_size_function=size_fn,
            check_hash_function=hash_fn,
        )

    def get_file(self, file_url: str) -> SingleFileAPI:
        path = self._resolve_file_path(file_url)
        file_row = self.single_file_cls(file_url=str(path), file_status=self.get_file_status(str(path)))
        file_row.store = self.name
        return file_row

    def true_files(self) -> Iterator[SingleFileAPI]:
        for path in self._root_path.rglob("*"):
            if not path.is_file():
                continue
            yield self.get_file(str(path))

    def _ingest_target_path(self, file_bytes: bytes) -> pathlib.Path:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        subdir = self._root_path / ".liuxin_ingest" / file_hash[:2]
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / "{}.bin".format(file_hash)

    def add_file(self, file_bytes: bytes, *, metadata=None) -> SingleFileAPI:
        target = self._ingest_target_path(file_bytes)
        if not target.exists():
            tmp = target.with_suffix(".{}.tmp".format(uuid.uuid4().hex))
            tmp.write_bytes(file_bytes)
            os.replace(tmp, target)
        return self.get_file(str(target))

    def delete_file(self, file_url: str) -> bool:
        path = self._resolve_file_path(file_url)
        if not path.is_file():
            return False
        path.unlink()
        return True

