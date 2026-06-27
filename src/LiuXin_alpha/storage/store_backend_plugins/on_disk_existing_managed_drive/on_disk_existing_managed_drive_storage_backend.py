"""Managed local-disk store plugin.

This plugin is for the case where LiuXin is allowed to *take over* one existing
directory tree on local storage. It treats the configured root as managed
territory: existing files remain valid locations, explicit writes can target any
store-relative path, and implicit writes land in a reserved LiuXin-managed area
at the store root.

Strict boundary notes
- one plugin, one writable root directory
- no database logic
- no replica or item semantics
- explicit writes may target any path inside the root
- implicit writes never spray files into the visible root; they go under a
  reserved LiuXin-managed subdirectory using a deterministic hash-based layout
- implicit writes must never silently overwrite an existing incompatible file
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import uuid

from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api import StorePluginAPI, StoreCheckStatus, StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.storage.errors import ManagedDriveImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.storage.local.local_store_smoke_test import StorageIOSmokeTest
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class OnDiskExistingManagedStorageBackend(StorePluginAPI):
    """Writable plugin for one managed directory tree on local disk.

    Existing files are part of the store and can be addressed directly. When the
    caller writes bytes without an explicit destination, the payload is stored in
    a reserved hidden area at ``<root>/.liuxin/managed_drive/<hash[:5]>/<hash>``.
    That keeps the root tidy while still making manual inspection
    straightforward. Implicit writes dedupe identical bytes, but must fail
    loudly if an incompatible file is already occupying the canonical path.
    """

    MANAGED_AREA_DIRNAME = ".liuxin"
    AUTO_WRITE_SUBDIRNAME = "managed_drive"
    AUTO_WRITE_BUCKET_LENGTH = 5

    location_cls: Type[OnDiskExistingManagedStoreLocation] = OnDiskExistingManagedStoreLocation

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
            details={
                "mode": "read_write",
                "plugin_layer": "raw_storage",
                "layout": "existing_directory_tree_with_reserved_managed_area",
                "managed_area_root": str(self.managed_area_root),
            },
        )
        self._cached_status = status
        return status

    def status(self) -> "StoreStatus":
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def location(self, *tokens: str) -> "OnDiskExistingManagedStoreLocation":
        return self.location_cls(*tokens, store=self)

    def _location_from_identifier(self, file_identifier: str | StoreLocationMixinAPI) -> OnDiskExistingManagedStoreLocation:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier
            file_identifier = file_identifier.file_url
        path = self._resolve_file_path(str(file_identifier))
        rel = path.relative_to(self._root_path)
        return self.location(*rel.parts)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> OnDiskExistingManagedStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            return self._location_from_identifier(file_identifier).is_file()
        except ValueError:
            return False

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> Optional[int]:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.is_file():
            return None
        return int(path.stat().st_size)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        path = self._location_from_identifier(file_identifier)._loc_path
        exists_fn, size_fn, hash_fn = self._make_status_functions()
        return SingleFileStatus(
            url=str(path),
            check_exists_function=exists_fn,
            check_size_function=size_fn,
            check_hash_function=hash_fn,
        )

    def iter_locations(self) -> Iterator[OnDiskExistingManagedStoreLocation]:
        for path in self._root_path.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self._root_path)
            yield self.location(*rel.parts)

    @property
    def managed_area_root(self) -> pathlib.Path:
        """Reserved root for LiuXin-owned implicit writes inside this store."""
        return self._root_path / self.MANAGED_AREA_DIRNAME / self.AUTO_WRITE_SUBDIRNAME

    def is_reserved_managed_path(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        """Return True when a location lives under the reserved LiuXin-managed area."""
        path = self._location_from_identifier(file_identifier)._loc_path
        return path.is_relative_to(self.managed_area_root)

    def _implicit_write_target_path(self, file_bytes: bytes) -> pathlib.Path:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        subdir = self.managed_area_root / file_hash[: self.AUTO_WRITE_BUCKET_LENGTH]
        return subdir / file_hash

    def _existing_path_matches_payload(self, target: pathlib.Path, file_bytes: bytes) -> bool:
        if not target.exists():
            return False
        if not target.is_file():
            return False
        try:
            return target.read_bytes() == file_bytes
        except Exception:
            return False

    def _raise_implicit_overwrite_error(self, target: pathlib.Path, file_bytes: bytes) -> None:
        if not target.exists():
            return
        if not target.is_file():
            raise ManagedDriveImplicitOverwriteError(
                "Implicit managed-drive write would collide with a non-file path at {!r}.".format(str(target))
            )
        try:
            existing_bytes = target.read_bytes()
        except Exception as exc:
            raise ManagedDriveImplicitOverwriteError(
                "Implicit managed-drive write could not safely verify existing target {!r}.".format(str(target))
            ) from exc
        if existing_bytes != file_bytes:
            raise ManagedDriveImplicitOverwriteError(
                "Implicit managed-drive write would overwrite existing bytes at {!r}. Use an explicit location if you really mean to replace that file.".format(
                    str(target)
                )
            )

    def _write_implicit_bytes_to_path(self, target: pathlib.Path, file_bytes: bytes) -> pathlib.Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            self._raise_implicit_overwrite_error(target, file_bytes)
            return target
        try:
            with target.open("xb") as fh:
                fh.write(file_bytes)
        except FileExistsError:
            self._raise_implicit_overwrite_error(target, file_bytes)
        return target

    def _write_bytes_to_path(self, target: pathlib.Path, file_bytes: bytes, *, ensure_parents: bool) -> pathlib.Path:
        if ensure_parents:
            target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".{}.tmp".format(uuid.uuid4().hex))
        tmp.write_bytes(file_bytes)
        os.replace(tmp, target)
        return target

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> OnDiskExistingManagedStoreLocation:
        if location is None:
            target = self._implicit_write_target_path(file_bytes)
            self._write_implicit_bytes_to_path(target, file_bytes)
        else:
            target = self._resolve_file_path(str(location))
            self._write_bytes_to_path(target, file_bytes, ensure_parents=True)
        rel = target.relative_to(self._root_path)
        return self.location(*rel.parts)

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> OnDiskExistingManagedStoreLocation:
        src = self._location_from_identifier(src_location)._loc_path
        if not src.is_file():
            raise FileNotFoundError(str(src))
        dst = self._location_from_identifier(dst_location)._loc_path
        payload = src.read_bytes()
        self._write_bytes_to_path(dst, payload, ensure_parents=True)
        rel = dst.relative_to(self._root_path)
        return self.location(*rel.parts)

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.is_file():
            return False
        path.unlink()
        return True

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.is_file():
            raise FileNotFoundError(str(path))
        mode = "ab" if append else "wb"
        with path.open(mode) as fh:
            fh.write(file_bytes)
        return True


__all__ = ["OnDiskExistingManagedStorageBackend"]
