"""Read-only local-disk reference store plugin.

`OnDiskUnmanagedStorageBackend` exposes one existing local directory tree for
reference and ingestion-by-copy workflows. It is intentionally strict about
separation of concerns:

- one plugin, one root directory
- path resolution stays inside that root
- no database logic
- no item/replica semantics
- no mutation through the plugin API

This is the plugin you point at a pile of already-existing files when LiuXin
needs to *see* them and refer to them, not rewrite them in place.
"""

from __future__ import annotations

import os
import pathlib

from collections.abc import Iterator
from typing import Optional, Type

from LiuXin_alpha.storage.api import StoreCheckStatus, StoreLocationMixinAPI, StorePluginAPI, StoreStatus
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class OnDiskUnmanagedStorageBackend(StorePluginAPI):
    """Read-only plugin for one existing directory tree on local disk.

    String identifiers are interpreted as either store-relative keys or absolute
    paths that must still resolve inside the configured root.
    """

    location_cls: Type[OnDiskUnmanagedStoreLocation] = OnDiskUnmanagedStoreLocation

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

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        self._cached_status = self.self_test()
        return self._cached_status

    def _resolve_file_path(self, file_url: str, *, require_inside_store: bool = True) -> pathlib.Path:
        candidate = pathlib.Path(file_url).expanduser()
        if not candidate.is_absolute():
            candidate = self._root_path.joinpath(candidate)

        resolved = candidate.resolve(strict=False)
        if require_inside_store and not resolved.is_relative_to(self._root_path):
            raise ValueError("File path escapes store root: {!r}".format(str(candidate)))
        return resolved

    def _location_from_identifier(
        self,
        file_identifier: str | StoreLocationMixinAPI,
    ) -> OnDiskUnmanagedStoreLocation:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier
            file_identifier = file_identifier.file_url
        path = self._resolve_file_path(str(file_identifier))
        rel = path.relative_to(self._root_path)
        return self.location(*rel.parts)

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
        list_ok = False
        file_count: int | None = None

        if root_ok and read_ok:
            try:
                file_count = sum(1 for path in self._root_path.rglob("*") if path.is_file())
                list_ok = True
            except Exception as exc:
                self._event_log.put("unmanaged store self_test listing failed: {!r}".format(exc))

        try:
            free_space = int(get_free_bytes(str(self._root_path)))
        except Exception:
            free_space = None

        check_status = StoreCheckStatus(
            store_marker_file=root_ok,
            read=read_ok,
            write=False,
            update=False,
            sundry=list_ok,
        )

        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=str(self._root_path),
            file_count=file_count,
            store_free_space=free_space,
            check_status=check_status,
            checked=bool(root_ok and read_ok and list_ok),
            good=bool(root_ok and read_ok and list_ok),
            event_log=self._event_log,
            details={
                "mode": "read_only",
                "layout": "existing_directory_tree",
                "plugin_layer": "raw_storage",
            },
        )
        self._cached_status = status
        return status

    def status(self) -> StoreStatus:
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def location(self, *tokens: str) -> OnDiskUnmanagedStoreLocation:
        return self.location_cls(*tokens, store=self)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> OnDiskUnmanagedStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            return self._location_from_identifier(file_identifier).is_file()
        except ValueError:
            return False

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
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

    def iter_locations(self) -> Iterator[OnDiskUnmanagedStoreLocation]:
        for path in self._root_path.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self._root_path)
            yield self.location(*rel.parts)

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> OnDiskUnmanagedStoreLocation:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> OnDiskUnmanagedStoreLocation:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")


__all__ = ["OnDiskUnmanagedStorageBackend"]
