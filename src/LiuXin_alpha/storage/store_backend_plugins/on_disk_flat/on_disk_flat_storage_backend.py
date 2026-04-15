"""Flat on-disk content-addressed store plugin.

`OnDiskFlatStorageBackend` is a deliberately tiny writable plugin for one local
folder containing files named by the SHA256 hash of their contents. It is
useful for prompt caches and other simple byte stores where a content-addressed,
transparent layout is ideal.

Strict boundary notes
- one plugin, one root directory
- no database logic
- no replica or item semantics
- no nested folder layout as part of the public contract
"""

from __future__ import annotations

import hashlib
import os
import pathlib

from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api import StoreCheckStatus, StoreLocationMixinAPI, StorePluginAPI, StoreStatus
from LiuXin_alpha.storage.errors import FlatStoreImplicitOverwriteError
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat.on_disk_flat_location import (
    OnDiskFlatStoreLocation,
)
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class OnDiskFlatStorageBackend(StorePluginAPI):
    """Writable local plugin storing files directly under one root by hash name."""

    location_cls: Type[OnDiskFlatStoreLocation] = OnDiskFlatStoreLocation

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

    def _canonical_hash_name(self, file_bytes: bytes) -> str:
        return hashlib.sha256(file_bytes).hexdigest()

    def _normalize_name(self, file_identifier: str | StoreLocationMixinAPI) -> str:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                parts = file_identifier.parts
                if len(parts) > 1:
                    raise ValueError("OnDiskFlat only supports one-level file names.")
                return "" if not parts else str(parts[0])
            file_identifier = file_identifier.file_url

        text = str(file_identifier).strip()
        if not text:
            return ""

        candidate = pathlib.Path(text).expanduser()
        if candidate.is_absolute():
            resolved = candidate.resolve(strict=False)
            if not resolved.is_relative_to(self._root_path):
                raise ValueError("File path escapes flat store root: {!r}".format(text))
            rel = resolved.relative_to(self._root_path)
            if len(rel.parts) > 1:
                raise ValueError("OnDiskFlat only supports one-level file names.")
            return "" if not rel.parts else str(rel.parts[0])

        text = text.replace("\\", "/")
        parts = [seg for seg in text.split("/") if seg not in ("", ".")]
        if any(seg == ".." for seg in parts):
            raise ValueError("OnDiskFlat does not allow '..' path segments.")
        if len(parts) > 1:
            raise ValueError("OnDiskFlat only supports one-level file names.")
        return "" if not parts else str(parts[0])

    def _path_for_name(self, name: str) -> pathlib.Path:
        return self._root_path / name

    def _location_from_identifier(self, file_identifier: str | StoreLocationMixinAPI) -> OnDiskFlatStoreLocation:
        name = self._normalize_name(file_identifier)
        if not name:
            return self.location()
        return self.location(name)

    def _make_status_functions(self):
        def _exists(url: str) -> bool:
            try:
                return self._location_from_identifier(url)._loc_path.is_file()
            except ValueError:
                return False

        def _size(url: str) -> int:
            try:
                path = self._location_from_identifier(url)._loc_path
            except ValueError:
                return 0
            if not path.is_file():
                return 0
            return int(path.stat().st_size)

        def _hash(url: str) -> str:
            try:
                loc = self._location_from_identifier(url)
            except ValueError:
                return ""
            if not loc._loc_path.is_file():
                return ""
            return loc.name

        return _exists, _size, _hash

    def self_test(self) -> StoreStatus:
        root_ok = self._root_path.exists() and self._root_path.is_dir()
        read_ok = bool(root_ok and os.access(self._root_path, os.R_OK))
        write_ok = bool(root_ok and os.access(self._root_path, os.W_OK))

        check_status = StoreCheckStatus(
            store_marker_file=root_ok,
            read=read_ok,
            write=write_ok,
            update=False,
            sundry=True,
        )

        file_count: int | None = None
        if root_ok:
            try:
                file_count = sum(1 for path in self._root_path.iterdir() if path.is_file())
            except Exception as exc:
                self._event_log.put("flat store self_test count failed: {!r}".format(exc))

        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=str(self._root_path),
            file_count=file_count,
            store_free_space=None,
            check_status=check_status,
            checked=bool(root_ok and read_ok and write_ok),
            good=bool(root_ok and read_ok and write_ok),
            event_log=self._event_log,
            details={
                "mode": "read_write",
                "plugin_layer": "raw_storage",
                "layout": "flat_hash_named",
            },
        )
        self._cached_status = status
        return status

    def status(self) -> StoreStatus:
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def location(self, *tokens: str) -> OnDiskFlatStoreLocation:
        return self.location_cls(*tokens, store=self)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> OnDiskFlatStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            location = self._location_from_identifier(file_identifier)
        except ValueError:
            return False
        return location.is_file()

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.is_file():
            return None
        return int(path.stat().st_size)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        location = self._location_from_identifier(file_identifier)
        exists_fn, size_fn, hash_fn = self._make_status_functions()
        return SingleFileStatus(
            url=str(location._loc_path),
            exists=location._loc_path.is_file(),
            size=int(location._loc_path.stat().st_size) if location._loc_path.is_file() else 0,
            file_hash=location.name if location._loc_path.is_file() else "",
            uuid=location.name if location._loc_path.is_file() else None,
            check_exists_function=exists_fn,
            check_size_function=size_fn,
            check_hash_function=hash_fn,
        )

    def iter_locations(self) -> Iterator[OnDiskFlatStoreLocation]:
        for path in sorted(self._root_path.iterdir()):
            if not path.is_file():
                continue
            yield self.location(path.name)

    def _raise_implicit_overwrite_error(self, target: pathlib.Path, file_bytes: bytes) -> None:
        if not target.exists():
            return
        if not target.is_file():
            raise FlatStoreImplicitOverwriteError(
                "Implicit flat-store write would collide with a non-file path at {!r}.".format(str(target))
            )
        try:
            existing_bytes = target.read_bytes()
        except Exception as exc:
            raise FlatStoreImplicitOverwriteError(
                "Implicit flat-store write could not safely verify existing target {!r}.".format(str(target))
            ) from exc
        if existing_bytes != file_bytes:
            raise FlatStoreImplicitOverwriteError(
                "Implicit flat-store write would overwrite existing bytes at {!r}.".format(str(target))
            )

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> OnDiskFlatStoreLocation:
        canonical_name = self._canonical_hash_name(file_bytes)
        if location is not None:
            requested_name = self._normalize_name(location)
            if requested_name != canonical_name:
                raise ValueError(
                    "OnDiskFlat filenames are content hashes; requested {!r} but payload hashes to {!r}.".format(
                        requested_name,
                        canonical_name,
                    )
                )
        target = self._path_for_name(canonical_name)
        if target.exists():
            self._raise_implicit_overwrite_error(target, file_bytes)
            return self.location(canonical_name)
        try:
            with target.open("xb") as fh:
                fh.write(file_bytes)
        except FileExistsError:
            self._raise_implicit_overwrite_error(target, file_bytes)
        return self.location(canonical_name)

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> OnDiskFlatStoreLocation:
        src = self._location_from_identifier(src_location)._loc_path
        if not src.is_file():
            raise FileNotFoundError(str(src))
        dst_name = self._normalize_name(dst_location)
        if dst_name != src.name:
            raise ValueError(
                "OnDiskFlat cannot copy to a different filename; filenames are canonical content hashes."
            )
        return self.location(src.name)

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
        raise PermissionError(
            "OnDiskFlat is content-addressed; in-place updates would invalidate the hash-named location."
        )


__all__ = ["OnDiskFlatStorageBackend"]
