"""Writable staging plugin for building SquashFS archives.

This plugin is intentionally narrow. It stages files locally, allows limited
write/update/delete operations against that staging area, and then seals the
whole pack into one SquashFS archive. It is closer to a backup/snapshot builder
than to a normal mutable store.
"""

from __future__ import annotations

import hashlib
import pathlib
import shutil
import subprocess
import tempfile
from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api import StoreCheckStatus, StoreLocationMixinAPI, StorePluginAPI, StoreStatus
from LiuXin_alpha.storage.errors import SquashfsBuildImplicitOverwriteError
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_location import SquashfsBuildStoreLocation
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import SquashfsReadOnlyStorageBackend
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class SquashfsBuildStorageBackend(StorePluginAPI):
    """Writable staging plugin that seals to one SquashFS archive.

    Design notes:
    - the configured ``url`` is the output archive file path
    - staged files live in a local staging root until ``seal()`` is called
    - implicit writes use a deterministic hash layout under ``objects/``
    - explicit writes are allowed to overwrite staged content; implicit writes
      must never silently overwrite incompatible existing bytes
    - designated source files are *copied* into staging so the pack behaves like
      a snapshot, not a moving hardlink target
    """

    DEFAULT_OBJECTS_DIRNAME = "objects"
    AUTO_WRITE_BUCKET_LENGTH = 5

    location_cls: Type[SquashfsBuildStoreLocation] = SquashfsBuildStoreLocation

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        *,
        mksquashfs_exe: str = "mksquashfs",
        compression: str = "zstd",
        deterministic: bool = False,
        staging_root: str | None = None,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._archive_path = pathlib.Path(self.url).expanduser().resolve()
        self._archive_path.parent.mkdir(parents=True, exist_ok=True)
        self._event_log = DefaultEventLog()
        self._cached_status: Optional[StoreStatus] = None
        self._mksquashfs_exe = str(mksquashfs_exe)
        self._compression = str(compression)
        self._deterministic = bool(deterministic)
        self._tempdir: tempfile.TemporaryDirectory[str] | None = None
        if staging_root is None:
            self._tempdir = tempfile.TemporaryDirectory(prefix="liuxin-squashfs-build-")
            self._staging_root = pathlib.Path(self._tempdir.name).resolve()
        else:
            self._staging_root = pathlib.Path(staging_root).expanduser().resolve()
            self._staging_root.mkdir(parents=True, exist_ok=True)
        self._built_store: SquashfsReadOnlyStorageBackend | None = None

    @property
    def archive_path(self) -> pathlib.Path:
        return self._archive_path

    @property
    def staging_root(self) -> pathlib.Path:
        return self._staging_root

    @property
    def root_path(self) -> pathlib.Path:
        return self._staging_root

    @property
    def built_store(self) -> SquashfsReadOnlyStorageBackend | None:
        return self._built_store

    def close(self) -> None:
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        self._cached_status = self.self_test()
        return self._cached_status

    def status(self) -> StoreStatus:
        return self.self_test() if self._cached_status is None else self._cached_status

    def _count_staged_files(self) -> int:
        return sum(1 for path in self._staging_root.rglob("*") if path.is_file())

    def self_test(self) -> StoreStatus:
        staging_ok = self._staging_root.exists() and self._staging_root.is_dir()
        archive_parent_ok = self._archive_path.parent.exists() and self._archive_path.parent.is_dir()
        read_ok = bool(staging_ok)
        write_ok = bool(staging_ok and archive_parent_ok)
        build_ok = shutil.which(self._mksquashfs_exe) is not None
        try:
            free_space = int(get_free_bytes(str(self._staging_root)))
        except Exception:
            free_space = None
        check = StoreCheckStatus(
            store_marker_file=staging_ok,
            read=read_ok,
            write=write_ok,
            sundry=build_ok,
        )
        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=str(self._archive_path),
            file_count=self._count_staged_files() if staging_ok else None,
            store_free_space=free_space,
            check_status=check,
            checked=bool(staging_ok and archive_parent_ok),
            good=bool(staging_ok and archive_parent_ok),
            event_log=self._event_log,
            details={
                "mode": "staging_then_seal",
                "plugin_layer": "raw_storage",
                "container": "squashfs_builder",
                "staging_root": str(self._staging_root),
                "output_archive": str(self._archive_path),
                "compression": self._compression,
                "deterministic": self._deterministic,
                "build_tool_available": build_ok,
            },
        )
        self._cached_status = status
        return status

    def location(self, *tokens: str) -> SquashfsBuildStoreLocation:
        return self.location_cls(*tokens, store=self)

    def _normalize_internal_path(self, file_identifier: str | None) -> str | None:
        if file_identifier is None:
            return None
        text = str(file_identifier).strip()
        prefix = self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            text = text[len(prefix):]
        text = text.replace("\\", "/")
        text = text.lstrip("/")
        if not text:
            return None
        parts: list[str] = []
        for part in text.split("/"):
            if part in {"", "."}:
                continue
            if part == "..":
                raise ValueError("Malformed staged archive path with '..': {!r}".format(file_identifier))
            parts.append(part)
        if not parts:
            return None
        return "/".join(parts)

    def _location_from_identifier(self, file_identifier: str | StoreLocationMixinAPI) -> SquashfsBuildStoreLocation:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier
            file_identifier = file_identifier.file_url
        internal = self._normalize_internal_path(str(file_identifier))
        if internal is None:
            return self.location()
        return self.location(*internal.split("/"))

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> SquashfsBuildStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            return self._location_from_identifier(file_identifier)._loc_path.is_file()
        except ValueError:
            return False

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.is_file():
            return None
        return int(path.stat().st_size)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        path = self._location_from_identifier(file_identifier)._loc_path
        if not path.exists():
            raise FileNotFoundError(str(path))

        def _exists(url: str) -> bool:
            return self.exists(url)

        def _size(url: str) -> int:
            size = self.file_size(url)
            return int(size or 0)

        def _hash(url: str) -> str:
            loc = self._location_from_identifier(url)
            if not loc._loc_path.is_file():
                return ""
            return get_file_hash(str(loc._loc_path))

        return SingleFileStatus(
            url=self._location_from_identifier(file_identifier).file_url,
            check_exists_function=_exists,
            check_size_function=_size,
            check_hash_function=_hash,
        )

    def iter_locations(self) -> Iterator[SquashfsBuildStoreLocation]:
        for path in self._staging_root.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self._staging_root)
            yield self.location(*rel.parts)

    def _implicit_write_target_path(self, file_bytes: bytes) -> pathlib.Path:
        digest = hashlib.sha256(file_bytes).hexdigest()
        bucket = digest[: self.AUTO_WRITE_BUCKET_LENGTH]
        return self._staging_root / self.DEFAULT_OBJECTS_DIRNAME / bucket / digest

    def _existing_path_matches_payload(self, target: pathlib.Path, file_bytes: bytes) -> bool:
        if not target.exists() or not target.is_file():
            return False
        try:
            return target.read_bytes() == file_bytes
        except Exception:
            return False

    def _raise_implicit_overwrite_error(self, target: pathlib.Path, file_bytes: bytes) -> None:
        if not target.exists():
            return
        if not target.is_file():
            raise SquashfsBuildImplicitOverwriteError(
                "Implicit SquashFS build write would collide with a non-file path at {!r}.".format(str(target))
            )
        try:
            existing = target.read_bytes()
        except Exception as exc:
            raise SquashfsBuildImplicitOverwriteError(
                "Implicit SquashFS build write could not verify existing staged target {!r}.".format(str(target))
            ) from exc
        if existing != file_bytes:
            raise SquashfsBuildImplicitOverwriteError(
                "Implicit SquashFS build write would overwrite existing bytes at {!r}. Use an explicit archive path if you really mean to replace that staged file.".format(
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
        target.write_bytes(file_bytes)
        return target

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> SquashfsBuildStoreLocation:
        if location is None:
            target = self._implicit_write_target_path(file_bytes)
            self._write_implicit_bytes_to_path(target, file_bytes)
        else:
            target = self._location_from_identifier(str(location))._loc_path
            self._write_bytes_to_path(target, file_bytes, ensure_parents=True)
        rel = target.relative_to(self._staging_root)
        return self.location(*rel.parts)

    def designate_file(
        self,
        source_path: str | pathlib.Path,
        *,
        archive_path: str | None = None,
    ) -> SquashfsBuildStoreLocation:
        source = pathlib.Path(source_path).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(str(source))
        payload = source.read_bytes()
        return self.write_bytes(payload, location=archive_path)

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

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> SquashfsBuildStoreLocation:
        src = self._location_from_identifier(src_location)._loc_path
        if not src.is_file():
            raise FileNotFoundError(str(src))
        dst = self._location_from_identifier(dst_location)._loc_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        rel = dst.relative_to(self._staging_root)
        return self.location(*rel.parts)

    def _run_mksquashfs(self, *, force: bool, quiet: bool) -> None:
        mksquashfs = shutil.which(self._mksquashfs_exe) or self._mksquashfs_exe
        if self._archive_path.exists():
            if not force:
                raise FileExistsError(
                    "Output archive already exists (use force=True to overwrite): {!r}".format(str(self._archive_path))
                )
            self._archive_path.unlink()
        cmd = [
            mksquashfs,
            str(self._staging_root),
            str(self._archive_path),
            "-noappend",
            "-comp",
            self._compression,
        ]
        if self._deterministic:
            cmd.extend(["-all-root", "-no-xattrs", "-all-time", "0", "-mkfs-time", "0"])
        if quiet:
            cmd.append("-quiet")
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if proc.returncode != 0:
            raise RuntimeError(
                "mksquashfs failed (rc={}): {}".format(
                    proc.returncode,
                    proc.stderr.decode("utf-8", "replace").strip(),
                )
            )

    def seal(self, *, force: bool = False, quiet: bool = True) -> SquashfsReadOnlyStorageBackend:
        if self._count_staged_files() <= 0:
            raise ValueError("Cannot build a SquashFS archive from an empty staging area.")
        self._archive_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_mksquashfs(force=force, quiet=quiet)
        self._built_store = SquashfsReadOnlyStorageBackend(url=str(self._archive_path))
        return self._built_store
