"""Transactional staging Store that seals snapshots into SquashFS archives."""

from __future__ import annotations

import dataclasses
import hashlib
import os
import pathlib
import shutil
import subprocess
import tempfile
import threading

from contextlib import contextmanager
from types import TracebackType
from typing import BinaryIO, Optional
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    FileInfo,
    Location,
    StoreConfiguration,
    StoreAlreadyExists,
    StoreIntegrityError,
    StorePreconditionFailed,
    StoreStatus,
    StoreUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.errors import SquashfsBuildImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class _TrackedWriteSession:
    """Release a builder's active-write lease exactly once."""

    def __init__(self, session, release) -> None:
        self._session = session
        self._release = release
        self._released = False

    def write(self, data: bytes) -> int:
        return self._session.write(data)

    def commit(self) -> FileInfo:
        try:
            return self._session.commit()
        finally:
            self._finish()

    def abort(self) -> None:
        try:
            self._session.abort()
        finally:
            self._finish()

    def __enter__(self):
        self._session.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self._session.__exit__(exc_type, exc, traceback)
        finally:
            self._finish()

    def _finish(self) -> None:
        if self._released:
            return
        self._released = True
        self._release()


class SquashfsBuildStorageBackend(FilesystemStore):
    """Collect committed staged objects, then atomically publish one archive."""

    store_kind = "squashfs_build"
    DEFAULT_OBJECTS_DIRNAME = "objects"
    AUTO_WRITE_BUCKET_LENGTH = 5

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        *,
        mksquashfs_exe: str = "mksquashfs",
        compression: str = "zstd",
        deterministic: bool = False,
        staging_root: str | None = None,
    ) -> None:
        self._archive_path = pathlib.Path(url).expanduser().resolve(strict=False)
        self._archive_path.parent.mkdir(parents=True, exist_ok=True)
        self._mksquashfs_exe = str(mksquashfs_exe)
        self._compression = str(compression)
        self._deterministic = bool(deterministic)
        self._tempdir: tempfile.TemporaryDirectory[str] | None = None
        if staging_root is None:
            self._tempdir = tempfile.TemporaryDirectory(
                prefix="liuxin-squashfs-build-"
            )
            stage = pathlib.Path(self._tempdir.name).resolve()
        else:
            stage = pathlib.Path(staging_root).expanduser().resolve(strict=False)
            stage.mkdir(parents=True, exist_ok=True)
        store_uuid = uuid4() if uuid is None else (
            uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        configuration = StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(str(self._archive_path)),
            store_kind=self.store_kind,
            store_root_uri=self._archive_path.as_uri(),
            store_url=self._archive_path.as_uri(),
            store_access_protocol="squashfs-build",
            read_only=False,
            supports_folders=True,
        )
        super().__init__(
            stage,
            configuration=configuration,
            allocation_prefix=self.DEFAULT_OBJECTS_DIRNAME,
        )
        self._built_store: SquashfsReadOnlyStorageBackend | None = None
        self._state_condition = threading.Condition(threading.RLock())
        self._active_mutations = 0
        self._sealing = False

    @property
    def archive_path(self) -> pathlib.Path:
        return self._archive_path

    @property
    def staging_root(self) -> pathlib.Path:
        return super().root_path

    @property
    def root_path(self) -> pathlib.Path:
        return self.staging_root

    @property
    def built_store(self) -> SquashfsReadOnlyStorageBackend | None:
        return self._built_store

    @staticmethod
    def url_to_name(url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        return self._decorate_status(super().startup())

    def probe(self) -> StoreStatus:
        return self._decorate_status(super().probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        return self._decorate_status(super().status(refresh=refresh))

    def self_test(self) -> StoreStatus:
        return self.probe()

    def _decorate_status(self, status: StoreStatus) -> StoreStatus:
        details = dict(status.details)
        details.update(
            {
                "mode": "staging_then_seal",
                "staging_root": str(self.staging_root),
                "output_archive": str(self._archive_path),
                "compression": self._compression,
                "deterministic": str(self._deterministic).lower(),
                "build_tool_available": str(
                    shutil.which(self._mksquashfs_exe) is not None
                ).lower(),
            }
        )
        return dataclasses.replace(
            status,
            details=tuple(sorted(details.items())),
        )

    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints=None,
    ):
        self._acquire_mutation()
        try:
            session = super().begin_write(
                location,
                mode=mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
                placement_hints=placement_hints,
            )
        except BaseException:
            self._release_mutation()
            raise
        return _TrackedWriteSession(session, self._release_mutation)

    def store_bytes(
        self,
        data: bytes,
        *,
        location: str | Location | None = None,
        name: str | None = None,
        metadata=None,
        write_mode: WriteMode | str | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        digest = expected_digest or Digest("sha256", hashlib.sha256(data).hexdigest())
        destination = location
        implicit = destination is None
        if implicit:
            destination = self._content_address(digest)
            existing = self.try_stat(self.locate(destination))
            if existing is not None:
                observed = self.compute_digest(existing.location, digest.algorithm)
                if observed == digest:
                    return existing
                raise SquashfsBuildImplicitOverwriteError(
                    "Implicit SquashFS staging target contains incompatible bytes."
                )
        try:
            return super().store_bytes(
                data,
                location=destination,
                name=name,
                metadata=metadata,
                write_mode=write_mode,
                expected_digest=digest,
                mode=mode,
            )
        except (StoreAlreadyExists, StoreIntegrityError) as error:
            if implicit:
                raise SquashfsBuildImplicitOverwriteError(str(error)) from error
            raise

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        location: str | Location | None = None,
        name: str | None = None,
        metadata=None,
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        source = pathlib.Path(path)
        digest = expected_digest or _file_digest(source)
        destination = location or self._content_address(digest)
        if location is None:
            existing = self.try_stat(self.locate(destination))
            if existing is not None:
                if self.compute_digest(existing.location, digest.algorithm) == digest:
                    return existing
                raise SquashfsBuildImplicitOverwriteError(
                    "Implicit SquashFS staging target contains incompatible bytes."
                )
        return super().store_file(
            source,
            location=destination,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_size=expected_size,
            expected_digest=digest,
            mode=mode,
        )

    def store_stream(
        self,
        source: BinaryIO,
        *,
        location: str | Location | None = None,
        expected_digest: Digest | None = None,
        **kwargs,
    ) -> FileInfo:
        if location is None and expected_digest is None:
            raise StoreUnsupportedOperation(
                "implicit SquashFS streaming writes require an expected digest."
            )
        destination = location or self._content_address(expected_digest)
        return super().store_stream(
            source,
            location=destination,
            expected_digest=expected_digest,
            **kwargs,
        )

    def designate_file(
        self,
        source_path: str | pathlib.Path,
        *,
        archive_path: str | None = None,
    ) -> FileInfo:
        """Copy a source snapshot into staged, commit-protected storage."""

        return self.store_file(source_path, location=archive_path)

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        with self._mutation_operation():
            super().delete(
                location,
                missing_ok=missing_ok,
                if_version=if_version,
            )

    def copy(self, source: Location, destination: Location, *, mode=WriteMode.CREATE_ONLY):
        with self._mutation_operation():
            return super().copy(source, destination, mode=mode)

    def move(self, source: Location, destination: Location, *, mode=WriteMode.CREATE_ONLY):
        with self._mutation_operation():
            return super().move(source, destination, mode=mode)

    def seal(
        self,
        *,
        force: bool = False,
        quiet: bool = True,
    ) -> SquashfsReadOnlyStorageBackend:
        """Build and atomically publish a complete archive snapshot.

        Existing archives remain untouched if the build fails. Sealing refuses
        to race an active staged write and blocks new mutations until the
        archive has either published or failed.
        """

        with self._state_condition:
            if self._sealing:
                raise StorePreconditionFailed("SquashFS sealing is already in progress.")
            if self._active_mutations:
                raise StorePreconditionFailed(
                    "cannot seal while staged mutations are active."
                )
            self._sealing = True
        temporary: pathlib.Path | None = None
        try:
            if not any(path.is_file() for path in self.staging_root.rglob("*")):
                raise ValueError(
                    "Cannot build a SquashFS archive from an empty staging area."
                )
            if self._archive_path.exists() and not force:
                raise FileExistsError(
                    f"Output archive already exists: {self._archive_path}"
                )
            temporary = self._temporary_archive_path()
            self._run_mksquashfs(temporary, quiet=quiet)
            if not temporary.is_file():
                raise StorageIntegrityError(
                    "mksquashfs reported success without producing an archive."
                )
            self._publish_archive(temporary, force=force)
            temporary = None
            self._built_store = SquashfsReadOnlyStorageBackend(
                url=str(self._archive_path),
                name=f"{self.configuration.store_name} (sealed)",
            )
            return self._built_store
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            with self._state_condition:
                self._sealing = False
                self._state_condition.notify_all()

    def _run_mksquashfs(
        self,
        output: pathlib.Path,
        *,
        quiet: bool,
    ) -> None:
        executable = shutil.which(self._mksquashfs_exe) or self._mksquashfs_exe
        command = [
            executable,
            str(self.staging_root),
            str(output),
            "-noappend",
            "-comp",
            self._compression,
        ]
        if self._deterministic:
            command.extend(
                ["-all-root", "-no-xattrs", "-all-time", "0", "-mkfs-time", "0"]
            )
        if quiet:
            command.append("-quiet")
        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if process.returncode:
            raise RuntimeError(
                "mksquashfs failed (rc={}): {}".format(
                    process.returncode,
                    process.stderr.decode("utf-8", "replace").strip(),
                )
            )

    def _temporary_archive_path(self) -> pathlib.Path:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self._archive_path.name}.",
            suffix=".part",
            dir=self._archive_path.parent,
        )
        os.close(descriptor)
        path = pathlib.Path(name)
        path.unlink()
        return path

    def _publish_archive(self, temporary: pathlib.Path, *, force: bool) -> None:
        if force:
            os.replace(temporary, self._archive_path)
        else:
            try:
                os.link(temporary, self._archive_path)
            except FileExistsError as error:
                raise FileExistsError(
                    f"Output archive appeared during build: {self._archive_path}"
                ) from error
            temporary.unlink()
        with self._archive_path.open("rb") as archive:
            os.fsync(archive.fileno())
        if hasattr(os, "O_DIRECTORY"):
            descriptor = os.open(
                self._archive_path.parent,
                os.O_RDONLY | os.O_DIRECTORY,
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _content_address(self, digest: Digest | None) -> str:
        if digest is None:
            raise StoreUnsupportedOperation(
                "content-addressed staging requires an expected digest."
            )
        return "/".join(
            (
                self.DEFAULT_OBJECTS_DIRNAME,
                digest.value[: self.AUTO_WRITE_BUCKET_LENGTH],
                digest.value,
            )
        )

    def _acquire_mutation(self) -> None:
        with self._state_condition:
            if self._sealing:
                raise StorePreconditionFailed(
                    "SquashFS staging is sealed for snapshot publication."
                )
            self._active_mutations += 1

    def _release_mutation(self) -> None:
        with self._state_condition:
            self._active_mutations -= 1
            self._state_condition.notify_all()

    @contextmanager
    def _mutation_operation(self):
        self._acquire_mutation()
        try:
            yield
        finally:
            self._release_mutation()

    def close(self) -> None:
        super().close()
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None


def _file_digest(path: pathlib.Path) -> Digest:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return Digest("sha256", hasher.hexdigest())


__all__ = ["SquashfsBuildStorageBackend"]
