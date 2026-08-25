"""Transactional staging Store that seals snapshots into SquashFS archives."""

from __future__ import annotations

import dataclasses
import hashlib
import math
import os
import pathlib
import shutil
import stat as stat_module
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
    StorageCharacteristics,
    StorageLimitation,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageWriteUsage,
    StoreConfiguration,
    StoreAlreadyExists,
    StoreIntegrityError,
    StorePreconditionFailed,
    StoreStatus,
    StoreUnavailable,
    StoreUnsupportedOperation,
    StorageTimeout,
    WriteMode,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
    archive_file_signature,
    canonical_archive_key,
)
from LiuXin_alpha.storage.drivers.squashfs import (
    DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO,
    DEFAULT_MAX_SQUASHFS_HEADER_BYTES,
    DEFAULT_MAX_SQUASHFS_MEMBER_BYTES,
    DEFAULT_MAX_SQUASHFS_PATH_BYTES,
    DEFAULT_MAX_SQUASHFS_STDERR_BYTES,
    DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES,
)
from LiuXin_alpha.storage.errors import SquashfsBuildImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


@dataclasses.dataclass(slots=True, frozen=True)
class _StagedMember:
    """Immutable evidence used to validate a sealed archive candidate."""

    size: int
    sha256: str


class _TrackedWriteSession:
    """Release a builder's active-write lease exactly once."""

    def __init__(self, session, release, *, max_size: int) -> None:
        self._session = session
        self._release = release
        self._max_size = max_size
        self._size = 0
        self._released = False

    def write(self, data: bytes) -> int:
        if self._size + len(data) > self._max_size:
            raise StoreUnsupportedOperation(
                f"SquashFS staged members are limited to {self._max_size} bytes."
            )
        written = self._session.write(data)
        self._size += written
        return written

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
        unsquashfs_exe: str = "unsquashfs",
        command_timeout_s: float = 300.0,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_SQUASHFS_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO,
        max_header_bytes: int = DEFAULT_MAX_SQUASHFS_HEADER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_path_bytes: int = DEFAULT_MAX_SQUASHFS_PATH_BYTES,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        self._archive_path = pathlib.Path(url).expanduser().resolve(strict=False)
        self._archive_path.parent.mkdir(parents=True, exist_ok=True)
        self._mksquashfs_exe = str(mksquashfs_exe)
        self._compression = str(compression)
        self._deterministic = bool(deterministic)
        for label, value in (
            ("command_timeout_s", command_timeout_s),
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_header_bytes", max_header_bytes),
            ("max_depth", max_depth),
            ("max_path_bytes", max_path_bytes),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive.")
        if not math.isfinite(max_compression_ratio) or max_compression_ratio < 1:
            raise ValueError("max_compression_ratio must be finite and at least 1.")
        self._unsquashfs_exe = str(unsquashfs_exe)
        self._command_timeout_s = float(command_timeout_s)
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_member_bytes = int(max_member_bytes)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        self._max_compression_ratio = float(max_compression_ratio)
        self._max_header_bytes = int(max_header_bytes)
        self._max_depth = int(max_depth)
        self._max_path_bytes = int(max_path_bytes)
        self._tempdir: tempfile.TemporaryDirectory[str] | None = None
        if staging_root is None:
            self._tempdir = tempfile.TemporaryDirectory(
                prefix="liuxin-squashfs-build-"
            )
            stage = pathlib.Path(self._tempdir.name).resolve()
        else:
            stage = pathlib.Path(staging_root).expanduser().resolve(strict=False)
            stage.mkdir(parents=True, exist_ok=True)
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else uuid4() if uuid is None else uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        if configuration is not None and uuid is not None and UUID(str(uuid)) != store_uuid:
            raise ValueError("configuration and explicit uuid identify different Stores.")
        options: list[tuple[str, object]] = [
            ("mksquashfs_exe", self._mksquashfs_exe),
            ("compression", self._compression),
            ("deterministic", self._deterministic),
            ("unsquashfs_exe", self._unsquashfs_exe),
            ("command_timeout_s", self._command_timeout_s),
            ("max_inventory_entries", self._max_inventory_entries),
            ("max_member_bytes", self._max_member_bytes),
            (
                "max_total_uncompressed_bytes",
                self._max_total_uncompressed_bytes,
            ),
            ("max_compression_ratio", self._max_compression_ratio),
            ("max_header_bytes", self._max_header_bytes),
            ("max_depth", self._max_depth),
            ("max_path_bytes", self._max_path_bytes),
        ]
        if staging_root is not None:
            options.append(("staging_root", str(stage)))
        effective_configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or self.url_to_name(str(self._archive_path)),
            store_kind=self.store_kind,
            store_root_uri=self._archive_path.as_uri(),
            store_url=self._archive_path.as_uri(),
            store_access_protocol="squashfs-build",
            read_only=False,
            supports_folders=True,
            backend_options=tuple(options),
        )
        super().__init__(
            stage,
            configuration=effective_configuration,
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

    @property
    def characteristics(self) -> StorageCharacteristics:
        """Describe mutable staging followed by explicit archive sealing.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.STAGING_THEN_SEAL: 'staging_then_seal'>

        :return: Buildable SquashFS lifecycle characteristics.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.STAGING_THEN_SEAL,
            temporary_space=StorageTemporarySpaceRequirement.STORE_COPY,
            recommended_write_usage=StorageWriteUsage.ARCHIVAL_SNAPSHOT,
            max_object_bytes=self._effective_member_limit,
            max_component_bytes=self._max_path_bytes,
            max_path_depth=self._max_depth,
            preserves_unmodelled_entries=True,
            rewrites_container_format=True,
            limitations=(
                StorageLimitation(
                    "explicit_seal_required",
                    "Staged objects enter the SquashFS archive only after seal().",
                ),
                StorageLimitation(
                    "sealed_store_read_only",
                    "A successfully sealed staging Store refuses further mutation.",
                ),
                StorageLimitation(
                    "external_mksquashfs_required",
                    "Sealing requires a compatible mksquashfs executable.",
                ),
                StorageLimitation(
                    "validated_bounded_seal",
                    "Sealing preflights the staging tree and verifies the candidate inventory and bytes within configured expansion limits before publication.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
            ),
        )

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
        if expected_size is not None and expected_size > self._effective_member_limit:
            raise StoreUnsupportedOperation(
                f"SquashFS staged members are limited to {self._effective_member_limit} bytes."
            )
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
        return _TrackedWriteSession(
            session,
            self._release_mutation,
            max_size=self._effective_member_limit,
        )

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
            if self._built_store is not None:
                raise StorePreconditionFailed(
                    "SquashFS staging has already been sealed."
                )
            if self._sealing:
                raise StorePreconditionFailed("SquashFS sealing is already in progress.")
            if self._active_mutations:
                raise StorePreconditionFailed(
                    "cannot seal while staged mutations are active."
                )
            self._sealing = True
        temporary: pathlib.Path | None = None
        try:
            manifest = self._staging_manifest()
            if not manifest:
                raise ValueError(
                    "Cannot build a SquashFS archive from an empty staging area."
                )
            if self._archive_path.exists() and not force:
                raise FileExistsError(
                    f"Output archive already exists: {self._archive_path}"
                )
            temporary = self._temporary_archive_path()
            self._run_mksquashfs(temporary, quiet=quiet)
            try:
                candidate_stat = temporary.lstat()
            except OSError as error:
                raise StoreIntegrityError(
                    "mksquashfs reported success without producing an archive."
                ) from error
            if (
                not stat_module.S_ISREG(candidate_stat.st_mode)
                or candidate_stat.st_nlink != 1
            ):
                raise StoreIntegrityError(
                    "mksquashfs output is not a private regular archive file."
                )
            self._validate_candidate(temporary, manifest)
            if archive_file_signature(temporary.lstat()) != archive_file_signature(
                candidate_stat
            ):
                raise StorePreconditionFailed(
                    "SquashFS candidate changed after validation."
                )
            if not temporary.is_file():
                raise StoreIntegrityError(
                    "mksquashfs reported success without producing an archive."
                )
            self._publish_archive(temporary, force=force)
            temporary = None
            built = SquashfsReadOnlyStorageBackend(
                url=str(self._archive_path),
                name=f"{self.configuration.store_name} (sealed)",
                unsquashfs_exe=self._unsquashfs_exe,
                timeout_s=self._command_timeout_s,
                max_inventory_entries=self._max_inventory_entries,
                max_member_bytes=self._max_member_bytes,
                max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_header_bytes=self._max_header_bytes,
                max_depth=self._max_depth,
                max_path_bytes=self._max_path_bytes,
            )
            built.startup()
            self._built_store = built
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
        stderr_buffer = bytearray()
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise StoreUnavailable(
                f"Could not start mksquashfs for {self._archive_path}: {error}."
            ) from error
        if process.stderr is None:
            process.kill()
            raise StoreUnavailable("mksquashfs did not provide a diagnostics pipe.")
        process_stderr = process.stderr

        def drain_stderr() -> None:
            while chunk := process_stderr.read(64 * 1024):
                remaining = DEFAULT_MAX_SQUASHFS_STDERR_BYTES - len(stderr_buffer)
                if remaining > 0:
                    stderr_buffer.extend(chunk[:remaining])

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()
        try:
            try:
                return_code = process.wait(timeout=self._command_timeout_s)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait()
                raise StorageTimeout(
                    f"mksquashfs exceeded {self._command_timeout_s:g} seconds."
                ) from error
        finally:
            stderr_thread.join(timeout=2)
            process_stderr.close()
        if stderr_thread.is_alive():
            raise StoreUnavailable("mksquashfs diagnostics pipe did not close.")
        if return_code:
            detail = bytes(stderr_buffer).decode("utf-8", "replace").strip()
            raise StoreUnavailable(
                f"mksquashfs failed (rc={return_code}): "
                f"{detail or 'no diagnostic was produced'}"
            )

    def _staging_manifest(self) -> dict[str, _StagedMember]:
        """Preflight every staged entry and hash every regular file."""

        manifest: dict[str, _StagedMember] = {}
        entry_count = 0
        total_bytes = 0
        pending = [self.staging_root]
        while pending:
            directory = pending.pop()
            try:
                entries = tuple(os.scandir(directory))
            except OSError as error:
                raise StoreUnavailable(
                    f"Could not inventory SquashFS staging directory {directory}: {error}."
                ) from error
            for entry in entries:
                entry_count += 1
                if entry_count > self._max_inventory_entries:
                    raise StoreUnsupportedOperation(
                        "SquashFS staging inventory exceeds "
                        f"{self._max_inventory_entries} entries."
                    )
                path = pathlib.Path(entry.path)
                if entry.is_symlink():
                    raise StoreUnsupportedOperation(
                        f"SquashFS staging contains symbolic link {path}."
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise StoreUnsupportedOperation(
                        f"SquashFS staging contains non-regular entry {path}."
                    )
                key = path.relative_to(self.staging_root).as_posix()
                canonical = canonical_archive_key(
                    key,
                    format_name="SquashFS",
                    max_depth=self._max_depth,
                    max_path_bytes=self._max_path_bytes,
                )
                if canonical != key:
                    raise StoreUnsupportedOperation(
                        f"SquashFS staging path {key!r} is not canonical."
                    )
                before = entry.stat(follow_symlinks=False)
                size = before.st_size
                if size < 0 or size > self._effective_member_limit:
                    raise StoreUnsupportedOperation(
                        f"SquashFS staged member {key!r} exceeds "
                        f"{self._effective_member_limit} bytes."
                    )
                total_bytes += size
                if total_bytes > self._max_total_uncompressed_bytes:
                    raise StoreUnsupportedOperation(
                        "SquashFS staged total size exceeds "
                        f"{self._max_total_uncompressed_bytes} bytes."
                    )
                digest = _file_digest(path).value
                after = path.stat(follow_symlinks=False)
                if archive_file_signature(before) != archive_file_signature(after):
                    raise StorePreconditionFailed(
                        f"SquashFS staged member {key!r} changed while hashing."
                    )
                manifest[key] = _StagedMember(size=size, sha256=digest)
        return manifest

    def _validate_candidate(
        self,
        candidate: pathlib.Path,
        manifest: dict[str, _StagedMember],
    ) -> None:
        """Verify candidate names, sizes, safety limits, and complete bytes."""

        validator = SquashfsReadOnlyStorageBackend(
            str(candidate),
            unsquashfs_exe=self._unsquashfs_exe,
            timeout_s=self._command_timeout_s,
            max_inventory_entries=self._max_inventory_entries,
            max_member_bytes=self._max_member_bytes,
            max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
            max_compression_ratio=self._max_compression_ratio,
            max_header_bytes=self._max_header_bytes,
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        try:
            status = validator.startup()
            if not status.available:
                raise StoreIntegrityError(
                    f"SquashFS candidate validation failed: {status.message}"
                )
            inventory = {
                location.key: validator.stat_file(location).size
                for location in validator.iter_locations()
            }
            expected_sizes = {key: item.size for key, item in manifest.items()}
            if inventory != expected_sizes:
                raise StoreIntegrityError(
                    "SquashFS candidate inventory differs from the staged manifest."
                )
            for key, expected in manifest.items():
                digest = validator.compute_digest(validator.locate(key), "sha256")
                if digest.value != expected.sha256:
                    raise StoreIntegrityError(
                        f"SquashFS candidate member {key!r} differs from staging."
                    )
        finally:
            validator.close()

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
            if self._built_store is not None:
                raise StorePreconditionFailed(
                    "SquashFS staging has already been sealed."
                )
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
