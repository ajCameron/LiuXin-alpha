"""
Transactional staging Store that seals one immutable RAR 4 archive.
"""

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
import zlib

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from types import TracebackType
from typing import BinaryIO
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    FileInfo,
    Location,
    StorageCharacteristics,
    StorageLimitation,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageWriteUsage,
    StoreAlreadyExists,
    StoreConfiguration,
    StoreIntegrityError,
    StorePreconditionFailed,
    StoreStatus,
    StoreUnavailable,
    StoreUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
    canonical_archive_key,
    fsync_directory,
)
from LiuXin_alpha.storage.drivers.rar import (
    DEFAULT_MAX_RAR_COMPRESSION_RATIO,
    DEFAULT_MAX_RAR_MEMBER_BYTES,
    DEFAULT_MAX_RAR_PATH_BYTES,
    DEFAULT_MAX_RAR_STDERR_BYTES,
    DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES,
    RarStorageDriver,
)
from LiuXin_alpha.storage.errors import RarBuildImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.rar_readonly import (
    RarReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


DEFAULT_RAR_BUILD_TIMEOUT_S = 3600.0
DEFAULT_RAR_COMPRESSION_LEVEL = 3
_COPY_CHUNK_SIZE = 1024 * 1024


@dataclasses.dataclass(slots=True, frozen=True)
class _StagedRarMember:
    """
    Record the size and CRC-32 expected in one sealed member.

    Example:
        >>> _StagedRarMember(4, 0x12345678).size
        4
    """

    size: int
    crc32: int


class _TrackedRarBuildWriteSession:
    """
    Release a RAR builder mutation lease exactly once.

    Example:
        >>> tracked = _TrackedRarBuildWriteSession(session, release)  # doctest: +SKIP
    """

    def __init__(self, session, release, *, max_size: int) -> None:
        """
        Bind one filesystem write session to its builder lease.

        Example:
            >>> tracked = _TrackedRarBuildWriteSession(session, release)  # doctest: +SKIP


        :param session:
        :param release:
        :return:
        """

        self._session = session
        self._release = release
        self._max_size = max_size
        self._size = 0
        self._released = False

    def write(self, data: bytes) -> int:
        """
        Forward bytes to the underlying private filesystem stage.

        Example:
            >>> tracked.write(b"book")  # doctest: +SKIP
            4


        :param data:
        :return:
        """

        if self._size + len(data) > self._max_size:
            raise StoreUnsupportedOperation(
                f"RAR staged members are limited to {self._max_size} bytes."
            )
        written = self._session.write(data)
        self._size += written
        return written

    def commit(self) -> FileInfo:
        """
        Commit the staged file and release its active mutation lease.

        Example:
            >>> info = tracked.commit()  # doctest: +SKIP


        :return:
        """

        try:
            return self._session.commit()
        finally:
            self._finish()

    def abort(self) -> None:
        """
        Abort the staged file and release its active mutation lease.

        Example:
            >>> tracked.abort()  # doctest: +SKIP


        :return:
        """

        try:
            self._session.abort()
        finally:
            self._finish()

    def __enter__(self):
        """
        Enter the underlying staged-write context.

        Example:
            >>> tracked.__enter__() is tracked  # doctest: +SKIP
            True


        :return:
        """

        self._session.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Exit the underlying session and release the builder lease.

        Example:
            >>> tracked.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        try:
            self._session.__exit__(exc_type, exc, traceback)
        finally:
            self._finish()

    def _finish(self) -> None:
        """
        Invoke the lease callback at most once.

        Example:
            >>> tracked._finish()  # doctest: +SKIP


        :return:
        """

        if self._released:
            return
        self._released = True
        self._release()


class RarBuildStorageBackend(FilesystemStore):
    """
    Collect committed files, then seal exactly one immutable RAR 4 archive.

    The output path is create-only. A successfully sealed builder permanently
    rejects mutation and returns a separate read-only RAR Store facade.

    Example:
        >>> store = RarBuildStorageBackend("backup.rar")  # doctest: +SKIP
    """

    store_kind = "rar_build"
    DEFAULT_OBJECTS_DIRNAME = "objects"
    AUTO_WRITE_BUCKET_LENGTH = 5

    def __init__(
        self,
        url: str,
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        configuration: StoreConfiguration | None = None,
        rar_exe: str = "rar",
        compression_level: int = DEFAULT_RAR_COMPRESSION_LEVEL,
        command_timeout_s: float = DEFAULT_RAR_BUILD_TIMEOUT_S,
        staging_root: str | None = None,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_RAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_RAR_COMPRESSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_RAR_PATH_BYTES,
    ) -> None:
        """
        Configure durable staging and a licensed external RAR creator.

        Example:
            >>> store = RarBuildStorageBackend("backup.rar", compression_level=0)  # doctest: +SKIP


        :param url:
        :param name:
        :param uuid:
        :param configuration:
        :param rar_exe:
        :param compression_level:
        :param command_timeout_s:
        :param staging_root:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
        """

        archive_path = pathlib.Path(url).expanduser().resolve(strict=False)
        level = int(compression_level)
        if not 0 <= level <= 5:
            raise ValueError("RAR compression_level must be between 0 and 5.")
        if command_timeout_s <= 0:
            raise ValueError("RAR command_timeout_s must be positive.")
        for label, value in (
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_depth", max_depth),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_path_bytes", max_path_bytes),
        ):
            if value < 1:
                raise ValueError(f"{label} must be positive.")
        if not math.isfinite(max_compression_ratio) or max_compression_ratio < 1:
            raise ValueError("max_compression_ratio must be finite and at least 1.")
        try:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="RAR build",
                operation="create output directory",
                target=archive_path.parent,
            ) from error
        stage = (
            archive_path.parent / f".{archive_path.name}.staging"
            if staging_root is None
            else pathlib.Path(staging_root).expanduser().resolve(strict=False)
        ).resolve(strict=False)
        if archive_path == stage or archive_path.is_relative_to(stage):
            raise ValueError("RAR output must be outside its staging directory.")
        try:
            stage.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="RAR build",
                operation="create staging directory",
                target=stage,
            ) from error
        store_uuid = _store_uuid(uuid, configuration)
        options: tuple[tuple[str, object], ...] = (
            ("rar_exe", str(rar_exe)),
            ("compression_level", level),
            ("command_timeout_s", float(command_timeout_s)),
            ("staging_root", str(stage)),
            ("max_inventory_entries", int(max_inventory_entries)),
            ("max_member_bytes", int(max_member_bytes)),
            ("max_depth", int(max_depth)),
            ("max_total_uncompressed_bytes", int(max_total_uncompressed_bytes)),
            ("max_compression_ratio", float(max_compression_ratio)),
            ("max_path_bytes", int(max_path_bytes)),
        )
        configured = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or safe_path_to_name(str(archive_path)),
            store_kind=self.store_kind,
            store_root_uri=archive_path.as_uri(),
            store_url=archive_path.as_uri(),
            store_access_protocol="rar-build",
            read_only=False,
            supports_folders=True,
            backend_options=options,
        )
        self._archive_path = archive_path
        self._rar_exe = str(rar_exe)
        self._compression_level = level
        self._command_timeout_s = float(command_timeout_s)
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_member_bytes = int(max_member_bytes)
        self._max_depth = int(max_depth)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        self._max_compression_ratio = float(max_compression_ratio)
        self._max_path_bytes = int(max_path_bytes)
        super().__init__(
            stage,
            configuration=configured,
            allocation_prefix=self.DEFAULT_OBJECTS_DIRNAME,
        )
        self._built_store: RarReadOnlyStorageBackend | None = None
        self._state_condition = threading.Condition(threading.RLock())
        self._active_mutations = 0
        self._sealing = False
        self._sealed = archive_path.exists()

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the create-only RAR output path.

        Example:
            >>> store.archive_path  # doctest: +SKIP


        :return:
        """

        return self._archive_path

    @property
    def staging_root(self) -> pathlib.Path:
        """
        Return the durable filesystem staging directory.

        Example:
            >>> store.staging_root.is_dir()  # doctest: +SKIP
            True


        :return:
        """

        return super().root_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Return the staging root used by ordinary Store operations.

        Example:
            >>> store.root_path == store.staging_root  # doctest: +SKIP
            True


        :return:
        """

        return self.staging_root

    @property
    def built_store(self) -> RarReadOnlyStorageBackend | None:
        """
        Return the read-only Store created by this process after sealing.

        Example:
            >>> store.built_store is None  # doctest: +SKIP
            True


        :return:
        """

        return self._built_store

    @property
    def characteristics(self) -> StorageCharacteristics:
        """
        Advertise mutable staging followed by irreversible RAR sealing.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.STAGING_THEN_SEAL: 'staging_then_seal'>


        :return:
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
                    "Staged objects enter the RAR archive only after seal().",
                ),
                StorageLimitation(
                    "sealed_store_read_only",
                    "A successfully sealed RAR builder permanently refuses mutation.",
                ),
                StorageLimitation(
                    "create_only_archive_publication",
                    "Sealing never replaces an existing output archive.",
                ),
                StorageLimitation(
                    "external_rar_creator_required",
                    "Sealing requires an operator-supplied licensed rar executable.",
                ),
                StorageLimitation(
                    "rar4_non_solid_output",
                    "The builder emits reader-compatible, non-solid RAR 4 archives.",
                ),
                StorageLimitation(
                    "rar_creation_license_operator_managed",
                    "Installation and licensing of the proprietary RAR creator are operator responsibilities.",
                ),
                StorageLimitation(
                    "validated_bounded_seal",
                    "Sealing preflights every staged entry and validates candidate expansion limits before create-only publication.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
            ),
        )

    @staticmethod
    def url_to_name(url: str) -> str:
        """
        Derive a safe display name from a RAR output path.

        Example:
            >>> bool(RarBuildStorageBackend.url_to_name("/archives/books.rar"))
            True


        :param url:
        :return:
        """

        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        """
        Probe durable staging and decorate it with sealing state.

        Example:
            >>> store.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self._decorate_status(super().startup())

    def probe(self) -> StoreStatus:
        """
        Refresh staging capacity, tool availability, and sealing state.

        Example:
            >>> store.probe().details  # doctest: +SKIP


        :return:
        """

        return self._decorate_status(super().probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return the last status, optionally refreshing the staging probe.

        Example:
            >>> store.status(refresh=True).available  # doctest: +SKIP
            True


        :param refresh:
        :return:
        """

        return self._decorate_status(super().status(refresh=refresh))

    def self_test(self) -> StoreStatus:
        """
        Run the standard active Store probe.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def _decorate_status(self, status: StoreStatus) -> StoreStatus:
        """
        Add builder state, tool discovery, and output collision warnings.

        Example:
            >>> decorated = store._decorate_status(status)  # doctest: +SKIP


        :param status:
        :return:
        """

        executable = self._rar_executable()
        sealed = self._sealed or self._archive_path.exists()
        warnings = list(status.warnings)
        if executable is None:
            warnings.append(
                "RAR staging is usable, but sealing requires a configured rar executable."
            )
        if sealed:
            warnings.append(
                "The RAR output exists; this create-only builder is permanently locked."
            )
        details = dict(status.details)
        details.update(
            {
                "mode": "staging_then_seal",
                "staging_root": str(self.staging_root),
                "output_archive": str(self._archive_path),
                "rar_format": "4",
                "solid": "false",
                "compression_level": str(self._compression_level),
                "build_tool_available": str(executable is not None).lower(),
                "sealed": str(sealed).lower(),
            }
        )
        return dataclasses.replace(
            status,
            writable=status.writable and not sealed,
            message=(
                "RAR staging is sealed and read-only."
                if sealed
                else "RAR staging is available; call seal() to publish the archive."
            ),
            warnings=tuple(warnings),
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
        """
        Begin one tracked staging write while sealing is not active.

        Example:
            >>> session = store.begin_write(location)  # doctest: +SKIP


        :param location:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param placement_hints:
        :return:
        """

        if expected_size is not None and expected_size > self._effective_member_limit:
            raise StoreUnsupportedOperation(
                f"RAR staged members are limited to {self._effective_member_limit} bytes."
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
        return _TrackedRarBuildWriteSession(
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
        """
        Stage bytes, using a deduplicating content address when implicit.

        Example:
            >>> info = store.store_bytes(b"book")  # doctest: +SKIP


        :param data:
        :param location:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_digest:
        :param mode:
        :return:
        """

        self._require_unsealed()
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
                raise RarBuildImplicitOverwriteError(
                    "Implicit RAR staging target contains incompatible bytes."
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
                raise RarBuildImplicitOverwriteError(str(error)) from error
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
        """
        Copy one local file into commit-protected RAR staging.

        Example:
            >>> info = store.store_file(path, location="books/book.epub")  # doctest: +SKIP


        :param path:
        :param location:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_size:
        :param expected_digest:
        :param mode:
        :return:
        """

        self._require_unsealed()
        source = pathlib.Path(path)
        digest = expected_digest or _file_digest(source)
        destination = location or self._content_address(digest)
        if location is None:
            existing = self.try_stat(self.locate(destination))
            if existing is not None:
                if self.compute_digest(existing.location, digest.algorithm) == digest:
                    return existing
                raise RarBuildImplicitOverwriteError(
                    "Implicit RAR staging target contains incompatible bytes."
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
        """
        Stage a stream, requiring a digest when its address is implicit.

        Example:
            >>> info = store.store_stream(source, expected_digest=digest)  # doctest: +SKIP


        :param source:
        :param location:
        :param expected_digest:
        :param kwargs:
        :return:
        """

        self._require_unsealed()
        if location is None and expected_digest is None:
            raise StoreUnsupportedOperation(
                "implicit RAR staging streams require an expected digest."
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
        """
        Copy a source snapshot into staged, commit-protected storage.

        Example:
            >>> info = store.designate_file(path, archive_path="books/book.epub")  # doctest: +SKIP


        :param source_path:
        :param archive_path:
        :return:
        """

        return self.store_file(source_path, location=archive_path)

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a staged member only before sealing starts.

        Example:
            >>> store.delete(location)  # doctest: +SKIP


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
        """

        with self._mutation_operation():
            super().delete(
                location,
                missing_ok=missing_ok,
                if_version=if_version,
            )

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Copy a staged member only before sealing starts.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """

        with self._mutation_operation():
            return super().copy(source, destination, mode=mode)

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Move a staged member only before sealing starts.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """

        with self._mutation_operation():
            return super().move(source, destination, mode=mode)

    def seal(self, *, quiet: bool = True) -> RarReadOnlyStorageBackend:
        """
        Build, test, validate, and create-only publish one immutable RAR.

        Sealing refuses active writes and never replaces an output that already
        exists. Failed builds leave both staging and any existing output
        untouched.

        Example:
            >>> readonly = store.seal()  # doctest: +SKIP


        :param quiet:
        :return:
        """

        with self._state_condition:
            if self._sealed:
                raise StorePreconditionFailed("RAR staging is already sealed.")
            if self._archive_path.exists():
                self._sealed = True
                raise StoreAlreadyExists(
                    f"RAR output archive already exists: {self._archive_path}"
                )
            if self._sealing:
                raise StorePreconditionFailed("RAR sealing is already in progress.")
            if self._active_mutations:
                raise StorePreconditionFailed(
                    "cannot seal RAR staging while mutations are active."
                )
            self._sealing = True
        candidate: pathlib.Path | None = None
        try:
            manifest = self._staging_manifest()
            executable = self._require_rar_executable()
            candidate = self._temporary_archive_path()
            self._run_rar_create(executable, candidate, quiet=quiet)
            try:
                candidate_stat = candidate.lstat()
            except OSError as error:
                raise StoreIntegrityError(
                    "rar reported success without producing an archive."
                ) from error
            if (
                not stat_module.S_ISREG(candidate_stat.st_mode)
                or candidate_stat.st_nlink != 1
                or candidate_stat.st_size == 0
            ):
                raise StoreIntegrityError(
                    "rar output is not a private non-empty regular archive file."
                )
            if self._staging_manifest() != manifest:
                raise StorePreconditionFailed(
                    "RAR staging changed outside the Store while sealing."
                )
            self._run_rar_test(executable, candidate, quiet=quiet)
            self._validate_candidate(candidate, manifest, extractor_exe=executable)
            if _file_identity(candidate_stat) != _file_identity(candidate.lstat()):
                raise StorePreconditionFailed(
                    "RAR candidate changed after validation."
                )
            self._publish_archive(candidate)
            candidate = None
            self._sealed = True
            self._built_store = RarReadOnlyStorageBackend(
                str(self._archive_path),
                name=f"{self.configuration.store_name} (sealed)",
                extractor_exe=executable,
                extract_timeout_s=self._command_timeout_s,
                max_inventory_entries=self._max_inventory_entries,
                max_member_bytes=self._max_member_bytes,
                max_depth=self._max_depth,
                max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_path_bytes=self._max_path_bytes,
            )
            return self._built_store
        finally:
            if candidate is not None:
                try:
                    candidate.unlink(missing_ok=True)
                except OSError:
                    pass
            with self._state_condition:
                self._sealing = False
                self._state_condition.notify_all()

    def _rar_executable(self) -> str | None:
        """
        Resolve the configured RAR creator without downloading or bundling it.

        Example:
            >>> executable = store._rar_executable()  # doctest: +SKIP


        :return:
        """

        return shutil.which(self._rar_exe)

    def _require_rar_executable(self) -> str:
        """
        Return the creator executable or fail with an explicit limitation.

        Example:
            >>> executable = store._require_rar_executable()  # doctest: +SKIP


        :return:
        """

        executable = self._rar_executable()
        if executable is None:
            raise StoreUnsupportedOperation(
                "RAR sealing requires an operator-supplied licensed rar executable."
            )
        return executable

    def _staging_manifest(self) -> dict[str, _StagedRarMember]:
        """
        Scan regular staged files and calculate the candidate verification plan.

        Example:
            >>> manifest = store._staging_manifest()  # doctest: +SKIP


        :return:
        """

        manifest: dict[str, _StagedRarMember] = {}
        entry_count = 0
        total_bytes = 0
        pending = [self.staging_root]
        while pending:
            directory = pending.pop()
            try:
                entries = tuple(os.scandir(directory))
            except OSError as error:
                raise StoreUnavailable(
                    f"Could not inventory RAR staging directory {directory}: {error}."
                ) from error
            for entry in entries:
                entry_count += 1
                if entry_count > self._max_inventory_entries:
                    raise StoreUnsupportedOperation(
                        f"RAR staging exceeds {self._max_inventory_entries} entries."
                    )
                path = pathlib.Path(entry.path)
                if entry.is_symlink():
                    raise StoreUnsupportedOperation(
                        f"RAR staging contains a symbolic link: {path}"
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise StoreUnsupportedOperation(
                        f"RAR staging contains a non-regular entry: {path}"
                    )
                key = canonical_archive_key(
                    path.relative_to(self.staging_root).as_posix(),
                    format_name="RAR",
                    max_depth=self._max_depth,
                    max_path_bytes=self._max_path_bytes,
                )
                if key in manifest:
                    raise StoreIntegrityError(f"duplicate staged RAR member: {key}")
                before = entry.stat(follow_symlinks=False)
                if before.st_size > self._effective_member_limit:
                    raise StoreUnsupportedOperation(
                        f"RAR member {key!r} exceeds {self._effective_member_limit} bytes."
                    )
                total_bytes += before.st_size
                if total_bytes > self._max_total_uncompressed_bytes:
                    raise StoreUnsupportedOperation(
                        "RAR staged total size exceeds "
                        f"{self._max_total_uncompressed_bytes} bytes."
                    )
                crc = _file_crc32(path)
                after = path.stat(follow_symlinks=False)
                if _file_identity(before) != _file_identity(after):
                    raise StorePreconditionFailed(
                        f"staged RAR member changed while it was inspected: {key}"
                    )
                manifest[key] = _StagedRarMember(after.st_size, crc)
        if not manifest:
            raise ValueError("Cannot seal an empty RAR staging Store.")
        return manifest

    def _run_rar_create(
        self,
        executable: str,
        output: pathlib.Path,
        *,
        quiet: bool,
    ) -> None:
        """
        Ask RAR to create a non-solid RAR 4 candidate from staging.

        Example:
            >>> store._run_rar_create(executable, output, quiet=True)  # doctest: +SKIP


        :param executable:
        :param output:
        :param quiet:
        :return:
        """

        command = [
            executable,
            "a",
            "-ma4",
            f"-m{self._compression_level}",
            "-s-",
            "-r",
            "-ep1",
            "-p-",
        ]
        if quiet:
            command.append("-inul")
        command.extend((str(output), "."))
        self._run_rar_command(command, cwd=self.staging_root, operation="create")

    def _run_rar_test(
        self,
        executable: str,
        archive: pathlib.Path,
        *,
        quiet: bool,
    ) -> None:
        """
        Ask RAR to test every candidate member before publication.

        Example:
            >>> store._run_rar_test(executable, archive, quiet=True)  # doctest: +SKIP


        :param executable:
        :param archive:
        :param quiet:
        :return:
        """

        command = [executable, "t", "-p-"]
        if quiet:
            command.append("-inul")
        command.append(str(archive))
        self._run_rar_command(command, cwd=None, operation="test")

    def _run_rar_command(
        self,
        command: list[str],
        *,
        cwd: pathlib.Path | None,
        operation: str,
    ) -> None:
        """
        Run one non-interactive RAR command with bounded logs and timeout.

        Example:
            >>> store._run_rar_command(command, cwd=None, operation="test")  # doctest: +SKIP


        :param command:
        :param cwd:
        :param operation:
        :return:
        """

        output_buffer = bytearray()
        process: subprocess.Popen[bytes] | None = None
        try:
            try:
                process = subprocess.Popen(
                    command,
                    cwd=None if cwd is None else str(cwd),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="RAR build",
                    operation=operation,
                    target=self._archive_path,
                ) from error
            if process.stdout is None:
                process.kill()
                raise StoreUnavailable("rar did not provide an output pipe.")
            process_stdout = process.stdout

            def drain_output() -> None:
                while chunk := process_stdout.read(64 * 1024):
                    remaining = DEFAULT_MAX_RAR_STDERR_BYTES - len(output_buffer)
                    if remaining > 0:
                        output_buffer.extend(chunk[:remaining])

            output_thread = threading.Thread(target=drain_output, daemon=True)
            output_thread.start()
            try:
                return_code = process.wait(timeout=self._command_timeout_s)
            except subprocess.TimeoutExpired as error:
                try:
                    process.kill()
                    process.wait()
                except OSError:
                    pass
                raise StorageTimeout(
                    driver_failure_message(
                        "RAR build",
                        operation,
                        target=self._archive_path,
                        reason=f"rar exceeded {self._command_timeout_s:g} seconds",
                    )
                ) from error
            finally:
                output_thread.join(timeout=2)
            if output_thread.is_alive():
                raise StoreUnavailable("rar output pipe did not close.")
            if return_code:
                reason = bytes(output_buffer).decode("utf-8", "replace").strip()
                raise StoreUnavailable(
                    driver_failure_message(
                        "RAR build",
                        operation,
                        target=self._archive_path,
                        reason=reason or f"rar exited with status {return_code}",
                    )
                )
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if process.stdout is not None:
                    process.stdout.close()

    def _validate_candidate(
        self,
        candidate: pathlib.Path,
        manifest: Mapping[str, _StagedRarMember],
        *,
        extractor_exe: str,
    ) -> None:
        """
        Require candidate keys, sizes, and declared CRCs to match staging.

        Example:
            >>> store._validate_candidate(candidate, manifest, extractor_exe=executable)  # doctest: +SKIP


        :param candidate:
        :param manifest:
        :param extractor_exe:
        :return:
        """

        driver = RarStorageDriver(
            candidate,
            address_space_uuid=uuid4(),
            extractor_exe=extractor_exe,
            extract_timeout_s=self._command_timeout_s,
            max_inventory_entries=self._max_inventory_entries,
            max_member_bytes=self._max_member_bytes,
            max_depth=self._max_depth,
            max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
            max_compression_ratio=self._max_compression_ratio,
            max_path_bytes=self._max_path_bytes,
        )
        observed: dict[str, _StagedRarMember] = {}
        for entry in driver.iter_inventory():
            if entry.size is None:
                raise StoreIntegrityError(
                    f"sealed RAR member lacks a declared size: {entry.object_address}"
                )
            metadata = dict(entry.hints.metadata)
            try:
                crc = int(metadata["crc32"], 16)
            except (KeyError, TypeError, ValueError) as error:
                raise StoreIntegrityError(
                    f"sealed RAR member lacks a valid CRC-32: {entry.object_address}"
                ) from error
            observed[str(entry.object_address)] = _StagedRarMember(entry.size, crc)
        if observed != dict(manifest):
            raise StoreIntegrityError(
                "sealed RAR candidate inventory, sizes, or CRC-32 values differ from staging."
            )

    def _temporary_archive_path(self) -> pathlib.Path:
        """
        Allocate an unpublished candidate path beside the final RAR.

        Example:
            >>> candidate = store._temporary_archive_path()  # doctest: +SKIP


        :return:
        """

        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{self._archive_path.name}.build-",
                suffix=".part.rar",
                dir=self._archive_path.parent,
            )
            os.close(descriptor)
            path = pathlib.Path(name)
            path.unlink()
            return path
        except OSError as error:
            raise translate_os_error(
                error,
                backend="RAR build",
                operation="create candidate path",
                target=self._archive_path,
            ) from error

    def _publish_archive(self, candidate: pathlib.Path) -> None:
        """
        Create-only publish a verified candidate and fsync its directory.

        Example:
            >>> store._publish_archive(candidate)  # doctest: +SKIP


        :param candidate:
        :return:
        """

        try:
            os.link(candidate, self._archive_path)
        except FileExistsError as error:
            self._sealed = True
            raise StoreAlreadyExists(
                f"RAR output appeared during sealing: {self._archive_path}"
            ) from error
        except OSError as error:
            raise translate_os_error(
                error,
                backend="RAR build",
                operation="publish archive",
                target=self._archive_path,
            ) from error
        self._sealed = True
        try:
            candidate.unlink()
        except OSError:
            pass
        try:
            with self._archive_path.open("rb") as archive:
                os.fsync(archive.fileno())
            fsync_directory(self._archive_path.parent)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="RAR build",
                operation="fsync published archive",
                target=self._archive_path,
            ) from error

    def _content_address(self, digest: Digest | None) -> str:
        """
        Render a stable SHA-style bucketed staging address.

        Example:
            >>> store._content_address(Digest("sha256", "a" * 64)).startswith("objects/aaaaa/")  # doctest: +SKIP
            True


        :param digest:
        :return:
        """

        if digest is None:
            raise StoreUnsupportedOperation(
                "content-addressed RAR staging requires an expected digest."
            )
        return "/".join(
            (
                self.DEFAULT_OBJECTS_DIRNAME,
                digest.value[: self.AUTO_WRITE_BUCKET_LENGTH],
                digest.value,
            )
        )

    def _require_unsealed(self) -> None:
        """
        Reject mutation after publication or an external output collision.

        Example:
            >>> store._require_unsealed()  # doctest: +SKIP


        :return:
        """

        with self._state_condition:
            if self._sealed or self._archive_path.exists():
                self._sealed = True
                raise StorePreconditionFailed(
                    "RAR staging is sealed because its create-only output exists."
                )

    def _acquire_mutation(self) -> None:
        """
        Acquire one mutation lease unless sealing or publication has begun.

        Example:
            >>> store._acquire_mutation()  # doctest: +SKIP


        :return:
        """

        with self._state_condition:
            if self._sealed or self._archive_path.exists():
                self._sealed = True
                raise StorePreconditionFailed("RAR staging is already sealed.")
            if self._sealing:
                raise StorePreconditionFailed(
                    "RAR staging is sealed for archive publication."
                )
            self._active_mutations += 1

    def _release_mutation(self) -> None:
        """
        Release one active mutation lease.

        Example:
            >>> store._release_mutation()  # doctest: +SKIP


        :return:
        """

        with self._state_condition:
            self._active_mutations -= 1
            self._state_condition.notify_all()

    @contextmanager
    def _mutation_operation(self) -> Iterator[None]:
        """
        Hold a mutation lease around an immediate staging operation.

        Example:
            >>> with store._mutation_operation():  # doctest: +SKIP
            ...     pass


        :return:
        """

        self._acquire_mutation()
        try:
            yield
        finally:
            self._release_mutation()


def _store_uuid(
    value: str | UUID | None,
    configuration: StoreConfiguration | None,
) -> UUID:
    """
    Reconcile explicit and configured identities for one RAR builder.

    Example:
        >>> _store_uuid(UUID(int=1), None).int
        1


    :param value:
    :param configuration:
    :return:
    """

    if configuration is not None:
        if value is not None and UUID(str(value)) != configuration.store_uuid:
            raise ValueError("configuration and explicit uuid identify different Stores.")
        return configuration.store_uuid
    if value is None:
        return uuid4()
    return value if isinstance(value, UUID) else UUID(value)


def _file_digest(path: pathlib.Path) -> Digest:
    """
    Calculate a staged source file's SHA-256 digest without buffering it.

    Example:
        >>> digest = _file_digest(path)  # doctest: +SKIP


    :param path:
    :return:
    """

    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_COPY_CHUNK_SIZE):
            hasher.update(chunk)
    return Digest("sha256", hasher.hexdigest())


def _file_crc32(path: pathlib.Path) -> int:
    """
    Calculate the CRC-32 recorded for one staged RAR member.

    Example:
        >>> _file_crc32(path)  # doctest: +SKIP


    :param path:
    :return:
    """

    checksum = 0
    with path.open("rb") as source:
        while chunk := source.read(_COPY_CHUNK_SIZE):
            checksum = zlib.crc32(chunk, checksum)
    return checksum & 0xFFFFFFFF


def _file_identity(result: os.stat_result) -> tuple[int, int, int, int]:
    """
    Return local fields used to detect a staged file changing during hashing.

    Example:
        >>> len(_file_identity(os.stat(__file__)))
        4


    :param result:
    :return:
    """

    return (
        int(result.st_ino),
        int(result.st_size),
        int(result.st_mtime_ns),
        int(result.st_ctime_ns),
    )


__all__ = [
    "DEFAULT_RAR_BUILD_TIMEOUT_S",
    "DEFAULT_RAR_COMPRESSION_LEVEL",
    "RarBuildStorageBackend",
]
