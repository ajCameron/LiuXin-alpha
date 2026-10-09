"""
Stage filesystem objects and explicitly publish a separate create-only RAR archive.

Mutation leases coordinate this builder instance with sealing. The external creator
is asked for RAR4 non-solid output; staging observations, tool testing, and candidate
inventory comparison precede hard-link publication. Output synchronization and
facade construction can fail after publication, while ordinary Store reads continue
to address the retained staging tree.
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
    Retain the byte size and CRC-32 used to compare staging with candidate inventory.

    The frozen record adds no validation; CRC-32 is comparison evidence, not a collision-resistant
    content identity.

    Example:
        >>> _StagedRarMember(4, 0x12345678).size
        4


    :ivar size: Observed staging size or declared candidate member bytes.
    :ivar crc32: Unsigned checksum calculated from staging or parsed from candidate metadata.
    """

    size: int
    crc32: int


class _TrackedRarBuildWriteSession:
    """
    Wrap a filesystem write session with a byte cap and one-time mutation-lease release.

    Commit, abort, and context exit attempt release even when delegation fails. The release marker
    is set before calling the callback, so a callback failure is not retried. Failed context entry
    has no corresponding automatic release in this wrapper.

    Example:
        >>> tracked = _TrackedRarBuildWriteSession(session, release, max_size=1024)  # doctest: +SKIP
    """

    def __init__(self, session, release, *, max_size: int) -> None:
        """
        Retain the staged session, release callback, and cap without acquiring a lease.

        The caller already owns the lease and supplies valid session/limit values.

        Example:
            >>> tracked = _TrackedRarBuildWriteSession(session, release, max_size=1024)  # doctest: +SKIP


        :param session: Filesystem write session receiving write/commit/abort/context calls.
        :param release: Zero-argument callback releasing the caller's active builder mutation.
        :param max_size: Maximum cumulative offered bytes before each write, supplied without validation here.
        :return: None after initializing the accepted-byte counter and unreleased marker.
        """

        self._session = session
        self._release = release
        self._max_size = max_size
        self._size = 0
        self._released = False

    @property
    def location(self) -> Location:
        """
        Return the staged destination exposed by the wrapped Store session.

        Tracking the RAR mutation lease and offered-byte ceiling does not alter the underlying
        publication target. Access performs no Store lookup or staging mutation.

        :return: Routed Location at which the wrapped session will publish.
        """

        return self._session.location

    def write(self, data: bytes) -> int:
        """
        Check the entire offered chunk against the cap, then forward it to staging.

        The underlying session validates byte type and lifecycle. Its reported accepted count is
        accumulated directly, without a None fallback or independent count validation.

        Example:
            >>> tracked.write(b"book")  # doctest: +SKIP
            4


        :param data: Byte chunk offered to the underlying write session.
        :return: Accepted byte count reported by the session.
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
        Commit the filesystem stage and attempt lease release even if commit fails.

        A release-callback exception can replace a commit result or failure.

        Example:
            >>> info = tracked.commit()  # doctest: +SKIP


        :return: FileInfo returned by the underlying staging commit when delegation and release succeed.
        """

        try:
            return self._session.commit()
        finally:
            self._finish()

    def abort(self) -> None:
        """
        Abort the filesystem stage and attempt lease release in finally.

        Example:
            >>> tracked.abort()  # doctest: +SKIP


        :return: None after delegated abort and release complete; either can raise.
        """

        try:
            self._session.abort()
        finally:
            self._finish()

    def __enter__(self):
        """
        Enter the underlying session and return this tracking wrapper.

        An entry failure propagates without invoking the release callback here.

        Example:
            >>> tracked.__enter__() is tracked  # doctest: +SKIP
            True


        :return: This wrapper after successful underlying context entry.
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
        Forward context-exit metadata and attempt lease release in finally.

        The underlying exit return value is ignored, so this wrapper does not suppress a with-block
        exception.

        Example:
            >>> tracked.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class from the with block, or None.
        :param exc: Escaping exception instance, or None.
        :param traceback: Escaping traceback, or None.
        :return: None after delegated context cleanup and lease release.
        """

        try:
            self._session.__exit__(exc_type, exc, traceback)
        finally:
            self._finish()

    def _finish(self) -> None:
        """
        Mark the lease released before invoking its callback at most once.

        Subsequent calls return even if the first callback invocation raised.

        Example:
            >>> tracked._finish()  # doctest: +SKIP


        :return: None after the first successful callback or when already marked released.
        """

        if self._released:
            return
        self._released = True
        self._release()


class RarBuildStorageBackend(FilesystemStore):
    """
    Keep mutable filesystem staging and explicitly publish a separate RAR Store once.

    Ordinary file reads continue to address staging after sealing; the returned read-only facade
    addresses the archive with a new Store identity. Mutation leases reject sealing while writes are
    active and reject new mutations during sealing. Publication is create-only, and errors after
    linking can leave the output visible with this builder permanently sealed. An existing output is
    not adopted or validated during construction.

    Example:
        >>> store = RarBuildStorageBackend("backup.rar")  # doctest: +SKIP
        >>> info = store.store_bytes(b"book", location="book.epub")  # doctest: +SKIP
        >>> readonly = store.seal()  # doctest: +SKIP
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
        Resolve output/staging paths, validate options, and initialize a filesystem-backed builder.

        The output parent is created before staging-layout validation, and staging is created before
        UUID reconciliation, so later failures can leave directories. Supplied configuration is
        retained; explicit runtime paths/options are not reconciled with it. Its read-only policy
        configures the staging driver, while seal itself has no separate read-only-policy check.
        Existing output presence initializes the sealed marker without opening it.

        Example:
            >>> store = RarBuildStorageBackend("backup.rar", compression_level=0)  # doctest: +SKIP


        :param url: Local create-only output filename, expanded and resolved without URI decoding.
        :param name: Display name for newly built configuration; false values derive it from the output path.
        :param uuid: Explicit UUID/text, required to agree with configuration when supplied; otherwise generated if absent.
        :param configuration: Durable configuration retained as-is, or None to snapshot these options.
        :param rar_exe: Creator executable name/path passed to shutil.which when tool discovery is needed.
        :param compression_level: Value converted to int and required to be between zero and five inclusive.
        :param command_timeout_s: Positive float-converted timeout for each process wait, without a separate finiteness check.
        :param staging_root: Optional filesystem directory; None uses a hidden sibling .<output-name>.staging. Output may not be inside this directory.
        :param max_inventory_entries: Positive maximum scanned staging entries, including directories, and candidate inventory entries.
        :param max_member_bytes: Positive maximum member bytes, further lowered by the total-byte limit.
        :param max_depth: Positive canonical member-key component cap applied when building the manifest/candidate inventory.
        :param max_total_uncompressed_bytes: Positive maximum summed regular-member bytes checked during sealing.
        :param max_compression_ratio: Finite ratio of at least one forwarded to candidate and sealed-reader validation.
        :param max_path_bytes: Positive whole-key UTF-8/surrogateescape byte cap for manifest and candidate validation.
        :return: None after binding staging, recording output presence, and initializing synchronization state.
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
        Return the resolved create-only output filename without checking its current contents.

        Example:
            >>> store.archive_path  # doctest: +SKIP


        :return: Path of the intended or published RAR output.
        """

        return self._archive_path

    @property
    def staging_root(self) -> pathlib.Path:
        """
        Return the filesystem driver's resolved staging directory.

        Example:
            >>> store.staging_root.is_absolute()  # doctest: +SKIP
            True


        :return: Physical directory used by ordinary Store file operations.
        """

        return super().root_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Expose staging as the legacy filesystem root, including after sealing.

        Example:
            >>> store.root_path == store.staging_root  # doctest: +SKIP
            True


        :return: Staging directory Path, not the output archive path.
        """

        return self.staging_root

    @property
    def built_store(self) -> RarReadOnlyStorageBackend | None:
        """
        Return the archive facade created by a successful seal in this process.

        Existing output files are not adopted, and publication can precede a failure that leaves
        this value None.

        Example:
            >>> store.built_store is None  # doctest: +SKIP
            True


        :return: Cached RarReadOnlyStorageBackend after successful facade construction, otherwise None.
        """

        return self._built_store

    @property
    def characteristics(self) -> StorageCharacteristics:
        """
        Describe mutable staging followed by create-only archival publication.

        These characteristics state intended workflow and configured bounds, not a fresh sealed/tool
        status. The creator command requests RAR4 non-solid output; candidate validation checks
        readable inventory and limits rather than independently enforcing those command-format
        flags.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.STAGING_THEN_SEAL: 'staging_then_seal'>


        :return: Staging-then-seal characteristics with Store-copy space and effective member/path limits.
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
        Derive a display name through the shared path-to-name helper without filesystem access.

        Example:
            >>> bool(RarBuildStorageBackend.url_to_name("/archives/books.rar"))
            True


        :param url: Output path text used only as a naming hint.
        :return: Display-name text from safe_path_to_name.
        """

        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        """
        Start/probe the filesystem staging driver and decorate its Store status with builder
        observations.

        Example:
            >>> status = store.startup()  # doctest: +SKIP


        :return: Staging status augmented with current creator discovery and output/sealed state.
        """

        return self._decorate_status(super().startup())

    def probe(self) -> StoreStatus:
        """
        Refresh the filesystem staging probe and add current creator/output observations.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return: Decorated StoreStatus; probe and discovery failures can propagate.
        """

        return self._decorate_status(super().probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Obtain cached or refreshed staging status, then observe builder/tool state again.

        Even refresh=False performs executable discovery and checks output presence through
        decoration.

        Example:
            >>> status = store.status(refresh=False)  # doctest: +SKIP


        :param refresh: Whether inherited status handling actively probes the staging driver.
        :return: Decorated StoreStatus without storing the decorated copy as the driver's status.
        """

        return self._decorate_status(super().status(refresh=refresh))

    def self_test(self) -> StoreStatus:
        """
        Run the builder's active staging/tool/output probe through its legacy entry point.

        Example:
            >>> status = store.self_test()  # doctest: +SKIP


        :return: The result of probe(), with its effects and failures.
        """

        return self.probe()

    def _decorate_status(self, status: StoreStatus) -> StoreStatus:
        """
        Add creator availability, output path, and apparent sealed state to a status copy.

        Missing tools warn without making staging unavailable. Output presence or the sealed marker
        masks writability, while other status fields remain inherited. This observation does not
        latch self._sealed and does not report _sealing; repeated decoration can duplicate warnings.

        Example:
            >>> decorated = store._decorate_status(status)  # doctest: +SKIP


        :param status: Underlying staging snapshot whose warnings and details are extended.
        :return: dataclasses.replace copy with builder message, warnings, sorted merged details, and masked writable flag.
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
        Acquire a mutation lease and open a bounded filesystem staging session.

        The early size check rejects an announced over-limit object; other
        expectation/location/write policy checks belong to the inherited session. Failure to begin
        releases the lease. The returned wrapper releases it on commit, abort, or context exit,
        rather than waiting for sealing.

        Example:
            >>> session = store.begin_write(store.locate("book.epub"), expected_size=4)  # doctest: +SKIP


        :param location: Owned staging Location to publish when the filesystem session commits.
        :param mode: Create/replace collision policy forwarded to staging.
        :param expected_size: Optional exact byte expectation; values above the effective member cap fail here.
        :param expected_digest: Optional content expectation forwarded to the staging session.
        :param placement_hints: Optional placement hints forwarded to the generic Store bridge.
        :return: Tracked session holding an active mutation lease and enforcing the effective offered-byte cap.
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
        Stage bytes at an explicit destination or a digest-derived implicit key.

        Use the supplied expected digest or compute SHA-256. For an implicit existing target, return
        it when its computed digest matches the expected digest, without comparing incoming data on
        that fast path. Incompatible implicit targets and delegated already-exists/integrity
        failures become RarBuildImplicitOverwriteError. Sealed state is checked before hashing or
        deduplication.

        Example:
            >>> info = store.store_bytes(b"book")  # doctest: +SKIP


        :param data: Bytes to hash when no digest is supplied and to stage unless an implicit match is returned.
        :param location: Explicit key/Location, or None for content-addressed lookup and deduplication.
        :param name: Filename hint forwarded to the inherited write convenience.
        :param metadata: Optional metadata forwarded to inherited normalization/placement handling.
        :param write_mode: Optional legacy write-mode spelling forwarded to the inherited convenience.
        :param expected_digest: Digest expectation and implicit-address input; None computes SHA-256 from data.
        :param mode: Optional write-mode alias reconciled by inherited handling.
        :return: FileInfo for an existing matching implicit target or a newly committed staging file.
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
        Copy a local source into staging, using digest-addressed lookup when location is absent.

        A supplied digest avoids the initial source-hashing pass. An existing implicit match returns
        before source copying or expected-size checking. A false explicit location also selects a
        content key but does not take the None-only deduplication branch. Delegated failures are not
        remapped to the byte helper's implicit-overwrite exception.

        Example:
            >>> info = store.store_file(path, location="books/book.epub")  # doctest: +SKIP


        :param path: Source filename read for hashing when needed and copied on the write path.
        :param location: Explicit key/Location; None permits deduplication, while other false values only select a content key.
        :param name: Optional filename hint forwarded to inherited file-copy handling.
        :param metadata: Optional metadata forwarded to inherited handling.
        :param write_mode: Optional write-mode spelling forwarded unchanged.
        :param expected_size: Optional copied-byte expectation checked only on the delegated write path.
        :param expected_digest: Digest used for lookup and copy verification; None first hashes the source with SHA-256.
        :param mode: Optional write-mode alias reconciled by inherited handling.
        :return: Existing matching implicit FileInfo or the newly committed staging FileInfo.
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
        Stage a stream at an explicit destination or a supplied digest-derived key.

        None location requires expected_digest. False destination values also fall through to
        content-key selection. No existing-object deduplication is performed here; source ownership
        and remaining write options follow the inherited stream helper.

        Example:
            >>> info = store.store_stream(source, expected_digest=digest)  # doctest: +SKIP


        :param source: Caller-provided binary stream read from its current position.
        :param location: Destination key/Location, or None to require content-addressed allocation.
        :param expected_digest: Digest expectation used to form an implicit key and forwarded to stream verification.
        :param kwargs: Remaining inherited stream-write options, such as expected_size, metadata, and write mode.
        :return: FileInfo from the completed staging stream write.
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
        Copy a source file into staging through store_file rather than retaining a live source
        reference.

        Example:
            >>> info = store.designate_file(path, archive_path="books/book.epub")  # doctest: +SKIP


        :param source_path: Local source to hash/copy according to store_file policy.
        :param archive_path: Explicit member key, or None for content-addressed lookup.
        :return: FileInfo for the staged copy or existing digest match.
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
        Hold a builder mutation lease around an inherited staged-file deletion.

        Example:
            >>> store.delete(location, if_version=version)  # doctest: +SKIP


        :param location: Owned staging Location to delete.
        :param missing_ok: Whether inherited deletion accepts an absent object.
        :param if_version: Optional staging-file version precondition.
        :return: None after deletion/accepted absence and lease release; inherited failures propagate.
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
        Hold a mutation lease around the inherited copy between staging Locations.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source: Owned source Location in staging.
        :param destination: Owned staging destination Location.
        :param mode: Destination collision policy forwarded to the filesystem Store bridge.
        :return: Destination FileInfo returned by inherited copy, after lease release.
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
        Hold a mutation lease around the inherited staged-file move.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source: Owned source Location in staging.
        :param destination: Owned staging destination Location.
        :param mode: Destination collision policy forwarded to the filesystem Store bridge.
        :return: Destination FileInfo returned by inherited move, after lease release.
        """

        with self._mutation_operation():
            return super().move(source, destination, mode=mode)

    def seal(self, *, quiet: bool = True) -> RarReadOnlyStorageBackend:
        """
        Validate staging, run creator/test commands, and publish one create-only archive.

        Reject sealed/existing output, concurrent sealing, or active mutations without waiting.
        Compare staging size/CRC manifests before and after creation, require a private nonempty
        regular candidate, run the external test, and compare candidate declared keys/sizes/CRCs.
        Candidate stat identity must still match its pre-validation observation before hard-link
        publication. Final fsync or facade construction can fail with output already visible and the
        builder sealed. Finally removes a retained candidate and clears the sealing flag; staging
        remains. This method has no separate configuration.read_only check.

        Example:
            >>> readonly = store.seal(quiet=True)  # doctest: +SKIP


        :param quiet: Whether create/test commands request suppressed normal output via -inul; pipes are captured either way.
        :return: New read-only RAR Store with a new identity, cached as built_store only after successful construction.
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
        Resolve the configured creator using shutil.which without testing or installing it.

        Example:
            >>> executable = store._rar_executable()  # doctest: +SKIP


        :return: Resolved creator path, or None when unavailable.
        """

        return shutil.which(self._rar_exe)

    def _require_rar_executable(self) -> str:
        """
        Require a discoverable creator before attempting archive creation.

        Example:
            >>> executable = store._require_rar_executable()  # doctest: +SKIP


        :return: Resolved creator path; absence raises StoreUnsupportedOperation with an actionable tool requirement.
        """

        executable = self._rar_executable()
        if executable is None:
            raise StoreUnsupportedOperation(
                "RAR sealing requires an operator-supplied licensed rar executable."
            )
        return executable

    def _staging_manifest(self) -> dict[str, _StagedRarMember]:
        """
        Walk staging and calculate bounded regular-file size/CRC observations for sealing.

        Each directory listing is materialized before the running entry cap is applied; directories
        count but only file keys undergo canonical path checks. Reject symlinks and other
        non-regular entries, then bound member/total bytes. Compare inode/size/time fields around
        each CRC read, without pinning a file descriptor across those path operations. Hard-link
        count is not checked for staged files. An empty regular-file manifest is rejected.

        Example:
            >>> manifest = store._staging_manifest()  # doctest: +SKIP


        :return: Key-to-_StagedRarMember map of observed file sizes and CRC-32 values; it is not a collision-resistant content snapshot.
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
        Request recursive, non-solid, password-free RAR4 creation from the staging directory.

        The command uses the configured compression level and a relative dot source, with staging as
        cwd. Successful command completion alone does not validate the resulting archive.

        Example:
            >>> store._run_rar_create(executable, candidate, quiet=True)  # doctest: +SKIP


        :param executable: Resolved creator command path.
        :param output: Candidate filename supplied to the creator.
        :param quiet: Whether to add -inul to suppress normal tool output.
        :return: None after the command runner reports success.
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
        Run the creator's password-free archive-test command before candidate metadata comparison.

        Example:
            >>> store._run_rar_test(executable, candidate, quiet=True)  # doctest: +SKIP


        :param executable: Resolved RAR tool path.
        :param archive: Candidate archive to pass to the t command.
        :param quiet: Whether to add -inul; output remains captured by the runner.
        :return: None when the tool reports successful testing.
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
        Run a non-interactive creator command while retaining a bounded prefix of combined output.

        A daemon thread drains stdout/stderr into a 64 KiB prefix buffer. Wait uses the configured
        timeout, followed by kill/wait on expiry and a two-second drain join; cleanup waits have no
        timeout. A still-live drainer or nonzero exit fails. Drainer exceptions are not captured
        back into this method, so thread termination alone does not establish a successful drain.
        Finally cleanup can itself raise.

        Example:
            >>> store._run_rar_command(command, cwd=None, operation="test")  # doctest: +SKIP


        :param command: Argument list passed directly to subprocess.Popen without a shell.
        :param cwd: Working directory for the command, or None to inherit the process cwd.
        :param operation: Create/test label for translated errors.
        :return: None after zero exit and a terminated drain thread; captured output is used only in failure diagnostics.
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
                """
                Drain the combined output pipe while retaining only its first 64 KiB.

                Read/buffer failures escape the thread rather than populating a shared failure
                record.

                Example:
                    >>> drain_output()  # doctest: +SKIP


                :return: None at EOF, with the diagnostic prefix retained in the enclosing buffer.
                """
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
        Compare bounded candidate inventory with staged keys, sizes, and declared CRC-32 values.

        Use a fresh RAR driver under the builder's limits. This method enumerates metadata without
        reading member payloads; external archive testing occurs separately. Missing size/CRC or any
        manifest mismatch fails.

        Example:
            >>> store._validate_candidate(candidate, manifest, extractor_exe=executable)  # doctest: +SKIP


        :param candidate: Archive file whose inventory is inspected.
        :param manifest: Staging key/size/CRC observations required to match exactly.
        :param extractor_exe: Tool path configured on the candidate driver; inventory does not itself extract payloads.
        :return: None for an exact declared-metadata match under candidate policy.
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
        Create, close, and unlink a sibling temporary file to choose a creator output path.

        The returned name is absent and unreserved, so later creation is a separate operation.
        Close/unlink failures are translated without an additional cleanup guard here.

        Example:
            >>> candidate = store._temporary_archive_path()  # doctest: +SKIP


        :return: Unpublished sibling .part.rar Path for the external creator.
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
        Hard-link a previously validated candidate to the output, latch sealed state, and
        synchronize.

        An existing output is retained and also latches sealed state. After a successful link,
        candidate-unlink OSErrors are suppressed; output fsync failures are translated with
        publication already visible. Directory fsync is best effort. Candidate validation belongs to
        the caller, and failed candidate removal can leave another hard link.

        Example:
            >>> store._publish_archive(candidate)  # doctest: +SKIP


        :param candidate: Validated local candidate path to link at the create-only output.
        :return: None after publication and output-file synchronization; failure does not imply output absence.
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
        Form a staging key from the first five digest-value characters and the whole value.

        The key starts with objects and omits the algorithm, so different algorithms can share a
        spelling. This helper does not validate the resulting key or reserve a destination.

        Example:
            >>> store._content_address(Digest("sha256", "a" * 64))  # doctest: +SKIP
            'objects/aaaaa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'


        :param digest: Digest supplying key text; None raises StoreUnsupportedOperation.
        :return: Slash-separated objects/bucket/value member-key text.
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
        Latch and reject sealed state when output presence or a prior publication is observed.

        This check is condition-locked but does not inspect the sealing-in-progress flag or acquire
        a mutation lease.

        Example:
            >>> store._require_unsealed()  # doctest: +SKIP


        :return: None when no output/sealed state is observed; otherwise raises StorePreconditionFailed.
        """

        with self._state_condition:
            if self._sealed or self._archive_path.exists():
                self._sealed = True
                raise StorePreconditionFailed(
                    "RAR staging is sealed because its create-only output exists."
                )

    def _acquire_mutation(self) -> None:
        """
        Increment the active mutation count only while the builder is unsealed and not sealing.

        Output presence permanently latches the sealed marker. Rejection is immediate rather than
        waiting on the condition.

        Example:
            >>> store._acquire_mutation()  # doctest: +SKIP


        :return: None after acquiring one count-based lease under the state condition.
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
        Decrement the active count and notify condition waiters without validating lease ownership.

        Callers must pair acquisitions/releases; this helper does not prevent a negative count.

        Example:
            >>> store._release_mutation()  # doctest: +SKIP


        :return: None after the condition-protected decrement and notification.
        """

        with self._state_condition:
            self._active_mutations -= 1
            self._state_condition.notify_all()

    @contextmanager
    def _mutation_operation(self) -> Iterator[None]:
        """
        Acquire one mutation lease for a with block and release it in finally.

        Example:
            >>> with store._mutation_operation():  # doctest: +SKIP
            ...     perform_staging_operation()


        :return: Context manager yielding None while its mutation lease is held.
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
    Select configured or explicit builder identity, rejecting a mismatch.

    Without either value, generate a new UUID; UUID parsing failures propagate.

    Example:
        >>> _store_uuid(UUID(int=1), None).int
        1


    :param value: Explicit UUID/text, or None to use configured/generated identity.
    :param configuration: Optional durable configuration whose UUID must match any explicit value.
    :return: Configured, retained, parsed, or newly generated UUID.
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
    Read a local file in 1 MiB chunks and calculate SHA-256.

    The file is closed by its context; path/read failures propagate without translation or a
    before/after identity check.

    Example:
        >>> digest = _file_digest(path)  # doctest: +SKIP


    :param path: Local file whose bytes are hashed from the start.
    :return: Digest containing sha256 and its hexadecimal value.
    """

    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_COPY_CHUNK_SIZE):
            hasher.update(chunk)
    return Digest("sha256", hasher.hexdigest())


def _file_crc32(path: pathlib.Path) -> int:
    """
    Read a local file in 1 MiB chunks and calculate its unsigned CRC-32.

    File identity checks belong to the caller, and I/O failures are not translated here.

    Example:
        >>> checksum = _file_crc32(path)  # doctest: +SKIP


    :param path: Local file opened from its beginning for checksum calculation.
    :return: CRC-32 integer masked to 32 bits.
    """

    checksum = 0
    with path.open("rb") as source:
        while chunk := source.read(_COPY_CHUNK_SIZE):
            checksum = zlib.crc32(chunk, checksum)
    return checksum & 0xFFFFFFFF


def _file_identity(result: os.stat_result) -> tuple[int, int, int, int]:
    """
    Extract inode, byte size, and nanosecond change timestamps for path-race observations.

    Device, mode, and link count are omitted; this tuple is not a content digest.

    Example:
        >>> len(_file_identity(os.stat(__file__)))
        4


    :param result: Stat/lstat result recorded before or after hashing or candidate validation.
    :return: Integer tuple of inode, size, mtime_ns, and ctime_ns.
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
