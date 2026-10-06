"""
Stage filesystem writes and seal validated snapshots into SquashFS images.

Mutation leases exclude this Store's wrapped writes during sealing. Candidate
inventory, size, and digest checks precede publication, while synchronization and
archive-Store startup follow it. External filesystem edits and post-publication
errors remain separate from the lease and candidate-validation guarantees.
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
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend import (
    SquashfsReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


@dataclasses.dataclass(slots=True, frozen=True)
class _StagedMember:
    """
    Retain the size and SHA-256 evidence observed while preflighting one staged file.

    The frozen record performs no validation and holds no live reference or lock on the source
    bytes.

    Example:
        >>> _StagedMember(4, "abcd").size
        4


    :ivar size: Observed staged-file length in bytes.
    :ivar sha256: Hexadecimal SHA-256 observed during preflight.
    """

    size: int
    sha256: str


class _TrackedWriteSession:
    """
    Wrap a staged writer with an offered-size ceiling and a one-shot mutation-lease release.

    Commit, abort, and context exit release the lease even if the wrapped operation fails. Releasing
    the lease does not independently finish the underlying session; its own state controls
    subsequent writes. Abandoned sessions have no destructor-based release here.

    Example:
        >>> with store.begin_write(location) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
    """

    def __init__(self, session, release, *, max_size: int) -> None:
        """
        Retain the wrapped session and release callback with fresh byte and lease accounting.

        Example:
            >>> tracked = _TrackedWriteSession(session, release, max_size=1024)  # doctest: +SKIP


        :param session: Underlying Store write session to which operations are delegated.
        :param release: Zero-argument callback releasing one already acquired mutation lease.
        :param max_size: Maximum offered cumulative byte count; this constructor does not validate it.
        :return: None after setting the accepted-byte count to zero and the lease-release flag to False.
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

        This wrapper adds byte-ceiling and mutation-lease behavior without changing the publication
        target. Access delegates to the underlying session and performs no storage operation.

        :return: Routed Location at which the wrapped session will publish.
        """

        return self._session.location

    def write(self, data: bytes) -> int:
        """
        Check offered length against the ceiling, delegate the write, and add its accepted count.

        The wrapper trusts the underlying return value and delegates type/state validation. A
        rejected over-limit offer leaves its counter unchanged and does not release the lease.

        Example:
            >>> tracked.write(b"book")  # doctest: +SKIP
            4


        :param data: Bytes offered to the wrapped session.
        :return: Accepted count returned by the underlying writer.
        """
        if self._size + len(data) > self._max_size:
            raise StoreUnsupportedOperation(
                f"SquashFS staged members are limited to {self._max_size} bytes."
            )
        written = self._session.write(data)
        self._size += written
        return written

    def commit(self) -> FileInfo:
        """
        Delegate commit and release the mutation lease in finally, including on failure.

        A release error can replace the underlying result or exception; the wrapper does not itself
        abort a failed commit.

        Example:
            >>> info = tracked.commit()  # doctest: +SKIP


        :return: Underlying FileInfo when commit and release succeed.
        """
        try:
            return self._session.commit()
        finally:
            self._finish()

    def abort(self) -> None:
        """
        Delegate abort and release the mutation lease in finally, including on failure.

        Example:
            >>> tracked.abort()  # doctest: +SKIP


        :return: None after both operations return; a release error can replace an abort error.
        """
        try:
            self._session.abort()
        finally:
            self._finish()

    def __enter__(self):
        """
        Enter the underlying session and return this counting wrapper.

        If the underlying entry raises, this method does not release the lease.

        Example:
            >>> with tracked as writer:  # doctest: +SKIP
            ...     writer.write(b"book")


        :return: This wrapper after underlying __enter__ succeeds.
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
        Forward context-exit arguments and release the lease in finally.

        The underlying exit return value is discarded, so this wrapper does not suppress a context
        exception. Release errors can replace the current exception.

        Example:
            >>> tracked.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class from the with body, or None on normal exit.
        :param exc: Active exception instance, or None.
        :param traceback: Traceback belonging to the active exception, or None.
        :return: None after delegated cleanup and lease release.
        """
        try:
            self._session.__exit__(exc_type, exc, traceback)
        finally:
            self._finish()

    def _finish(self) -> None:
        """
        Mark the lease released before invoking its callback once.

        Later calls return immediately, including when the first callback raised.

        Example:
            >>> tracked._finish()  # doctest: +SKIP


        :return: None after the first callback returns or when already marked released.
        """
        if self._released:
            return
        self._released = True
        self._release()


class SquashfsBuildStorageBackend(FilesystemStore):
    """
    Collect committed filesystem objects and seal their validated contents into one SquashFS image.

    Ordinary reads and status describe the staging filesystem. Successful sealing retains a separate
    read-only archive Store and refuses new leased mutations. Repeated implicit writes may still
    return matching staged data through their read-only deduplication branch. The lease covers this
    instance's wrapped mutation paths, not external staging edits or direct raw-driver calls.

    Candidate validation compares names, sizes, and SHA-256 with preflight evidence. Publication and
    subsequent synchronization/startup are distinct steps; a later error can leave a new image
    visible without recording built_store.

    Example:
        >>> store = SquashfsBuildStorageBackend("backup.sqsh")  # doctest: +SKIP
        >>> store.store_bytes(b"book", location="books/a")  # doctest: +SKIP
        >>> built = store.seal()  # doctest: +SKIP
    """

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
        """
        Resolve the output path, prepare staging, and initialize a filesystem Store plus sealing
        state.

        Output parent creation precedes validation. Positive counts/timeouts are checked before
        numeric conversion; timeout has no finiteness check, while the ratio does. A temporary
        staging directory is owned when staging_root is omitted; explicit staging is retained.

        A supplied configuration provides identity and facade policy and must agree with an explicit
        uuid. Its options/name/URI are retained rather than used to replace runtime arguments.
        Staging allocation precedes UUID agreement and inherited initialization, without a
        compensating constructor cleanup guard.

        Example:
            >>> store = SquashfsBuildStorageBackend("backup.sqsh", staging_root="stage")  # doctest: +SKIP


        :param url: Local destination image pathname, expanded and resolved.
        :param name: Nonempty display name when configuration is omitted; otherwise ignored.
        :param uuid: Optional UUID or UUID string; must match supplied configuration when both are given.
        :param mksquashfs_exe: Build executable name or path, resolved at invocation.
        :param compression: Codec argument passed through to mksquashfs.
        :param deterministic: Request fixed ownership/timestamps and no xattrs during sealing.
        :param staging_root: Existing or creatable staging directory; None allocates an owned temporary directory.
        :param unsquashfs_exe: Reader executable used for candidate validation and the returned Store.
        :param command_timeout_s: Positive build/reader wait timeout in seconds; cleanup can exceed it.
        :param max_inventory_entries: Positive ceiling on staging entries and non-root candidate inventory records, including directories.
        :param max_member_bytes: Positive member byte ceiling, also limited by the total budget.
        :param max_total_uncompressed_bytes: Positive total regular-file byte budget checked at sealing.
        :param max_compression_ratio: Finite candidate declared-size/image-size ratio ceiling, at least one.
        :param max_header_bytes: Positive candidate pseudo-header byte ceiling.
        :param max_depth: Positive maximum regular-member key component count.
        :param max_path_bytes: Positive maximum full-key UTF-8 surrogateescape byte count.
        :param configuration: Configuration to retain, or None to synthesize archive-facing identity/options.
        :return: None after creating the staging Store, condition lock, zero mutation count, and unsealed state.
        """
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
        """
        Expose the resolved destination image pathname without probing it.

        Example:
            >>> store.archive_path.name  # doctest: +SKIP
            'backup.sqsh'


        :return: Configured archive Path, which need not exist before sealing.
        """
        return self._archive_path

    @property
    def staging_root(self) -> pathlib.Path:
        """
        Expose the filesystem driver's staging directory.

        Example:
            >>> store.staging_root.is_dir()  # doctest: +SKIP
            True


        :return: Underlying filesystem root Path.
        """
        return super().root_path

    @property
    def root_path(self) -> pathlib.Path:
        """
        Keep the inherited filesystem root view pointed at staging.

        Example:
            >>> store.root_path == store.staging_root  # doctest: +SKIP
            True


        :return: The staging directory Path, including after sealing.
        """
        return self.staging_root

    @property
    def built_store(self) -> SquashfsReadOnlyStorageBackend | None:
        """
        Expose the archive Store recorded after publication and startup return.

        Example:
            >>> store.built_store is None  # doctest: +SKIP
            True


        :return: Retained SquashfsReadOnlyStorageBackend, or None before it is assigned; None alone does not prove no image was published.
        """
        return self._built_store

    @property
    def characteristics(self) -> StorageCharacteristics:
        """
        Describe committed staging followed by explicit validated archive publication.

        The component-byte field reports the full-key ceiling. These are declared capabilities and
        limitations, not a fresh tool or staging probe. External recursive ingestion still needs a
        cumulative expansion budget.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.STAGING_THEN_SEAL: 'staging_then_seal'>


        :return: Fresh staging-then-seal characteristics with Store-copy space, archival use, effective size limit, and sealing/tool/policy limitations.
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
        """
        Derive an archive display name through the shared path sanitizer.

        Example:
            >>> SquashfsBuildStorageBackend.url_to_name("backup.sqsh") == safe_path_to_name("backup.sqsh")
            True


        :param url: Path-like text passed directly to safe_path_to_name.
        :return: Sanitized name without opening the destination.
        """
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        """
        Start the underlying filesystem Store and decorate its status with build configuration.

        Example:
            >>> store.startup().available  # doctest: +SKIP
            True


        :return: Staging StoreStatus with build details; no archive is built or validated.
        """
        return self._decorate_status(super().startup())

    def probe(self) -> StoreStatus:
        """
        Probe the staging filesystem and attach build-tool/configuration details.

        Example:
            >>> dict(store.probe().details)["mode"]  # doctest: +SKIP
            'staging_then_seal'


        :return: Decorated staging status; the tool lookup does not run mksquashfs.
        """
        return self._decorate_status(super().probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Obtain cached or refreshed filesystem status, then attach current build details.

        Availability and writability continue to describe staging rather than the seal-state guard
        or the published image. The decoration performs a fresh executable lookup even without
        refresh.

        Example:
            >>> dict(store.status().details)["output_archive"] == str(store.archive_path)  # doctest: +SKIP
            True


        :param refresh: True to probe the underlying Store; False to use its cached driver status.
        :return: Decorated staging StoreStatus.
        """
        return self._decorate_status(super().status(refresh=refresh))

    def self_test(self) -> StoreStatus:
        """
        Delegate the compatibility self-test hook to the normal staging probe.

        Example:
            >>> store.self_test().available  # doctest: +SKIP
            True


        :return: Decorated StoreStatus; no full build or candidate validation occurs.
        """
        return self.probe()

    def _decorate_status(self, status: StoreStatus) -> StoreStatus:
        """
        Copy status details, override build-context keys, and sort the resulting pairs.

        Duplicate input keys collapse through dict. Build-tool availability is a which lookup, not a
        command invocation or validation of supported flags. Other status fields are preserved.

        Example:
            >>> decorated = store._decorate_status(status)  # doctest: +SKIP


        :param status: Underlying staging status to copy and annotate.
        :return: Dataclass replacement with mode, paths, codec, deterministic flag, and tool-lookup details.
        """
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
        """
        Check the declared member ceiling, acquire a mutation lease, and begin a filesystem write.

        Underlying startup failures release the lease even for BaseException. A returned wrapper
        enforces offered-size bounds and retains the lease until commit, abort, or context exit.
        Negative expectations, location ownership, collision policy, and hints are delegated to the
        Store/driver.

        Example:
            >>> with store.begin_write(location, expected_size=4) as writer:  # doctest: +SKIP
            ...     writer.write(b"book")
            ...     info = writer.commit()


        :param location: Owned destination Location in staging.
        :param mode: Explicit collision policy passed to the filesystem Store.
        :param expected_size: Expected byte count, or None; a value above the effective member ceiling rejects before lease acquisition.
        :param expected_digest: Optional algorithm/value expectation for accepted payload bytes.
        :param placement_hints: Optional hints forwarded to the underlying Store write boundary.
        :return: Tracked write session; sealing or already sealed state rejects lease acquisition.
        """
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
        """
        Choose a digest destination when omitted, reuse matching staged bytes, or delegate a checked
        write.

        An explicit expected_digest is trusted when looking up existing data: a matching existing
        digest returns before validating the offered bytes, other write arguments, or seal state.
        Otherwise the delegated write checks the payload against that digest. Implicit
        StoreAlreadyExists/StoreIntegrityError failures from the delegated write are translated to
        SquashfsBuildImplicitOverwriteError.

        Example:
            >>> info = store.store_bytes(b"book")  # doctest: +SKIP
            >>> info.location.key.startswith("objects/")  # doctest: +SKIP
            True


        :param data: Payload bytes to hash when no expectation is supplied and to offer when writing.
        :param location: Explicit staging key/Location, or None for a digest-derived destination.
        :param name: Optional filename hint forwarded when a write is needed; it does not replace the chosen key.
        :param metadata: Optional hint source forwarded to the inherited write convenience method.
        :param write_mode: Optional WriteMode or spelling; inherited policy defaults to create-only.
        :param expected_digest: Supplied digest expectation, or None to compute SHA-256 before choosing a destination.
        :param mode: Alternative collision-mode argument; supplying both mode and write_mode rejects even if they agree.
        :return: Existing matching FileInfo or newly committed staged FileInfo.
        """
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
        """
        Hash or accept a file digest, choose a destination, and reuse matching implicit staged data
        when present.

        Deduplication runs only when location is None and can return before source size/content or
        other write arguments are checked. Otherwise the inherited helper stats, opens, and streams
        the source with expectations. A false explicit location selects the digest path but skips
        this deduplication branch. Unlike store_bytes, delegated collision/integrity errors are not
        translated here.

        Example:
            >>> info = store.store_file("source.epub", location="books/a.epub")  # doctest: +SKIP


        :param path: Local source pathname; SHA-256 is computed before the inherited copy unless a digest is supplied.
        :param location: Explicit staging key/Location, or None for a digest-derived destination.
        :param name: Optional filename hint forwarded when a write is needed; it does not replace the chosen key.
        :param metadata: Optional hint source forwarded to the inherited write convenience method.
        :param write_mode: Optional WriteMode or spelling; inherited policy defaults to create-only.
        :param expected_size: Optional required source size checked by the inherited copy when that copy occurs.
        :param expected_digest: Supplied digest expectation, or None to compute SHA-256 before choosing a destination.
        :param mode: Alternative collision-mode argument; supplying both mode and write_mode rejects even if they agree.
        :return: Existing digest-matching FileInfo or the committed staged-file result; source opens are independently owned and closed.
        """
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
        """
        Require a digest for implicit streaming destinations and delegate the bounded staged write.

        This path does not pre-read or deduplicate existing content. A false location value falls
        back to the digest address; an empty string without a digest consequently rejects too. The
        inherited helper borrows the stream at its current position.

        Example:
            >>> info = store.store_stream(source, location="books/a.epub")  # doctest: +SKIP


        :param source: Borrowed readable binary stream; this method does not close it.
        :param location: Explicit staging key/Location, or None to require a digest-derived target.
        :param expected_digest: Optional payload digest; required when no usable explicit target is supplied.
        :param kwargs: Arguments forwarded to Store.store_stream, such as expected_size, name, metadata, write_mode, and mode.
        :return: Committed FileInfo returned by the inherited stream write; duplicate-target policy is delegated.
        """
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
        """
        Delegate a source-file copy into committed staging, using an explicit key or the digest
        layout.

        Example:
            >>> info = store.designate_file("source.epub", archive_path="books/a.epub")  # doctest: +SKIP


        :param source_path: Local source passed to store_file.
        :param archive_path: Explicit internal key, or None for digest-based placement and deduplication.
        :return: Staged FileInfo; the source is not added as a live reference to the later build.
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
        Hold a mutation lease while delegating deletion from staging.

        The lease releases in finally; ownership, missing-object policy, and metadata-version checks
        remain with the filesystem Store.

        Example:
            >>> store.delete(location, missing_ok=True)  # doctest: +SKIP


        :param location: Owned staging Location to remove.
        :param missing_ok: Allow an absent target when the underlying deletion supports it.
        :param if_version: Optional required filesystem-object metadata version.
        :return: None after deletion or accepted absence; sealing/sealed state rejects before delegation.
        """
        with self._mutation_operation():
            super().delete(
                location,
                missing_ok=missing_ok,
                if_version=if_version,
            )

    def copy(self, source: Location, destination: Location, *, mode=WriteMode.CREATE_ONLY):
        """
        Copy a staged object under a mutation lease using the inherited Store implementation.

        The lease is released on return or failure. Native/fallback operation choice and collision
        checks are delegated; this wrapper does not independently apply aggregate sealing limits.

        Example:
            >>> info = store.copy(source_location, destination_location)  # doctest: +SKIP


        :param source: Owned source Location in staging.
        :param destination: Owned destination Location in staging.
        :param mode: Destination collision policy forwarded unchanged.
        :return: Destination FileInfo from the inherited operation.
        """
        with self._mutation_operation():
            return super().copy(source, destination, mode=mode)

    def move(self, source: Location, destination: Location, *, mode=WriteMode.CREATE_ONLY):
        """
        Move a staged object under a mutation lease using the inherited Store implementation.

        The lease is released on return or failure. Native/fallback operation choice and collision
        checks are delegated; this wrapper does not independently apply aggregate sealing limits.

        Example:
            >>> info = store.move(source_location, destination_location)  # doctest: +SKIP


        :param source: Owned source Location in staging.
        :param destination: Owned destination Location in staging.
        :param mode: Destination collision policy forwarded unchanged.
        :return: Destination FileInfo from the inherited operation.
        """
        with self._mutation_operation():
            return super().move(source, destination, mode=mode)

    def seal(
        self,
        *,
        force: bool = False,
        quiet: bool = True,
    ) -> SquashfsReadOnlyStorageBackend:
        """
        Exclude leased mutations, validate a staged snapshot, and publish a separate archive
        candidate.

        The condition lock rejects an existing seal, another seal, or active mutations rather than
        waiting. Preflight hashes every regular staged member. A nonempty candidate must be a
        private regular file, match names/sizes/digests under reader limits, and retain its metadata
        signature through validation. These observations do not prevent external staging edits or
        pathname replacement between checks.

        Without force, hard-link publication rejects a raced destination; force replaces it. Fsync
        and returned-Store construction/startup follow publication and can fail after new bytes are
        visible. Startup status is not checked for availability before built_store is assigned. No
        rollback restores the previous image.

        Finally removes a remaining candidate before clearing the sealing flag and notifying
        waiters. An unlink failure can prevent that state reset and replace an earlier error.
        Successful sealing leaves ordinary builder reads pointed at staging and returns a new
        archive Store with its own UUID.

        Example:
            >>> built = store.seal(force=False)  # doctest: +SKIP
            >>> built.read_file("books/a")  # doctest: +SKIP
            b'book'


        :param force: Allow replacement of an existing output at publication; False uses create-only hard linking.
        :param quiet: Add -quiet to the build command; stdout is discarded in both modes.
        :return: Recorded read-only archive Store after its startup returns; errors can occur before or after publication.
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
        """
        Build at the supplied path with stdin/stdout disabled and bounded retained stderr.

        The configured codec and optional fixed-owner/time/no-xattr flags are passed through. Start
        OSErrors become StoreUnavailable. Missing stderr is rejected before the normal cleanup
        guard. A daemon drainer retains 64 KiB but does not report thread read exceptions to the
        coordinator.

        The process wait uses the configured timeout, followed on expiry by kill and an unbounded
        wait. Finally joins stderr for two seconds and closes its pipe; a live thread or nonzero
        exit rejects afterward. Other wait/cleanup exceptions propagate, without a general
        process-termination guard.

        Example:
            >>> store._run_mksquashfs(candidate, quiet=True)  # doctest: +SKIP


        :param output: Candidate pathname passed directly to mksquashfs.
        :param quiet: Whether to append -quiet; process stdout is always discarded.
        :return: None after a zero exit and stopped diagnostic thread; output existence/content is validated separately.
        """
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
            """
            Drain captured mksquashfs diagnostics and retain only the first 64 KiB.

            Read exceptions escape the daemon thread and are not added to a shared failure result.
            Pipe closure belongs to the coordinator.

            Example:
                >>> drain_stderr()  # doctest: +SKIP


            :return: None at EOF, after updating the captured diagnostic prefix.
            """
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
        """
        Walk staging without following directory symlinks and hash its regular files.

        Every entry counts toward the ceiling, including directories. A directory's scandir results
        are first materialized as a tuple, before individual count checks. Symlinks and other
        non-regular entries reject; canonical key/depth/byte checks apply to files, while
        directories are checked later by candidate inventory. Hard-linked regular files are allowed.

        Each file is checked against member/total budgets, hashed, and restatted; differing
        signatures raise StorePreconditionFailed. This does not freeze the tree or bind pathname
        opens against races. Only directory scandir OSErrors receive the local StoreUnavailable
        translation.

        Example:
            >>> manifest = store._staging_manifest()  # doctest: +SKIP
            >>> manifest["books/a"].size  # doctest: +SKIP
            4


        :return: New mapping of relative file keys to size/SHA-256 evidence; may be empty before seal rejects an empty snapshot.
        """

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
        """
        Use the configured read-only adapter to compare candidate names, sizes, and complete member
        SHA-256.

        Unavailable startup status becomes StoreIntegrityError. The candidate reader applies
        configured topology/expansion policy before inventory comparison and each digest read. The
        validator is closed in finally after construction succeeds; constructor failures occur
        before that guard.

        Example:
            >>> store._validate_candidate(candidate, manifest)  # doctest: +SKIP


        :param candidate: Image pathname to inspect and read.
        :param manifest: Expected relative keys and preflight size/SHA-256 evidence.
        :return: None when inventory and all digests match; differences raise StoreIntegrityError and reader/cleanup errors can propagate.
        """

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
        """
        Allocate a sibling .part pathname, close its descriptor, and unlink it for mksquashfs to
        create.

        The returned pathname is not reserved. Allocation, close, or unlink errors propagate without
        a compensating cleanup guard.

        Example:
            >>> candidate = store._temporary_archive_path()  # doctest: +SKIP
            >>> candidate.exists()  # doctest: +SKIP
            False


        :return: Currently absent candidate Path in the output directory; the caller owns later cleanup.
        """
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
        """
        Replace the destination or hard-link it create-only, then synchronize the visible image and
        parent.

        Create-only publication unlinks the candidate after linking; force uses os.replace.
        Publication can succeed before candidate unlink, image open/fsync, or directory
        synchronization fails. Directory fsync is skipped only when O_DIRECTORY is absent. There is
        no rollback or local error suppression.

        Example:
            >>> store._publish_archive(candidate, force=False)  # doctest: +SKIP


        :param temporary: Validated candidate pathname whose bytes will become the output image.
        :param force: True to replace the destination; False to reject an existing/raced target through os.link.
        :return: None after publication and required synchronization succeed; raised errors can leave the output published.
        """
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
        """
        Form objects/<first-five-digest-characters>/<digest-value> without including the algorithm.

        No existence, key-length, or digest-value validation occurs here; downstream Location
        parsing and staging checks apply their own policy.

        Example:
            >>> store._content_address(Digest("sha256", "abcdef"))  # doctest: +SKIP
            'objects/abcde/abcdef'


        :param digest: Digest supplying the value; None raises StoreUnsupportedOperation.
        :return: Relative content-layout key using the class prefix and bucket-length constants.
        """
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
        """
        Reject sealing/sealed state and increment the active-mutation count under the condition
        lock.

        Example:
            >>> store._acquire_mutation()  # doctest: +SKIP
            >>> store._release_mutation()  # doctest: +SKIP


        :return: None after acquiring one logical lease; no filesystem lock is taken.
        """
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
        """
        Decrement the active-mutation count and notify condition waiters under the lock.

        The caller must hold one lease; this helper does not prevent underflow or repeated release.

        Example:
            >>> store._release_mutation()  # doctest: +SKIP


        :return: None after updating the count and notifying waiters.
        """
        with self._state_condition:
            self._active_mutations -= 1
            self._state_condition.notify_all()

    @contextmanager
    def _mutation_operation(self):
        """
        Acquire one mutation lease for a context and release it in finally after entry succeeds.

        Example:
            >>> with store._mutation_operation():  # doctest: +SKIP
            ...     perform_staged_operation()


        :return: Context manager yielding None while the lease is active; body errors propagate after release.
        """
        self._acquire_mutation()
        try:
            yield
        finally:
            self._release_mutation()

    def close(self) -> None:
        """
        Close the inherited filesystem driver and clean up only internally owned temporary staging.

        Explicit staging is retained. Cleanup is not coordinated with active leases or sealing and
        can raise; the temporary-owner reference clears only after cleanup succeeds. This method
        does not close the retained archive Store or remove a published image.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return: None after delegated closure and any temporary-directory cleanup return.
        """
        super().close()
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None


def _file_digest(path: pathlib.Path) -> Digest:
    """
    Hash a source pathname in 1 MiB binary read requests and close the stream on exit.

    This helper has no independent size limit or before/after identity check; preflight callers
    supply those observations.

    Example:
        >>> digest = _file_digest(pathlib.Path("source.epub"))  # doctest: +SKIP
        >>> digest.algorithm  # doctest: +SKIP
        'sha256'


    :param path: Local file path opened for binary reading.
    :return: Digest containing SHA-256 and the lowercase hex result; file/read errors propagate.
    """
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return Digest("sha256", hasher.hexdigest())


__all__ = ["SquashfsBuildStorageBackend"]
