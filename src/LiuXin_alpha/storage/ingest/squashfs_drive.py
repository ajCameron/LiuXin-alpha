"""
Discover local SquashFS candidates and adopt image/member Locations through a manager.

The workflow creates or reuses source and archive Stores, binds image Assets to
their archive views, and records incremental adoption receipts without copying
bytes. Durability belongs to the supplied manager; no whole-run transaction is
added. Suffix/magic detection is preliminary, and per-stage error continuation
can retain useful partial work. Reports describe observed operations, not an
independent proof of complete enumeration or crash-safe persistence.
"""

from __future__ import annotations

import dataclasses
import mimetypes
import os

from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID, uuid5

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


_SQUASHFS_MAGIC = b"hsqs"
_SQUASHFS_SUFFIXES = frozenset({".sfs", ".sqfs", ".sqsh", ".squashfs"})
_OPERATION_NAMESPACE = UUID("64642314-a830-59ce-9995-f8f744446c29")
_WORKFLOW_VERSION = "squashfs-drive-v1"

ProgressCallback = Callable[[str, Mapping[str, object]], None]
MemberMetadataFactory = Callable[
    [Path, api.StoreInventoryEntry], api.DigitalAssetMetadata
]


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsDriveIngestIssue:
    """
    Retain one discovery/adoption failure with optional archive/member context.

    Frozen fields are descriptive values, not validated paths or a rollback receipt. Error text is
    not generically redacted.

    Example:
        >>> SquashfsDriveIngestIssue('identify', '/drive/book', 'unreadable', 'OSError').stage
        'identify'


    :ivar stage: Workflow boundary that classified the issue.
    :ivar path: Text path supplied at that boundary.
    :ivar message: Recorded diagnostic text, potentially containing raw path/error details.
    :ivar error_type: Exception class name or synthetic limit label.
    :ivar archive_path: Archive context, or None for discovery/identification issues.
    :ivar member_path: Opaque member key, or None when no member is selected.
    """

    stage: str
    path: str
    message: str
    error_type: str
    archive_path: str | None = None
    member_path: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsArchiveIngestReport:
    """
    Describe one archive attempt, its observed creation counters, and any partial failure.

    Fields are unchecked frozen values. Existing-location observations suppress reported creation
    flags even when a manager replays a prior receipt. No all-archive rollback or durability
    assertion is encoded here.

    Example:
        >>> SquashfsArchiveIngestReport('/drive/pack.sfs').ok
        True


    :ivar archive_path: Selected image path as text.
    :ivar store_ref: Archive Store identity, or None if setup failed before a detailed report.
    :ivar store_created: Whether this attempt created the archive Store configuration.
    :ivar archive_digital_asset_id: Adopted image Asset ID, or None if unavailable.
    :ivar archive_replica_id: Adopted source-image Replica ID, or None.
    :ivar archive_asset_created: Manager creation receipt adjusted for an already known image Location.
    :ivar archive_replica_created: Manager replica receipt adjusted for an already known image Location.
    :ivar members_discovered: Inventory entries counted before each member attempt, including failures.
    :ivar member_assets_created: Successful adoption Asset creations after existing-location adjustment.
    :ivar member_replicas_created: Successful adoption Replica creations after existing-location adjustment.
    :ivar member_assets_deduplicated: Successful adoptions not counted as new Assets, including repeated Locations.
    :ivar member_locations_existing: Successful adoptions whose Locations were already in the workflow cache.
    :ivar truncated: Whether an extra entry was observed beyond the configured member count.
    :ivar issues: Ordered reported failures/limit issues; no automatic scrubbing or validation.
    """

    archive_path: str
    store_ref: UUID | None = None
    store_created: bool = False
    archive_digital_asset_id: int | None = None
    archive_replica_id: int | None = None
    archive_asset_created: bool = False
    archive_replica_created: bool = False
    members_discovered: int = 0
    member_assets_created: int = 0
    member_replicas_created: int = 0
    member_assets_deduplicated: int = 0
    member_locations_existing: int = 0
    truncated: bool = False
    issues: tuple[SquashfsDriveIngestIssue, ...] = ()

    @property
    def ok(self) -> bool:
        """
        Test whether the record has neither issues nor member-limit truncation.

        An empty manually constructed record passes; this predicate does not inspect a Store or
        prove that any bytes were adopted.

        Example:
            >>> SquashfsArchiveIngestReport('pack.sfs', truncated=True).ok
            False


        :return: True exactly when issues is empty and truncated is false.
        """

        return not self.issues and not self.truncated


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsDriveIngestReport:
    """
    Aggregate the selected source scan and archive reports without revalidating them.

    Frozen fields do not establish complete filesystem enumeration, persistent manager state, or
    atomic ingestion. Archive failure totals include truncated archive reports.

    Example:
        >>> report = SquashfsDriveIngestReport('/drive', UUID(int=1), False, 0, 0, 0, 0)
        >>> report.ok, report.archives_succeeded
        (True, 0)


    :ivar source_root: Expanded/resolved source directory recorded by the workflow.
    :ivar source_store_ref: Source Store identity established before discovery.
    :ivar source_store_created: Whether this attempt created the source Store configuration.
    :ivar files_examined: Nonsymlink regular files considered for identification.
    :ivar non_squashfs_files: Examined files not accepted as candidates, including handled identify failures.
    :ivar skipped_symlinks: Symlink entries skipped during discovery.
    :ivar archives_discovered: Accepted candidate images retained by discovery.
    :ivar archives: One detailed or fallback report per attempted candidate.
    :ivar issues: Source discovery/identification/limit issues, separate from archive issues.
    :ivar truncated: Whether the archive discovery count was exceeded.
    """

    source_root: str
    source_store_ref: UUID
    source_store_created: bool
    files_examined: int
    non_squashfs_files: int
    skipped_symlinks: int
    archives_discovered: int
    archives: tuple[SquashfsArchiveIngestReport, ...] = ()
    issues: tuple[SquashfsDriveIngestIssue, ...] = ()
    truncated: bool = False

    @property
    def archives_succeeded(self) -> int:
        """
        Count archive records whose ok property is true.

        Example:
            >>> report.archives_succeeded  # doctest: +SKIP


        :return: Number of nontruncated archive reports with no recorded issues.
        """
        return sum(archive.ok for archive in self.archives)

    @property
    def archives_failed(self) -> int:
        """
        Count archive reports that failed their ok predicate, including truncation.

        Example:
            >>> report.archives_failed  # doctest: +SKIP


        :return: Recorded archive count minus archives_succeeded.
        """
        return len(self.archives) - self.archives_succeeded

    @property
    def members_discovered(self) -> int:
        """
        Sum counted member attempts across all archive reports, including failed attempts.

        Example:
            >>> report.members_discovered  # doctest: +SKIP


        :return: Sum of reported members_discovered counters.
        """
        return sum(archive.members_discovered for archive in self.archives)

    @property
    def member_assets_created(self) -> int:
        """
        Sum adjusted new-Asset counters from the archive reports.

        Example:
            >>> report.member_assets_created  # doctest: +SKIP


        :return: Sum of reported member_assets_created values, without querying the manager.
        """
        return sum(archive.member_assets_created for archive in self.archives)

    @property
    def member_replicas_created(self) -> int:
        """
        Sum adjusted new-Replica counters from the archive reports.

        Example:
            >>> report.member_replicas_created  # doctest: +SKIP


        :return: Sum of reported member_replicas_created values, without querying the manager.
        """
        return sum(archive.member_replicas_created for archive in self.archives)

    @property
    def ok(self) -> bool:
        """
        Require no source issues/truncation and successful status on every archive report.

        An empty scan can pass; no independent persistence or completeness check is performed.

        Example:
            >>> report.ok  # doctest: +SKIP


        :return: Whether the stored report fields meet the aggregate success predicate.
        """
        return (
            not self.issues
            and not self.truncated
            and all(archive.ok for archive in self.archives)
        )


@dataclasses.dataclass(slots=True)
class _DiscoveryResult:
    """
    Accumulate candidate paths, counters, and source-scan issues before adoption.

    Each instance owns fresh lists. The scanner mutates these fields without enforcing cross-counter
    invariants in the record itself.

    Example:
        >>> _DiscoveryResult().archives
        []


    :ivar archives: Candidate paths in traversal order until a completed scan sorts them.
    :ivar issues: Discovery, identification, and archive-limit diagnostics.
    :ivar files_examined: Regular files passed to candidate detection.
    :ivar non_squashfs_files: Files rejected by detection, including handled read errors.
    :ivar skipped_symlinks: Symlink entries skipped before type checks.
    :ivar truncated: Whether an extra candidate exceeded max_archives.
    """
    archives: list[Path] = dataclasses.field(default_factory=list)
    issues: list[SquashfsDriveIngestIssue] = dataclasses.field(default_factory=list)
    files_examined: int = 0
    non_squashfs_files: int = 0
    skipped_symlinks: int = 0
    truncated: bool = False


class SquashfsDriveIngestWorkflow:
    """
    Adopt local SquashFS images and their members without copying their bytes.

    Use a database-backed manager with persisted configurations loaded for operational durability;
    an in-memory manager remains valid but transient. The workflow borrows its manager, mutates
    Store/Asset/Replica state incrementally, and adds no all-run transaction. Its Location cache
    survives repeated ingest calls and does not track external deletions or concurrent writers.

    Example:
        >>> report = SquashfsDriveIngestWorkflow(manager).ingest(source_root)  # doctest: +SKIP
    """

    def __init__(
        self,
        manager: api.StorageManagerAPI,
        *,
        recursive: bool = True,
        continue_on_error: bool = True,
        max_archives: int | None = None,
        max_members_per_archive: int | None = None,
        verify_archive_images: bool = False,
        verify_members: bool = False,
        unsquashfs_exe: str = "unsquashfs",
        timeout_s: float = 60.0,
        member_metadata_factory: MemberMetadataFactory | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        """
        Retain the manager, normalize options, and start an empty per-Store Location cache.

        Check positive count/time limits before storing them; integer types and finite numbers are
        not comprehensively enforced. Boolean flags are coerced. No manager load, Store
        construction, scan, or subprocess runs here.

        Example:
            >>> workflow = SquashfsDriveIngestWorkflow(manager, max_archives=10)  # doctest: +SKIP


        :param manager: Caller-owned StorageManagerAPI supplying live Stores and metadata operations.
        :param recursive: Whether discovery descends into nonsymlink subdirectories.
        :param continue_on_error: Whether handled discovery/archive/member failures become report issues.
        :param max_archives: Positive candidate count, or None without an archive-count limit.
        :param max_members_per_archive: Positive per-image entry count, or None without a member-count limit.
        :param verify_archive_images: Forward verification policy to source-image adoption.
        :param verify_members: Forward verification policy to each archive-member adoption.
        :param unsquashfs_exe: Executable selector recorded only on newly created archive configurations.
        :param timeout_s: Positive backend timeout seconds for newly created archive Stores, not a run deadline.
        :param member_metadata_factory: Truthy callback for member metadata, or the built-in hint-based factory.
        :param progress_callback: Optional synchronous observer whose exceptions follow the enclosing call boundary.
        :return: None after storing workflow policy and fresh cache state.
        """
        if max_archives is not None and max_archives <= 0:
            raise ValueError("max_archives must be positive when supplied.")
        if max_members_per_archive is not None and max_members_per_archive <= 0:
            raise ValueError(
                "max_members_per_archive must be positive when supplied."
            )
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive.")
        self.manager = manager
        self.recursive = bool(recursive)
        self.continue_on_error = bool(continue_on_error)
        self.max_archives = max_archives
        self.max_members_per_archive = max_members_per_archive
        self.verify_archive_images = bool(verify_archive_images)
        self.verify_members = bool(verify_members)
        self.unsquashfs_exe = str(unsquashfs_exe)
        self.timeout_s = float(timeout_s)
        self.member_metadata_factory = (
            member_metadata_factory or _default_member_metadata
        )
        self.progress_callback = progress_callback
        self._replica_locations_by_store: dict[UUID, set[api.Location]] = {}

    def ingest(self, source_root: str | os.PathLike[str]) -> SquashfsDriveIngestReport:
        """
        Resolve a source directory, ensure its Store, discover candidates, and adopt each selected
        archive.

        Source setup precedes discovery and can persist even if scanning fails. With continuation
        enabled, archive-level Exceptions become fallback reports; earlier writes remain. The
        manager is neither closed nor reloaded. Most top-level progress callbacks run outside
        failure handling and can abort without a final report. Existing replica-location cache
        entries are retained across calls.

        Example:
            >>> report = workflow.ingest(source_root)  # doctest: +SKIP


        :param source_root: Existing directory expanded with expanduser and resolved without requiring every path component first.
        :return: Aggregate discovery/adoption report after the final complete callback succeeds.
        """

        root = Path(source_root).expanduser().resolve(strict=False)
        if not root.exists():
            raise FileNotFoundError(str(root))
        if not root.is_dir():
            raise NotADirectoryError(str(root))

        source_configuration, source_created = self._ensure_source_store(root)
        discovery = self._discover(root)
        self._progress(
            "discovery_complete",
            source_root=str(root),
            archives_discovered=len(discovery.archives),
            files_examined=discovery.files_examined,
        )
        archive_reports: list[SquashfsArchiveIngestReport] = []
        for ordinal, archive_path in enumerate(discovery.archives, start=1):
            self._progress(
                "archive_started",
                archive_path=str(archive_path),
                archive_number=ordinal,
                archive_count=len(discovery.archives),
            )
            try:
                report = self._ingest_archive(
                    root,
                    source_configuration.store_uuid,
                    archive_path,
                )
            except Exception as error:
                if not self.continue_on_error:
                    raise
                issue = _issue("archive", archive_path, error)
                report = SquashfsArchiveIngestReport(
                    archive_path=str(archive_path),
                    issues=(issue,),
                )
            archive_reports.append(report)
            self._progress(
                "archive_complete",
                archive_path=str(archive_path),
                ok=report.ok,
                members_discovered=report.members_discovered,
                member_replicas_created=report.member_replicas_created,
                issue_count=len(report.issues),
            )

        result = SquashfsDriveIngestReport(
            source_root=str(root),
            source_store_ref=source_configuration.store_uuid,
            source_store_created=source_created,
            files_examined=discovery.files_examined,
            non_squashfs_files=discovery.non_squashfs_files,
            skipped_symlinks=discovery.skipped_symlinks,
            archives_discovered=len(discovery.archives),
            archives=tuple(archive_reports),
            issues=tuple(discovery.issues),
            truncated=discovery.truncated,
        )
        self._progress(
            "complete",
            source_root=str(root),
            ok=result.ok,
            archives_succeeded=result.archives_succeeded,
            archives_failed=result.archives_failed,
            members_discovered=result.members_discovered,
        )
        return result

    def _discover(self, root: Path) -> _DiscoveryResult:
        """
        Walk nonsymlink entries and collect suffix/magic candidates under the configured archive
        limit.

        Each directory is byte-name sorted, but subdirectories use a LIFO stack. A full scan
        globally sorts candidates; an early limit return keeps traversal order. Truncation is
        detected on an extra candidate, not merely reaching the limit. Each directory is
        materialized before scanning, so max_archives is not a memory bound. Symlink/type
        observations and later opens are separate filesystem operations. Handled OSErrors become
        issues; progress exceptions are not caught here.

        Example:
            >>> discovery = workflow._discover(root)  # doctest: +SKIP


        :param root: Source directory from which to begin scanning.
        :return: Mutable candidate/counter/issue accumulator, possibly truncated.
        """
        result = _DiscoveryResult()
        pending = [root]
        while pending:
            directory = pending.pop()
            try:
                with os.scandir(directory) as iterator:
                    entries = sorted(
                        iterator, key=lambda item: os.fsencode(item.name)
                    )
            except OSError as error:
                result.issues.append(_issue("discovery", directory, error))
                if not self.continue_on_error:
                    raise
                continue
            for entry in entries:
                path = Path(entry.path)
                try:
                    if entry.is_symlink():
                        result.skipped_symlinks += 1
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        if self.recursive:
                            pending.append(path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                except OSError as error:
                    result.issues.append(_issue("discovery", path, error))
                    if not self.continue_on_error:
                        raise
                    continue
                result.files_examined += 1
                if self._is_squashfs_candidate(path, result):
                    if (
                        self.max_archives is not None
                        and len(result.archives) >= self.max_archives
                    ):
                        result.truncated = True
                        result.issues.append(
                            SquashfsDriveIngestIssue(
                                stage="discovery_limit",
                                path=str(path),
                                message=(
                                    "archive discovery stopped at configured "
                                    f"limit {self.max_archives}"
                                ),
                                error_type="ArchiveLimitReached",
                            )
                        )
                        return result
                    result.archives.append(path)
                    self._progress(
                        "archive_discovered",
                        archive_path=str(path),
                        files_examined=result.files_examined,
                    )
                else:
                    result.non_squashfs_files += 1
        result.archives.sort(key=lambda path: os.fsencode(str(path)))
        return result

    def _is_squashfs_candidate(self, path: Path, result: _DiscoveryResult) -> bool:
        """
        Accept a recognized suffix without reading, otherwise compare the first four bytes with
        SquashFS magic.

        Suffix matches can be corrupt images. Other read OSErrors are recorded and return False when
        continuation is enabled; the caller then counts them as non-SquashFS. No full archive
        validation or race-free open is performed.

        Example:
            >>> accepted = workflow._is_squashfs_candidate(path, discovery)  # doctest: +SKIP


        :param path: Regular-file candidate observed by discovery.
        :param result: Mutable scan result receiving handled identification errors.
        :return: Whether the suffix or short magic read accepts the path.
        """
        if path.suffix.lower() in _SQUASHFS_SUFFIXES:
            return True
        try:
            with path.open("rb") as source:
                return source.read(len(_SQUASHFS_MAGIC)) == _SQUASHFS_MAGIC
        except OSError as error:
            result.issues.append(_issue("identify", path, error))
            if not self.continue_on_error:
                raise
            return False

    def _ingest_archive(
        self,
        source_root: Path,
        source_store_ref: UUID,
        archive_path: Path,
    ) -> SquashfsArchiveIngestReport:
        """
        Ensure an archive Store, adopt the source image, bind its backing, and adopt selected
        members.

        Archive Store setup errors escape to the outer workflow. Handled image/backing failures do
        not prevent member enumeration; a missing image digest leaves the earlier Store-root
        identity as the operation-ID seed. Member limits are checked after obtaining the next
        inventory entry. Existing Location observations adjust counters but do not skip manager
        adoption.

        Each successful mutation survives subsequent failures. A member progress callback can fail
        after adoption/counter updates and become a member issue. With continuation disabled, caught
        errors are re-raised. The returned report reflects observed receipts, not an independent
        durability or complete-inventory check.

        Example:
            >>> report = workflow._ingest_archive(root, source_store_ref, archive_path)  # doctest: +SKIP


        :param source_root: Resolved source directory used to derive the image key.
        :param source_store_ref: Source Store identity holding the image Location.
        :param archive_path: Selected candidate image beneath the source root.
        :return: Archive report retaining available identities, adjusted counters, and handled issues.
        """
        issues: list[SquashfsDriveIngestIssue] = []
        archive_configuration, store_created = self._ensure_archive_store(
            source_root, source_store_ref, archive_path
        )
        archive_asset_id: int | None = None
        archive_replica_id: int | None = None
        archive_asset_created = False
        archive_replica_created = False
        archive_identity = archive_configuration.store_root_uri

        try:
            source_store = self.manager.get_store(source_store_ref)
            source_key = archive_path.relative_to(source_root).as_posix()
            source_location = source_store.locate(source_key)
            source_info = source_store.stat(source_location)
            archive_location_existed = self._has_replica_at(source_location)
            archive_result = self.manager.adopt_location(
                source_location,
                operation_id=_operation_id(
                    "archive",
                    str(source_store_ref),
                    source_key,
                    source_info.version or str(source_info.size),
                ),
                metadata=_archive_metadata(archive_path),
                replica_mode=api.ReplicaMode.UNMANAGED,
                verify=self.verify_archive_images,
            )
            archive_asset_id = int(
                archive_result.asset_record.digital_asset_id
            )
            archive_replica_id = int(archive_result.replica_record.replica_id)
            archive_asset_created = (
                archive_result.asset_created and not archive_location_existed
            )
            archive_replica_created = (
                archive_result.replica_created and not archive_location_existed
            )
            self._remember_replica(archive_result.replica_record.location)
            archive_identity = _sha256_value(archive_result.asset_record)
        except Exception as error:
            if not self.continue_on_error:
                raise
            issues.append(_issue("archive_asset", archive_path, error))
        else:
            try:
                archive_configuration = self._bind_archive_backing(
                    archive_configuration,
                    archive_result,
                    archive_path,
                )
            except Exception as error:
                if not self.continue_on_error:
                    raise
                issues.append(_issue("archive_backing", archive_path, error))

        members_discovered = 0
        assets_created = 0
        replicas_created = 0
        assets_deduplicated = 0
        locations_existing = 0
        truncated = False
        try:
            archive_store = self.manager.get_store(
                archive_configuration.store_uuid
            )
            entries = archive_store.iter_inventory_entries()
            for entry in entries:
                if (
                    self.max_members_per_archive is not None
                    and members_discovered >= self.max_members_per_archive
                ):
                    truncated = True
                    issues.append(
                        SquashfsDriveIngestIssue(
                            stage="member_limit",
                            path=str(archive_path),
                            archive_path=str(archive_path),
                            message=(
                                "member ingestion stopped at configured limit "
                                f"{self.max_members_per_archive}"
                            ),
                            error_type="MemberLimitReached",
                        )
                    )
                    break
                members_discovered += 1
                try:
                    location_existed = self._has_replica_at(entry.location)
                    member_result = self.manager.adopt_location(
                        entry.location,
                        operation_id=_operation_id(
                            "member",
                            archive_identity,
                            str(archive_configuration.store_uuid),
                            entry.location.key,
                        ),
                        metadata=self.member_metadata_factory(
                            archive_path, entry
                        ),
                        replica_mode=api.ReplicaMode.ARCHIVE,
                        verify=self.verify_members,
                    )
                    asset_created_now = (
                        member_result.asset_created and not location_existed
                    )
                    replica_created_now = (
                        member_result.replica_created and not location_existed
                    )
                    assets_created += int(asset_created_now)
                    replicas_created += int(replica_created_now)
                    assets_deduplicated += int(not asset_created_now)
                    locations_existing += int(location_existed)
                    self._remember_replica(member_result.replica_record.location)
                    self._progress(
                        "member_ingested",
                        archive_path=str(archive_path),
                        member_path=entry.location.key,
                        digital_asset_id=int(
                            member_result.asset_record.digital_asset_id
                        ),
                        replica_id=int(member_result.replica_record.replica_id),
                        asset_created=asset_created_now,
                        replica_created=replica_created_now,
                    )
                except Exception as error:
                    if not self.continue_on_error:
                        raise
                    issues.append(
                        _issue(
                            "member",
                            archive_path,
                            error,
                            member_path=entry.location.key,
                        )
                    )
        except Exception as error:
            if not self.continue_on_error:
                raise
            issues.append(_issue("inventory", archive_path, error))

        return SquashfsArchiveIngestReport(
            archive_path=str(archive_path),
            store_ref=archive_configuration.store_uuid,
            store_created=store_created,
            archive_digital_asset_id=archive_asset_id,
            archive_replica_id=archive_replica_id,
            archive_asset_created=archive_asset_created,
            archive_replica_created=archive_replica_created,
            members_discovered=members_discovered,
            member_assets_created=assets_created,
            member_replicas_created=replicas_created,
            member_assets_deduplicated=assets_deduplicated,
            member_locations_existing=locations_existing,
            truncated=truncated,
            issues=tuple(issues),
        )

    def _ensure_source_store(
        self, root: Path
    ) -> tuple[api.StoreConfiguration, bool]:
        """
        Reuse a compatible Store claiming the source root or create a read-only unmanaged
        declaration.

        Existing filesystem/managed/unmanaged kinds are accepted without rewriting their policies.
        Availability is checked. New construction/probe failures trigger best-effort removal of the
        new configuration, which can itself fail silently.

        Example:
            >>> configuration, created = workflow._ensure_source_store(root)  # doctest: +SKIP


        :param root: Resolved local directory whose file URI selects a configuration.
        :return: Available source configuration and whether this call created it.
        """
        root_uri = root.as_uri()
        existing = self._configuration_for_root(root_uri)
        if existing is not None:
            canonical = DEFAULT_BACKEND_REGISTRY.canonical_kind(
                existing.store_kind
            )
            if canonical not in {
                "filesystem",
                "on_disk_existing_managed_drive",
                "on_disk_existing_unmanaged_drive",
            }:
                raise api.StoragePreconditionFailed(
                    f"Store root {root_uri!r} is already configured as "
                    f"incompatible backend {existing.store_kind!r}."
                )
            self._require_available(existing, created=False)
            return existing, False

        configuration = api.StoreConfiguration.for_backend(
            _store_name("ingest-source", root),
            "on_disk_existing_unmanaged_drive",
            root,
            protocol="file",
            tags=("ingest-source", "unmanaged"),
            modes=(api.ReplicaMode.UNMANAGED,),
            # The portable schema's operational vocabulary predates this
            # workflow; tags carry the more precise ingest-source meaning.
            operational_role="live",
            read_only=True,
            folders=True,
        )
        try:
            self.manager.create_store(configuration, startup=True)
            self._require_available(configuration, created=True)
        except Exception:
            self._forget_failed_store(configuration.store_uuid)
            raise
        return configuration, True

    def _ensure_archive_store(
        self,
        source_root: Path,
        source_store_ref: UUID,
        archive_path: Path,
    ) -> tuple[api.StoreConfiguration, bool]:
        """
        Reuse the image-root or backing-Asset SquashFS Store, otherwise create a read-only archive
        declaration.

        Search the exact canonical root first, then source-Location backing. Existing configuration
        options are retained, even if this workflow requests other tool/time settings. New
        creation/probe failures attempt best-effort configuration cleanup.

        Example:
            >>> configuration, created = workflow._ensure_archive_store(root, source_store_ref, image)  # doctest: +SKIP


        :param source_root: Resolved source directory for relative-key/name construction.
        :param source_store_ref: Source Store used to resolve the image Location on backing lookup.
        :param archive_path: Local image path whose URI initially identifies the archive Store.
        :return: Available SquashFS configuration and whether it was newly created.
        """
        root_uri = archive_path.as_uri()
        existing = self._configuration_for_root(root_uri)
        if existing is None:
            source = self.manager.get_store(source_store_ref)
            source_location = source.locate(
                archive_path.relative_to(source_root).as_posix()
            )
            existing = self._configuration_for_backing_location(
                source_location
            )
        if existing is not None:
            if (
                DEFAULT_BACKEND_REGISTRY.canonical_kind(existing.store_kind)
                != "squashfs_readonly"
            ):
                raise api.StoragePreconditionFailed(
                    f"Archive {root_uri!r} is already configured as "
                    f"backend {existing.store_kind!r}, not SquashFS."
                )
            self._require_available(existing, created=False)
            return existing, False

        relative = archive_path.relative_to(source_root)
        configuration = api.StoreConfiguration.for_backend(
            _store_name("squashfs", relative),
            "squashfs_readonly",
            archive_path,
            protocol="squashfs",
            tags=("ingest-source", "archive", "squashfs"),
            modes=(api.ReplicaMode.ARCHIVE,),
            operational_role="archive",
            read_only=True,
            folders=True,
            options={
                "unsquashfs_exe": self.unsquashfs_exe,
                "timeout_s": self.timeout_s,
            },
        )
        try:
            self.manager.create_store(configuration, startup=True)
            self._require_available(configuration, created=True)
        except Exception:
            self._forget_failed_store(configuration.store_uuid)
            raise
        return configuration, True

    def _configuration_for_backing_location(
        self,
        location: api.Location,
    ) -> api.StoreConfiguration | None:
        """
        Find one SquashFS configuration backed by any nondeleted Replica Asset at the given
        Location.

        Replica states other than DELETED qualify without availability/verification checks. Only
        backing Asset IDs are compared, not preferred Replica IDs. Multiple matching configurations
        are rejected.

        Example:
            >>> configuration = workflow._configuration_for_backing_location(location)  # doctest: +SKIP


        :param location: Exact source Store/Location identity used to select candidate Assets.
        :return: Unique matching configuration, or None; ambiguity raises StoragePreconditionFailed.
        """

        asset_ids = {
            record.digital_asset_id
            for record in self.manager.iter_replica_records(
                store_ref=location.store_ref
            )
            if record.location == location
            and record.state is not api.ReplicaState.DELETED
        }
        matches = tuple(
            configuration
            for configuration in self.manager.iter_store_configurations()
            if configuration.backing is not None
            and configuration.backing.digital_asset_id in asset_ids
            and DEFAULT_BACKEND_REGISTRY.canonical_kind(
                configuration.store_kind
            )
            == "squashfs_readonly"
        )
        if len(matches) > 1:
            raise api.StoragePreconditionFailed(
                f"Multiple SquashFS Stores expose the Asset at {location!r}."
            )
        return matches[0] if matches else None

    def _bind_archive_backing(
        self,
        configuration: api.StoreConfiguration,
        result: api.DigitalAssetIngestResult,
        archive_path: Path,
    ) -> api.StoreConfiguration:
        """
        Point the archive configuration at the adopted Asset and preferred source-image Replica.

        An already equal backing returns unchanged, without repairing other fields. A backing for a
        different Asset is refused. Otherwise replace the root with asset://digital-asset/ID, retain
        the image file URI as store_url, force read-only, and delegate persistence/rebinding to the
        manager without rollback here.

        Example:
            >>> configuration = workflow._bind_archive_backing(configuration, result, image)  # doctest: +SKIP


        :param configuration: Archive Store configuration to retain or replace.
        :param result: Image adoption receipt supplying the Asset and preferred Replica IDs.
        :param archive_path: Local image path retained as the replacement Store URL.
        :return: Original configuration or the manager update result; conflicting backing raises.
        """

        expected = api.StoreBackingReference(
            result.asset_record.digital_asset_id,
            preferred_replica_id=result.replica_record.replica_id,
        )
        if configuration.backing == expected:
            return configuration
        if (
            configuration.backing is not None
            and configuration.backing.digital_asset_id
            != result.asset_record.digital_asset_id
        ):
            raise api.StoragePreconditionFailed(
                "configured SquashFS Store is backed by another Digital Asset."
            )
        replacement = dataclasses.replace(
            configuration,
            store_root_uri=(
                f"asset://digital-asset/"
                f"{int(result.asset_record.digital_asset_id)}"
            ),
            store_url=archive_path.as_uri(),
            backing=expected,
            read_only=True,
        )
        return self.manager.update_store(
            configuration.store_uuid,
            replacement,
        )

    def _configuration_for_root(
        self, root_uri: str
    ) -> api.StoreConfiguration | None:
        """
        Select the unique configuration whose root canonicalizes to the requested local URI.

        Canonicalization can inspect filesystem path resolution; it does not probe Store
        availability. Multiple claims raise StoragePreconditionFailed rather than choosing one.

        Example:
            >>> configuration = workflow._configuration_for_root(root.as_uri())  # doctest: +SKIP


        :param root_uri: Local path/URI or opaque non-file URI used as the comparison target.
        :return: Unique matching configuration, or None when no root matches.
        """
        target = _canonical_local_uri(root_uri)
        matches = tuple(
            configuration
            for configuration in self.manager.iter_store_configurations()
            if _canonical_local_uri(configuration.store_root_uri) == target
        )
        if len(matches) > 1:
            raise api.StoragePreconditionFailed(
                f"Multiple configured Stores claim local root {root_uri!r}."
            )
        return matches[0] if matches else None

    def _has_replica_at(self, location: api.Location) -> bool:
        """
        Check the lazily cached set of nondeleted Replica Locations for one Store.

        Populate once per Store and retain across runs; later external changes are not refreshed.
        States other than DELETED count, without verifying bytes or availability.

        Example:
            >>> existed = workflow._has_replica_at(location)  # doctest: +SKIP


        :param location: Exact Location whose prior presence adjusts creation counters.
        :return: Whether the cached nondeleted-Location set contains the supplied value.
        """
        locations = self._replica_locations_by_store.get(location.store_ref)
        if locations is None:
            locations = {
                record.location
                for record in self.manager.iter_replica_records(
                    store_ref=location.store_ref
                )
                if record.state is not api.ReplicaState.DELETED
            }
            self._replica_locations_by_store[location.store_ref] = locations
        return location in locations

    def _remember_replica(self, location: api.Location) -> None:
        """
        Add an adopted Location to the per-Store observation cache.

        If called before that Store is loaded, this creates only a one-entry set rather than
        fetching all existing Replicas.

        Example:
            >>> workflow._remember_replica(location)  # doctest: +SKIP


        :param location: Location reported by a successful manager adoption.
        :return: None after creating/updating the cached set.
        """
        self._replica_locations_by_store.setdefault(
            location.store_ref, set()
        ).add(location)

    def _require_available(
        self,
        configuration: api.StoreConfiguration,
        *,
        created: bool,
    ) -> None:
        """
        Resolve a Store and require an available refreshed status, optionally rebinding an existing
        declaration.

        Only StoreUnavailable from the initial lookup triggers rebind, and only for an existing
        configuration. Other lookup/status errors propagate. Availability is not a writable-state or
        independent integrity check.

        Example:
            >>> workflow._require_available(configuration, created=False)  # doctest: +SKIP


        :param configuration: Store identity/configuration used for lookup and possible rebind.
        :param created: True to propagate initial unavailability without rebinding a new Store.
        :return: None after an available refreshed status; otherwise propagate/raise StoreUnavailable.
        """
        try:
            store = self.manager.get_store(configuration.store_uuid)
        except api.StoreUnavailable:
            if created:
                raise
            self.manager.update_store(configuration.store_uuid, configuration)
            store = self.manager.get_store(configuration.store_uuid)
        status = store.status(refresh=True)
        if not status.available:
            raise api.StoreUnavailable(
                status.message
                or f"Store {configuration.store_name!r} is unavailable."
            )

    def _forget_failed_store(self, store_ref: UUID) -> None:
        """
        Attempt to remove a failed new Store and its configuration without masking the original
        failure.

        Ordinary cleanup Exceptions are ignored, so absence is not guaranteed. BaseException still
        propagates.

        Example:
            >>> workflow._forget_failed_store(store_ref)  # doctest: +SKIP


        :param store_ref: New Store identity to pass to remove_store with forget_configuration=True.
        :return: None after successful removal or a caught cleanup Exception.
        """
        try:
            self.manager.remove_store(store_ref, forget_configuration=True)
        except Exception:
            # Preserve the original, more useful construction/probe failure.
            pass

    def _progress(self, event: str, **details: object) -> None:
        """
        Synchronously deliver the event and its keyword-detail dictionary to the configured
        observer.

        No copy beyond keyword construction, redaction, exception handling, or event persistence is
        added. The enclosing caller determines whether a raised Exception aborts or becomes an
        issue.

        Example:
            >>> workflow._progress("archive_started", archive_path="pack.sfs")  # doctest: +SKIP


        :param event: Event name forwarded without coercion.
        :param details: Keyword facts passed as one dictionary to the observer.
        :return: None after delivery, or immediately when no observer is configured.
        """
        if self.progress_callback is not None:
            self.progress_callback(event, details)


def ingest_squashfs_drive(
    manager: api.StorageManagerAPI,
    source_root: str | os.PathLike[str],
    *,
    recursive: bool = True,
    continue_on_error: bool = True,
    max_archives: int | None = None,
    max_members_per_archive: int | None = None,
    verify_archive_images: bool = False,
    verify_members: bool = False,
    unsquashfs_exe: str = "unsquashfs",
    timeout_s: float = 60.0,
    member_metadata_factory: MemberMetadataFactory | None = None,
    progress_callback: ProgressCallback | None = None,
) -> SquashfsDriveIngestReport:
    """
    Construct a fresh SquashFS drive workflow and run one source ingestion.

    Each call starts a fresh Location cache but borrows the same caller-owned manager. All policy
    arguments retain the workflow constructor semantics; errors and partial effects are not wrapped
    or rolled back.

    Example:
        >>> report = ingest_squashfs_drive(manager, source_root, max_archives=10)  # doctest: +SKIP


    :param manager: Caller-owned StorageManagerAPI supplying live Stores and metadata operations.
    :param source_root: Existing local directory passed unchanged to the new workflow ingest call.
    :param recursive: Whether discovery descends into nonsymlink subdirectories.
    :param continue_on_error: Whether handled discovery/archive/member failures become report issues.
    :param max_archives: Positive candidate count, or None without an archive-count limit.
    :param max_members_per_archive: Positive per-image entry count, or None without a member-count limit.
    :param verify_archive_images: Forward verification policy to source-image adoption.
    :param verify_members: Forward verification policy to each archive-member adoption.
    :param unsquashfs_exe: Executable selector recorded only on newly created archive configurations.
    :param timeout_s: Positive backend timeout seconds for newly created archive Stores, not a run deadline.
    :param member_metadata_factory: Truthy callback for member metadata, or the built-in hint-based factory.
    :param progress_callback: Optional synchronous observer whose exceptions follow the enclosing call boundary.
    :return: Aggregate report from the workflow after its final progress event succeeds.
    """

    return SquashfsDriveIngestWorkflow(
        manager,
        recursive=recursive,
        continue_on_error=continue_on_error,
        max_archives=max_archives,
        max_members_per_archive=max_members_per_archive,
        verify_archive_images=verify_archive_images,
        verify_members=verify_members,
        unsquashfs_exe=unsquashfs_exe,
        timeout_s=timeout_s,
        member_metadata_factory=member_metadata_factory,
        progress_callback=progress_callback,
    ).ingest(source_root)


def _walk_identity_parts(*parts: str) -> str:
    # ``uuid5`` accepts text but encodes it as strict UTF-8 internally.  Use
    # an ASCII representation so surrogate-escaped POSIX names are both valid
    # inputs and distinct from a literal backslash escape spelling.
    """
    Encode identity components as UTF-8 surrogatepass hex separated by NUL.

    The ASCII representation is accepted by uuid5 and distinguishes surrogate-escaped names from
    literal backslash spellings. It is encoding, not hashing or normalization.

    Example:
        >>> _walk_identity_parts('a', 'b').split(chr(0))
        ['61', '62']


    :param parts: Ordered text identity components, including possible unpaired surrogates.
    :return: NUL-delimited ASCII identity text.
    """
    return "\0".join(
        part.encode("utf-8", "surrogatepass").hex() for part in parts
    )


def _operation_id(kind: str, *parts: str) -> UUID:
    """
    Derive a deterministic UUID5 from workflow version, operation kind, and encoded components.

    Stable inputs reproduce an ID; this is an idempotency label, not content verification or a
    reservation.

    Example:
        >>> _operation_id('member', 'one') == _operation_id('member', 'one')
        True


    :param kind: Operation category included after the workflow version.
    :param parts: Ordered additional identity components encoded without normalization.
    :return: UUID in the fixed SquashFS-drive operation namespace.
    """
    return uuid5(
        _OPERATION_NAMESPACE,
        _walk_identity_parts(_WORKFLOW_VERSION, kind, *parts),
    )


def _sha256_value(record: api.DigitalAssetRecord) -> str:
    """
    Read the first exactly named sha256 digest claim from an Asset record.

    No digest-shape validation or byte hashing occurs here.

    Example:
        >>> digest = _sha256_value(record)  # doctest: +SKIP


    :param record: Asset record whose ordered digests are inspected.
    :return: First matching digest value; absence raises StorageIntegrityError.
    """
    for digest in record.digests:
        if digest.algorithm == "sha256":
            return digest.value
    raise api.StorageIntegrityError(
        f"Digital Asset {record.digital_asset_id} has no SHA-256 identity."
    )


def _archive_metadata(path: Path) -> api.DigitalAssetMetadata:
    """
    Describe the image by its path basename and fixed SquashFS provenance.

    No filesystem read or content detection is performed.

    Example:
        >>> _archive_metadata(Path('/drive/pack.sfs')).media_type
        'application/vnd.squashfs'


    :param path: Image path supplying its name and original_name.
    :return: DigitalAssetMetadata carrying the image name, MIME, and workflow/format attributes.
    """
    return api.DigitalAssetMetadata(
        name=path.name,
        media_type="application/vnd.squashfs",
        original_name=path.name,
        attributes=(
            ("ingest.origin", "squashfs-drive"),
            ("container.format", "squashfs"),
        ),
    )


def _default_member_metadata(
    _archive_path: Path,
    entry: api.StoreInventoryEntry,
) -> api.DigitalAssetMetadata:
    """
    Build member metadata from advisory hints with a POSIX-key basename fallback.

    Prefer the hinted MIME or guess from the selected name. Last hint values override duplicate
    built-in provenance keys through dict construction. The archive path is accepted for callback
    compatibility but unused.

    Example:
        >>> metadata = _default_member_metadata(image, entry)  # doctest: +SKIP


    :param _archive_path: Unused archive path supplied by the workflow callback contract.
    :param entry: Inventory entry supplying Location, suggested filename, MIME, and metadata pairs.
    :return: Metadata with one value per attribute key, without content inspection.
    """
    filename = (
        entry.hints.suggested_filename
        or PurePosixPath(entry.location.key).name
    )
    media_type = entry.hints.media_type or mimetypes.guess_type(filename)[0]
    attributes = [
        ("ingest.origin", "squashfs-drive"),
        ("container.format", "squashfs"),
    ]
    attributes.extend(entry.hints.metadata)
    # Driver metadata is advisory and must not create duplicate attribute keys.
    deduplicated = tuple(dict(attributes).items())
    return api.DigitalAssetMetadata(
        name=filename,
        media_type=media_type,
        original_name=filename,
        attributes=deduplicated,
    )


def _issue(
    stage: str,
    path: Path,
    error: BaseException,
    *,
    member_path: str | None = None,
) -> SquashfsDriveIngestIssue:
    """
    Project an exception and path into an issue record without scrubbing or changing the exception.

    Use the exception class name when its string is empty. Archive context is omitted only for
    discovery and identify stages; other stages reuse path as the archive path.

    Example:
        >>> _issue('identify', Path('book'), OSError('unreadable')).archive_path is None
        True


    :param stage: Boundary label stored verbatim.
    :param path: Path converted to text for the issue and optional archive context.
    :param error: Exception supplying type name and message text.
    :param member_path: Optional member key retained without normalization.
    :return: Frozen issue value; failures while stringifying inputs propagate.
    """
    message = str(error) or type(error).__name__
    return SquashfsDriveIngestIssue(
        stage=stage,
        path=str(path),
        archive_path=(str(path) if stage not in {"discovery", "identify"} else None),
        member_path=member_path,
        message=message,
        error_type=type(error).__name__,
    )


def _canonical_local_uri(value: str) -> str:
    """
    Canonicalize plain paths and local file URIs while retaining other schemes/authorities.

    Decode percent bytes with filesystem surrogateescape semantics, then expand/resolve locally
    without requiring existence. Nonlocal file authorities and non-file schemes pass through
    unchanged; local URI query/fragment data is discarded by path extraction.

    Example:
        >>> _canonical_local_uri('asset://digital-asset/7')
        'asset://digital-asset/7'


    :param value: Path or URI text used for configured-root comparisons.
    :return: Resolved local file URI or unchanged opaque/nonlocal URI text.
    """
    parsed = urlparse(value)
    if parsed.scheme == "file":
        if parsed.netloc not in {"", "localhost"}:
            return value
        path = Path(os.fsdecode(unquote_to_bytes(parsed.path)))
        return path.expanduser().resolve(strict=False).as_uri()
    if parsed.scheme:
        return value
    return Path(value).expanduser().resolve(strict=False).as_uri()


def _store_name(prefix: str, path: Path) -> str:
    """
    Prefix the shared sanitized path name with a workflow role and colon.

    The path helper limits its own name to 160 characters and appends a short hash; the prefix is
    additional and uniqueness is not guaranteed.

    Example:
        >>> _store_name('squashfs', Path('pack.sfs')).startswith('squashfs:')
        True


    :param prefix: Role label prepended without sanitization.
    :param path: Path passed to safe_path_to_name with max_len=160.
    :return: Generated human-readable Store name.
    """
    return f"{prefix}:{safe_path_to_name(path, max_len=160)}"


__all__ = [
    "MemberMetadataFactory",
    "ProgressCallback",
    "SquashfsArchiveIngestReport",
    "SquashfsDriveIngestIssue",
    "SquashfsDriveIngestReport",
    "SquashfsDriveIngestWorkflow",
    "ingest_squashfs_drive",
]
