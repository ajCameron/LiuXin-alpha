"""Discover and catalogue existing SquashFS archives on a local drive.

This workflow deliberately adopts bytes in place.  The source tree becomes a
read-only unmanaged Store, each valid image becomes an immutable SquashFS
Store, and both archive images and regular archive members receive durable
Digital Asset and Replica records through ``StorageManagerAPI``.

The scanner is intentionally narrow.  It recognizes SquashFS by common suffix
or by the on-disk magic and exposes a report/callback boundary that later mess
ingestors can reuse for ISO, RAR, ordinary ebooks, and metadata enrichment.
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
    """One isolated discovery or ingestion failure with its exact context."""

    stage: str
    path: str
    message: str
    error_type: str
    archive_path: str | None = None
    member_path: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsArchiveIngestReport:
    """Bounded result for one discovered SquashFS image."""

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
        """Return whether the complete selected archive was catalogued."""

        return not self.issues and not self.truncated


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsDriveIngestReport:
    """Aggregate, restart-friendly result for one local source tree."""

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
        return sum(archive.ok for archive in self.archives)

    @property
    def archives_failed(self) -> int:
        return len(self.archives) - self.archives_succeeded

    @property
    def members_discovered(self) -> int:
        return sum(archive.members_discovered for archive in self.archives)

    @property
    def member_assets_created(self) -> int:
        return sum(archive.member_assets_created for archive in self.archives)

    @property
    def member_replicas_created(self) -> int:
        return sum(archive.member_replicas_created for archive in self.archives)

    @property
    def ok(self) -> bool:
        return (
            not self.issues
            and not self.truncated
            and all(archive.ok for archive in self.archives)
        )


@dataclasses.dataclass(slots=True)
class _DiscoveryResult:
    archives: list[Path] = dataclasses.field(default_factory=list)
    issues: list[SquashfsDriveIngestIssue] = dataclasses.field(default_factory=list)
    files_examined: int = 0
    non_squashfs_files: int = 0
    skipped_symlinks: int = 0
    truncated: bool = False


class SquashfsDriveIngestWorkflow:
    """Catalogue SquashFS images and their members without copying the bytes.

    A database-backed manager should be used for operational ingestion.  Call
    ``load_from_database()`` before constructing this workflow when reopening
    an existing catalogue so persisted Store configurations are live.
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
        """Scan one existing directory and durably adopt all readable images."""

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
        """Find the SquashFS view backed by the Asset at one Location."""

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
        """Persist the archive Store -> container Asset relationship."""

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
        self._replica_locations_by_store.setdefault(
            location.store_ref, set()
        ).add(location)

    def _require_available(
        self,
        configuration: api.StoreConfiguration,
        *,
        created: bool,
    ) -> None:
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
        try:
            self.manager.remove_store(store_ref, forget_configuration=True)
        except Exception:
            # Preserve the original, more useful construction/probe failure.
            pass

    def _progress(self, event: str, **details: object) -> None:
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
    """Convenience wrapper around :class:`SquashfsDriveIngestWorkflow`."""

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
    return "\0".join(
        part.encode("utf-8", "surrogatepass").hex() for part in parts
    )


def _operation_id(kind: str, *parts: str) -> UUID:
    return uuid5(
        _OPERATION_NAMESPACE,
        _walk_identity_parts(_WORKFLOW_VERSION, kind, *parts),
    )


def _sha256_value(record: api.DigitalAssetRecord) -> str:
    for digest in record.digests:
        if digest.algorithm == "sha256":
            return digest.value
    raise api.StorageIntegrityError(
        f"Digital Asset {record.digital_asset_id} has no SHA-256 identity."
    )


def _archive_metadata(path: Path) -> api.DigitalAssetMetadata:
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
