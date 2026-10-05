"""
Copy or adopt enumerated Store objects with bounded batching and optional prefix resume.

Sources expose inventory and read capabilities; manager calls own asset/replica
publication. Page cursors resume enumeration, while object checkpoints retain
locally staged byte prefixes for stable-range sources. These are separate retry
mechanisms. Per-object continuation does not catch setup/enumeration errors or
make the complete run transactional. Caller-owned Stores and the manager are not
closed here; completed writes and retained files can survive later failures.
"""

from __future__ import annotations

import hashlib
import mimetypes
import os

from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import TypeAlias
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4

from LiuXin_alpha.ingest.models import (
    StoreIngestCheckpointedError,
    StoreIngestFailure,
    StoreIngestItem,
    StoreIngestMode,
    StoreIngestObjectCheckpoint,
    StoreIngestReport,
)
from LiuXin_alpha.storage.api import (
    Digest,
    DigitalAssetIngestResult,
    DigitalAssetMetadata,
    FileInfo,
    IngestObjectResume,
    IngestReadConsistency,
    IngestSourceStoreAPI,
    Location,
    PreparedIngestObject,
    ReplicaMode,
    StorageManagerAPI,
    StoragePreconditionFailed,
    StorageHintValue,
    StoragePlacementHints,
    StoreAPI,
    StoreConfiguration,
    StoreInventoryEntry,
    StoreIntegrityError,
    StoreUnsupportedOperation,
    StoreUUID,
)


StoreIngestSource: TypeAlias = StoreAPI | StoreConfiguration | StoreUUID
StoreIngestInfo: TypeAlias = FileInfo | StoreInventoryEntry
StoreMetadataInput: TypeAlias = (
    DigitalAssetMetadata | Callable[[StoreIngestInfo], DigitalAssetMetadata]
)
StorePlacementInput: TypeAlias = (
    StoragePlacementHints
    | Callable[[StoreIngestInfo], StoragePlacementHints | None]
)


# A final circuit breaker for custom or third-party paged Store plugins. Native
# backends should enforce their own tighter protocol-specific limits as well.
MAX_STORE_INGEST_INVENTORY_PAGES = 10_000
MAX_STORE_INGEST_INVENTORY_ENTRIES = 100_000
MAX_STORE_INGEST_CURSOR_CHARS = 4_096


def ingest_store(
    manager: StorageManagerAPI,
    source: StoreIngestSource,
    *,
    destination: StoreIngestSource | None = None,
    prefix: str | Location | None = None,
    extensions: Iterable[str] | None = None,
    metadata: StoreMetadataInput | None = None,
    placement_hints: StorePlacementInput | None = None,
    inspect: bool = True,
    replica_mode: ReplicaMode | str = ReplicaMode.ACTIVE,
    verify: bool = True,
    continue_on_error: bool = True,
    cursor: str | None = None,
    snapshot_token: str | None = None,
    page_size: int | None = None,
    max_files: int | None = None,
    workers: int | None = 1,
    object_staging_directory: str | os.PathLike[str] | None = None,
    resume_checkpoints: Iterable[StoreIngestObjectCheckpoint] = (),
) -> StoreIngestReport:
    """
    Copy selected source objects into the manager's chosen destination Store.

    A Store instance can be independent of the manager; configurations/UUIDs
    resolve attached sources. Explicit destinations must be attached. Filename
    hints select extensions before and after optional preparation/inspection.
    Unsupported stat falls back to listing metadata for non-prepared sources.
    Any paging option requests resumable inventory support; max_files counts
    listed entries, including skipped/failed objects, not successful imports.

    Report items remain in inventory order even with worker threads. With
    continuation enabled, object failures become records; a failed page stops
    after that page and retains its input cursor for retry. Setup/enumeration
    errors propagate and earlier writes are not rolled back. Partial-object
    staging is opt-in and requires stable-range capability for retention/resume.

    Example:
        >>> report = ingest_store(manager, source, extensions={'epub'}, workers=1)  # doctest: +SKIP


    :param manager: Caller-owned manager providing destination selection and publication.
    :param source: Independent Store instance, or attached Store configuration/UUID.
    :param destination: Attached destination selector, or None for the manager default.
    :param prefix: Optional source-relative selector converted by source.locate.
    :param extensions: Case-insensitive suffixes; None selects all, an empty iterable none.
    :param metadata: Shared metadata or per-observation factory; None derives discovery hints.
    :param placement_hints: Shared hints or factory; None/factory None derives defaults.
    :param inspect: Request richer source metadata through preparation or stat.
    :param replica_mode: ReplicaMode or enum value string passed to manager publication.
    :param verify: Manager verification policy for the acquired object.
    :param continue_on_error: Record object Exceptions instead of re-raising them.
    :param cursor: Initial inventory continuation token, or None for the first page.
    :param snapshot_token: Optional source-provided inventory snapshot token.
    :param page_size: Positive requested page length, or None for backend/default limits.
    :param max_files: Positive listed-entry ceiling for this paged call, or None.
    :param workers: Positive worker count, or None for source/destination capability selection.
    :param object_staging_directory: Local prefix-retention directory, or None to disable it.
    :param resume_checkpoints: Prior object checkpoints, requiring the same source and staging root.
    :return: Processed items/failures and enumeration/resume facts, not a full-run atomic receipt.
    :raises ValueError: Source equals destination or scalar/checkpoint settings are invalid.
    :raises StoreUnsupportedOperation: Requested paging/concurrency is not supported.
    """

    source_store = _resolve_source(manager, source)
    destination_ref = _destination_ref(manager, destination)
    if source_store.store_ref == destination_ref:
        raise ValueError(
            "copy ingest source and destination must differ; use adopt_store() "
            + "to register bytes already in managed storage."
        )
    return _ingest_store(
        manager,
        source_store,
        mode=StoreIngestMode.COPY,
        destination_ref=destination_ref,
        prefix=prefix,
        extensions=extensions,
        metadata=metadata,
        placement_hints=placement_hints,
        inspect=inspect,
        replica_mode=ReplicaMode(replica_mode),
        verify=verify,
        continue_on_error=continue_on_error,
        cursor=cursor,
        snapshot_token=snapshot_token,
        page_size=page_size,
        max_files=max_files,
        workers=workers,
        object_staging_directory=object_staging_directory,
        resume_checkpoints=resume_checkpoints,
    )


def adopt_store(
    manager: StorageManagerAPI,
    source: StoreIngestSource,
    *,
    prefix: str | Location | None = None,
    extensions: Iterable[str] | None = None,
    metadata: StoreMetadataInput | None = None,
    inspect: bool = True,
    replica_mode: ReplicaMode | str = ReplicaMode.UNMANAGED,
    verify: bool = False,
    continue_on_error: bool = True,
    cursor: str | None = None,
    snapshot_token: str | None = None,
    page_size: int | None = None,
    max_files: int | None = None,
    workers: int | None = 1,
) -> StoreIngestReport:
    """
    Register source Locations through the manager without publishing a second copy.

    Resolve even a supplied Store instance through manager.get_store, because
    adopted Replica Locations must be manager-routed. Filtering, paging,
    inspection, ordered results, and object-failure handling match copy ingest.
    There is no destination placement or retained-prefix staging in this path.
    Per-object metadata work is not an all-run transaction.

    Example:
        >>> report = adopt_store(manager, source, extensions={'epub'}, verify=False)  # doctest: +SKIP


    :param manager: Caller-owned manager to register observed objects and Replicas.
    :param source: Attached Store instance, configuration, or UUID used for lookup.
    :param prefix: Optional source selector resolved to a Location by the attached Store.
    :param extensions: Normalized suffix filter, or None to select every listed object.
    :param metadata: Shared metadata or observation factory; None derives source hints.
    :param inspect: Request preparation/stat metadata before registering the Location.
    :param replica_mode: Replica classification, defaulting to UNMANAGED for adoption.
    :param verify: Manager adoption verification policy, disabled by default.
    :param continue_on_error: Retain object Exceptions as failures instead of raising.
    :param cursor: Optional starting inventory cursor; supplying one requests paging.
    :param snapshot_token: Optional snapshot identity passed to inventory pages.
    :param page_size: Positive requested page length, or None for backend/default limits.
    :param max_files: Positive listed-entry ceiling, or None; includes skipped objects.
    :param workers: Positive read-worker count, or None for the source recommendation.
    :return: Adoption results and retry state for the inventory portion actually processed.
    :raises StoreUnsupportedOperation: Paging or selected read concurrency is unsupported.
    """

    source_ref = _store_ref(source)
    source_store = manager.get_store(source_ref)
    return _ingest_store(
        manager,
        source_store,
        mode=StoreIngestMode.ADOPT,
        destination_ref=None,
        prefix=prefix,
        extensions=extensions,
        metadata=metadata,
        placement_hints=None,
        inspect=inspect,
        replica_mode=ReplicaMode(replica_mode),
        verify=verify,
        continue_on_error=continue_on_error,
        cursor=cursor,
        snapshot_token=snapshot_token,
        page_size=page_size,
        max_files=max_files,
        workers=workers,
        object_staging_directory=None,
        resume_checkpoints=(),
    )


def _ingest_store(
    manager: StorageManagerAPI,
    source: StoreAPI,
    *,
    mode: StoreIngestMode,
    destination_ref: StoreUUID | None,
    prefix: str | Location | None,
    extensions: Iterable[str] | None,
    metadata: StoreMetadataInput | None,
    placement_hints: StorePlacementInput | None,
    inspect: bool,
    replica_mode: ReplicaMode,
    verify: bool,
    continue_on_error: bool,
    cursor: str | None,
    snapshot_token: str | None,
    page_size: int | None,
    max_files: int | None,
    workers: int | None,
    object_staging_directory: str | os.PathLike[str] | None,
    resume_checkpoints: Iterable[StoreIngestObjectCheckpoint],
) -> StoreIngestReport:
    """
    Process inventory batches with shared filtering, preparation, and outcome accounting.

    Resolve prefix/create staging/index checkpoints/select workers before paging
    validation, so setup refusal can leave a newly created directory. The first
    extension test occurs outside per-object exception handling. Inventory/setup
    errors likewise propagate; continue_on_error applies only within consumption.

    Count a complete batch as scanned before consuming its outcomes. Threaded
    results retain input order, and shutdown waits for submitted work even after
    an error; other objects may already have completed. A recorded failure in
    paged mode finishes that page, retains its input cursor, and stops. The
    report's enumeration field is the source capability, not observed completion.

    Example:
        >>> report = adopt_store(manager, attached_source)  # doctest: +SKIP


    :param manager: Publication/adoption services, possibly shared by worker threads.
    :param source: Resolved Store whose inventory and object reads are consumed.
    :param mode: COPY for acquisition or ADOPT for in-place registration.
    :param destination_ref: Copy destination UUID, or None for adoption.
    :param prefix: Optional selector normalized by the source before enumeration.
    :param extensions: Suffix filter normalized once for both observation stages.
    :param metadata: Shared or callable metadata override, or None for derived values.
    :param placement_hints: Copy-placement override/factory, or None for derived hints.
    :param inspect: Whether preparation/stat should enrich listing observations.
    :param replica_mode: Classification forwarded to manager operations.
    :param verify: Verification flag forwarded to manager operations.
    :param continue_on_error: Whether caught object failures become report records.
    :param cursor: First page cursor, or None.
    :param snapshot_token: Initial snapshot token, or None.
    :param page_size: Optional positive requested page length.
    :param max_files: Optional positive bound on listed entries, not successful objects.
    :param workers: Explicit worker count or None for capability-derived selection.
    :param object_staging_directory: Optional local directory for resumable prefixes.
    :param resume_checkpoints: Checkpoints indexed by source Location before processing.
    :return: Ordered successes/failures, counters, and applicable enumeration retry state.
    """
    selected_extensions = _extensions(extensions)
    source_prefix = None if prefix is None else source.locate(prefix)
    staging_directory = _object_staging_directory(
        object_staging_directory,
    )
    checkpoints = _resume_checkpoints(
        source,
        staging_directory,
        resume_checkpoints,
    )
    worker_count = _worker_count(source, manager, destination_ref, mode, workers)
    paged = any(
        value is not None
        for value in (cursor, snapshot_token, page_size, max_files)
    )
    if paged and not source.capabilities.paged_enumeration:
        raise StoreUnsupportedOperation(
            f"{source.configuration.store_name} does not support resumable inventory pages."
        )
    if page_size is not None and page_size < 1:
        raise ValueError("page_size must be at least one.")
    if max_files is not None and max_files < 1:
        raise ValueError("max_files must be at least one.")
    scanned = 0
    skipped = 0
    items: list[StoreIngestItem] = []
    failures: list[StoreIngestFailure] = []
    resume_cursor = cursor
    observed_snapshot = snapshot_token

    def _consume(
        listed_info: StoreIngestInfo,
    ) -> tuple[StoreIngestItem | None, StoreIngestFailure | None, bool]:
        """
        Filter, prepare, and copy/adopt one entry using this call's captured settings.

        Initial filename filtering is outside the catch. After preparation/stat,
        filter again, derive provenance/metadata, then choose adoption, direct
        acquisition, or checkpointed acquisition. Unsupported stat alone falls
        back to the listing. With continuation enabled, caught Exceptions become
        failures at the originally listed Location; checkpoint wrappers expose
        their underlying cause. Error strings are not sanitized here.

        Example:
            >>> item, failure, skipped = _consume(listed_info)  # doctest: +SKIP


        :param listed_info: One original inventory observation, before optional enrichment.
        :return: Success/failure/skip triple; at most one outcome is populated.
        """
        if not _selected(listed_info, selected_extensions):
            return None, None, True
        try:
            info: StoreIngestInfo = listed_info
            prepared = None
            if isinstance(source, IngestSourceStoreAPI):
                prepared = source.prepare_ingest(
                    listed_info,
                    inspect=inspect,
                )
                info = prepared.info
            elif inspect:
                try:
                    info = source.stat(listed_info.location)
                except StoreUnsupportedOperation:
                    # Some streaming sources cannot authoritatively stat a
                    # chunked object. Their inventory entry is still readable.
                    info = listed_info
            if not _selected(info, selected_extensions):
                return None, None, True
            source_uri = _safe_source_uri(
                prepared.provenance_uri
                if prepared is not None
                else source.location_uri(info.location)
            )
            asset_metadata = _metadata(info, source_uri, metadata)
            if mode is StoreIngestMode.ADOPT:
                result = manager.adopt_location(
                    info.location,
                    metadata=asset_metadata,
                    replica_mode=replica_mode,
                    verify=verify,
                )
            else:
                hints = _placement_hints(
                    info,
                    source_uri,
                    asset_metadata,
                    placement_hints,
                )
                checkpoint = checkpoints.get(info.location)
                if prepared is None:
                    if checkpoint is not None:
                        raise StoragePreconditionFailed(
                            "source no longer supports its object checkpoint."
                        )
                    result = manager.ingest_store_object(
                        source,
                        info,
                        metadata=asset_metadata,
                        placement_hints=hints,
                        preferred_store_ref=destination_ref,
                        replica_mode=replica_mode,
                        verify=verify,
                    )
                elif staging_directory is not None and (
                    checkpoint is not None
                    or _supports_object_checkpoint(source, prepared)
                ):
                    result = _ingest_checkpointed_prepared_object(
                        manager,
                        source,
                        prepared,
                        staging_directory=staging_directory,
                        checkpoint=checkpoint,
                        metadata=asset_metadata,
                        placement_hints=hints,
                        preferred_store_ref=destination_ref,
                        replica_mode=replica_mode,
                        verify=verify,
                    )
                else:
                    result = manager.ingest_prepared_store_object(
                        source,
                        prepared,
                        metadata=asset_metadata,
                        placement_hints=hints,
                        preferred_store_ref=destination_ref,
                        replica_mode=replica_mode,
                        verify=verify,
                    )
            return StoreIngestItem(info, source_uri, result), None, False
        except StoreIngestCheckpointedError as error:
            if not continue_on_error:
                raise
            return None, (
                StoreIngestFailure(
                    listed_info.location,
                    type(error.cause).__name__,
                    str(error.cause),
                    error.checkpoint,
                )
            ), False
        except Exception as error:
            if not continue_on_error:
                raise
            return None, (
                StoreIngestFailure(
                    listed_info.location,
                    type(error).__name__,
                    str(error),
                )
            ), False

    executor = (
        None
        if worker_count == 1
        else ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="store-ingest",
        )
    )
    try:
        for batch, batch_cursor, next_cursor, page_snapshot in _inventory_batches(
            source,
            prefix=source_prefix,
            cursor=cursor,
            snapshot_token=snapshot_token,
            page_size=page_size,
            max_files=max_files,
            worker_count=worker_count,
            paged=paged,
        ):
            scanned += len(batch)
            batch_failure_count = len(failures)
            outcomes = (
                map(_consume, batch)
                if executor is None
                else executor.map(_consume, batch)
            )
            for item, failure, was_skipped in outcomes:
                skipped += int(was_skipped)
                if item is not None:
                    items.append(item)
                if failure is not None:
                    failures.append(failure)
            observed_snapshot = page_snapshot or observed_snapshot
            if len(failures) != batch_failure_count and paged:
                # Retrying this page is safe: manager ingest is content-
                # deduplicating, while advancing would abandon failed entries.
                resume_cursor = batch_cursor
                break
            resume_cursor = next_cursor
    finally:
        if executor is not None:
            executor.shutdown(wait=True)

    return StoreIngestReport(
        mode=mode,
        source_store_ref=source.store_ref,
        destination_store_ref=destination_ref,
        enumeration=source.capabilities.enumeration,
        scanned_files=scanned,
        skipped_files=skipped,
        items=tuple(items),
        failures=tuple(failures),
        next_cursor=resume_cursor if paged else None,
        snapshot_token=observed_snapshot if paged else None,
    )


def _resolve_source(
    manager: StorageManagerAPI,
    source: StoreIngestSource,
) -> StoreAPI:
    """
    Keep an independent Store instance or resolve a configuration/UUID through the manager.

    Example:
        >>> _resolve_source(manager, source) is source  # doctest: +SKIP
        True


    :param manager: Attached-Store lookup service for non-instance selectors.
    :param source: Store instance, configuration, or UUID; instances need not be attached.
    :return: Original instance or manager lookup result, without startup/close handling.
    """
    return (
        source
        if isinstance(source, StoreAPI)
        else manager.get_store(_store_ref(source))
    )


def _store_ref(source: StoreIngestSource) -> StoreUUID:
    """
    Extract an instance/configuration UUID or pass another selector through unchanged.

    The final branch trusts the type contract rather than validating a UUID.

    Example:
        >>> from uuid import UUID
        >>> _store_ref(UUID(int=1)) == UUID(int=1)
        True


    :param source: Store, StoreConfiguration, or already selected UUID.
    :return: Store identity without a manager lookup.
    """
    if isinstance(source, StoreAPI):
        return source.store_ref
    if isinstance(source, StoreConfiguration):
        return source.store_uuid
    return source


def _destination_ref(
    manager: StorageManagerAPI,
    destination: StoreIngestSource | None,
) -> StoreUUID:
    """
    Select the manager default or verify an explicit destination is attached.

    Explicit instances are used only for their UUID; writability/capabilities
    are not probed here. The default branch trusts get_default_store_ref.

    Example:
        >>> _destination_ref(manager, None) == manager.get_default_store_ref()  # doctest: +SKIP
        True


    :param manager: Default-selector and attached-Store lookup owner.
    :param destination: Explicit Store selector, or None for the manager default.
    :return: Selected destination UUID after any required attachment lookup.
    """
    if destination is None:
        return manager.get_default_store_ref()
    store_ref = _store_ref(destination)
    _ = manager.get_store(store_ref)
    return store_ref


def _worker_count(
    source: StoreAPI,
    manager: StorageManagerAPI,
    destination_ref: StoreUUID | None,
    mode: StoreIngestMode,
    requested: int | None,
) -> int:
    """
    Select workers and reject concurrency the participating Stores do not advertise.

    None uses the source recommendation or one; for copy it falls back to one
    if the destination cannot write concurrently. Explicit counts instead fail
    when unsupported. Booleans and counts below one are rejected, but no integer
    coercion is performed. Manager thread-safety is not independently tested.

    Example:
        >>> _worker_count(source, manager, destination_ref, StoreIngestMode.COPY, 1)  # doctest: +SKIP
        1


    :param source: Read concurrency capability provider.
    :param manager: Lookup owner for the selected copy destination.
    :param destination_ref: Required destination UUID in COPY mode; unused for ADOPT.
    :param mode: Whether destination write concurrency also constrains the result.
    :param requested: Positive explicit count, or None for automatic selection.
    :return: Selected worker count without starting threads.
    :raises ValueError: An explicit count is boolean or less than one.
    :raises StoreUnsupportedOperation: Selected parallel reads/writes are unsupported.
    """
    if requested is None:
        count = source.capabilities.concurrency.recommended_parallel_reads or 1
        if mode is StoreIngestMode.COPY:
            assert destination_ref is not None
            destination = manager.get_store(destination_ref)
            if not destination.capabilities.concurrency.concurrent_writes:
                count = 1
    else:
        if isinstance(requested, bool) or requested < 1:
            raise ValueError("workers must be at least one or None.")
        count = requested
    if count > 1 and not source.capabilities.concurrency.concurrent_reads:
        raise StoreUnsupportedOperation(
            f"{source.configuration.store_name} does not support concurrent reads."
        )
    if count > 1 and mode is StoreIngestMode.COPY:
        assert destination_ref is not None
        destination = manager.get_store(destination_ref)
        if not destination.capabilities.concurrency.concurrent_writes:
            raise StoreUnsupportedOperation(
                f"{destination.configuration.store_name} does not support concurrent writes."
            )
    return count


def _object_staging_directory(
    value: str | os.PathLike[str] | None,
) -> Path | None:
    """
    Resolve and create an optional local object-prefix directory.

    New final-directory creation requests mode 0700, subject to platform/umask;
    existing permissions are not tightened. This does not prove ownership,
    reserve filenames, exclude source trees, or prevent later symlink replacement.

    Example:
        >>> _object_staging_directory(None) is None
        True


    :param value: Path expanded/resolved before mkdir, or None to disable staging.
    :return: Resolved existing directory, or None.
    :raises ValueError: The post-creation path observation is not a directory.
    :raises OSError: Resolution or directory creation fails.
    """
    if value is None:
        return None
    directory = Path(value).expanduser().resolve(strict=False)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("object staging directory is not a directory.")
    return directory


def _resume_checkpoints(
    source: StoreAPI,
    staging_directory: Path | None,
    values: Iterable[StoreIngestObjectCheckpoint],
) -> dict[Location, StoreIngestObjectCheckpoint]:
    """
    Index supplied checkpoints after checking their type, source, and unique Location.

    A staging directory is required only when a checkpoint is encountered. Its
    files are not inspected here, nor is stable-range capability checked yet.

    Example:
        >>> _resume_checkpoints(source, None, ())  # doctest: +SKIP
        {}


    :param source: Store identity that every supplied checkpoint must reference.
    :param staging_directory: Selected prefix directory, or None when no resume is allowed.
    :param values: Iterable consumed once, retaining each original checkpoint object.
    :return: New Location-to-checkpoint dictionary with no duplicate keys.
    :raises TypeError: An element is not StoreIngestObjectCheckpoint.
    :raises ValueError: A checkpoint lacks a staging root or repeats a Location.
    :raises StoragePreconditionFailed: A checkpoint belongs to another Store.
    """
    checkpoints: dict[Location, StoreIngestObjectCheckpoint] = {}
    for checkpoint in values:
        if not isinstance(checkpoint, StoreIngestObjectCheckpoint):
            raise TypeError(
                "resume_checkpoints must contain object checkpoints."
            )
        if staging_directory is None:
            raise ValueError(
                "resume_checkpoints require object_staging_directory."
            )
        if checkpoint.source_store_ref != source.store_ref:
            raise StoragePreconditionFailed(
                "object checkpoint belongs to another source Store."
            )
        if checkpoint.source_location in checkpoints:
            raise ValueError(
                "resume_checkpoints contain a duplicate source Location."
            )
        checkpoints[checkpoint.source_location] = checkpoint
    return checkpoints


def _supports_object_checkpoint(
    source: IngestSourceStoreAPI,
    prepared: PreparedIngestObject,
) -> bool:
    """
    Check stable-range declaration and guarded read mode, without probing the object.

    Identity comparisons assume normalized enum fields; this helper does not
    validate prepared metadata or establish that the declared capability works.

    Example:
        >>> can_retain_prefix = _supports_object_checkpoint(source, prepared)  # doctest: +SKIP


    :param source: Prepared-ingest source advertising an object-resume policy.
    :param prepared: Object declaration whose read consistency must not be UNGUARDED.
    :return: True for STABLE_RANGE resume with a guarded prepared object.
    """
    return (
        source.ingest_capabilities.object_resume
        is IngestObjectResume.STABLE_RANGE
        and prepared.read_consistency is not IngestReadConsistency.UNGUARDED
    )


def _ingest_checkpointed_prepared_object(
    manager: StorageManagerAPI,
    source: IngestSourceStoreAPI,
    prepared: PreparedIngestObject,
    *,
    staging_directory: Path,
    checkpoint: StoreIngestObjectCheckpoint | None,
    metadata: DigitalAssetMetadata,
    placement_hints: StoragePlacementHints | None,
    preferred_store_ref: StoreUUID | None,
    replica_mode: ReplicaMode,
    verify: bool,
) -> DigitalAssetIngestResult:
    """
    Acquire a stable source into a local prefix file, then publish through the manager.

    Validate source declarations and any existing checkpoint before acquisition.
    Otherwise create an exclusive mode-0600 random .part file. Append bytes in
    one-MiB reads from the current offset; a fully staged known-size object skips
    reopening the source. Overflow removes the stage before raising; short input,
    non-byte chunks, write failures, and manager failures normally retain it.

    A known size plus authoritative SHA-256 selects ingest_identified_stream;
    other cases use ingest_stream. On caught Exceptions, a remaining file is
    fsynced/hashed into a checkpoint and the cause is wrapped. Checkpoint creation
    can itself fail and mask that cause. Validation, initial creation/stat, and
    BaseException failures are outside this retention catch. Success attempts
    unlink but ignores OSError, so a complete stage may remain after publication.
    No all-operation rollback, filesystem race protection, or staging quota is
    provided here beyond any known object size.

    Example:
        >>> result = _ingest_checkpointed_prepared_object(manager, source, prepared, staging_directory=staging, checkpoint=None, metadata=metadata, placement_hints=None, preferred_store_ref=destination_ref, replica_mode=ReplicaMode.ACTIVE, verify=True)  # doctest: +SKIP


    :param manager: Stream-publication services invoked after local acquisition completes.
    :param source: Prepared-ingest Store declaring stable range reads.
    :param prepared: Validated object observation, read-consistency mode, and digest claims.
    :param staging_directory: Existing local root for random or checkpoint-selected prefix files.
    :param checkpoint: Prior matching prefix declaration, or None to start an exclusive file.
    :param metadata: Asset metadata forwarded to the manager.
    :param placement_hints: Destination placement hints forwarded unchanged.
    :param preferred_store_ref: Preferred copy destination UUID, or None.
    :param replica_mode: Replica classification passed to publication.
    :param verify: Manager verification policy for the complete staged stream.
    :return: Manager receipt after publication and best-effort stage removal.
    :raises StoreIntegrityError: Prepared claims, staged identity, or unretained bytes are invalid.
    :raises StoragePreconditionFailed: Stable resume or checkpoint/source identity is unsupported.
    :raises StoreIngestCheckpointedError: A caught failure leaves a successfully described prefix.
    """
    try:
        source.ingest_capabilities.validate_prepared(prepared)
    except ValueError as error:
        raise StoreIntegrityError(str(error)) from error
    if not _supports_object_checkpoint(source, prepared):
        raise StoragePreconditionFailed(
            "prepared source object does not support stable range resume."
        )
    if checkpoint is not None:
        _require_matching_checkpoint(source, prepared, checkpoint)
        path = staging_directory / checkpoint.staging_name
        _validate_checkpoint_file(path, checkpoint)
    else:
        staging_name = f"liuxin-ingest-{uuid4().hex}.part"
        path = staging_directory / staging_name
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)

    expected_size = prepared.info.size
    offset = path.stat().st_size
    try:
        if expected_size is None or offset < expected_size:
            with source.open_prepared_ingest(
                prepared,
                offset=offset,
            ) as input_stream:
                with path.open("ab") as output_stream:
                    while True:
                        chunk = input_stream.read(1024 * 1024)
                        if not chunk:
                            break
                        if not isinstance(chunk, bytes):
                            raise TypeError(
                                "prepared ingest streams must return bytes."
                            )
                        if (
                            expected_size is not None
                            and offset + len(chunk) > expected_size
                        ):
                            path.unlink(missing_ok=True)
                            raise StoreIntegrityError(
                                "prepared source exceeded its expected size."
                            )
                        accepted = output_stream.write(chunk)
                        if accepted != len(chunk):
                            raise OSError(
                                "object checkpoint staging write was incomplete."
                            )
                        offset += accepted
        if expected_size is not None and offset != expected_size:
            raise StoreIntegrityError(
                f"expected {expected_size} source bytes, staged {offset}."
            )

        with path.open("rb") as staged_stream:
            if (
                expected_size is not None
                and any(
                    digest.algorithm == "sha256"
                    for digest in prepared.authoritative_digests
                )
            ):
                result = manager.ingest_identified_stream(
                    staged_stream,
                    size_bytes=expected_size,
                    authoritative_digests=prepared.authoritative_digests,
                    metadata=metadata,
                    placement_hints=placement_hints,
                    preferred_store_ref=preferred_store_ref,
                    replica_mode=replica_mode,
                    verify=verify,
                )
            else:
                result = manager.ingest_stream(
                    staged_stream,
                    expected_size=expected_size,
                    expected_digests=prepared.authoritative_digests,
                    metadata=metadata,
                    placement_hints=placement_hints,
                    preferred_store_ref=preferred_store_ref,
                    replica_mode=replica_mode,
                    verify=verify,
                )
    except StoreIngestCheckpointedError:
        raise
    except Exception as error:
        if path.exists():
            retained = _checkpoint_for_file(source, prepared, path)
            raise StoreIngestCheckpointedError(retained, error) from error
        raise
    else:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return result


def _require_matching_checkpoint(
    source: IngestSourceStoreAPI,
    prepared: PreparedIngestObject,
    checkpoint: StoreIngestObjectCheckpoint,
) -> None:
    """
    Match checkpoint identity/consistency/version/size against the current prepared object.

    Then ask the source to validate the checkpoint Location. This does not read
    source bytes, compare authoritative digests, or inspect a staging file.

    Example:
        >>> _require_matching_checkpoint(source, prepared, checkpoint)  # doctest: +SKIP


    :param source: Store whose require_location validates ownership after field matching.
    :param prepared: Current source observation and guarded-read declaration.
    :param checkpoint: Prior declaration whose Store, Location, mode, version, and size must match.
    :return: None when every comparison and source Location validation succeeds.
    :raises StoragePreconditionFailed: Any compared checkpoint/source field differs.
    """
    if (
        checkpoint.source_store_ref != prepared.info.location.store_ref
        or checkpoint.source_location != prepared.info.location
        or checkpoint.read_consistency is not prepared.read_consistency
        or checkpoint.source_version != prepared.info.version
        or checkpoint.expected_size != prepared.info.size
    ):
        raise StoragePreconditionFailed(
            "object checkpoint does not match the prepared source version."
        )
    source.require_location(checkpoint.source_location)


def _validate_checkpoint_file(
    path: Path,
    checkpoint: StoreIngestObjectCheckpoint,
) -> None:
    """
    Reject an observed symlink/non-file and compare staged size plus SHA-256 identity.

    Path checks and subsequent open/hash are separate, leaving a replacement
    race. This is not an ownership/permission check or a source-version probe.

    Example:
        >>> _validate_checkpoint_file(staging / checkpoint.staging_name, checkpoint)  # doctest: +SKIP


    :param path: Local prefix path to inspect and read completely.
    :param checkpoint: Expected byte count and digest for the retained prefix.
    :return: None after matching both observed size and exact Digest value.
    :raises StoragePreconditionFailed: The path is a symlink or not a regular file.
    :raises StoreIntegrityError: Observed byte count or digest differs from the checkpoint.
    """
    if path.is_symlink() or not path.is_file():
        raise StoragePreconditionFailed(
            "object checkpoint staging file is missing or unsafe."
        )
    size, digest = _staged_file_identity(path)
    if (
        size != checkpoint.bytes_staged
        or digest != checkpoint.prefix_digest
    ):
        raise StoreIntegrityError(
            "object checkpoint staging file failed integrity validation."
        )


def _checkpoint_for_file(
    source: IngestSourceStoreAPI,
    prepared: PreparedIngestObject,
    path: Path,
) -> StoreIngestObjectCheckpoint:
    """
    Fsync the current stage and describe its read-back identity for later resume.

    Append-open can create a missing file and follows links; callers own path
    safety. Hashing is a subsequent open, not a locked snapshot. Directory state
    is not fsynced, and file/source mutation between observations is not excluded.

    Example:
        >>> checkpoint = _checkpoint_for_file(source, prepared, path)  # doctest: +SKIP


    :param source: Store whose UUID identifies the staged object's owner.
    :param prepared: Current object Location, consistency, version, and expected size.
    :param path: Local staged file, retained rather than removed by this helper.
    :return: Validated checkpoint containing current byte count, SHA-256, and basename.
    """
    with path.open("ab") as staged:
        staged.flush()
        os.fsync(staged.fileno())
    size, digest = _staged_file_identity(path)
    return StoreIngestObjectCheckpoint(
        source_store_ref=source.store_ref,
        source_location=prepared.info.location,
        read_consistency=prepared.read_consistency,
        source_version=prepared.info.version,
        bytes_staged=size,
        prefix_digest=digest,
        staging_name=path.name,
        expected_size=prepared.info.size,
    )


def _staged_file_identity(path: Path) -> tuple[int, Digest]:
    """
    Hash all readable staged bytes in one-MiB chunks while counting their length.

    The path is followed normally without locking, regular-file validation, or
    size/change limits; callers must establish any required safety beforehand.

    Example:
        >>> size, digest = _staged_file_identity(staged_path)  # doctest: +SKIP


    :param path: File opened in binary-read mode and closed after hashing.
    :return: Bytes actually read and their SHA-256 Digest.
    """
    hasher = hashlib.sha256()
    total = 0
    with path.open("rb") as staged:
        while chunk := staged.read(1024 * 1024):
            total += len(chunk)
            hasher.update(chunk)
    return total, Digest("sha256", hasher.hexdigest())


def _inventory_batches(
    source: StoreAPI,
    *,
    prefix: Location | None,
    cursor: str | None,
    snapshot_token: str | None,
    page_size: int | None,
    max_files: int | None,
    worker_count: int,
    paged: bool,
) -> Iterator[
    tuple[
        tuple[StoreIngestInfo, ...],
        str | None,
        str | None,
        str | None,
    ]
]:
    """
    Yield bounded listing batches or validated resumable pages with their cursor context.

    Nonpaged listing batches contain at most max(1, workers*4) entries. Paged
    requests cap limit by remaining max_files, reject overfull responses and
    repeated cursors, and carry forward the last nonempty snapshot. Snapshot
    changes are accepted rather than compared for equality. Empty advancing
    pages are allowed. Global entry/page ceilings are checked before yielding
    each batch/page; earlier yields may already have been ingested on failure.

    max_files counts listed entries, not filter matches. The generator can
    finish at that bound with a non-None next cursor in its last yielded page.
    No deduplication of repeated Locations is performed.

    Example:
        >>> batches = _inventory_batches(source, prefix=None, cursor=None, snapshot_token=None, page_size=None, max_files=None, worker_count=1, paged=False)  # doctest: +SKIP


    :param source: Store implementing ordinary inventory or resumable inventory_page.
    :param prefix: Optional resolved source Location limiting enumeration.
    :param cursor: First-page continuation token, validated only in paged mode.
    :param snapshot_token: Initial snapshot token, validated only in paged mode.
    :param page_size: Caller-validated positive page limit, or None.
    :param max_files: Caller-validated positive listed-entry budget, or None.
    :param worker_count: Worker count used to size nonpaged batches.
    :param paged: Whether to use inventory_page instead of the ordinary iterator.
    :return: Iterator of entries, input cursor, next cursor, and returned snapshot tuples.
    :raises StoreIntegrityError: Token, cursor progress, page length, or global ceilings fail.
    """
    if not paged:
        entries = source.iter_inventory_entries(prefix=prefix)
        batch_size = max(1, worker_count * 4)
        observed_entries = 0
        while batch := tuple(islice(entries, batch_size)):
            observed_entries += len(batch)
            if observed_entries > MAX_STORE_INGEST_INVENTORY_ENTRIES:
                raise StoreIntegrityError(
                    "Store inventory exceeded the configured ingest entry limit."
                )
            yield batch, None, None, None
        return

    continuation = _validated_inventory_token(cursor, label="cursor")
    seen_cursors = {continuation} if continuation is not None else set()
    current_snapshot = _validated_inventory_token(
        snapshot_token,
        label="snapshot token",
    )
    remaining = max_files
    page_count = 0
    observed_entries = 0
    while True:
        page_count += 1
        if page_count > MAX_STORE_INGEST_INVENTORY_PAGES:
            raise StoreIntegrityError(
                "Store inventory exceeded the configured ingest page limit."
            )
        limit = page_size
        if remaining is not None:
            limit = remaining if limit is None else min(limit, remaining)
        page = source.inventory_page(
            prefix=prefix,
            cursor=continuation,
            limit=limit,
            snapshot_token=current_snapshot,
        )
        if limit is not None and len(page.entries) > limit:
            raise StoreIntegrityError(
                "Store inventory page exceeded its requested limit."
            )
        observed_entries += len(page.entries)
        if observed_entries > MAX_STORE_INGEST_INVENTORY_ENTRIES:
            raise StoreIntegrityError(
                "Store inventory exceeded the configured ingest entry limit."
            )
        next_cursor = _validated_inventory_token(
            page.next_cursor,
            label="next cursor",
        )
        page_snapshot = _validated_inventory_token(
            page.snapshot_token,
            label="snapshot token",
        )
        if next_cursor is not None and next_cursor in seen_cursors:
            raise StoreIntegrityError(
                "Store inventory returned a repeated or non-advancing cursor."
            )
        yield page.entries, continuation, next_cursor, page_snapshot
        if remaining is not None:
            remaining -= len(page.entries)
            if remaining <= 0:
                return
        if next_cursor is None:
            return
        seen_cursors.add(next_cursor)
        continuation = next_cursor
        current_snapshot = page_snapshot or current_snapshot


def _validated_inventory_token(
    value: object,
    *,
    label: str,
) -> str | None:
    """
    Accept None or a nonempty, UTF-8-encodable token within the character ceiling.

    The limit counts Python characters, not encoded bytes. Whitespace, NUL,
    and other controls are not otherwise rejected or normalized.

    Example:
        >>> _validated_inventory_token(' page-2 ', label='cursor')
        ' page-2 '


    :param value: Opaque backend/client token to validate without coercing its type.
    :param label: Diagnostic name inserted in raised errors, not token content.
    :return: Original string or None.
    :raises StoreIntegrityError: Value is not a valid bounded Unicode token.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise StoreIntegrityError(
            f"Store inventory returned an invalid {label}."
        )
    if len(value) > MAX_STORE_INGEST_CURSOR_CHARS:
        raise StoreIntegrityError(
            f"Store inventory {label} exceeded the configured size limit."
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise StoreIntegrityError(
            f"Store inventory returned malformed Unicode in its {label}."
        ) from error
    return value


def _extensions(values: Iterable[str] | None) -> frozenset[str] | None:
    """
    Normalize suffix selections by string conversion, trimming, lowercasing, and dot removal.

    Empty normalized entries disappear. None means no filter; an empty result
    selects nothing. Pass a collection of strings: a bare string is iterated as
    characters, not interpreted as one extension.

    Example:
        >>> _extensions([' .EPUB ', '..mobi', '']) == frozenset({'epub', 'mobi'})
        True


    :param values: Extension items, or None to disable extension filtering.
    :return: Frozen normalized suffix set, or None.
    """
    if values is None:
        return None
    return frozenset(
        text
        for value in values
        if (text := str(value).strip().lower().lstrip("."))
    )


def _selected(
    info: StoreIngestInfo,
    extensions: frozenset[str] | None,
) -> bool:
    """
    Match the final suggested-filename suffix against an already normalized filter.

    Backslashes are treated as path separators. Content, Location keys, and
    MIME type do not determine selection. A missing filename fails only when
    a filter exists; None accepts every observation without inspecting hints.

    Example:
        >>> _selected(info, None)  # doctest: +SKIP
        True


    :param info: Inventory/stat observation containing filename hints.
    :param extensions: Lowercase dotless suffix set, or None to accept everything.
    :return: Whether the observation passes this filename-only selection.
    """
    if extensions is None:
        return True
    filename = info.hints.suggested_filename
    if filename is None:
        return False
    suffix = PurePosixPath(
        filename.replace("\\", "/")
    ).suffix.lower().lstrip(".")
    return suffix in extensions


def _metadata(
    info: StoreIngestInfo,
    source_uri: str | None,
    supplied: StoreMetadataInput | None,
) -> DigitalAssetMetadata:
    """
    Use supplied metadata verbatim or derive asset fields and provenance attributes.

    A supplied factory's result is not validated or merged here. Defaults use
    filename/media hints, mimetypes fallback, and a nonblank placement title.
    Keys with recognized sensitive queries become SHA-256 fingerprints; other
    keys remain literal. Caller-provided metadata, source_uri, and hint metadata
    are trusted, so this helper is not a comprehensive secret scrubber.

    Example:
        >>> metadata = _metadata(info, None, None)  # doctest: +SKIP


    :param info: Source observation supplying hints and Location identity.
    :param source_uri: Already filtered provenance URI, or None to omit its attribute.
    :param supplied: Explicit metadata/factory override, or None to derive defaults.
    :return: Original override/factory result, or newly constructed asset metadata.
    """
    if supplied is not None:
        return supplied(info) if callable(supplied) else supplied
    filename = info.hints.suggested_filename
    media_type = info.hints.media_type
    if media_type is None and filename is not None:
        media_type = mimetypes.guess_type(filename)[0]
    placement = _hint_mapping(info.hints.placement_hints)
    title = placement.get("title")
    name = title.strip() if isinstance(title, str) and title.strip() else None
    attributes = [("ingest.source_store_uuid", str(info.location.store_ref))]
    if _contains_sensitive_query(info.location.key):
        attributes.append(
            (
                "ingest.source_location_fingerprint",
                hashlib.sha256(info.location.key.encode("utf-8")).hexdigest(),
            )
        )
    else:
        attributes.append(("ingest.source_location_key", info.location.key))
    if source_uri is not None:
        attributes.append(("ingest.source_uri", source_uri))
    attributes.extend(
        (f"ingest.source_metadata.{name}", value)
        for name, value in info.hints.metadata
    )
    return DigitalAssetMetadata(
        name=name,
        media_type=media_type,
        original_name=filename,
        attributes=tuple(attributes),
    )


def _placement_hints(
    info: StoreIngestInfo,
    source_uri: str | None,
    metadata: DigitalAssetMetadata,
    supplied: StorePlacementInput | None,
) -> StoragePlacementHints:
    """
    Select an explicit placement override or merge source hints with derived routing facts.

    A factory returning None falls back to defaults. Defaults shallow-copy
    source placement hints and overwrite name/type/Store identity plus the chosen
    key-or-fingerprint and optional URI fields. Unoverwritten keys remain, even
    conflicting provenance or secret-bearing hints; filtering is not exhaustive.

    Example:
        >>> hints = _placement_hints(info, None, metadata, None)  # doctest: +SKIP


    :param info: Source observation whose placement hints seed the default mapping.
    :param source_uri: Filtered provenance URI, or None to leave any existing URI hint alone.
    :param metadata: Selected asset metadata supplying original_name and media_type.
    :param supplied: Explicit hints/factory, or None; a factory may decline with None.
    :return: Selected override by reference, or a new merged default dictionary.
    """
    if supplied is not None:
        selected = supplied(info) if callable(supplied) else supplied
        if selected is not None:
            return selected
    hints: dict[str, StorageHintValue] = dict(
        _hint_mapping(info.hints.placement_hints)
    )
    hints.update({
        "original_name": metadata.original_name,
        "media_type": metadata.media_type,
        "source_store_uuid": str(info.location.store_ref),
    })
    if _contains_sensitive_query(info.location.key):
        hints["source_location_fingerprint"] = hashlib.sha256(
            info.location.key.encode("utf-8")
        ).hexdigest()
    else:
        hints["source_location_key"] = info.location.key
    if source_uri is not None:
        hints["source_uri"] = source_uri
    return hints


def _hint_mapping(
    hints: StoragePlacementHints | None,
) -> Mapping[str, StorageHintValue]:
    """
    Adapt optional placement hints without copying an existing mapping.

    Example:
        >>> hints = {'title': 'A book'}
        >>> _hint_mapping(hints) is hints
        True


    :param hints: Mapping, object with to_mapping(), or None.
    :return: Original mapping, to_mapping result, or a fresh empty dictionary.
    """
    if hints is None:
        return {}
    if isinstance(hints, Mapping):
        return hints
    return hints.to_mapping()


_SENSITIVE_QUERY_NAMES = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "credential",
    "key",
    "password",
    "secret",
    "sig",
    "signature",
    "token",
}


def _contains_sensitive_query(value: str) -> bool:
    """
    Detect known credential-like query parameter names without inspecting their values.

    URL query names are percent-decoded, stripped/lowercased, and hyphens become
    underscores. Match the explicit name set or cloud-signing prefixes. Unknown
    names, path/fragment secrets, and userinfo are outside this predicate.

    Example:
        >>> _contains_sensitive_query('https://example.test/book?X-Amz-Signature=abc')
        True


    :param value: Text coerced with str before URL query parsing.
    :return: Whether any parsed query name matches the sensitive-name heuristic.
    :raises ValueError: URL parsing rejects malformed authority syntax.
    """
    query = urlsplit(str(value)).query
    for name, _value in parse_qsl(query, keep_blank_values=True):
        normalized = name.strip().lower().replace("-", "_")
        if (
            normalized in _SENSITIVE_QUERY_NAMES
            or normalized.startswith("x_amz_")
            or normalized.startswith("x_goog_")
            or normalized.startswith("x_ms_")
        ):
            return True
    return False


def _safe_source_uri(uri: str | None) -> str | None:
    """
    Suppress provenance URIs containing userinfo or a recognized sensitive query key.

    Accepted values are returned unchanged: schemes, hosts, paths, fragments,
    and unknown query names are not normalized or comprehensively vetted.
    A rejected URI is omitted, not partially redacted.

    Example:
        >>> _safe_source_uri('https://reader:secret@example.test/book') is None
        True


    :param uri: Candidate provenance URI, or None when unavailable.
    :return: Original accepted URI or None.
    :raises ValueError: URL parsing rejects malformed authority syntax.
    """
    if uri is None:
        return None
    parsed = urlsplit(uri)
    if parsed.username is not None or parsed.password is not None:
        return None
    return None if _contains_sensitive_query(uri) else uri


__all__ = [
    "StoreIngestSource",
    "StoreIngestInfo",
    "StoreMetadataInput",
    "StorePlacementInput",
    "adopt_store",
    "ingest_store",
]
