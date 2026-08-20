"""Backend-neutral ingest from any enumerable configured Store."""

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
    """Copy selected objects from any enumerable Store into managed storage.

    The source may be an attached Store UUID or an independent Store instance.
    An omitted destination uses the manager's default Store. Discovery hints
    supply the original filename, media type, provenance, and destination
    placement metadata without requiring backend-specific ingest branches.
    ``extensions=None`` ingests every enumerated object. By default each
    selected inventory entry is inspected with ``stat`` so rich remote object
    metadata is retained; set ``inspect=False`` to favor a cheaper listing-only
    scan. Supplying ``object_staging_directory`` enables retained partial-file
    checkpoints for sources with stable range reads. A prior report's
    ``object_checkpoints`` may be passed as ``resume_checkpoints`` to continue
    without rereading completed prefixes.
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
    """Register selected objects already held by an attached managed Store.

    Adoption does not publish a second copy. The source must be attached to
    ``manager`` because the resulting Replica Location is manager-routed.
    ``inspect`` has the same rich-metadata versus listing-cost trade-off as
    :func:`ingest_store`.
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
    return (
        source
        if isinstance(source, StoreAPI)
        else manager.get_store(_store_ref(source))
    )


def _store_ref(source: StoreIngestSource) -> StoreUUID:
    if isinstance(source, StoreAPI):
        return source.store_ref
    if isinstance(source, StoreConfiguration):
        return source.store_uuid
    return source


def _destination_ref(
    manager: StorageManagerAPI,
    destination: StoreIngestSource | None,
) -> StoreUUID:
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
    if not paged:
        entries = source.iter_inventory_entries(prefix=prefix)
        batch_size = max(1, worker_count * 4)
        while batch := tuple(islice(entries, batch_size)):
            yield batch, None, None, None
        return

    continuation = cursor
    current_snapshot = snapshot_token
    remaining = max_files
    while True:
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
        next_cursor = page.next_cursor
        if next_cursor is not None and next_cursor == continuation:
            raise StoreIntegrityError(
                "Store inventory returned a non-advancing cursor."
            )
        yield page.entries, continuation, next_cursor, page.snapshot_token
        if remaining is not None:
            remaining -= len(page.entries)
            if remaining <= 0:
                return
        if next_cursor is None:
            return
        continuation = next_cursor
        current_snapshot = page.snapshot_token or current_snapshot


def _extensions(values: Iterable[str] | None) -> frozenset[str] | None:
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
