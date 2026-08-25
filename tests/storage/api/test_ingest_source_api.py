"""Contracts for the optional advanced Store ingest-source API."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.ingest import StoreIngestCheckpointedError, ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
    FtpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import (
    FilesystemStore,
    S3Store,
    SQLiteStore,
)


class _ObservedFilesystemStore(FilesystemStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.prepare_calls: list[bool] = []
        self.prepared_open_calls = 0

    def prepare_ingest(self, info, *, inspect=True):
        self.prepare_calls.append(bool(inspect))
        return super().prepare_ingest(info, inspect=inspect)

    def open_prepared_ingest(self, prepared, *, offset=0):
        self.prepared_open_calls += 1
        return super().open_prepared_ingest(prepared, offset=offset)


class _ObservedManager(InMemoryStorageManager):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.identified_ingests = 0

    def ingest_identified_stream(self, *args, **kwargs):
        self.identified_ingests += 1
        return super().ingest_identified_stream(*args, **kwargs)


class _DishonestFilesystemStore(FilesystemStore):
    def prepare_ingest(self, info, *, inspect=True):
        prepared = super().prepare_ingest(info, inspect=inspect)
        return api.PreparedIngestObject(
            prepared.info,
            prepared.read_consistency,
            authoritative_digests=(api.Digest("sha256", "00"),),
            provenance_uri=prepared.provenance_uri,
        )


class _InterruptingStream:
    def __init__(self, source, *, prefix_size: int) -> None:
        self._source = source
        self._remaining = prefix_size
        self._interrupted = False

    def read(self, size=-1):
        if self._remaining:
            requested = self._remaining if size < 0 else min(
                size,
                self._remaining,
            )
            chunk = self._source.read(requested)
            self._remaining -= len(chunk)
            return chunk
        if not self._interrupted:
            self._interrupted = True
            raise OSError("simulated interrupted source read")
        return self._source.read(size)

    def close(self) -> None:
        self._source.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.close()


class _InterruptOnceFilesystemStore(FilesystemStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.open_offsets: list[int] = []
        self._interrupt_next_read = True

    def open_prepared_ingest(self, prepared, *, offset=0):
        self.open_offsets.append(offset)
        source = super().open_prepared_ingest(prepared, offset=offset)
        if self._interrupt_next_read:
            self._interrupt_next_read = False
            return _InterruptingStream(source, prefix_size=4)
        return source


def test_ingest_source_capabilities_normalize_and_reject_contradictions() -> None:
    profile = api.IngestSourceCapabilities(
        read_consistency="version_pinned",
        object_delivery="streaming",
        inventory_resume="cursor",
        object_resume="stable_range",
        authoritative_digest_algorithms=("SHA256", "md5"),
        metadata_availability="inspection",
    )

    assert profile.read_consistency is api.IngestReadConsistency.VERSION_PINNED
    assert profile.object_delivery is api.IngestObjectDelivery.STREAMING
    assert profile.inventory_resume is api.IngestInventoryResume.CURSOR
    assert profile.object_resume is api.IngestObjectResume.STABLE_RANGE
    assert profile.authoritative_digest_algorithms == ("sha256", "md5")
    assert (
        profile.metadata_availability
        is api.IngestMetadataAvailability.INSPECTION
    )

    with pytest.raises(ValueError, match="stable reads"):
        api.IngestSourceCapabilities(
            api.IngestReadConsistency.UNGUARDED,
            api.IngestObjectDelivery.STREAMING,
            object_resume=api.IngestObjectResume.STABLE_RANGE,
        )
    with pytest.raises(ValueError, match="unique"):
        api.IngestSourceCapabilities(
            api.IngestReadConsistency.VERSION_PINNED,
            api.IngestObjectDelivery.STREAMING,
            authoritative_digest_algorithms=("SHA256", "sha256"),
        )


def test_prepared_ingest_requires_a_version_for_version_pinning() -> None:
    entry = api.StoreInventoryEntry(
        api.Location(uuid4(), "incoming/book.epub")
    )

    with pytest.raises(ValueError, match="requires an object version"):
        api.PreparedIngestObject(
            entry,
            api.IngestReadConsistency.VERSION_PINNED,
        )


def test_backend_plugins_advertise_conservative_ingest_profiles(
    tmp_path: Path,
) -> None:
    ftp = FtpReadOnlyStorageBackend("ftp://example.com/library")
    rclone = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(max_http_requests_per_hour=0),
    )
    s3 = S3Store("s3://library", client=object())
    sqlite = SQLiteStore(tmp_path / "profile.sqlite")

    assert ftp.ingest_capabilities == api.IngestSourceCapabilities(
        read_consistency=api.IngestReadConsistency.UNGUARDED,
        object_delivery=api.IngestObjectDelivery.DISK_SPOOLED,
        metadata_availability=api.IngestMetadataAvailability.INSPECTION,
    )
    assert rclone.ingest_capabilities == api.IngestSourceCapabilities(
        read_consistency=api.IngestReadConsistency.UNGUARDED,
        object_delivery=api.IngestObjectDelivery.STREAMING,
        authoritative_digest_algorithms=("sha256", "sha1", "md5"),
        metadata_availability=api.IngestMetadataAvailability.INSPECTION,
    )
    assert s3.ingest_capabilities == api.IngestSourceCapabilities(
        read_consistency=api.IngestReadConsistency.VERSION_PINNED,
        object_delivery=api.IngestObjectDelivery.STREAMING,
        inventory_resume=api.IngestInventoryResume.CURSOR,
        object_resume=api.IngestObjectResume.STABLE_RANGE,
        authoritative_digest_algorithms=("sha256",),
        metadata_availability=api.IngestMetadataAvailability.INSPECTION,
    )
    assert sqlite.ingest_capabilities == api.IngestSourceCapabilities(
        read_consistency=api.IngestReadConsistency.VERSION_PINNED,
        object_delivery=api.IngestObjectDelivery.MEMORY_BUFFERED,
        object_resume=api.IngestObjectResume.STABLE_RANGE,
        authoritative_digest_algorithms=("sha256",),
    )


def test_driver_backed_store_prepares_and_opens_a_stable_resumable_read(
    tmp_path: Path,
) -> None:
    store = FilesystemStore(tmp_path / "source")
    stored = store.store_bytes(b"abcdefgh", location="incoming/book.epub")
    [entry] = list(store.iter_inventory_entries())

    assert isinstance(store, api.IngestSourceStoreAPI)
    profile = store.ingest_capabilities
    assert profile.read_consistency is api.IngestReadConsistency.VERSION_PINNED
    assert profile.object_delivery is api.IngestObjectDelivery.STREAMING
    assert profile.object_resume is api.IngestObjectResume.STABLE_RANGE

    prepared = store.prepare_ingest(entry)
    assert isinstance(prepared.info, api.FileInfo)
    assert prepared.info.location == stored.location
    assert prepared.read_consistency is api.IngestReadConsistency.VERSION_PINNED
    assert prepared.provenance_uri == store.location_uri(stored.location)
    with store.open_prepared_ingest(prepared, offset=3) as source:
        assert source.read() == b"defgh"

    unguarded = store.prepare_ingest(
        api.StoreInventoryEntry(stored.location),
        inspect=False,
    )
    assert unguarded.read_consistency is api.IngestReadConsistency.UNGUARDED
    with pytest.raises(api.StoreUnsupportedOperation, match="stable ingest"):
        store.open_prepared_ingest(unguarded, offset=3)

    store.store_bytes(
        b"replacement",
        location=stored.location,
        write_mode="replace",
    )
    with pytest.raises(api.StorePreconditionFailed):
        store.open_prepared_ingest(prepared)


def test_sqlite_profile_supplies_authoritative_identity_to_manager_fast_path(
    tmp_path: Path,
) -> None:
    source = SQLiteStore(tmp_path / "source.sqlite")
    destination = FilesystemStore(tmp_path / "destination")
    stored = source.store_bytes(b"identified", location="book")
    manager = _ObservedManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    profile = source.ingest_capabilities
    assert profile.object_delivery is api.IngestObjectDelivery.MEMORY_BUFFERED
    assert profile.authoritative_digest_algorithms == ("sha256",)
    prepared = source.prepare_ingest(source.stat_file(stored))
    assert prepared.authoritative_digests == (prepared.info.digest,)

    result = manager.ingest_store_object(source, prepared.info)

    assert manager.identified_ingests == 1
    assert manager.read_file(result.asset_record) == b"identified"


def test_manager_rejects_unadvertised_prepared_identity(tmp_path: Path) -> None:
    source = _DishonestFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    stored = source.store_bytes(b"payload", location="book")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StorageIntegrityError, match="unadvertised"):
        manager.ingest_store_object(source, stored)


def test_store_ingest_uses_optional_preparation_without_backend_checks(
    tmp_path: Path,
) -> None:
    source = _ObservedFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"prepared", location="incoming/book.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    assert source.prepare_calls == [True]
    assert source.prepared_open_calls == 1
    assert manager.read_file(report.items[0].result.asset_record) == b"prepared"


def test_store_ingest_resumes_a_validated_partial_object_checkpoint(
    tmp_path: Path,
) -> None:
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    payload = b"resumable payload"
    source.store_bytes(payload, location="incoming/book.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    staging_directory = tmp_path / "object-checkpoints"

    first = ingest_store(
        manager,
        source,
        object_staging_directory=staging_directory,
    )

    assert not first.ok and first.ingested_files == 0
    [checkpoint] = first.object_checkpoints
    assert checkpoint.bytes_staged == 4
    assert checkpoint.expected_size == len(payload)
    assert (staging_directory / checkpoint.staging_name).read_bytes() == b"resu"
    assert source.open_offsets == [0]

    second = ingest_store(
        manager,
        source,
        object_staging_directory=staging_directory,
        resume_checkpoints=first.object_checkpoints,
    )

    assert second.ok and second.ingested_files == 1
    assert second.object_checkpoints == ()
    assert source.open_offsets == [0, 4]
    assert manager.read_file(second.items[0].result.asset_record) == payload
    assert list(staging_directory.iterdir()) == []


@pytest.mark.parametrize("invalidate", ["stage", "source"])
def test_store_ingest_rejects_an_invalid_object_checkpoint(
    tmp_path: Path,
    invalidate: str,
) -> None:
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    stored = source.store_bytes(b"original payload", location="book.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    staging_directory = tmp_path / "object-checkpoints"
    first = ingest_store(
        manager,
        source,
        object_staging_directory=staging_directory,
    )
    [checkpoint] = first.object_checkpoints

    if invalidate == "stage":
        (staging_directory / checkpoint.staging_name).write_bytes(b"evil")
        expected_error = "StorageIntegrityError"
    else:
        source.store_bytes(
            b"replacement payload",
            location=stored.location,
            write_mode="replace",
        )
        expected_error = "StoragePreconditionFailed"

    second = ingest_store(
        manager,
        source,
        object_staging_directory=staging_directory,
        resume_checkpoints=first.object_checkpoints,
    )

    assert not second.ok and second.ingested_files == 0
    assert second.failures[0].error_type == expected_error
    assert source.open_offsets == [0]


def test_strict_store_ingest_exposes_its_retained_checkpoint(
    tmp_path: Path,
) -> None:
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"strict payload", location="book.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(StoreIngestCheckpointedError) as failure:
        ingest_store(
            manager,
            source,
            object_staging_directory=tmp_path / "checkpoints",
            continue_on_error=False,
        )

    assert failure.value.checkpoint.bytes_staged == 4
    assert isinstance(failure.value.cause, OSError)
