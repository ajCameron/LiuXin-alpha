"""
Exercise advanced Store ingest profiles, version binding, trusted identity, and resume.

Filesystem/SQLite operations use real temporary bytes; the storage manager keeps
records in memory. Small observation and interruption subclasses preserve real
Store behavior while recording calls or injecting one read failure. Remote
backend profile checks make no claim about live FTP, rclone, or S3 transfers.

Checkpoint regressions distinguish successful offset resumption, tampered staging,
source replacement, and strict failure reporting without changing production
ingest policy or the existing assertions.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.ingest.models import StoreIngestCheckpointedError
from LiuXin_alpha.ingest.stores import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
    FtpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import (
    FilesystemStore,
    S3Store,
    SQLiteStore,
)


class _ObservedFilesystemStore(FilesystemStore):
    """
    Record preparation and prepared-open calls while using real filesystem storage.

    Example:
        >>> source = _ObservedFilesystemStore(tmp_path / "source")  # doctest: +SKIP
    """
    def __init__(self, root: Path) -> None:
        """
        Initialize the concrete filesystem Store and empty observation counters.

        Example:
            >>> source = _ObservedFilesystemStore(tmp_path / "source")  # doctest: +SKIP


        :param root: Temporary filesystem root passed to FilesystemStore construction.
        :return: None after updating the test double state.
        """
        super().__init__(root)
        self.prepare_calls: list[bool] = []
        self.prepared_open_calls = 0

    def prepare_ingest(self, info, *, inspect=True):
        """
        Record the truth value of inspect before delegating unchanged preparation arguments.

        Example:
            >>> prepared = source.prepare_ingest(entry, inspect=False)  # doctest: +SKIP


        :param info: Candidate FileInfo or inventory entry forwarded to the real Store.
        :param inspect: Inspection request recorded as bool but passed through without coercion.
        :return: Real Store preparation; the recorded call survives a later delegate failure.
        """
        self.prepare_calls.append(bool(inspect))
        return super().prepare_ingest(info, inspect=inspect)

    def open_prepared_ingest(self, prepared, *, offset=0):
        """
        Count the open attempt before delegating its preparation and byte offset.

        Example:
            >>> stream = source.open_prepared_ingest(prepared, offset=4)  # doctest: +SKIP


        :param prepared: Preparation forwarded to the real filesystem implementation.
        :param offset: Requested starting byte offset, zero by default.
        :return: Real source stream for the caller to close; the counter also records failed attempts.
        """
        self.prepared_open_calls += 1
        return super().open_prepared_ingest(prepared, offset=offset)


class _ObservedManager(TransientStorageManager):
    """
    Count identified-stream ingest attempts while retaining in-memory manager behavior.

    Example:
        >>> manager = _ObservedManager()
        >>> manager.identified_ingests
        0
    """
    def __init__(self, *args, **kwargs) -> None:
        """
        Construct the in-memory manager before initializing the fast-path call counter.

        Example:
            >>> manager = _ObservedManager()
            >>> manager.identified_ingests
            0


        :param args: Positional arguments forwarded to TransientStorageManager.
        :param kwargs: Keyword configuration, including Store registrations and default identity.
        :return: None after updating the test double state.
        """
        super().__init__(*args, **kwargs)
        self.identified_ingests = 0

    def ingest_identified_stream(self, *args, **kwargs):
        """
        Count an identified-stream ingest attempt and return the real manager result.

        Example:
            >>> result = manager.ingest_identified_stream(  # doctest: +SKIP
            ...     source, size_bytes=size, authoritative_digests=(digest,),
            ... )


        :param args: Positional identified-stream arguments forwarded unchanged.
        :param kwargs: Keyword identity, destination, and ingest options forwarded unchanged.
        :return: Result from TransientStorageManager; the increment remains if delegation fails.
        """
        self.identified_ingests += 1
        return super().ingest_identified_stream(*args, **kwargs)


class _DishonestFilesystemStore(FilesystemStore):
    """
    Inject a fabricated authoritative SHA-256 claim into otherwise real preparation.

    Example:
        >>> source = _DishonestFilesystemStore(tmp_path / "source")  # doctest: +SKIP
    """
    def prepare_ingest(self, info, *, inspect=True):
        """
        Replace authoritative digests with an unadvertised claim while retaining observations and
        provenance.

        Example:
            >>> prepared = source.prepare_ingest(entry)  # doctest: +SKIP


        :param info: Candidate forwarded to the real filesystem preparation.
        :param inspect: Whether the concrete preparation should enrich the candidate.
        :return: PreparedIngestObject containing Digest("sha256", "00"); this does not hash source bytes.
        """
        prepared = super().prepare_ingest(info, inspect=inspect)
        return api.PreparedIngestObject(
            prepared.info,
            prepared.read_consistency,
            authoritative_digests=(api.Digest("sha256", "00"),),
            provenance_uri=prepared.provenance_uri,
        )


class _InterruptingStream:
    """
    Wrap an owned read stream with a prefix budget and one synthetic read interruption.

    After the prefix is exhausted, the next read raises once; later reads delegate normally. The
    wrapper does not validate prefix_size or emulate every binary stream operation.

    Example:
        >>> import io
        >>> stream = _InterruptingStream(io.BytesIO(b"abcdef"), prefix_size=3)
        >>> stream.read()
        b'abc'
    """
    def __init__(self, source, *, prefix_size: int) -> None:
        """
        Retain the source and initialize the unvalidated prefix budget and interruption flag.

        Example:
            >>> import io
            >>> stream = _InterruptingStream(io.BytesIO(b"abc"), prefix_size=2)


        :param source: Underlying binary stream whose close operation this wrapper owns.
        :param prefix_size: Initial number of bytes allowed before the synthetic interruption.
        :return: None after updating the test double state.
        """
        self._source = source
        self._remaining = prefix_size
        self._interrupted = False

    def read(self, size=-1):
        """
        Return up to the remaining prefix, raise once after exhaustion, then resume delegated reads.

        A short source read decreases the budget only by bytes actually returned. EOF before
        consuming the full prefix does not itself trigger the synthetic failure; even a zero-length
        read can trigger it after the budget reaches zero.

        Example:
            >>> import io
            >>> stream = _InterruptingStream(io.BytesIO(b"abcdef"), prefix_size=2)
            >>> stream.read(8)
            b'ab'
            >>> stream.read()
            Traceback (most recent call last):
            ...
            OSError: simulated interrupted source read
            >>> stream.read()
            b'cdef'


        :param size: Requested read length; a negative value consumes the remaining prefix before interruption.
        :return: Underlying read result, except for the single OSError injected after the prefix.
        """
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
        """
        Close the retained source without resetting its prefix or interruption state.

        Example:
            >>> import io
            >>> raw = io.BytesIO(b"book")
            >>> _InterruptingStream(raw, prefix_size=2).close()
            >>> raw.closed
            True


        :return: None after updating the test double state.
        """
        self._source.close()

    def __enter__(self):
        """
        Return the wrapper without entering or reading the underlying stream.

        Example:
            >>> import io
            >>> stream = _InterruptingStream(io.BytesIO(), prefix_size=0)
            >>> stream.__enter__() is stream
            True


        :return: This same interruption wrapper.
        """
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """
        Discard the context exception arguments and close the owned source.

        Returning None leaves a body exception unsuppressed; a close failure can replace it.

        Example:
            >>> import io
            >>> raw = io.BytesIO(b"book")
            >>> with _InterruptingStream(raw, prefix_size=4):
            ...     pass
            >>> raw.closed
            True


        :param exc_type: Context-body exception class, or None; unused.
        :param exc: Context-body exception instance, or None; unused.
        :param traceback: Context-body traceback, or None; unused.
        :return: None after close succeeds.
        """
        del exc_type, exc, traceback
        self.close()


class _InterruptOnceFilesystemStore(FilesystemStore):
    """
    Record requested offsets and interrupt only the first successfully opened prepared stream.

    Example:
        >>> source = _InterruptOnceFilesystemStore(tmp_path / "source")  # doctest: +SKIP
    """
    def __init__(self, root: Path) -> None:
        """
        Initialize filesystem storage, an empty offset history, and one pending interruption.

        Example:
            >>> source = _InterruptOnceFilesystemStore(tmp_path / "source")  # doctest: +SKIP


        :param root: Temporary source root supplied to FilesystemStore.
        :return: None after updating the test double state.
        """
        super().__init__(root)
        self.open_offsets: list[int] = []
        self._interrupt_next_read = True

    def open_prepared_ingest(self, prepared, *, offset=0):
        """
        Record the offset and wrap the first successful read stream with a four-byte interruption.

        The pending flag is cleared after opening succeeds, before the wrapper is consumed. Failed
        opens retain the pending interruption; later successful opens return the concrete stream
        directly.

        Example:
            >>> with source.open_prepared_ingest(prepared, offset=4) as stream:  # doctest: +SKIP
            ...     remaining = stream.read()


        :param prepared: Preparation passed to the real version/range checks.
        :param offset: Requested byte offset, recorded before opening.
        :return: First successful stream wrapped for interruption, or a later real stream owned by the caller.
        """
        self.open_offsets.append(offset)
        source = super().open_prepared_ingest(prepared, offset=offset)
        if self._interrupt_next_read:
            self._interrupt_next_read = False
            return _InterruptingStream(source, prefix_size=4)
        return source


def test_ingest_source_capabilities_normalize_and_reject_contradictions() -> None:
    """
    Verify enum/digest-name normalization, duplicate algorithm rejection, and rejection of
    stable-range resume with unguarded reads.

    Example:
        >>> test_ingest_source_capabilities_normalize_and_reject_contradictions()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify that a version-pinned preparation rejects an inventory entry with no version token.

    Example:
        >>> test_prepared_ingest_requires_a_version_for_version_pinning()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Compare FTP, rclone, S3, and SQLite source profiles with their expected conservative
    declarations.

    S3 receives a placeholder client and the remote sources are not read; these assertions validate
    declared profiles rather than live transport behavior. SQLite uses a real temporary database
    path.

    Example:
        >>> test_backend_plugins_advertise_conservative_ingest_profiles(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Exercise real filesystem preparation, an offset read, rejection of unguarded resume, and
    rejection of a stale prepared version after replacement.

    Example:
        >>> test_driver_backed_store_prepares_and_opens_a_stable_resumable_read(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify that SQLite preparation supplies its authoritative digest and the manager takes the
    identified-stream path once while preserving destination bytes.

    The Stores use real temporary storage; manager records and the observed call counter remain in
    memory.

    Example:
        >>> test_sqlite_profile_supplies_authoritative_identity_to_manager_fast_path(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
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

    result = manager.ingest_prepared_object(source, prepared)

    assert manager.identified_ingests == 1
    assert manager.read_file(result.asset_record) == b"identified"


def test_manager_rejects_unadvertised_prepared_identity(tmp_path: Path) -> None:
    """
    Verify that a fabricated authoritative digest absent from the source profile raises
    StorageIntegrityError at the manager ingest boundary.

    Example:
        >>> test_manager_rejects_unadvertised_prepared_identity(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
    source = _DishonestFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    stored = source.store_bytes(b"payload", location="book")
    manager = TransientStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StorageIntegrityError, match="unadvertised"):
        manager.ingest_object_from_store(source, stored)


def test_store_ingest_uses_optional_preparation_without_backend_checks(
    tmp_path: Path,
) -> None:
    """
    Verify one preparation and one prepared-open call through generic Store ingest and check the
    resulting destination bytes.

    Example:
        >>> test_store_ingest_uses_optional_preparation_without_backend_checks(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
    source = _ObservedFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"prepared", location="incoming/book.epub")
    manager = TransientStorageManager(
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
    """
    Interrupt after four real bytes, verify the retained staging prefix and checkpoint size, then
    resume at offset four and verify full destination bytes and staging cleanup.

    Example:
        >>> test_store_ingest_resumes_a_validated_partial_object_checkpoint(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    payload = b"resumable payload"
    source.store_bytes(payload, location="incoming/book.epub")
    manager = TransientStorageManager(
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
    """
    Reject a resumed checkpoint after either staging-byte tampering or source replacement.

    The report distinguishes integrity failure from a stale source precondition and the offset
    history proves no second prepared stream was opened.

    Example:
        >>> test_store_ingest_rejects_an_invalid_object_checkpoint(tmp_path, invalidate)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :param invalidate: Parameterized corruption target: stage changes staged bytes; source replaces the source object.
    :return: None after the stated regression assertions pass.
    """
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    stored = source.store_bytes(b"original payload", location="book.epub")
    manager = TransientStorageManager(
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
    """
    Verify strict ingest raises StoreIngestCheckpointedError carrying the four-byte checkpoint and
    original OSError cause.

    Example:
        >>> test_strict_store_ingest_exposes_its_retained_checkpoint(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated temporary roots for actual source/destination bytes, SQLite files, and resumable staging.
    :return: None after the stated regression assertions pass.
    """
    source = _InterruptOnceFilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"strict payload", location="book.epub")
    manager = TransientStorageManager(
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
