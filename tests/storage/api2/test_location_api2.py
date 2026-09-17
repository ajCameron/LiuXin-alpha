"""
Verify passive Location values and lazy bound-router handles.

The recording router keeps bytes in memory and pins delegation, fresh observations,
and error visibility. Its simplified versions/capabilities are test fixtures,
not a model of complete backend publication or digest guarantees.
"""

from __future__ import annotations

import dataclasses
import io
import json
import os
import pickle

from collections.abc import Iterator
from typing import BinaryIO
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api


PRIMARY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
ARCHIVE_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")


class _RecordingRouter(api.StorageRouterAPI):
    """
    Record bound-location delegation against in-memory byte payloads. Methods model only the
    behavior needed by these tests: versions encode length, expected digests are logged without
    checking, and iteration uses raw string prefixes. The static capability record is fixture data,
    not evidence of a complete backend implementation.

    Payloads, recorded calls, and availability remain mutable so tests can expose fresh reads and
    error propagation.

    Example:
        >>> router = _RecordingRouter()
        >>> (router.payloads, router.calls, router.available)
        ({}, [], True)
    """
    def __init__(self) -> None:
        """
        Start an available router with empty byte storage and an empty ordered call log. No backend,
        filesystem, or persistent catalogue is created.

        Example:
            >>> router = _RecordingRouter()
            >>> router.calls
            []


        :return: None after initializing the mutable test state.
        """
        self.payloads: dict[api.Location, bytes] = {}
        self.calls: list[tuple[object, ...]] = []
        self.available = True

    def _require_available(self) -> None:
        """
        Reject operations when the test availability switch is false. Checking availability does not
        append a call record.

        Example:
            >>> router = _RecordingRouter()
            >>> router.available = False
            >>> router._require_available()
            Traceback (most recent call last):
            ...
            LiuXin_alpha.storage.api.errors.StorageUnavailable: router is offline


        :return: None while available; otherwise raises StoreUnavailable.
        """
        if not self.available:
            raise api.StoreUnavailable("router is offline")

    def stat(self, location: api.Location) -> api.FileInfo:
        """
        Record a metadata request and report current payload length with a synthetic v-length
        version. Availability is checked before logging. Missing locations raise StoreNotFound
        chained from the dictionary KeyError; no digest is computed.

        Example:
            >>> router = _RecordingRouter()
            >>> location = api.Location(PRIMARY_STORE_UUID, "book")
            >>> router.payloads[location] = b"book"
            >>> router.stat(location).version
            'v4'


        :param location: Exact Location used as the in-memory payload key.
        :return: New FileInfo reflecting the currently stored payload size.
        """
        self._require_available()
        self.calls.append(("stat", location))
        try:
            payload = self.payloads[location]
        except KeyError as error:
            raise api.StoreNotFound(location.key) from error
        return api.FileInfo(location, len(payload), version=f"v{len(payload)}")

    def get(
        self,
        location: api.Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Record a read and return a new BytesIO containing the requested Python slice. Availability
        is checked before logging and missing payloads raise StoreNotFound. Offsets/lengths are not
        validated: ordinary slicing also accepts negative values. The caller owns the returned
        stream.

        Example:
            >>> router = _RecordingRouter()
            >>> location = api.Location(PRIMARY_STORE_UUID, "book")
            >>> router.payloads[location] = b"book"
            >>> with router.get(location, offset=1, length=2) as stream:
            ...     stream.read()
            b'oo'


        :param location: Exact in-memory payload address.
        :param offset: Start byte index applied by slicing, defaulting to zero.
        :param length: Optional second-slice length; None retains the remainder.
        :return: New caller-owned binary stream for the selected bytes.
        """
        self._require_available()
        self.calls.append(("get", location, offset, length))
        try:
            payload = self.payloads[location][offset:]
        except KeyError as error:
            raise api.StoreNotFound(location.key) from error
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def put(
        self,
        location: api.Location,
        source: BinaryIO,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
    ) -> api.FileInfo:
        """
        Record publication arguments, read the source once, and store bytes after fixture checks.
        The source is borrowed and not closed. An expected-size mismatch raises StoreIntegrityError
        before mutation. Identity comparison with CREATE_ONLY or REPLACE enforces their presence
        rules; other mode values impose neither check. expected_digest is recorded but not verified.

        Example:
            >>> router = _RecordingRouter()
            >>> location = api.Location(PRIMARY_STORE_UUID, "book")
            >>> with io.BytesIO(b"book") as source:
            ...     info = router.put(location, source, expected_size=4)
            >>> info.size
            4


        :param location: Destination dictionary key.
        :param source: Borrowed binary stream read once from its current position without a size bound.
        :param mode: Collision policy interpreted by enum-member identity.
        :param expected_size: Optional exact byte count checked before replacing payload state.
        :param expected_digest: Digest argument retained only in the call log for forwarding assertions.
        :return: New FileInfo for stored bytes with a synthetic length-based version.
        """
        self._require_available()
        self.calls.append(
            ("put", location, mode, expected_size, expected_digest)
        )
        payload = source.read()
        if expected_size is not None and len(payload) != expected_size:
            raise api.StoreIntegrityError("size mismatch")
        if mode is api.WriteMode.CREATE_ONLY and location in self.payloads:
            raise api.StoreAlreadyExists(location.key)
        if mode is api.WriteMode.REPLACE and location not in self.payloads:
            raise api.StoreNotFound(location.key)
        self.payloads[location] = payload
        return api.FileInfo(location, len(payload), version=f"v{len(payload)}")

    def delete(
        self,
        location: api.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Record deletion, handle permitted absence, and enforce a synthetic length version before
        removing bytes. Unavailable requests are not logged. Missing locations return early only
        when missing_ok is true; otherwise absence or version mismatch raises the matching Store
        error.

        Example:
            >>> router = _RecordingRouter()
            >>> router.delete(api.Location(PRIMARY_STORE_UUID, "absent"), missing_ok=True)
            >>> len(router.calls)
            1


        :param location: Exact payload key to remove.
        :param missing_ok: Whether an absent key is a successful no-op before version checking.
        :param if_version: Optional v-length value required to match the current payload length.
        :return: None after successful deletion or allowed absence.
        """
        self._require_available()
        self.calls.append(("delete", location, missing_ok, if_version))
        if location not in self.payloads:
            if missing_ok:
                return
            raise api.StoreNotFound(location.key)
        expected_version = f"v{len(self.payloads[location])}"
        if if_version is not None and if_version != expected_version:
            raise api.StorePreconditionFailed(location.key)
        del self.payloads[location]

    def iter_locations(
        self,
        *,
        store_ref: api.StoreUUID | None = None,
        prefix: api.Location | None = None,
    ) -> Iterator[api.Location]:
        """
        Yield current payload keys in dictionary order with optional UUID and raw-prefix filters.
        Iteration neither checks availability nor records calls. The prefix Store UUID and
        path-component boundaries are ignored; concurrent dictionary changes can invalidate
        iteration.

        Example:
            >>> router = _RecordingRouter()
            >>> location = api.Location(PRIMARY_STORE_UUID, "books/5")
            >>> router.payloads[location] = b"book"
            >>> list(router.iter_locations(store_ref=PRIMARY_STORE_UUID)) == [location]
            True


        :param store_ref: Optional exact Store UUID filter, independent of prefix.
        :param prefix: Optional Location whose key is passed to str.startswith.
        :return: Iterator over matching keys in the live payload dictionary.
        """
        for location in self.payloads:
            if store_ref is not None and location.store_ref != store_ref:
                continue
            if prefix is not None and not location.key.startswith(prefix.key):
                continue
            yield location

    def capabilities(self, store_ref: api.StoreUUID) -> api.StoreCapabilities:
        """
        Return static capability fixture data without inspecting Store identity or availability.
        Some advertised properties, such as authoritative stat digests, are not implemented by this
        narrow delegation double.

        Example:
            >>> _RecordingRouter().capabilities(PRIMARY_STORE_UUID).conditional_delete
            True


        :param store_ref: Store UUID accepted for interface compatibility and ignored.
        :return: New writable/ranged/complete-enumeration capability record used by the fixture.
        """
        return api.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            range_reads=True,
            stat_digest_authoritative=True,
            enumeration=api.EnumerationCompleteness.COMPLETE,
        )

    def status(self, store_ref: api.StoreUUID) -> api.StoreStatus:
        """
        Expose the current availability switch as both available and writable. Store identity is
        ignored and no call is logged or external resource probed.

        Example:
            >>> router = _RecordingRouter()
            >>> router.available = False
            >>> router.status(PRIMARY_STORE_UUID).writable
            False


        :param store_ref: Store UUID accepted for interface compatibility and ignored.
        :return: New status whose availability and writability reflect the same test switch.
        """
        return api.StoreStatus(self.available, self.available)


def test_location_is_an_immutable_serializable_opaque_value() -> None:
    """
    Verify a correctly typed Location preserves opaque key spelling, equal-value hashes, pickle
    round trips, and the JSON form of its dataclass mapping. Reassigning key must raise
    FrozenInstanceError; backend path acceptance is not tested.

    Example:
        >>> test_location_is_an_immutable_serializable_opaque_value()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    location = api.Location(ARCHIVE_STORE_UUID, "../opaque//pack:item")

    assert location.key == "../opaque//pack:item"
    assert hash(location) == hash(
        api.Location(ARCHIVE_STORE_UUID, "../opaque//pack:item")
    )
    assert pickle.loads(pickle.dumps(location)) == location
    assert json.loads(json.dumps(dataclasses.asdict(location), default=str)) == {
        "store_ref": str(ARCHIVE_STORE_UUID),
        "key": "../opaque//pack:item",
    }

    with pytest.raises(dataclasses.FrozenInstanceError):
        location.key = "changed"  # type: ignore[misc]


def test_location_exposes_no_path_file_or_backend_operations() -> None:
    """
    Verify plain Location values expose neither filesystem path coercion nor the listed navigation,
    I/O, or cached-metadata operations. This pins the passive address boundary independently of
    bound handles.

    Example:
        >>> test_location_exposes_no_path_file_or_backend_operations()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    location = api.Location(ARCHIVE_STORE_UUID, "objects/42")

    assert not isinstance(location, os.PathLike)
    assert not hasattr(location, "__fspath__")
    for path_or_operation in (
        "parent",
        "parents",
        "joinpath",
        "with_name",
        "open",
        "stat",
        "exists",
        "read_bytes",
        "write_bytes",
        "delete",
        "cached_hash",
        "cached_size",
    ):
        assert not hasattr(location, path_or_operation)


def test_manager_binding_is_lazy_and_keeps_the_plain_location_visible() -> None:
    """
    Verify router binding retains the exact Location without recording I/O. The returned
    BoundLocation exposes UUID/key but none of the asserted filesystem navigation or PathLike
    behavior.

    Example:
        >>> test_manager_binding_is_lazy_and_keeps_the_plain_location_visible()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    router = _RecordingRouter()
    location = api.Location(PRIMARY_STORE_UUID, "objects/42")

    bound = router.bind(location)

    assert isinstance(bound, api.BoundLocation)
    assert bound.location is location
    assert bound.store_ref == PRIMARY_STORE_UUID
    assert bound.key == "objects/42"
    assert router.calls == []
    assert not isinstance(bound, os.PathLike)
    assert not hasattr(bound, "parent")
    assert not hasattr(bound, "joinpath")


def test_bound_location_delegates_reads_and_never_caches_metadata() -> None:
    """
    Verify repeated stat observes replacement payload size and ranged byte/stream reads forward
    exact offset and length values. The ordered router log proves each asserted operation delegated
    to the fixture; the opened read stream is closed by its context manager.

    Example:
        >>> test_bound_location_delegates_reads_and_never_caches_metadata()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    router = _RecordingRouter()
    location = api.Location(PRIMARY_STORE_UUID, "objects/42")
    router.payloads[location] = b"first"
    bound = router.bind(location)

    assert bound.stat().size == 5
    router.payloads[location] = b"replacement"
    assert bound.stat().size == 11
    assert bound.read_bytes(offset=1, length=4) == b"epla"
    with bound.open_read(offset=4, length=3) as source:
        assert source.read() == b"ace"

    assert router.calls == [
        ("stat", location),
        ("stat", location),
        ("get", location, 1, 4),
        ("get", location, 4, 3),
    ]


def test_bound_location_preserves_transactional_write_and_delete_arguments() -> None:
    """
    Verify bound put, write_bytes, and delete forward collision mode, expected size/digest, and
    conditional version. The dummy digest is only logged by this router, so these assertions
    establish argument forwarding rather than digest verification or persistent transaction
    semantics.

    Example:
        >>> test_bound_location_preserves_transactional_write_and_delete_arguments()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    router = _RecordingRouter()
    location = api.Location(PRIMARY_STORE_UUID, "objects/42")
    digest = api.Digest("sha256", "abc123")
    bound = router.bind(location)

    info = bound.put(
        io.BytesIO(b"book"),
        expected_size=4,
        expected_digest=digest,
    )
    assert info.size == 4

    replaced = bound.write_bytes(
        b"replacement",
        mode=api.WriteMode.REPLACE,
        expected_digest=digest,
    )
    assert replaced.size == 11
    bound.delete(if_version="v11")

    assert router.calls == [
        (
            "put",
            location,
            api.WriteMode.CREATE_ONLY,
            4,
            digest,
        ),
        (
            "put",
            location,
            api.WriteMode.REPLACE,
            11,
            digest,
        ),
        ("delete", location, False, "v11"),
    ]


def test_bound_try_stat_suppresses_only_not_found() -> None:
    """
    Verify absent payloads become None/False through try_stat and exists, while an unavailable
    router raises StoreUnavailable through both conveniences.

    Example:
        >>> test_bound_try_stat_suppresses_only_not_found()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    router = _RecordingRouter()
    bound = router.bind(api.Location(PRIMARY_STORE_UUID, "missing"))

    assert bound.try_stat() is None
    assert not bound.exists()

    router.available = False
    with pytest.raises(api.StoreUnavailable):
        bound.try_stat()
    with pytest.raises(api.StoreUnavailable):
        bound.exists()


def test_location_rejects_database_ids_names_and_uuid_strings() -> None:
    """
    Verify Location requires an actual UUID Store reference and rejects integer IDs, names, and
    UUID-shaped text with the UUID TypeError message.

    Example:
        >>> test_location_rejects_database_ids_names_and_uuid_strings()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    for invalid_store_ref in (7, "primary", str(PRIMARY_STORE_UUID)):
        with pytest.raises(TypeError, match="UUID"):
            api.Location(invalid_store_ref, "objects/42")  # type: ignore[arg-type]


def test_location_and_bound_facade_have_segregated_explicit_exports() -> None:
    """
    Verify the public API exposes the canonical Location/StoreUUID values and the manager-owned
    BoundLocation facade. The manager export list must contain unique names.

    Example:
        >>> test_location_and_bound_facade_have_segregated_explicit_exports()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import models
    from LiuXin_alpha.storage.api import storage_manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.location_api import BoundLocation

    assert models.Location is api.Location
    assert models.StoreUUID is api.StoreUUID
    assert storage_manager_api.BoundLocation is BoundLocation is api.BoundLocation
    assert len(storage_manager_api.__all__) == len(set(storage_manager_api.__all__))
