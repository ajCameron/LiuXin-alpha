"""Contracts for durable Locations and short-lived bound handles."""

from __future__ import annotations

import dataclasses
import io
import json
import os
import pickle

from collections.abc import Iterator
from typing import BinaryIO

import pytest

import LiuXin_alpha.storage.api2 as api2


class _RecordingRouter(api2.StorageRouterAPI):
    def __init__(self) -> None:
        self.payloads: dict[api2.Location, bytes] = {}
        self.calls: list[tuple[object, ...]] = []
        self.available = True

    def _require_available(self) -> None:
        if not self.available:
            raise api2.StoreUnavailable("router is offline")

    def stat(self, location: api2.Location) -> api2.FileInfo:
        self._require_available()
        self.calls.append(("stat", location))
        try:
            payload = self.payloads[location]
        except KeyError as error:
            raise api2.StoreNotFound(location.key) from error
        return api2.FileInfo(location, len(payload), version=f"v{len(payload)}")

    def get(
        self,
        location: api2.Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        self._require_available()
        self.calls.append(("get", location, offset, length))
        try:
            payload = self.payloads[location][offset:]
        except KeyError as error:
            raise api2.StoreNotFound(location.key) from error
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def put(
        self,
        location: api2.Location,
        source: BinaryIO,
        *,
        mode: api2.WriteMode = api2.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api2.Digest | None = None,
    ) -> api2.FileInfo:
        self._require_available()
        self.calls.append(
            ("put", location, mode, expected_size, expected_digest)
        )
        payload = source.read()
        if expected_size is not None and len(payload) != expected_size:
            raise api2.StoreIntegrityError("size mismatch")
        if mode is api2.WriteMode.CREATE_ONLY and location in self.payloads:
            raise api2.StoreAlreadyExists(location.key)
        if mode is api2.WriteMode.REPLACE and location not in self.payloads:
            raise api2.StoreNotFound(location.key)
        self.payloads[location] = payload
        return api2.FileInfo(location, len(payload), version=f"v{len(payload)}")

    def delete(
        self,
        location: api2.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        self._require_available()
        self.calls.append(("delete", location, missing_ok, if_version))
        if location not in self.payloads:
            if missing_ok:
                return
            raise api2.StoreNotFound(location.key)
        expected_version = f"v{len(self.payloads[location])}"
        if if_version is not None and if_version != expected_version:
            raise api2.StorePreconditionFailed(location.key)
        del self.payloads[location]

    def iter_locations(
        self,
        *,
        store_ref: api2.StoreRef | None = None,
        prefix: api2.Location | None = None,
    ) -> Iterator[api2.Location]:
        for location in self.payloads:
            if store_ref is not None and location.store_ref != store_ref:
                continue
            if prefix is not None and not location.key.startswith(prefix.key):
                continue
            yield location

    def capabilities(self, store_ref: api2.StoreRef) -> api2.StoreCapabilities:
        return api2.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            atomic_publish=True,
            range_reads=True,
            authoritative_digest=False,
            enumeration=api2.EnumerationCompleteness.COMPLETE,
        )

    def status(self, store_ref: api2.StoreRef) -> api2.StoreStatus:
        return api2.StoreStatus(self.available, self.available)


def test_location_is_an_immutable_serializable_opaque_value() -> None:
    location = api2.Location("archive", "../opaque//pack:item")

    assert location.key == "../opaque//pack:item"
    assert hash(location) == hash(api2.Location("archive", "../opaque//pack:item"))
    assert pickle.loads(pickle.dumps(location)) == location
    assert json.loads(json.dumps(dataclasses.asdict(location))) == {
        "store_ref": "archive",
        "key": "../opaque//pack:item",
    }

    with pytest.raises(dataclasses.FrozenInstanceError):
        location.key = "changed"  # type: ignore[misc]


def test_location_exposes_no_path_file_or_backend_operations() -> None:
    location = api2.Location("archive", "objects/42")

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
    router = _RecordingRouter()
    location = api2.Location("primary", "objects/42")

    bound = router.bind(location)

    assert isinstance(bound, api2.BoundLocation)
    assert bound.location is location
    assert bound.store_ref == "primary"
    assert bound.key == "objects/42"
    assert router.calls == []
    assert not isinstance(bound, os.PathLike)
    assert not hasattr(bound, "parent")
    assert not hasattr(bound, "joinpath")


def test_bound_location_delegates_reads_and_never_caches_metadata() -> None:
    router = _RecordingRouter()
    location = api2.Location("primary", "objects/42")
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
    router = _RecordingRouter()
    location = api2.Location("primary", "objects/42")
    digest = api2.Digest("sha256", "abc123")
    bound = router.bind(location)

    info = bound.put(
        io.BytesIO(b"book"),
        expected_size=4,
        expected_digest=digest,
    )
    assert info.size == 4

    replaced = bound.write_bytes(
        b"replacement",
        mode=api2.WriteMode.REPLACE,
        expected_digest=digest,
    )
    assert replaced.size == 11
    bound.delete(if_version="v11")

    assert router.calls == [
        (
            "put",
            location,
            api2.WriteMode.CREATE_ONLY,
            4,
            digest,
        ),
        (
            "put",
            location,
            api2.WriteMode.REPLACE,
            11,
            digest,
        ),
        ("delete", location, False, "v11"),
    ]


def test_bound_try_stat_suppresses_only_not_found() -> None:
    router = _RecordingRouter()
    bound = router.bind(api2.Location("primary", "missing"))

    assert bound.try_stat() is None
    assert not bound.exists()

    router.available = False
    with pytest.raises(api2.StoreUnavailable):
        bound.try_stat()
    with pytest.raises(api2.StoreUnavailable):
        bound.exists()


def test_location_and_bound_facade_have_segregated_explicit_exports() -> None:
    from LiuXin_alpha.storage.api2 import location_api
    from LiuXin_alpha.storage.api2 import storage_manager_api
    from LiuXin_alpha.storage.api2.storage_manager_api.location_api import BoundLocation

    assert location_api.Location is api2.Location
    assert location_api.StoreRef is api2.StoreRef
    assert location_api.__all__ == ["Location", "StoreRef"]
    assert storage_manager_api.BoundLocation is BoundLocation is api2.BoundLocation
    assert len(storage_manager_api.__all__) == len(set(storage_manager_api.__all__))
