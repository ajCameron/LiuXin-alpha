"""Exercise the production process-local cache driver and configured Store."""

from __future__ import annotations

import hashlib
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.drivers import MemoryStorageDriver
from LiuXin_alpha.storage.stores import MemoryStore
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    StoragePathCase,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case

STORE_UUID = UUID("00000000-0000-0000-0000-000000000123")


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
def test_memory_store_obeys_unicode_path_contract(case: StoragePathCase) -> None:
    """Preserve each opaque Unicode key through memory routing, inventory, reads, and URI parsing.

    :param case: Exact key, filename hint, and payload from the shared Unicode matrix.
    :return: None after the shared Store contract passes for a fresh process-local backend.
    """

    store = MemoryStore()
    store.startup()
    exercise_unicode_path_case(
        store,
        case,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_uri_round_trip=True,
    )
    store.close()


def test_memory_store_round_trips_versions_ranges_inventory_and_lifecycle() -> None:
    """Use the full Store facade and prove snapshot, version, URI, and restart behavior."""
    store = MemoryStore(uuid=STORE_UUID, max_bytes=32)
    assert dict(store.configuration.backend_options) == {"max_bytes": 32}
    assert not store.status().available
    assert store.startup().free_bytes == 32

    first = store.store_bytes(b"abcdef", location="books/a file.epub")
    assert first.size == 6
    assert first.digest == api.Digest(
        "sha256",
        hashlib.sha256(b"abcdef").hexdigest(),
    )
    with store.open_read(
        first.location,
        offset=1,
        length=3,
        if_version=first.version,
    ) as source:
        assert source.read() == b"bcd"

    uri = store.location_uri(first.location)
    assert uri is not None and "%20" in uri
    assert store.location_from_uri(uri) == first.location
    assert tuple(store.iter_locations()) == (first.location,)

    replaced = store.store_bytes(
        b"replacement",
        location=first.location,
        mode=api.WriteMode.REPLACE,
    )
    assert replaced.version != first.version
    with pytest.raises(api.StorePreconditionFailed):
        store.open_read(replaced.location, if_version=first.version)

    store.close()
    with pytest.raises(api.StoreUnavailable):
        store.read_bytes(replaced.location)
    assert store.startup().available
    assert store.read_bytes(replaced.location) == b"replacement"


def test_memory_store_configuration_round_trip_preserves_capacity_limit() -> None:
    """Rebuild a bounded transient Store without silently turning it into an unbounded cache."""

    original = MemoryStore(uuid=STORE_UUID, max_bytes=32)
    rebuilt = MemoryStore.from_configuration(original.configuration)

    assert rebuilt.configuration is original.configuration
    assert rebuilt.driver.max_bytes == 32
    assert rebuilt.startup().total_bytes == 32


def test_memory_driver_enforces_atomic_integrity_collisions_and_capacity() -> None:
    """Reject failed commits without changing an existing object or capacity accounting."""
    driver = MemoryStorageDriver(
        address_space_uuid=STORE_UUID,
        max_bytes=8,
    )
    driver.startup()
    address = driver.parse_object_address("object")
    original = driver.store_bytes(b"123456", object_address=address)

    with pytest.raises(api.StorageAlreadyExists):
        driver.store_bytes(b"x", object_address=address)
    with pytest.raises(api.StorageIntegrityError):
        driver.store_bytes(
            b"changed",
            object_address=address,
            mode=api.WriteMode.REPLACE,
            expected_digest=api.Digest("sha256", "00"),
        )
    assert driver.read_bytes(address) == b"123456"
    assert driver.stat(address).version == original.version

    other = driver.parse_object_address("other")
    with pytest.raises(api.StorageNoSpace):
        driver.store_bytes(b"abc", object_address=other)
    assert not driver.exists(other)

    replaced = driver.store_bytes(
        b"12345678",
        object_address=address,
        mode=api.WriteMode.REPLACE,
    )
    assert replaced.version != original.version
    assert driver.status().free_bytes == 0
    with pytest.raises(api.StoragePreconditionFailed):
        driver.delete(address, if_version=original.version)
    driver.delete(address, if_version=replaced.version)
    assert driver.status().free_bytes == 8


def test_memory_driver_preserves_native_metadata_and_snapshots_inventory() -> None:
    """Expose write metadata and stable sorted inventory through the raw-driver boundary."""
    driver = MemoryStorageDriver(address_space_uuid=STORE_UUID)
    driver.startup()
    second = driver.parse_object_address("cache/z")
    first = driver.parse_object_address("cache/a")
    driver.store_bytes(b"z", object_address=second)
    with driver.begin_write(first, metadata=(("source", "conversion"),)) as session:
        assert session.write(b"a") == 1
        session.commit()

    prefix = driver.parse_object_address("cache/")
    entries = tuple(driver.iter_inventory(prefix=prefix))
    assert tuple(str(entry.object_address) for entry in entries) == (
        "cache/a",
        "cache/z",
    )
    assert entries[0].hints.metadata == (("source", "conversion"),)
    assert entries[0].digest == driver.stat(first).digest


def test_registry_builds_empty_memory_store_with_declared_cache_policy() -> None:
    """Resolve the memory alias, retain configuration, and enforce its persisted byte ceiling."""
    configuration = api.StoreConfiguration.for_backend(
        "conversion cache",
        "ram",
        f"memory://{STORE_UUID}",
        store_uuid=STORE_UUID,
        protocol="memory",
        modes=(api.ReplicaMode.CACHE, api.ReplicaMode.TRANSIENT),
        operational_role="cache",
        options={"max_bytes": 5},
    )
    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert isinstance(store, MemoryStore)
    assert store.configuration is configuration
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("in_memory") == "memory"
    assert (
        DEFAULT_BACKEND_REGISTRY.descriptor("memory").characteristics.limitation(
            "process_local_non_durable"
        )
        is not None
    )
    store.startup()
    assert store.store_bytes(b"12345", location="full").size == 5
    with pytest.raises(api.StoreNoSpace):
        store.store_bytes(b"x", location="overflow")


def test_memory_store_rejects_unknown_or_invalid_persisted_options() -> None:
    """Fail construction rather than silently ignoring misspelled cache limits."""
    unknown = api.StoreConfiguration.for_backend(
        "cache",
        "memory",
        f"memory://{STORE_UUID}",
        store_uuid=STORE_UUID,
        options={"max_byte": 4},
    )
    with pytest.raises(ValueError, match="unsupported memory backend options"):
        MemoryStore.from_configuration(unknown)

    invalid = api.StoreConfiguration.for_backend(
        "cache",
        "memory",
        f"memory://{STORE_UUID}",
        store_uuid=STORE_UUID,
        options={"max_bytes": "many"},
    )
    with pytest.raises(ValueError, match="integer text"):
        MemoryStore.from_configuration(invalid)
