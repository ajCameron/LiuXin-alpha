"""Transactional contract tests for the second-generation storage API."""

from __future__ import annotations

import hashlib
import io

from collections.abc import Iterator
from dataclasses import dataclass, replace
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage import utils as storage_utils


MEMORY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
MAIN_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")
OTHER_STORE_UUID = UUID("00000000-0000-0000-0000-000000000003")
ARCHIVE_STORE_UUID = UUID("00000000-0000-0000-0000-000000000004")
HOST_A_UUID = UUID("00000000-0000-0000-0000-000000000101")
HOST_B_UUID = UUID("00000000-0000-0000-0000-000000000102")
DEVICE_A_UUID = UUID("00000000-0000-0000-0000-000000000201")
DEVICE_B_UUID = UUID("00000000-0000-0000-0000-000000000202")


class _MemoryWriteSession:
    def __init__(
        self,
        store: "_MemoryStore",
        location: api.Location,
        *,
        mode: api.WriteMode,
        expected_size: int | None,
        expected_digest: api.Digest | None,
    ) -> None:
        self.store = store
        self.location = location
        self.mode = mode
        self.expected_size = expected_size
        self.expected_digest = expected_digest
        self.buffer = bytearray()
        self.committed = False
        self.aborted = False

    def write(self, data: bytes) -> int:
        if self.committed or self.aborted:
            raise api.StoreError("write session is already finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api.FileInfo:
        if self.committed or self.aborted:
            raise api.StoreError("write session is already finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api.StoreIntegrityError("digest mismatch")

        exists = self.location.key in self.store.files
        if self.mode is api.WriteMode.CREATE_ONLY and exists:
            raise api.StoreAlreadyExists(self.location.key)
        if self.mode is api.WriteMode.REPLACE and not exists:
            raise api.StoreNotFound(self.location.key)

        self.store.files[self.location.key] = payload
        self.store.version_counter += 1
        self.store.versions[self.location.key] = str(self.store.version_counter)
        self.committed = True
        return self.store.stat(self.location)

    def abort(self) -> None:
        if self.committed:
            return
        self.buffer.clear()
        self.aborted = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if not self.committed:
            self.abort()


class _MemoryStore(api.StoreAPI):
    def __init__(self, store_ref: api.StoreRef = MEMORY_STORE_UUID) -> None:
        store_name = f"store-{store_ref.hex[:8]}"
        self._spec = api.StoreSpec(
            store_uuid=store_ref,
            store_name=store_name,
            store_kind="memory",
            store_root_uri=f"memory://{store_name}",
        )
        self.files: dict[str, bytes] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.online = True
        self.read_only = False
        self._capabilities = api.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            range_reads=True,
            stat_digest_authoritative=True,
            enumeration=api.EnumerationCompleteness.COMPLETE,
        )

    @property
    def spec(self) -> api.StoreSpec:
        return self._spec

    @property
    def capabilities(self) -> api.StoreCapabilities:
        return self._capabilities

    def location(self, *tokens: str) -> api.Location:
        key = "/".join(token.strip("/") for token in tokens if token.strip("/"))
        return api.Location(self.store_ref, key)

    def _key(self, location: api.Location) -> str:
        if location.store_ref != self.store_ref:
            raise api.StoreInvalidLocation(str(location))
        return location.key

    def _require_online(self) -> None:
        if not self.online:
            raise api.StoreUnavailable(str(self.store_ref))

    def stat(self, location: api.Location) -> api.FileInfo:
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api.StoreNotFound(key)
        payload = self.files[key]
        return api.FileInfo(
            location=location,
            size=len(payload),
            digest=api.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[key],
        )

    def open_read(
        self,
        location: api.Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api.StoreNotFound(key)
        if offset < 0 or (length is not None and length < 0):
            raise api.StoreInvalidLocation("negative read range")
        payload = self.files[key][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        location: api.Location,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
    ) -> _MemoryWriteSession:
        self._require_online()
        self._key(location)
        if self.read_only:
            raise api.StoreReadOnly(str(self.store_ref))
        return _MemoryWriteSession(
            self,
            location,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        location: api.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        self._require_online()
        key = self._key(location)
        if self.read_only:
            raise api.StoreReadOnly(str(self.store_ref))
        if key not in self.files:
            if missing_ok:
                return
            raise api.StoreNotFound(key)
        if if_version is not None and self.versions[key] != if_version:
            raise api.StorePreconditionFailed(key)
        del self.files[key]
        del self.versions[key]

    def iter_locations(
        self,
        *,
        prefix: api.Location | None = None,
    ) -> Iterator[api.Location]:
        self._require_online()
        prefix_key = "" if prefix is None else self._key(prefix)
        for key in sorted(self.files):
            if key.startswith(prefix_key):
                yield api.Location(self.store_ref, key)

    def startup(self) -> api.StoreStatus:
        self.online = True
        return self.status()

    def probe(self) -> api.StoreStatus:
        return self.status()

    def status(self, *, refresh: bool = False) -> api.StoreStatus:
        return api.StoreStatus(
            available=self.online,
            writable=self.online and not self.read_only,
            total_bytes=1024 * 1024,
            free_bytes=1024 * 1024 - sum(map(len, self.files.values())),
        )

    def close(self) -> None:
        self.online = False


class _MemoryManager(api.StorageRouterAPI):
    def __init__(self, store: _MemoryStore) -> None:
        self.store = store

    def _route(self, location: api.Location) -> _MemoryStore:
        if location.store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(location))
        return self.store

    def stat(self, location):
        return self._route(location).stat(location)

    def get(self, location, *, offset=0, length=None):
        return self._route(location).open_read(location, offset=offset, length=length)

    def put(
        self,
        location,
        source,
        *,
        mode=api.WriteMode.CREATE_ONLY,
        expected_size=None,
        expected_digest=None,
    ):
        return storage_utils.put(
            self._route(location),
            location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(self, location, *, missing_ok=False, if_version=None):
        self._route(location).delete(
            location,
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(self, *, store_ref=None, prefix=None):
        if store_ref is not None and store_ref != self.store.store_ref:
            return iter(())
        return self.store.iter_locations(prefix=prefix)

    def capabilities(self, store_ref):
        if store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(store_ref))
        return self.store.capabilities

    def status(self, store_ref):
        if store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(store_ref))
        return self.store.status()


def _sha256(data: bytes) -> api.Digest:
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def _asset(asset_id: int = 1, payload: bytes = b"payload") -> api.DigitalAsset:
    return api.DigitalAsset(
        api.DigitalAssetID(asset_id),
        len(payload),
        (_sha256(payload),),
    )


def _replica(
    replica_id: int = 2,
    *,
    asset: api.DigitalAsset | None = None,
    store_ref: api.StoreRef = MAIN_STORE_UUID,
) -> api.Replica:
    selected_asset = _asset() if asset is None else asset
    return api.Replica(
        api.ReplicaID(replica_id),
        selected_asset.digital_asset_id,
        api.Location(store_ref, f"assets/{selected_asset.digital_asset_id}"),
        api.ReplicaMode.ACTIVE,
        api.ReplicaObservation(api.ReplicaState.VERIFIED),
    )


class _IngestHarness(api.AssetIngestAPI):
    def __init__(self) -> None:
        self.observed: bytes | None = None
        self.size: int | None = None

    def ingest_stream(self, stream, **kwargs):
        self.observed = stream.read()
        self.size = kwargs["expected_size"]
        asset = _asset()
        return api.IngestResult(
            kwargs["operation_id"] or UUID(int=10),
            asset,
            _replica(asset=asset),
            True,
            True,
        )

    def adopt_location(self, location, **kwargs):
        asset = _asset()
        replica = api.Replica(
            api.ReplicaID(2),
            asset.digital_asset_id,
            location,
            api.ReplicaMode.UNMANAGED,
            api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
        )
        return api.IngestResult(
            kwargs.get("operation_id") or UUID(int=11),
            asset,
            replica,
            False,
            True,
        )


class _RetrievalHarness(api.AssetRetrievalAPI):
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def select_replica(self, digital_asset_id, **kwargs):
        return self.resolve_digital_asset(digital_asset_id, **kwargs).replica

    def resolve_digital_asset(
        self,
        digital_asset_id,
        *,
        preferred_store=None,
        mode=api.ReplicaMode.ACTIVE,
        require_verified=False,
    ):
        self.calls.append(
            (
                "digital_asset",
                digital_asset_id,
                preferred_store,
                require_verified,
            )
        )
        if digital_asset_id == 404:
            raise api.NoReadableReplica("digital asset has no readable replica")
        asset = _asset(int(digital_asset_id))
        replica = _replica(
            int(digital_asset_id),
            asset=asset,
            store_ref=preferred_store or MAIN_STORE_UUID,
        )
        return api.ResolvedAsset(asset, replica)

    def locate_replica(self, replica_id):
        self.calls.append(("replica", replica_id))
        return api.Location(MAIN_STORE_UUID, f"replicas/{replica_id}")

    def materialize_digital_asset(self, digital_asset_id, **kwargs):
        raise NotImplementedError

    def resolve_item_asset(self, item_id, **kwargs):
        raise NotImplementedError


class _TopologyHarness:
    compare_location_hosts = api.StoreAdministrationAPI.compare_location_hosts
    compare_location_devices = api.StoreAdministrationAPI.compare_location_devices

    def __init__(self, specs: tuple[api.StoreSpec, ...]) -> None:
        self.specs = {spec.store_uuid: spec for spec in specs}

    def get_store_spec(self, store_ref: api.StoreRef) -> api.StoreSpec:
        return self.specs[store_ref]


def test_public_surface_is_small_complete_and_unique() -> None:
    assert len(api.__all__) == len(set(api.__all__))
    assert all(hasattr(api, name) for name in api.__all__)
    assert api.StorageRouterAPI.__abstractmethods__ == {
        "stat",
        "get",
        "put",
        "delete",
        "iter_locations",
        "capabilities",
        "status",
    }
    with pytest.raises(TypeError):
        api.StorageRouterAPI()
    with pytest.raises(TypeError):
        api.StorageManagerAPI()


def test_full_manager_layers_catalogue_and_policy_above_the_small_router() -> None:
    facade_bases = {
        api.StoreAdministrationAPI,
        api.AssetRegistryAPI,
        api.AssetIngestAPI,
        api.AssetRetrievalAPI,
        api.ReplicaLifecycleAPI,
        api.StoragePolicyAPI,
        api.CompositeAssetAPI,
        api.StorageReconciliationAPI,
    }
    assert issubclass(api.StorageManagerAPI, api.StorageRouterAPI)
    assert facade_bases.issubset(set(api.StorageManagerAPI.__mro__))
    assert {
        "begin_write",
    }.isdisjoint(api.StorageManagerAPI.__abstractmethods__)
    assert {
        "ingest_stream", "resolve_digital_asset", "replicate_digital_asset",
        "verify_replica", "resolve_effective_policies",
        "declare_composite_digital_asset", "plan_reconciliation",
        "get_store", "iter_stores",
    }.issubset(api.StorageManagerAPI.__abstractmethods__)


def test_storage_manager_package_exposes_stable_segregated_import_paths() -> None:
    from LiuXin_alpha.storage.api import storage_manager_api as manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaState
    from LiuXin_alpha.storage.api.storage_manager_api.models.policies import ReplicationPolicy
    from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
    from LiuXin_alpha.storage.api.storage_manager_api.policies_api import StoragePolicyAPI
    from LiuXin_alpha.storage.api.storage_manager_api.router_api import StorageRouterAPI

    assert manager_api.StorageManagerAPI is api.StorageManagerAPI
    assert manager_api.StoragePolicyAPI is StoragePolicyAPI is api.StoragePolicyAPI
    assert manager_api.StorageRouterAPI is StorageRouterAPI is api.StorageRouterAPI
    assert manager_api.ReplicaState is ReplicaState is api.ReplicaState
    assert manager_api.ReplicationPolicy is ReplicationPolicy is api.ReplicationPolicy
    assert manager_api.LocationFactory is LocationFactory is api.LocationFactory
    assert len(manager_api.__all__) == len(set(manager_api.__all__))


def test_location_factory_resolves_asset_and_replica_ids_through_manager() -> None:
    manager = _RetrievalHarness()
    factory = manager.location_factory

    selected = factory.from_id(
        7,
        preferred_store=ARCHIVE_STORE_UUID,
        require_verified=True,
    )
    explicit = factory.from_digital_asset_id(8)
    replica = factory.from_replica_id(12)

    assert isinstance(factory, api.LocationFactory)
    assert selected == api.Location(ARCHIVE_STORE_UUID, "assets/7")
    assert explicit == api.Location(MAIN_STORE_UUID, "assets/8")
    assert replica == api.Location(MAIN_STORE_UUID, "replicas/12")
    assert manager.calls == [
        ("digital_asset", 7, ARCHIVE_STORE_UUID, True),
        ("digital_asset", 8, None, False),
        ("replica", 12),
    ]

    with pytest.raises(api.NoReadableReplica):
        factory.from_id(404)


def test_structural_protocols_accept_a_complete_backend_and_session() -> None:
    store = _MemoryStore()
    session = store.begin_write(api.Location(MEMORY_STORE_UUID, "book.epub"))

    assert isinstance(store, api.StoreAPI)
    assert isinstance(store, api.FileStore)
    assert isinstance(session, api.WriteSession)
    assert not store.capabilities.native_copy


def test_store_api_composes_identity_lifecycle_and_transactional_files() -> None:
    from LiuXin_alpha.storage.api import store_api
    from LiuXin_alpha.storage.api.store_api.file_api import StoreFileAPI
    from LiuXin_alpha.storage.api.store_api.identity_api import StoreIdentityAPI
    from LiuXin_alpha.storage.api.store_api.lifecycle_api import StoreLifecycleAPI

    assert store_api.StoreAPI is api.StoreAPI
    assert len(store_api.__all__) == len(set(store_api.__all__))
    assert all(hasattr(store_api, name) for name in store_api.__all__)
    assert issubclass(api.StoreAPI, StoreIdentityAPI)
    assert issubclass(api.StoreAPI, StoreLifecycleAPI)
    assert issubclass(api.StoreAPI, StoreFileAPI)
    assert api.StoreAPI.__abstractmethods__ == {
        "begin_write",
        "capabilities",
        "close",
        "delete",
        "iter_locations",
        "location",
        "open_read",
        "probe",
        "spec",
        "startup",
        "stat",
        "status",
    }

    store = _MemoryStore()
    location = api.Location(store.store_ref, "objects/42")
    assert isinstance(store.spec, api.StoreSpecAPI)
    assert store.require_location(location) is location
    assert store.owns_location(location)
    with pytest.raises(api.StoreInvalidLocation):
        store.require_location(api.Location(OTHER_STORE_UUID, "objects/42"))

    info = store.write_bytes(location, b"book")
    assert info.size == 4
    assert store.read_bytes(location) == b"book"
    assert store.compute_digest(location) == _sha256(b"book")

    copied = api.Location(store.store_ref, "objects/copied")
    moved = api.Location(store.store_ref, "objects/moved")
    assert store.copy(location, copied).size == 4
    assert store.read_bytes(copied) == b"book"
    assert store.move(copied, moved).location == moved
    assert not store.exists(copied)
    assert store.read_bytes(moved) == b"book"

    with store as entered:
        assert entered is store
    assert not store.status().available
    assert store.startup().available


def test_models_are_explicit_stable_and_validated() -> None:
    location = api.Location(MAIN_STORE_UUID, "opaque/object-key")
    digest = api.Digest(" SHA256 ", " ABCDEF ")
    capabilities = api.StoreCapabilities(
        create=True,
        replace=False,
        delete=False,
        atomic_publish=True,
        range_reads=False,
        stat_digest_authoritative=True,
        enumeration=api.EnumerationCompleteness.PARTIAL,
    )

    assert location.key == "opaque/object-key"
    assert digest == api.Digest("sha256", "abcdef")
    assert capabilities.enumeration is api.EnumerationCompleteness.PARTIAL
    assert api.WriteMode.CREATE_ONLY.value == "create_only"

    with pytest.raises(ValueError, match="empty"):
        api.Location(MAIN_STORE_UUID, "")
    with pytest.raises(ValueError, match="conditional_delete requires"):
        api.StoreCapabilities(
            create=False,
            replace=False,
            delete=False,
            atomic_publish=False,
            range_reads=False,
            stat_digest_authoritative=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            conditional_delete=True,
        )
    with pytest.raises(TypeError, match="store_uuid"):
        api.StoreSpec(
            str(MAIN_STORE_UUID),  # type: ignore[arg-type]
            "main",
            "memory",
            "memory://main",
        )
    with pytest.raises(ValueError, match="negative"):
        api.FileInfo(location, -1)
    with pytest.raises(ValueError, match="exceed"):
        api.StoreStatus(True, True, total_bytes=10, free_bytes=11)


def test_error_family_preserves_actionable_failure_categories() -> None:
    error_types = (
        api.StoreNotFound,
        api.StoreAlreadyExists,
        api.StoreInvalidLocation,
        api.StoreReadOnly,
        api.StoreNoSpace,
        api.StorePreconditionFailed,
        api.StoreIntegrityError,
        api.StoreUnavailable,
        api.StoreUnsupportedOperation,
    )
    assert all(issubclass(error_type, api.StoreError) for error_type in error_types)


def test_free_operations_are_segregated_from_contract_exports() -> None:
    utility_names = {
        "compute_digest",
        "copy",
        "exists",
        "get",
        "iter_infos",
        "iter_object_addresses",
        "materialize_object",
        "move",
        "move_between_drivers",
        "normalize_archive_path",
        "put",
        "put_object",
        "read_bytes",
        "transfer_between_drivers",
        "try_stat",
        "write_all",
        "write_bytes",
        "write_object_bytes",
    }

    assert not utility_names & set(api.__all__)
    assert utility_names <= set(storage_utils.__all__)
    assert storage_utils.try_stat.__module__ == (
        "LiuXin_alpha.storage.utils.store"
    )
    assert storage_utils.transfer_between_drivers.__module__ == (
        "LiuXin_alpha.storage.utils.driver"
    )
    assert storage_utils.normalize_archive_path.__module__ == (
        "LiuXin_alpha.storage.utils.workflow"
    )


def test_create_only_is_safe_and_final_location_changes_only_on_commit() -> None:
    store = _MemoryStore()
    location = api.Location(MEMORY_STORE_UUID, "book.epub")
    session = store.begin_write(
        location,
        expected_size=7,
        expected_digest=_sha256(b"payload"),
    )

    with session:
        session.write(b"payload")
        assert storage_utils.try_stat(store, location) is None
        info = session.commit()

    assert info.size == 7
    assert storage_utils.read_bytes(store, location) == b"payload"
    with pytest.raises(api.StoreAlreadyExists):
        storage_utils.write_bytes(store, location, b"replacement")

    storage_utils.write_bytes(
        store,
        location,
        b"replacement",
        mode=api.WriteMode.REPLACE,
    )
    assert storage_utils.read_bytes(store, location) == b"replacement"


def test_failed_commit_and_context_exit_leave_no_partial_publication() -> None:
    store = _MemoryStore()
    existing = api.Location(MEMORY_STORE_UUID, "existing")
    new = api.Location(MEMORY_STORE_UUID, "new")
    storage_utils.write_bytes(store, existing, b"original")

    session = store.begin_write(
        existing,
        mode=api.WriteMode.REPLACE,
        expected_digest=_sha256(b"different"),
    )
    with pytest.raises(api.StoreIntegrityError):
        with session:
            session.write(b"wrong")
            session.commit()
    assert storage_utils.read_bytes(store, existing) == b"original"
    session.abort()
    session.abort()

    with store.begin_write(new) as uncommitted:
        uncommitted.write(b"never published")
    assert storage_utils.try_stat(store, new) is None


def test_try_stat_suppresses_only_not_found() -> None:
    store = _MemoryStore()
    missing = api.Location(MEMORY_STORE_UUID, "missing")
    assert storage_utils.try_stat(store, missing) is None
    assert not storage_utils.exists(store, missing)

    store.online = False
    with pytest.raises(api.StoreUnavailable):
        storage_utils.try_stat(store, missing)
    with pytest.raises(api.StoreUnavailable):
        storage_utils.exists(store, missing)


def test_read_ranges_delete_preconditions_and_idempotence_are_explicit() -> None:
    store = _MemoryStore()
    location = api.Location(MEMORY_STORE_UUID, "alphabet")
    info = storage_utils.write_bytes(store, location, b"abcdefghij")

    assert storage_utils.read_bytes(store, location, offset=2, length=4) == b"cdef"
    with pytest.raises(api.StorePreconditionFailed):
        store.delete(location, if_version="stale-version")
    assert storage_utils.exists(store, location)

    store.delete(location, if_version=info.version)
    store.delete(location, missing_ok=True)
    with pytest.raises(api.StoreNotFound):
        store.delete(location)


def test_enumeration_and_iter_infos_are_files_only_and_prefix_filtered() -> None:
    store = _MemoryStore()
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "books/a.epub"), b"a")
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "books/b.epub"), b"bb")
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "covers/a.jpg"), b"jpg")

    prefix = api.Location(MEMORY_STORE_UUID, "books/")
    assert [location.key for location in store.iter_locations(prefix=prefix)] == [
        "books/a.epub",
        "books/b.epub",
    ]
    assert [info.size for info in storage_utils.iter_infos(store, prefix=prefix)] == [1, 2]
    assert store.capabilities.enumeration is api.EnumerationCompleteness.COMPLETE


def test_copy_move_and_digest_have_safe_generic_fallbacks() -> None:
    store = _MemoryStore()
    source = api.Location(MEMORY_STORE_UUID, "source")
    copied = api.Location(MEMORY_STORE_UUID, "copied")
    moved = api.Location(MEMORY_STORE_UUID, "moved")
    storage_utils.write_bytes(store, source, b"payload")

    copy_info = storage_utils.copy(store, source, copied)
    assert copy_info.digest == _sha256(b"payload")
    assert storage_utils.read_bytes(store, copied) == b"payload"
    assert storage_utils.compute_digest(store, source) == _sha256(b"payload")
    with pytest.raises(ValueError, match="chunk_size"):
        storage_utils.compute_digest(store, source, chunk_size=0)
    with pytest.raises(api.StoreUnsupportedOperation):
        storage_utils.compute_digest(store, source, "not-a-real-digest")

    move_info = storage_utils.move(store, copied, moved)
    assert move_info.size == 7
    assert storage_utils.try_stat(store, copied) is None
    assert storage_utils.read_bytes(store, moved) == b"payload"


def test_store_and_manager_moves_refuse_unprotected_fallbacks_before_copy() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    source = api.Location(MAIN_STORE_UUID, "source")
    utility_destination = api.Location(MAIN_STORE_UUID, "utility-moved")
    manager_destination = api.Location(MAIN_STORE_UUID, "manager-moved")
    storage_utils.write_bytes(store, source, b"payload")
    store._capabilities = replace(
        store.capabilities,
        conditional_delete=False,
    )

    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        storage_utils.move(store, source, utility_destination)

    manager = _MemoryManager(store)
    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        manager.move(source, manager_destination)

    assert store.exists(source)
    assert not store.exists(utility_destination)
    assert not store.exists(manager_destination)


def test_store_and_manager_moves_require_a_source_version_before_copy() -> None:
    class _UnversionedMemoryStore(_MemoryStore):
        def stat(self, location: api.Location) -> api.FileInfo:
            return replace(super().stat(location), version=None)

    store = _UnversionedMemoryStore(MAIN_STORE_UUID)
    source = api.Location(MAIN_STORE_UUID, "source")
    store_destination = api.Location(MAIN_STORE_UUID, "store-moved")
    manager_destination = api.Location(MAIN_STORE_UUID, "manager-moved")
    storage_utils.write_bytes(store, source, b"payload")

    with pytest.raises(api.StoreUnsupportedOperation, match="source version"):
        store.move(source, store_destination)

    manager = _MemoryManager(store)
    with pytest.raises(api.StoreUnsupportedOperation, match="source version"):
        manager.move(source, manager_destination)

    assert store.exists(source)
    assert not store.exists(store_destination)
    assert not store.exists(manager_destination)


def test_manager_routes_primitives_and_derives_only_small_conveniences() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = _MemoryManager(store)
    location = api.Location(MAIN_STORE_UUID, "book.epub")

    info = manager.write_bytes(location, b"payload", expected_digest=_sha256(b"payload"))

    assert info.size == 7
    assert manager.exists(location)
    assert manager.read_bytes(location, offset=1, length=3) == b"ayl"
    assert [item.location for item in manager.iter_infos()] == [location]
    assert manager.capabilities(MAIN_STORE_UUID).atomic_publish
    assert manager.status(MAIN_STORE_UUID).available

    copied = api.Location(MAIN_STORE_UUID, "book-copy.epub")
    moved = api.Location(MAIN_STORE_UUID, "book-moved.epub")
    assert manager.copy(location, copied).location == copied
    assert manager.move(copied, moved).location == moved
    assert manager.try_stat(copied) is None
    assert manager.read_bytes(moved) == b"payload"


def test_location_topology_distinguishes_same_different_and_unknown() -> None:
    main = api.StoreSpec(
        MAIN_STORE_UUID,
        "main",
        "filesystem",
        "file:///main",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_A_UUID,
    )
    archive = api.StoreSpec(
        ARCHIVE_STORE_UUID,
        "archive",
        "filesystem",
        "file:///archive",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_B_UUID,
    )
    remote = api.StoreSpec(
        OTHER_STORE_UUID,
        "remote",
        "filesystem",
        "file:///remote",
        store_host_uuid=HOST_B_UUID,
    )
    manager = _TopologyHarness((main, archive, remote))
    source = api.Location(MAIN_STORE_UUID, "objects/source")
    same_host = api.Location(ARCHIVE_STORE_UUID, "objects/destination")
    remote_host = api.Location(OTHER_STORE_UUID, "objects/destination")

    assert (
        manager.compare_location_hosts(source, same_host)
        is api.TopologyRelation.SAME
    )
    assert (
        manager.compare_location_devices(source, same_host)
        is api.TopologyRelation.DIFFERENT
    )
    assert (
        manager.compare_location_hosts(source, remote_host)
        is api.TopologyRelation.DIFFERENT
    )
    assert (
        manager.compare_location_devices(source, remote_host)
        is api.TopologyRelation.UNKNOWN
    )


def test_facade_models_cover_store_policy_and_replica_state() -> None:
    spec = api.StoreSpec(
        store_uuid=ARCHIVE_STORE_UUID,
        store_name="archive",
        store_kind="squashfs_readonly",
        store_root_uri="/srv/archive.sqsh",
        supported_replica_modes=frozenset(
            {api.ReplicaMode.BACKUP, api.ReplicaMode.ARCHIVE}
        ),
        read_only=True,
    )
    replication = api.ReplicationPolicy(min_copies=2)
    backup = api.BackupPolicy(
        min_copies=2, target_copies=3, mode=api.ReplicaMode.ARCHIVE,
    )

    assert spec.store_uuid == ARCHIVE_STORE_UUID
    assert replication.effective_target_copies == 2
    assert backup.effective_target_copies == 3
    assert api.ReplicaState.UNAVAILABLE != api.ReplicaState.MISSING
    with pytest.raises(ValueError, match="copy target"):
        api.ReplicationPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="backup policy mode"):
        api.BackupPolicy(mode=api.ReplicaMode.ACTIVE)


def test_asset_and_replica_domain_values_are_not_partial_records() -> None:
    digest = _sha256(b"book")
    spec = api.DigitalAssetSpec(
        4,
        (digest,),
        api.DigitalAssetMetadata(
            media_type="application/epub+zip",
            original_name="book.epub",
        ),
    )
    asset = api.DigitalAsset(
        api.DigitalAssetID(7),
        spec.size_bytes,
        spec.digests,
        spec.metadata,
        revision="asset-v1",
    )
    replica_spec = api.ReplicaSpec(
        asset.digital_asset_id,
        api.Location(MAIN_STORE_UUID, "objects/7"),
        observation=api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
    )
    replica = api.Replica(
        api.ReplicaID(12),
        replica_spec.digital_asset_id,
        replica_spec.location,
        replica_spec.mode,
        replica_spec.observation,
        revision="replica-v1",
    )

    assert asset.size_bytes == 4
    assert asset.digests == (digest,)
    assert replica.digital_asset_id == asset.digital_asset_id
    assert replica.location.store_ref == MAIN_STORE_UUID
    assert not hasattr(asset, "record")
    assert not hasattr(replica, "asset_replica_id")
    with pytest.raises(ValueError, match="at least one digest"):
        api.DigitalAssetSpec(4, ())
    with pytest.raises(ValueError, match="positive"):
        replace(asset, digital_asset_id=api.DigitalAssetID(0))


def test_repository_ports_operate_on_domain_values_not_record_protocols() -> None:
    class _AssetRepository:
        def add(self, spec):
            return api.DigitalAsset(
                api.DigitalAssetID(7), spec.size_bytes, spec.digests,
                spec.metadata,
            )

        def get(self, digital_asset_id):
            return _asset(int(digital_asset_id), b"book")

        def replace_metadata(self, digital_asset_id, metadata, *, if_revision=None):
            return replace(self.get(digital_asset_id), metadata=metadata)

        def find_by_digest(self, digest, *, size_bytes=None):
            return None

        def iter_assets(self):
            return iter(())

        def remove(self, digital_asset_id, *, if_revision=None):
            return True

    repository = _AssetRepository()
    assert isinstance(repository, api.DigitalAssetRepositoryAPI)
    created = repository.add(api.DigitalAssetSpec(4, (_sha256(b"book"),)))
    assert isinstance(created, api.DigitalAsset)
    assert "RecordAPI" not in api.__all__


def test_composite_resolution_preserves_relationship_metadata() -> None:
    asset = _asset(7)
    resolved = api.ResolvedAsset(asset, _replica(asset=asset))
    relationship = api.CompositeMemberSpec(
        asset.digital_asset_id,
        0,
        role="audio",
        logical_name="chapter-01.mp3",
        logical_path="disc-1/chapter-01.mp3",
        title="Chapter One",
    )
    member = api.ResolvedCompositeMember(relationship, resolved)

    assert member.location == resolved.location
    assert member.member.logical_path == "disc-1/chapter-01.mp3"
    assert member.member.title == "Chapter One"


def test_health_and_reconciliation_do_not_collapse_distinct_states() -> None:
    replication = api.PolicyStatus(
        api.DigitalAssetID(7),
        "live",
        api.ReplicaMode.ACTIVE,
        meets_minimum=False,
    )
    backup = api.PolicyStatus(
        api.DigitalAssetID(7),
        "backup",
        api.ReplicaMode.BACKUP,
        meets_minimum=True,
    )
    health = api.DigitalAssetStorageHealth(
        api.DigitalAssetID(7),
        replication,
        backup,
        (api.ReplicaID(12),),
    )
    partial_plan = api.ReconciliationPlan(
        UUID(int=21),
        MAIN_STORE_UUID,
        False,
        api.EnumerationCompleteness.PARTIAL,
    )

    assert health.readable
    assert health.at_risk
    assert not health.replication_satisfied
    assert health.backup_satisfied
    assert not api.ReconciliationReport(partial_plan, applied=False).clean


def test_ingest_bytes_remains_a_small_wrapper_over_transactional_stream_ingest() -> None:
    manager = _IngestHarness()
    result = manager.ingest_bytes(
        b"payload", item_id=api.ItemID(7), role="primary_payload",
        preferred_store=MAIN_STORE_UUID,
    )

    assert manager.observed == b"payload"
    assert manager.size == 7
    assert result.asset.digital_asset_id == api.DigitalAssetID(1)
    assert result.replica.replica_id == api.ReplicaID(2)
    assert result.location is result.replica.location


def test_verification_and_reconciliation_results_preserve_operational_distinctions() -> None:
    unavailable = api.ReplicaVerificationResult(
        api.ReplicaID(1), api.DigitalAssetID(9),
        api.ReplicaState.UNAVAILABLE, None, errors=("offline",),
    )
    corrupt = api.ReplicaVerificationResult(
        api.ReplicaID(2), api.DigitalAssetID(9),
        api.ReplicaState.CORRUPT, True, digest_matches=False,
    )
    verified = api.ReplicaVerificationResult(
        api.ReplicaID(3), api.DigitalAssetID(9),
        api.ReplicaState.VERIFIED, True,
        size_matches=True, digest_matches=True,
    )
    dirty_plan = api.ReconciliationPlan(
        UUID(int=20), MAIN_STORE_UUID, True,
        api.EnumerationCompleteness.COMPLETE,
        missing_replica_ids=(api.ReplicaID(1),),
    )
    dirty = api.ReconciliationReport(dirty_plan, applied=False)

    assert not unavailable.healthy
    assert not corrupt.healthy
    assert verified.healthy
    assert api.AssetVerificationResult(
        api.DigitalAssetID(9), (unavailable, verified)
    ).readable
    assert not dirty.clean
