"""Transactional contract tests for the second-generation storage API."""

from __future__ import annotations

import hashlib
import inspect
import io
import zipfile

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage import utils as storage_utils
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager


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
    def __init__(self, store_ref: api.StoreUUID = MEMORY_STORE_UUID) -> None:
        store_name = f"store-{store_ref.hex[:8]}"
        self._configuration = api.StoreConfiguration(
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
    def configuration(self) -> api.StoreConfiguration:
        return self._configuration

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


class _PlacementAwareMemoryStore(_MemoryStore):
    def __init__(self, store_ref: api.StoreUUID = MEMORY_STORE_UUID) -> None:
        super().__init__(store_ref)
        self._capabilities = replace(
            self._capabilities,
            placement_hints=True,
        )
        self.allocation_hints: api.StoragePlacementHints | None = None
        self.write_hints: api.StoragePlacementHints | None = None

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        name_hint: str | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> api.Location:
        self.allocation_hints = placement_hints
        title = (
            placement_hints.get("title")
            if isinstance(placement_hints, Mapping)
            else getattr(placement_hints, "title", None)
        )
        return self.location("rich", str(title or name_hint or "untitled"))

    def begin_write(
        self,
        location: api.Location,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> _MemoryWriteSession:
        self.write_hints = placement_hints
        return super().begin_write(
            location,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )


class _CharacteristicMemoryStore(_MemoryStore):
    def __init__(
        self,
        store_ref: api.StoreUUID,
        characteristics: api.StorageCharacteristics,
        *,
        warnings: tuple[str, ...] = (),
    ) -> None:
        super().__init__(store_ref)
        self._characteristics = characteristics
        self._status_warnings = warnings

    @property
    def characteristics(self) -> api.StorageCharacteristics:
        return self._characteristics

    def status(self, *, refresh: bool = False) -> api.StoreStatus:
        del refresh
        return replace(super().status(), warnings=self._status_warnings)


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


def _asset(asset_id: int = 1, payload: bytes = b"payload") -> api.DigitalAssetRecord:
    return api.DigitalAssetRecord(
        api.DigitalAssetID(asset_id),
        len(payload),
        (_sha256(payload),),
    )


def _replica(
    replica_id: int = 2,
    *,
    asset: api.DigitalAssetRecord | None = None,
    store_ref: api.StoreUUID = MAIN_STORE_UUID,
) -> api.ReplicaRecord:
    selected_asset = _asset() if asset is None else asset
    return api.ReplicaRecord(
        api.ReplicaID(replica_id),
        selected_asset.digital_asset_id,
        api.Location(store_ref, f"assets/{selected_asset.digital_asset_id}"),
        api.ReplicaMode.ACTIVE,
        api.ReplicaObservation(api.ReplicaState.VERIFIED),
    )


class _IngestHarness(api.DigitalAssetIngestAPI):
    def __init__(self) -> None:
        self.observed: bytes | None = None
        self.size: int | None = None

    def ingest_stream(self, stream, **kwargs):
        self.observed = stream.read()
        self.size = kwargs["expected_size"]
        asset = _asset()
        return api.DigitalAssetIngestResult(
            kwargs["operation_id"] or UUID(int=10),
            asset,
            _replica(asset=asset),
            True,
            True,
        )

    def adopt_location(self, location, **kwargs):
        asset = _asset()
        replica = api.ReplicaRecord(
            api.ReplicaID(2),
            asset.digital_asset_id,
            location,
            api.ReplicaMode.UNMANAGED,
            api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
        )
        return api.DigitalAssetIngestResult(
            kwargs.get("operation_id") or UUID(int=11),
            asset,
            replica,
            False,
            True,
        )


class _RetrievalHarness(api.DigitalAssetRetrievalAPI):
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def select_replica(self, digital_asset_id, **kwargs):
        return self.resolve_digital_asset(
            digital_asset_id, **kwargs
        ).replica_record

    def resolve_digital_asset(
        self,
        digital_asset_id,
        *,
        preferred_store_ref=None,
        mode=api.ReplicaMode.ACTIVE,
        require_verified=False,
    ):
        self.calls.append(
            (
                "digital_asset",
                digital_asset_id,
                preferred_store_ref,
                require_verified,
            )
        )
        if digital_asset_id == 404:
            raise api.NoReadableReplica("digital asset has no readable replica")
        asset = _asset(int(digital_asset_id))
        replica = _replica(
            int(digital_asset_id),
            asset=asset,
            store_ref=preferred_store_ref or MAIN_STORE_UUID,
        )
        return api.DigitalAssetResolution(asset, replica)

    def locate_replica(self, replica_id):
        self.calls.append(("replica", replica_id))
        return api.Location(MAIN_STORE_UUID, f"replicas/{replica_id}")

    def materialize_digital_asset(self, digital_asset_id, **kwargs):
        raise NotImplementedError

    def resolve_item_digital_asset(self, item_id, **kwargs):
        raise NotImplementedError


class _TopologyHarness:
    compare_location_hosts = api.StoreAdministrationAPI.compare_location_hosts
    compare_location_devices = api.StoreAdministrationAPI.compare_location_devices

    def __init__(
        self,
        configurations: tuple[api.StoreConfiguration, ...],
    ) -> None:
        self.configurations = {
            configuration.store_uuid: configuration
            for configuration in configurations
        }

    def get_store_configuration(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StoreConfiguration:
        return self.configurations[store_ref]


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
        api.DigitalAssetRegistryAPI,
        api.DigitalAssetIngestAPI,
        api.DigitalAssetRetrievalAPI,
        api.ItemDigitalAssetLinkAPI,
        api.ReplicaLifecycleAPI,
        api.StoragePolicyAPI,
        api.CompositeDigitalAssetAPI,
        api.DigitalAssetDerivationRegistryAPI,
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
        "record_digital_asset_derivation",
        "iter_digital_asset_derivation_records",
        "get_derivation_graph", "plan_digital_asset_recreation",
        "link_item_to_digital_asset", "unlink_item_digital_asset",
        "get_store", "iter_stores",
    }.issubset(api.StorageManagerAPI.__abstractmethods__)


def test_storage_manager_package_exposes_stable_segregated_import_paths() -> None:
    from LiuXin_alpha.storage.api import storage_manager_api as manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaState
    from LiuXin_alpha.storage.api.storage_manager_api.models.policies import ReplicationPolicy
    from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
    from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import DigitalAssetDerivationRegistryAPI
    from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import ItemDigitalAssetLinkAPI
    from LiuXin_alpha.storage.api.storage_manager_api.policies_api import StoragePolicyAPI
    from LiuXin_alpha.storage.api.storage_manager_api.router_api import StorageRouterAPI

    assert manager_api.StorageManagerAPI is api.StorageManagerAPI
    assert manager_api.DigitalAssetDerivationRegistryAPI is DigitalAssetDerivationRegistryAPI is api.DigitalAssetDerivationRegistryAPI
    assert manager_api.ItemDigitalAssetLinkAPI is ItemDigitalAssetLinkAPI is api.ItemDigitalAssetLinkAPI
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
        preferred_store_ref=ARCHIVE_STORE_UUID,
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
    assert isinstance(store, api.StoreCoreAPI)
    assert isinstance(session, api.WriteSessionAPI)
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
        "configuration",
        "startup",
        "stat",
        "status",
    }

    store = _MemoryStore()
    location = api.Location(store.store_ref, "objects/42")
    assert isinstance(store.configuration, api.StoreConfigurationAPI)
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
        api.StoreConfiguration(
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
    assert issubclass(
        api.StoreConfigurationNotFound,
        api.StorageManagementError,
    )
    assert not issubclass(api.StoreConfigurationNotFound, api.StoreNotFound)


def test_free_operations_are_segregated_from_contract_exports() -> None:
    utility_names = {
        "compute_digest",
        "copy",
        "exists",
        "get",
        "iter_file_infos",
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
    assert [
        info.size
        for info in storage_utils.iter_file_infos(store, prefix=prefix)
    ] == [1, 2]
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
    assert [item.location for item in manager.iter_file_infos()] == [location]
    assert manager.capabilities(MAIN_STORE_UUID).atomic_publish
    assert manager.status(MAIN_STORE_UUID).available

    copied = api.Location(MAIN_STORE_UUID, "book-copy.epub")
    moved = api.Location(MAIN_STORE_UUID, "book-moved.epub")
    assert manager.copy(location, copied).location == copied
    assert manager.move(copied, moved).location == moved
    assert manager.try_stat(copied) is None
    assert manager.read_bytes(moved) == b"payload"


def test_manager_exposes_characteristics_and_preflights_declared_size() -> None:
    profile = api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.PER_OBJECT,
        max_object_bytes=4,
    )
    store = _CharacteristicMemoryStore(MAIN_STORE_UUID, profile)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = io.BytesIO(b"payload")
    location = api.Location(MAIN_STORE_UUID, "too-large.bin")

    assert manager.characteristics(MAIN_STORE_UUID) is profile
    with pytest.raises(api.StoreUnsupportedOperation, match="up to 4 bytes"):
        manager.put(location, source, expected_size=7)
    assert source.tell() == 0
    assert store.files == {}

    accepted = manager.write_bytes(
        api.Location(MAIN_STORE_UUID, "fits.bin"),
        b"four",
    )
    assert accepted.size == 4


def test_automatic_active_placement_avoids_archival_snapshot_writers() -> None:
    profile = api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.WHOLE_STORE_REBUILD,
        recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
    )
    archive = _CharacteristicMemoryStore(ARCHIVE_STORE_UUID, profile)
    manager = InMemoryStorageManager(
        store_registrations=((archive.configuration, archive),),
    )

    assert manager._plan_destination_stores(
        api.ReplicationPolicy(min_copies=1),
        (),
        1,
        expected_size=4,
    ) == ()
    assert manager._plan_destination_stores(
        api.BackupPolicy(min_copies=1, mode=api.ReplicaMode.ARCHIVE),
        (),
        1,
        expected_size=4,
    ) == (ARCHIVE_STORE_UUID,)


def test_store_status_warnings_are_promoted_to_operational_issues() -> None:
    store = _CharacteristicMemoryStore(
        MAIN_STORE_UUID,
        api.StorageCharacteristics(),
        warnings=("normalization requires explicit approval",),
    )
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    status = manager.get_operational_status(refresh_stores=True)

    warnings = status.issues_for("store_warning")
    assert len(warnings) == 1
    assert warnings[0].store_ref == MAIN_STORE_UUID
    assert "explicit approval" in warnings[0].message


def test_location_topology_distinguishes_same_different_and_unknown() -> None:
    main = api.StoreConfiguration(
        MAIN_STORE_UUID,
        "main",
        "filesystem",
        "file:///main",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_A_UUID,
    )
    archive = api.StoreConfiguration(
        ARCHIVE_STORE_UUID,
        "archive",
        "filesystem",
        "file:///archive",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_B_UUID,
    )
    remote = api.StoreConfiguration(
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
    configuration = api.StoreConfiguration(
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

    assert configuration.store_uuid == ARCHIVE_STORE_UUID
    assert replication.effective_target_copies == 2
    assert backup.effective_target_copies == 3
    assert api.ReplicaState.UNAVAILABLE != api.ReplicaState.MISSING
    with pytest.raises(ValueError, match="copy target"):
        api.ReplicationPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="backup policy mode"):
        api.BackupPolicy(mode=api.ReplicaMode.ACTIVE)


def test_asset_and_replica_records_are_explicit_public_values() -> None:
    digest = _sha256(b"book")
    declaration = api.DigitalAssetDeclaration(
        4,
        (digest,),
        api.DigitalAssetMetadata(
            media_type="application/epub+zip",
            original_name="book.epub",
        ),
    )
    asset = api.DigitalAssetRecord(
        api.DigitalAssetID(7),
        declaration.size_bytes,
        declaration.digests,
        declaration.metadata,
        revision="asset-v1",
    )
    replica_declaration = api.ReplicaDeclaration(
        asset.digital_asset_id,
        api.Location(MAIN_STORE_UUID, "objects/7"),
        observation=api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
    )
    replica = api.ReplicaRecord(
        api.ReplicaID(12),
        replica_declaration.digital_asset_id,
        replica_declaration.location,
        replica_declaration.mode,
        replica_declaration.observation,
        revision="replica-v1",
    )

    assert asset.size_bytes == 4
    assert asset.digests == (digest,)
    assert replica.digital_asset_id == asset.digital_asset_id
    assert replica.location.store_ref == MAIN_STORE_UUID
    assert not hasattr(asset, "record")
    assert not hasattr(replica, "asset_replica_id")
    with pytest.raises(ValueError, match="at least one digest"):
        api.DigitalAssetDeclaration(4, ())
    with pytest.raises(ValueError, match="positive"):
        replace(asset, digital_asset_id=api.DigitalAssetID(0))


def test_public_exports_reject_ambiguous_legacy_value_names() -> None:
    retired_names = {
        "AssetDerivation",
        "AssetDerivationDeclaration",
        "AssetDerivationID",
        "AssetDerivationNotFound",
        "AssetDerivationRecord",
        "AssetDerivationRegistryAPI",
        "AssetDerivationRepositoryAPI",
        "AssetDerivationSpec",
        "AssetLossAction",
        "BackupPlan",
        "CompositeAssetAvailabilityAssessment",
        "CompositeAssetMembership",
        "CompositeAssetNotFound",
        "CompositeAssetRepositoryAPI",
        "CompositeDigitalAsset",
        "CompositeDigitalAssetSpec",
        "CompositeIncomplete",
        "CompositeMemberResolution",
        "DerivationSource",
        "DerivationKind",
        "DigitalAsset",
        "DigitalAssetSpec",
        "DigitalAssetStorageHealth",
        "DistinctBy",
        "DriverFileInfo",
        "DriverObjectEntry",
        "EffectiveStoragePolicies",
        "ItemAssetSelection",
        "ItemAssetResolution",
        "PolicyStatus",
        "PolicyUnsatisfied",
        "ReconciliationPlan",
        "ReconciliationPlanStale",
        "ReconciliationReport",
        "RecipeArtifact",
        "RecipeArtifactReference",
        "RecipeInput",
        "RecipeInputReference",
        "RegisteredBackupArtifact",
        "Replica",
        "ReplicaSpec",
        "ReplicationPlan",
        "ResolvedAsset",
        "StoreRef",
        "StoreSpec",
        "StoredBackupPolicy",
        "StoredReplicationPolicy",
    }

    assert not retired_names & set(api.__all__)
    assert {
        "DigitalAssetDerivationRecord",
        "DigitalAssetDerivationGraph",
        "DigitalAssetDerivationGraphDirection",
        "DigitalAssetRecreationPlan",
        "CompositeDigitalAssetMembership",
        "DigitalAssetDeclaration",
        "DigitalAssetRecord",
        "DriverInventoryEntry",
        "DriverObjectInfo",
        "ReproductionRecipeArtifactReference",
        "ReplicaRecord",
        "StoreConfiguration",
        "StoreStatusObservation",
    } <= set(api.__all__)


def test_repository_ports_operate_on_domain_values_not_record_protocols() -> None:
    class _AssetRepository:
        def add(self, declaration):
            return api.DigitalAssetRecord(
                api.DigitalAssetID(7), declaration.size_bytes,
                declaration.digests, declaration.metadata,
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
    created = repository.add(api.DigitalAssetDeclaration(4, (_sha256(b"book"),)))
    assert isinstance(created, api.DigitalAssetRecord)
    assert "RecordAPI" not in api.__all__


def test_composite_resolution_preserves_relationship_metadata() -> None:
    asset = _asset(7)
    resolved = api.DigitalAssetResolution(asset, _replica(asset=asset))
    relationship = api.CompositeDigitalAssetMembership(
        asset.digital_asset_id,
        0,
        role="audio",
        logical_name="chapter-01.mp3",
        logical_path="disc-1/chapter-01.mp3",
        title="Chapter One",
    )
    member = api.CompositeDigitalAssetMemberResolution(relationship, resolved)

    assert member.location == resolved.location
    assert member.membership.logical_path == "disc-1/chapter-01.mp3"
    assert member.membership.title == "Chapter One"


def test_exact_derivation_recipe_pins_everything_needed_for_replay() -> None:
    source_digest = _sha256(b"book")
    cover_digest = _sha256(b"cover")
    tool_digest = _sha256(b"extractor")
    recipe = api.ReproductionRecipe(
        recipe_type="extract_epub_cover",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                api.DigitalAssetID(7),
                4,
                (source_digest,),
                "book.epub",
                role="primary",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "liuxin-cover-extractor",
            tool_digest,
            version="1.0.0",
            digital_asset_id=api.DigitalAssetID(20),
        ),
        parameters_json='{"cover_index":0}',
        environment_json='{"locale":"C","timezone":"UTC"}',
        command=("liuxin-cover-extractor", "book.epub", "cover.jpg"),
        output_path="cover.jpg",
        expected_output_size=5,
        expected_output_digests=(cover_digest,),
    )
    declaration = api.DigitalAssetDerivationDeclaration(
        result_digital_asset_id=api.DigitalAssetID(8),
        sources=(
            api.DigitalAssetDerivationSourceReference(
                0,
                digital_asset_id=api.DigitalAssetID(7),
                role="primary",
            ),
        ),
        kind=api.DigitalAssetDerivationKind.EXTRACT,
        recipe=recipe,
        output_role="cover",
    )
    derivation = api.DigitalAssetDerivationRecord(
        api.DigitalAssetDerivationID(11), declaration,
    )

    assert derivation.can_recreate_exactly
    assert recipe.inputs[0].digests == (source_digest,)
    assert recipe.executor is not None
    assert recipe.executor.digital_asset_id == api.DigitalAssetID(20)
    assert recipe.expected_output_digests == (cover_digest,)
    assert not hasattr(api, "DerivedDigitalAsset")


def test_composite_derivation_provenance_uses_flattened_atomic_recipe_inputs() -> None:
    recipe = api.ReproductionRecipe(
        recipe_type="package_audiobook",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0, api.DigitalAssetID(7), 3, (_sha256(b"one"),),
                "disc-1/track-01.mp3", role="audio",
            ),
            api.ReproductionRecipeInputReference(
                1, api.DigitalAssetID(8), 3, (_sha256(b"two"),),
                "disc-1/track-02.mp3", role="audio",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "packager", _sha256(b"tool"),
            digital_asset_id=api.DigitalAssetID(20),
        ),
        command=("packager", "disc-1", "audiobook.m4b"),
        output_path="audiobook.m4b",
        expected_output_size=3,
        expected_output_digests=(_sha256(b"m4b"),),
    )
    declaration = api.DigitalAssetDerivationDeclaration(
        api.DigitalAssetID(9),
        (
            api.DigitalAssetDerivationSourceReference(
                0,
                composite_digital_asset_id=api.CompositeDigitalAssetID(3),
                role="source_assembly",
            ),
        ),
        api.DigitalAssetDerivationKind.PACKAGE,
        recipe,
    )

    assert (
        declaration.sources[0].composite_digital_asset_id
        == api.CompositeDigitalAssetID(3)
    )
    assert tuple(input_.digital_asset_id for input_ in recipe.inputs) == (7, 8)


def test_exact_complete_recipe_rejects_missing_replay_evidence() -> None:
    input_ = api.ReproductionRecipeInputReference(
        0, api.DigitalAssetID(7), 4, (_sha256(b"book"),), "book.epub",
    )

    with pytest.raises(ValueError, match="pinned executor"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.EXACT, True, (input_,),
            command=("extract",),
            output_path="cover.jpg",
            expected_output_size=5,
            expected_output_digests=(_sha256(b"cover"),),
        )
    with pytest.raises(ValueError, match="replay command"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.EXACT, True, (input_,),
            executor=api.ReproductionRecipeArtifactReference(
                "extract", _sha256(b"tool"),
                digital_asset_id=api.DigitalAssetID(20),
            ),
            output_path="cover.jpg",
            expected_output_size=5,
            expected_output_digests=(_sha256(b"cover"),),
        )
    with pytest.raises(ValueError, match="canonical JSON"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.BEST_EFFORT, False, (input_,),
            parameters_json='{ "cover_index": 0 }',
        )
    with pytest.raises(ValueError, match="inside the recipe workspace"):
        replace(
            api.ReproductionRecipe(
                "extract", api.Reproducibility.BEST_EFFORT, False, (input_,),
            ),
            output_path="../cover.jpg",
        )
    with pytest.raises(ValueError, match="workflow_id"):
        api.DigitalAssetDerivationDeclaration(
            api.DigitalAssetID(8),
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=api.DigitalAssetID(7),
                ),
            ),
            api.DigitalAssetDerivationKind.CONVERT,
            workflow_id=0,
        )
    with pytest.raises(ValueError, match="workflow_reference"):
        api.DigitalAssetDerivationDeclaration(
            api.DigitalAssetID(8),
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=api.DigitalAssetID(7),
                ),
            ),
            api.DigitalAssetDerivationKind.CONVERT,
            workflow_reference=" ",
        )


def test_derivative_policy_can_trade_copies_for_exact_recreation() -> None:
    original = api.ReplicationPolicy(name="original")
    derivative = api.ReplicationPolicy(
        name="recreatable_derivative",
        min_copies=0,
        synchronous_write_copies=0,
        loss_action=api.DigitalAssetLossAction.RECREATE,
        retention_priority=10,
    )
    no_backup = api.BackupPolicy(
        name="no_derivative_backup",
        min_copies=0,
        retention_priority=10,
    )
    recreation = api.DigitalAssetDerivationID(11)
    empty_status = api.StoragePolicyAssessment(
        api.DigitalAssetID(8), "recreatable_derivative", api.ReplicaMode.ACTIVE,
        meets_minimum=True, meets_target=True,
    )
    empty_backup = api.StoragePolicyAssessment(
        api.DigitalAssetID(8), "no_derivative_backup", api.ReplicaMode.BACKUP,
        meets_minimum=True, meets_target=True,
    )
    health = api.DigitalAssetStorageAssessment(
        api.DigitalAssetID(8),
        empty_status,
        empty_backup,
        exact_recreation_derivation_ids=(recreation,),
    )
    plan = api.DigitalAssetReplicationPlan(
        api.DigitalAssetID(8), exact_recreation_derivation_id=recreation,
    )

    assert original.loss_action is api.DigitalAssetLossAction.REQUIRE_COPY
    assert original.retention_priority > derivative.retention_priority
    assert derivative.effective_target_copies == 0
    assert no_backup.effective_target_copies == 0
    assert health.recreatable and health.recoverable and not health.irrecoverable
    assert plan.exact_recreation_derivation_id == recreation


def test_zero_copy_policy_must_admit_loss_or_recreation() -> None:
    with pytest.raises(ValueError, match="explicitly permit"):
        api.ReplicationPolicy(min_copies=0, synchronous_write_copies=0)
    with pytest.raises(ValueError, match="retention locked"):
        api.BackupPolicy(min_copies=0, retention_locked=True)


def test_health_and_reconciliation_do_not_collapse_distinct_states() -> None:
    replication = api.StoragePolicyAssessment(
        api.DigitalAssetID(7),
        "live",
        api.ReplicaMode.ACTIVE,
        meets_minimum=False,
    )
    backup = api.StoragePolicyAssessment(
        api.DigitalAssetID(7),
        "backup",
        api.ReplicaMode.BACKUP,
        meets_minimum=True,
    )
    health = api.DigitalAssetStorageAssessment(
        api.DigitalAssetID(7),
        replication,
        backup,
        (api.ReplicaID(12),),
    )
    partial_plan = api.StoreReconciliationPlan(
        UUID(int=21),
        MAIN_STORE_UUID,
        False,
        api.EnumerationCompleteness.PARTIAL,
    )

    assert health.readable
    assert health.at_risk
    assert not health.replication_satisfied
    assert health.backup_satisfied
    assert not api.StoreReconciliationReport(partial_plan, applied=False).clean


def test_ingest_bytes_remains_a_small_wrapper_over_transactional_stream_ingest() -> None:
    manager = _IngestHarness()
    result = manager.ingest_bytes(
        b"payload", item_id=api.ItemID(7), role="primary_payload",
        preferred_store_ref=MAIN_STORE_UUID,
    )

    assert manager.observed == b"payload"
    assert manager.size == 7
    assert result.asset_record.digital_asset_id == api.DigitalAssetID(1)
    assert result.replica_record.replica_id == api.ReplicaID(2)
    assert result.location is result.replica_record.location


def test_verification_and_reconciliation_results_preserve_operational_distinctions() -> None:
    unavailable = api.ReplicaVerificationReport(
        api.ReplicaID(1), api.DigitalAssetID(9),
        api.ReplicaState.UNAVAILABLE, None, errors=("offline",),
    )
    corrupt = api.ReplicaVerificationReport(
        api.ReplicaID(2), api.DigitalAssetID(9),
        api.ReplicaState.CORRUPT, True, digest_matches=False,
    )
    verified = api.ReplicaVerificationReport(
        api.ReplicaID(3), api.DigitalAssetID(9),
        api.ReplicaState.VERIFIED, True,
        size_matches=True, digest_matches=True,
    )
    dirty_plan = api.StoreReconciliationPlan(
        UUID(int=20), MAIN_STORE_UUID, True,
        api.EnumerationCompleteness.COMPLETE,
        missing_replica_ids=(api.ReplicaID(1),),
    )
    dirty = api.StoreReconciliationReport(dirty_plan, applied=False)

    assert not unavailable.healthy
    assert not corrupt.healthy
    assert verified.healthy
    assert api.DigitalAssetVerificationReport(
        api.DigitalAssetID(9), (unavailable, verified)
    ).readable
    assert not dirty.clean


def test_reference_manager_is_concrete_and_ingest_is_idempotent() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000901")

    first = manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        item_id=api.ItemID(7),
        expected_digests=(_sha256(b"payload"),),
    )
    retried = manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        item_id=api.ItemID(7),
        expected_digests=(_sha256(b"payload"),),
    )
    deduplicated = manager.ingest_bytes(b"payload")

    assert not InMemoryStorageManager.__abstractmethods__
    assert retried == first
    assert deduplicated.asset_record == first.asset_record
    assert (
        deduplicated.replica_record.replica_id
        == first.replica_record.replica_id
    )
    assert deduplicated.location == first.location
    assert first.verified
    assert manager.read_bytes(first.location) == b"payload"
    assert manager.resolve_item_digital_asset(
        api.ItemID(7)
    ).digital_asset_resolution == manager.resolve_digital_asset(
        first.asset_record.digital_asset_id,
        require_verified=True,
    )

    with pytest.raises(api.StoragePreconditionFailed):
        manager.ingest_bytes(b"different", operation_id=operation_id)


def test_operational_status_reports_replica_and_policy_recovery_actions() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    result = manager.ingest_bytes(b"health payload", verify=True)

    initial = manager.get_operational_status()
    assert not initial.healthy
    assert initial.issues_for("replication_policy_violation") == ()
    assert len(initial.issues_for("backup_policy_violation")) == 1
    assert any(
        action.action == "plan_backup"
        and action.digital_asset_id == result.asset_record.digital_asset_id
        for action in initial.recovery_actions
    )

    store.files[result.location.key] = b"corrupt payload"
    verification = manager.verify_replica(result.replica_record.replica_id)
    assert verification.state is api.ReplicaState.CORRUPT

    degraded = manager.get_operational_status(refresh_stores=True)
    corrupt = degraded.issues_for("replica_corrupt")
    assert len(corrupt) == 1
    assert corrupt[0].replica_id == result.replica_record.replica_id
    assert len(degraded.issues_for("replication_policy_violation")) == 1
    assert any(
        action.action == "replicate_digital_asset"
        and action.replica_id == result.replica_record.replica_id
        for action in degraded.recovery_actions
    )


def test_failed_manager_publication_leaves_no_phantom_asset(
    monkeypatch,
) -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    def _fail_put(*args, **kwargs):
        del args, kwargs
        raise api.StoreUnavailable("destination disconnected during publish")

    monkeypatch.setattr(store, "put", _fail_put)

    with pytest.raises(api.StoreUnavailable, match="disconnected"):
        manager.ingest_bytes(b"payload")

    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()


def test_adopt_location_preserves_metadata_for_a_new_asset() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    location = store.write_bytes(store.location("incoming/book.epub"), b"book").location
    metadata = api.DigitalAssetMetadata(
        original_name="book.epub",
        media_type="application/epub+zip",
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000902")

    result = manager.adopt_location(
        location,
        operation_id=operation_id,
        metadata=metadata,
    )
    retried = manager.adopt_location(
        location,
        operation_id=operation_id,
        metadata=metadata,
    )

    assert result.asset_created
    assert retried == result
    assert result.asset_record.metadata == metadata
    assert result.location == location
    assert result.replica_record.mode is api.ReplicaMode.UNMANAGED
    with pytest.raises(api.StoragePreconditionFailed):
        manager.adopt_location(
            location,
            operation_id=operation_id,
            metadata=api.DigitalAssetMetadata(original_name="different.epub"),
        )


def test_reference_manager_replicates_verifies_and_reconciles() -> None:
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    ingested = manager.ingest_bytes(b"replicated")
    replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )

    assert replica.state is api.ReplicaState.VERIFIED
    assert other.read_bytes(replica.location) == b"replicated"
    other.files[replica.location.key] = b"corrupt"
    corrupt = manager.verify_replica(replica.replica_id)
    assert corrupt.state is api.ReplicaState.CORRUPT

    plan = manager.plan_reconciliation(OTHER_STORE_UUID, verify_digests=True)
    assert plan.corrupt_replica_ids == (replica.replica_id,)
    report = manager.apply_reconciliation(plan)
    assert report.applied
    assert report.updated_replica_ids == (replica.replica_id,)
    assert manager.get_replica_record(
        replica.replica_id
    ).state is api.ReplicaState.CORRUPT

    stale = manager.plan_reconciliation(MAIN_STORE_UUID)
    manager.ingest_bytes(b"changes repository generation")
    with pytest.raises(api.StoreReconciliationPlanStale):
        manager.apply_reconciliation(stale)


def test_policy_plans_do_not_place_independent_modes_on_an_occupied_store() -> None:
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    archive = _MemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    asset = manager.ingest_bytes(b"separate policy modes").asset_record

    backup_plan = manager.plan_backup(asset.digital_asset_id)
    assert len(backup_plan.destination_store_refs) == 1
    assert backup_plan.destination_store_refs[0] != MAIN_STORE_UUID
    backup_store_ref = backup_plan.destination_store_refs[0]
    manager.replicate_digital_asset(
        asset.digital_asset_id,
        destination_store_ref=backup_store_ref,
        mode=api.ReplicaMode.BACKUP,
    )

    two_live_copies = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="two-live-copies",
            min_copies=2,
            target_copies=2,
        )
    )
    manager.set_digital_asset_policies(
        asset.digital_asset_id,
        replication_policy_id=two_live_copies.replication_policy_id,
    )
    replication_plan = manager.plan_replication(asset.digital_asset_id)
    assert len(replication_plan.destination_store_refs) == 1
    assert replication_plan.destination_store_refs[0] not in {
        MAIN_STORE_UUID,
        backup_store_ref,
    }


def test_detailed_file_ingest_returns_result_and_defaults_original_name(
    tmp_path,
) -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = tmp_path / "Tortured-Caf\u00e9-Cafe\u0301.epub"
    source.write_bytes(b"file ingest")

    result = manager.ingest_file(
        source,
        metadata=api.DigitalAssetMetadata(name="Detailed ingest"),
    )

    assert isinstance(result, api.DigitalAssetIngestResult)
    assert result.asset_record.metadata.name == "Detailed ingest"
    assert result.asset_record.metadata.original_name == source.name
    assert manager.read_file(result.asset_record) == b"file ingest"
    with pytest.raises(api.StorageIntegrityError, match="expected 999"):
        manager.ingest_file(source, expected_size=999)


def test_replication_reuses_and_can_override_recorded_placement_hints() -> None:
    main = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    other = _PlacementAwareMemoryStore(OTHER_STORE_UUID)
    archive = _PlacementAwareMemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    initial_hints = {"title": "Original placement", "work_id": 42}
    ingested = manager.ingest_bytes(
        b"rich replication",
        placement_hints=initial_hints,
    )

    inherited = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    override_hints = {"title": "Archive placement", "work_id": 42}
    overridden = manager.replicate_asset(
        ingested.asset_record,
        to=archive,
        metadata=override_hints,
    )

    assert ingested.replica_record.placement_hints == initial_hints
    assert inherited.placement_hints == initial_hints
    assert other.allocation_hints == initial_hints
    assert other.write_hints == initial_hints
    assert overridden.placement_hints == override_hints
    assert archive.allocation_hints == override_hints
    assert archive.write_hints == override_hints


def test_verify_digital_asset_supports_exact_ordered_replica_subsets() -> None:
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    archive = _MemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    ingested = manager.ingest_bytes(b"verify subset")
    other_replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    archive_replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=ARCHIVE_STORE_UUID,
    )

    report = manager.verify_digital_asset(
        ingested.asset_record.digital_asset_id,
        replica_ids=(archive_replica.replica_id, other_replica.replica_id),
    )

    assert tuple(item.replica_id for item in report.replica_reports) == (
        archive_replica.replica_id,
        other_replica.replica_id,
    )
    first_only = manager.verify_digital_asset(
        ingested.asset_record.digital_asset_id,
        replica_ids=(archive_replica.replica_id, other_replica.replica_id),
        stop_after_first_healthy=True,
    )
    assert len(first_only.replica_reports) == 1
    with pytest.raises(ValueError, match="mutually exclusive"):
        manager.verify_digital_asset(
            ingested.asset_record.digital_asset_id,
            stop_after_first_healthy=True,
            all_replicas=True,
        )


def test_composite_convenience_ingests_and_exports_members(tmp_path) -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    composite = manager.store_composite(
        {
            "text/chapter 1.txt": b"chapter one",
            "images/Caf\u00e9-Cafe\u0301.bin": b"cover bytes",
        },
        name="exportable package",
    )

    exported = manager.export_composite_to_directory(
        composite,
        tmp_path / "exported",
    )
    assert {path.relative_to(tmp_path / "exported").as_posix() for path in exported} == {
        "text/chapter 1.txt",
        "images/Caf\u00e9-Cafe\u0301.bin",
    }
    assert (tmp_path / "exported/text/chapter 1.txt").read_bytes() == b"chapter one"

    with manager.open_composite_zip(composite) as stream:
        with zipfile.ZipFile(stream) as archive_file:
            assert set(archive_file.namelist()) == {
                "text/chapter 1.txt",
                "images/Caf\u00e9-Cafe\u0301.bin",
            }
            assert archive_file.read("images/Caf\u00e9-Cafe\u0301.bin") == b"cover bytes"

    with pytest.raises(ValueError, match="logical path"):
        manager.store_composite({"../escape.bin": b"escape"})


def test_persistence_ports_have_a_dedicated_spi_with_compatibility_imports() -> None:
    from LiuXin_alpha.storage.api.persistence_api import (
        DigitalAssetRepositoryAPI as PersistenceRepository,
        StorageUnitOfWorkAPI as PersistenceUnitOfWork,
    )
    from LiuXin_alpha.storage.api.storage_manager_api.repositories_api import (
        DigitalAssetRepositoryAPI as CompatibilityRepository,
        StorageUnitOfWorkAPI as CompatibilityUnitOfWork,
    )

    assert PersistenceRepository is CompatibilityRepository
    assert PersistenceUnitOfWork is CompatibilityUnitOfWork


def test_reference_manager_records_exact_derivation_and_disposable_policy() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    executor = manager.ingest_bytes(b"tool").asset_record
    result = manager.ingest_bytes(b"cover").asset_record
    recipe = api.ReproductionRecipe(
        recipe_type="extract_cover",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                source.digital_asset_id,
                source.size_bytes,
                source.digests,
                "source.epub",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "cover-extractor",
            executor.digests[0],
            digital_asset_id=executor.digital_asset_id,
        ),
        command=("cover-extractor", "source.epub", "cover.jpg"),
        output_path="cover.jpg",
        expected_output_size=result.size_bytes,
        expected_output_digests=result.digests,
    )
    derivation = manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            result.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=source.digital_asset_id,
                    role="source",
                ),
            ),
            api.DigitalAssetDerivationKind.EXTRACT,
            recipe,
            output_role="cover",
        )
    )
    disposable = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="recreate-derived",
            min_copies=0,
            synchronous_write_copies=0,
            loss_action=api.DigitalAssetLossAction.RECREATE,
        )
    )
    no_backup = manager.create_backup_policy(
        api.BackupPolicy(name="no-derived-backup", min_copies=0)
    )
    manager.set_digital_asset_policies(
        result.digital_asset_id,
        replication_policy_id=disposable.replication_policy_id,
        backup_policy_id=no_backup.backup_policy_id,
    )

    result_replica = next(
        manager.iter_replica_records(
            digital_asset_id=result.digital_asset_id
        )
    )
    manager.remove_replica(result_replica.replica_id)
    assessment = manager.assess_digital_asset(result.digital_asset_id)

    assert derivation.can_recreate_exactly
    assert assessment.unavailable
    assert assessment.recreatable
    assert assessment.recoverable
    assert manager.plan_replication(
        result.digital_asset_id
    ).exact_recreation_derivation_id == derivation.digital_asset_derivation_id


def test_reference_manager_validates_composites_and_derivation_cycles() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    first = manager.ingest_bytes(b"first").asset_record
    second = manager.ingest_bytes(b"second").asset_record
    composite = manager.declare_composite_digital_asset(
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(
                    first.digital_asset_id,
                    0,
                    logical_path="one.bin",
                ),
                api.CompositeDigitalAssetMembership(
                    second.digital_asset_id,
                    1,
                    logical_path="two.bin",
                ),
            ),
            name="pair",
        )
    )
    manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            second.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=first.digital_asset_id,
                ),
            ),
            api.DigitalAssetDerivationKind.OTHER,
        )
    )

    assert len(
        manager.resolve_composite_digital_asset(
            composite.composite_digital_asset_id
        )
    ) == 2
    with pytest.raises(api.StoragePreconditionFailed, match="cycle"):
        manager.record_digital_asset_derivation(
            api.DigitalAssetDerivationDeclaration(
                first.digital_asset_id,
                (
                    api.DigitalAssetDerivationSourceReference(
                        0,
                        digital_asset_id=second.digital_asset_id,
                    ),
                ),
                api.DigitalAssetDerivationKind.OTHER,
            )
        )


def test_derivation_graph_traverses_chains_branches_and_workflows() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    html = manager.ingest_bytes(b"html").asset_record
    epub = manager.ingest_bytes(b"epub").asset_record
    mobi = manager.ingest_bytes(b"mobi").asset_record
    azw3 = manager.ingest_bytes(b"azw3").asset_record

    html_to_epub = manager.record_derivation(
        epub,
        [html],
        kind="convert",
        workflow_id=42,
        notes="html to epub",
    )
    epub_to_mobi = manager.record_derivation(
        mobi,
        [epub],
        kind="convert",
        workflow_id=42,
        notes="epub to mobi",
    )
    epub_to_azw3 = manager.record_derivation(
        azw3,
        [epub],
        kind="convert",
        workflow_id=42,
        notes="epub to azw3",
    )
    direct_html_to_mobi = manager.record_derivation(
        mobi,
        [html],
        kind="convert",
        workflow_id=99,
        notes="direct alternative",
    )

    ancestors = tuple(manager.iter_derivation_ancestors(mobi.digital_asset_id))
    descendants = tuple(
        manager.iter_derivation_descendants(html.digital_asset_id)
    )
    workflow_chain = manager.get_derivation_graph(
        mobi.digital_asset_id,
        direction="ancestors",
        workflow_id=42,
    )
    shallow = manager.get_derivation_graph(
        mobi.digital_asset_id,
        direction=api.DigitalAssetDerivationGraphDirection.ANCESTORS,
        max_depth=1,
        workflow_id=42,
    )

    assert tuple(
        record.digital_asset_derivation_id for record in ancestors
    ) == (
        epub_to_mobi.digital_asset_derivation_id,
        direct_html_to_mobi.digital_asset_derivation_id,
        html_to_epub.digital_asset_derivation_id,
    )
    assert tuple(
        record.digital_asset_derivation_id for record in descendants
    ) == (
        html_to_epub.digital_asset_derivation_id,
        direct_html_to_mobi.digital_asset_derivation_id,
        epub_to_mobi.digital_asset_derivation_id,
        epub_to_azw3.digital_asset_derivation_id,
    )
    assert workflow_chain.digital_asset_ids == (
        mobi.digital_asset_id,
        epub.digital_asset_id,
        html.digital_asset_id,
    )
    assert tuple(
        record.digital_asset_derivation_id
        for record in workflow_chain.derivation_records
    ) == (
        epub_to_mobi.digital_asset_derivation_id,
        html_to_epub.digital_asset_derivation_id,
    )
    assert tuple(
        record.digital_asset_derivation_id
        for record in shallow.derivation_records
    ) == (epub_to_mobi.digital_asset_derivation_id,)
    assert shallow.truncated


def test_namespaced_workflow_references_filter_derivations_and_graphs() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    packaged = manager.ingest_bytes(b"package").asset_record
    recorded = manager.record_derivation(
        packaged,
        [source],
        kind="package",
        workflow_reference="backup:17",
    )

    assert tuple(
        manager.iter_digital_asset_derivation_records(
            workflow_reference="backup:17",
        )
    ) == (recorded,)
    assert tuple(
        manager.get_derivation_graph(
            packaged.digital_asset_id,
            direction="ancestors",
            workflow_reference="backup:17",
        ).derivation_records
    ) == (recorded,)
    assert not tuple(
        manager.iter_digital_asset_derivation_records(
            workflow_reference="backup:18",
        )
    )
    with pytest.raises(ValueError, match="workflow_reference"):
        tuple(
            manager.iter_digital_asset_derivation_records(
                workflow_reference="",
            )
        )


def test_recreation_plan_selects_shortest_route_and_orders_chain() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    html_ingest = manager.ingest_bytes(b"html")
    epub_ingest = manager.ingest_bytes(b"epub")
    mobi_ingest = manager.ingest_bytes(b"mobi")
    tool = manager.ingest_bytes(b"converter").asset_record
    html = html_ingest.asset_record
    epub = epub_ingest.asset_record
    mobi = mobi_ingest.asset_record

    def exact_conversion(
        source: api.DigitalAssetRecord,
        result: api.DigitalAssetRecord,
        recipe_type: str,
        *,
        workflow_id: int,
    ) -> api.DigitalAssetDerivationRecord:
        recipe = api.ReproductionRecipe(
            recipe_type=recipe_type,
            reproducibility=api.Reproducibility.EXACT,
            complete=True,
            inputs=(
                api.ReproductionRecipeInputReference(
                    0,
                    source.digital_asset_id,
                    source.size_bytes,
                    source.digests,
                    "input.bin",
                ),
            ),
            executor=api.ReproductionRecipeArtifactReference(
                "converter",
                tool.digests[0],
                digital_asset_id=tool.digital_asset_id,
            ),
            parameters_json=f'{{"profile":"{recipe_type}"}}',
            command=("converter", "input.bin", "output.bin"),
            output_path="output.bin",
            expected_output_size=result.size_bytes,
            expected_output_digests=result.digests,
        )
        return manager.record_derivation(
            result,
            [source],
            kind="convert",
            recipe=recipe,
            workflow_id=workflow_id,
        )

    html_to_epub = exact_conversion(
        html, epub, "html_to_epub", workflow_id=42,
    )
    epub_to_mobi = exact_conversion(
        epub, mobi, "epub_to_mobi", workflow_id=42,
    )
    direct = exact_conversion(
        html, mobi, "html_to_mobi", workflow_id=99,
    )

    available = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert available.already_available
    assert not available.requires_replay

    manager.remove_replica(epub_ingest.replica_record.replica_id)
    manager.remove_replica(mobi_ingest.replica_record.replica_id)

    shortest = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert shortest.can_recreate_exactly
    assert shortest.selected_derivation_id == direct.digital_asset_derivation_id
    assert tuple(
        step.digital_asset_derivation_id for step in shortest.steps
    ) == (direct.digital_asset_derivation_id,)
    assert shortest.alternative_derivation_ids == (
        epub_to_mobi.digital_asset_derivation_id,
    )
    assert set(shortest.available_digital_asset_ids) == {
        html.digital_asset_id,
        tool.digital_asset_id,
    }

    assert manager.forget_digital_asset_derivation(
        direct.digital_asset_derivation_id
    )
    chained = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert chained.can_recreate_exactly
    assert chained.selected_derivation_id == (
        epub_to_mobi.digital_asset_derivation_id
    )
    assert tuple(
        step.digital_asset_derivation_id for step in chained.steps
    ) == (
        html_to_epub.digital_asset_derivation_id,
        epub_to_mobi.digital_asset_derivation_id,
    )
    assert tuple(
        step.declaration.recipe.recipe_type
        for step in chained.steps
        if step.declaration.recipe is not None
    ) == ("html_to_epub", "epub_to_mobi")
    assert tuple(
        step.declaration.recipe.parameters_json
        for step in chained.steps
        if step.declaration.recipe is not None
    ) == (
        '{"profile":"html_to_epub"}',
        '{"profile":"epub_to_mobi"}',
    )


def test_reference_manager_store_lifecycle_uses_injected_factory() -> None:
    created: list[api.StoreUUID] = []

    def factory(configuration: api.StoreConfiguration) -> _MemoryStore:
        created.append(configuration.store_uuid)
        return _MemoryStore(configuration.store_uuid)

    manager = InMemoryStorageManager(store_factory=factory)
    configuration = api.StoreConfiguration(
        MAIN_STORE_UUID,
        "main",
        "memory",
        "memory://main",
    )
    manager.create_store(configuration)

    assert manager.get_default_store_ref() == MAIN_STORE_UUID
    assert manager.get_store_configuration(MAIN_STORE_UUID) == configuration
    assert manager.reload_stores().loaded_stores == 1
    assert created == [MAIN_STORE_UUID, MAIN_STORE_UUID]
    assert manager.remove_store(MAIN_STORE_UUID, forget_configuration=True)
    with pytest.raises(api.StoreConfigurationNotFound):
        manager.get_store_configuration(MAIN_STORE_UUID)


def test_reference_manager_distinguishes_unknown_and_unavailable_stores() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    with pytest.raises(api.StoreConfigurationNotFound):
        manager.try_stat(api.Location(OTHER_STORE_UUID, "missing"))

    assert manager.remove_store(MAIN_STORE_UUID)
    with pytest.raises(api.StoreUnavailable):
        manager.get_store(MAIN_STORE_UUID)
    observations = tuple(manager.iter_store_statuses())
    assert len(observations) == 1
    assert observations[0].store_ref == MAIN_STORE_UUID
    assert not observations[0].status.available


def test_store_default_policies_are_captured_at_first_placement() -> None:
    manager = InMemoryStorageManager()
    main_policy = manager.create_replication_policy(
        api.ReplicationPolicy(name="main-policy")
    )
    other_policy = manager.create_replication_policy(
        api.ReplicationPolicy(name="other-policy")
    )
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    main_configuration = replace(
        main.configuration,
        store_default_replication_policy_id=(
            main_policy.replication_policy_id
        ),
    )
    other_configuration = replace(
        other.configuration,
        store_default_replication_policy_id=(
            other_policy.replication_policy_id
        ),
    )
    manager.attach_store(main_configuration, main)
    manager.attach_store(other_configuration, other)

    result = manager.ingest_bytes(
        b"placement-policy",
        preferred_store_ref=MAIN_STORE_UUID,
    )
    manager.replicate_digital_asset(
        result.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    policies = manager.resolve_effective_policies(
        result.asset_record.digital_asset_id
    )

    assert (
        result.asset_record.replication_policy_id
        == main_policy.replication_policy_id
    )
    assert policies.replication.name == "main-policy"
    assert policies.replication_source == "digital_asset"


def test_policy_updates_validate_recreation_and_revision_transactionally() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    asset = manager.ingest_bytes(b"no-recipe").asset_record
    policy_record = manager.create_replication_policy(
        api.ReplicationPolicy(name="retained")
    )
    manager.set_digital_asset_policies(
        asset.digital_asset_id,
        replication_policy_id=policy_record.replication_policy_id,
    )
    recreate = api.ReplicationPolicy(
        name="unsafe-recreate",
        min_copies=0,
        synchronous_write_copies=0,
        loss_action=api.DigitalAssetLossAction.RECREATE,
    )

    with pytest.raises(api.StoragePolicyUnsatisfied):
        manager.update_replication_policy(
            policy_record.replication_policy_id,
            recreate,
            if_revision=policy_record.revision,
        )
    assert manager.get_replication_policy_record(
        policy_record.replication_policy_id
    ) == policy_record

    with pytest.raises(api.StoragePreconditionFailed, match="revision"):
        manager.update_replication_policy(
            policy_record.replication_policy_id,
            api.ReplicationPolicy(name="changed"),
            if_revision="stale",
        )

    backup_record = manager.create_backup_policy(
        api.BackupPolicy(name="backup")
    )
    updated_backup = manager.update_backup_policy(
        backup_record.backup_policy_id,
        api.BackupPolicy(name="updated-backup"),
        if_revision=backup_record.revision,
    )
    assert updated_backup.revision != backup_record.revision
    with pytest.raises(api.StoragePreconditionFailed, match="revision"):
        manager.update_backup_policy(
            backup_record.backup_policy_id,
            api.BackupPolicy(name="stale-backup"),
            if_revision=backup_record.revision,
        )


def test_uri_only_recipe_artifacts_require_an_availability_resolver() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    result = manager.ingest_bytes(b"derived").asset_record
    recipe = api.ReproductionRecipe(
        recipe_type="derive",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                source.digital_asset_id,
                source.size_bytes,
                source.digests,
                "source.bin",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "external-tool",
            _sha256(b"tool"),
            uri="file:///not-a-managed-artifact/tool",
        ),
        command=("external-tool", "source.bin", "derived.bin"),
        output_path="derived.bin",
        expected_output_size=result.size_bytes,
        expected_output_digests=result.digests,
    )
    manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            result.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=source.digital_asset_id,
                ),
            ),
            api.DigitalAssetDerivationKind.OTHER,
            recipe,
        )
    )
    recreate = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="recreate",
            min_copies=0,
            synchronous_write_copies=0,
            loss_action=api.DigitalAssetLossAction.RECREATE,
        )
    )

    assert not manager.assess_digital_asset(result.digital_asset_id).recreatable
    result_replica = next(
        manager.iter_replica_records(
            digital_asset_id=result.digital_asset_id
        )
    )
    manager.remove_replica(result_replica.replica_id)
    recreation_plan = manager.plan_digital_asset_recreation(
        result.digital_asset_id
    )
    assert not recreation_plan.can_recreate_exactly
    assert result.digital_asset_id in (
        recreation_plan.unavailable_digital_asset_ids
    )
    assert any(
        "unavailable artefact" in warning
        for warning in recreation_plan.warnings
    )
    with pytest.raises(api.StoragePolicyUnsatisfied):
        manager.set_digital_asset_policies(
            result.digital_asset_id,
            replication_policy_id=recreate.replication_policy_id,
        )


def test_optional_composite_members_do_not_make_assessment_unreadable() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    required = manager.ingest_bytes(b"required").asset_record
    optional_result = manager.ingest_bytes(b"optional")
    manager.remove_replica(optional_result.replica_record.replica_id)
    composite = manager.declare_composite_digital_asset(
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(
                    required.digital_asset_id,
                    0,
                    required=True,
                ),
                api.CompositeDigitalAssetMembership(
                    optional_result.asset_record.digital_asset_id,
                    1,
                    required=False,
                ),
            )
        )
    )

    assessment = manager.assess_composite_digital_asset(
        composite.composite_digital_asset_id
    )
    assert assessment.readable
    assert assessment.expected_members == 1
    assert not assessment.errors


def test_staged_replica_is_not_selected_or_counted_as_readable() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    asset = manager.declare_digital_asset(
        api.DigitalAssetDeclaration(6, (_sha256(b"staged"),))
    )
    location = store.location("staged.bin")
    storage_utils.write_bytes(store, location, b"staged")
    replica = manager._add_replica(  # noqa: SLF001 - reference-state fixture
        api.ReplicaDeclaration(
            asset.digital_asset_id,
            location,
            observation=api.ReplicaObservation(api.ReplicaState.STAGED),
        )
    )

    with pytest.raises(api.NoReadableReplica):
        manager.select_replica(asset.digital_asset_id)
    assert replica.replica_id not in manager.assess_digital_asset(
        asset.digital_asset_id
    ).readable_replica_ids


def test_ingest_operation_id_binds_the_complete_request() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000902")
    manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        metadata=api.DigitalAssetMetadata(name="first"),
    )

    with pytest.raises(api.StoragePreconditionFailed, match="different request"):
        manager.ingest_bytes(
            b"payload",
            operation_id=operation_id,
            metadata=api.DigitalAssetMetadata(name="changed"),
        )


def test_ingest_operation_id_binds_placement_hints() -> None:
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000903")
    manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        placement_hints={"title": "First"},
    )

    with pytest.raises(api.StoragePreconditionFailed, match="different request"):
        manager.ingest_bytes(
            b"payload",
            operation_id=operation_id,
            placement_hints={"title": "Changed"},
        )


def test_ingest_republishes_when_a_matching_replica_is_missing() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    first = manager.ingest_bytes(b"replace-missing")
    store.delete(first.location)
    assert manager.verify_replica(
        first.replica_record.replica_id
    ).state is api.ReplicaState.MISSING

    repaired = manager.ingest_bytes(b"replace-missing")
    assert repaired.replica_created
    assert repaired.replica_record.replica_id != first.replica_record.replica_id
    assert manager.read_bytes(repaired.location) == b"replace-missing"


def test_storage_manager_exposes_concrete_convenience_operations() -> None:
    from LiuXin_alpha.storage.api import storage_manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.convenience_api import (
        DigitalAssetFileIdentifier,
        StorageConvenienceAPI,
    )

    assert issubclass(api.StorageManagerAPI, StorageConvenienceAPI)
    assert api.StorageConvenienceAPI is StorageConvenienceAPI
    assert api.DigitalAssetFileIdentifier is DigitalAssetFileIdentifier
    assert "DigitalAssetFileIdentifier" in storage_manager_api.__all__
    assert (
        inspect.signature(api.StorageManagerAPI.get_file)
        .parameters["identifier"]
        .annotation
        == "DigitalAssetFileIdentifier"
    )
    assert {
        "store",
        "store_bytes",
        "store_stream",
        "store_file",
        "get_file",
        "open_file",
        "read_file",
        "declare_asset",
        "open_asset",
        "read_asset",
        "replicate_asset",
        "link",
        "unlink",
        "create_composite",
        "define_replication_policy",
        "define_backup_policy",
        "record_derivation",
    }.isdisjoint(api.StorageManagerAPI.__abstractmethods__)
    assert "add_store" in api.StorageManagerAPI.__abstractmethods__
    assert "add_store" not in InMemoryStorageManager.__abstractmethods__
    assert {
        "list_ingest_operations",
        "recover_pending_ingests",
        "retry_ingest_operation",
    }.issubset(api.StorageManagerAPI.__abstractmethods__)
    assert {
        "list_ingest_operations",
        "recover_pending_ingests",
        "retry_ingest_operation",
    }.isdisjoint(InMemoryStorageManager.__abstractmethods__)


def test_convenience_storage_and_retrieval_accept_ordinary_inputs(
    tmp_path,
) -> None:
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )

    book = manager.store(
        b"book payload",
        name="Book",
        media_type="application/epub+zip",
        original_name="book.epub",
        attributes={"language": "en"},
        item=9,
        store=main,
    )
    streamed = manager.store(
        io.BytesIO(b"streamed"),
        expected_size=8,
        name="Streamed",
    )
    source_path = tmp_path / "cover.jpg"
    source_path.write_bytes(b"cover")
    cover = manager.store(
        source_path,
        media_type="image/jpeg",
    )

    assert isinstance(book, api.DigitalAssetRecord)
    assert book.metadata.attributes == (("language", "en"),)
    assert manager.read_asset(book) == b"book payload"
    assert manager.read_asset(book, offset=5, length=7) == b"payload"
    with manager.open_file(book.digital_asset_id) as source:
        assert source.read() == b"book payload"
    with manager.get_file(book.digital_asset_id) as source:
        assert source.read() == b"book payload"
    assert manager.read_file(_sha256(b"book payload")) == b"book payload"
    assert manager.read_file(
        _sha256(b"book payload").value,
        offset=5,
        length=7,
    ) == b"payload"
    with manager.open_asset(streamed, verified=True) as source:
        assert source.read() == b"streamed"
    assert cover.metadata.original_name == "cover.jpg"
    assert manager.read_asset(cover) == b"cover"
    assert manager.resolve_item_digital_asset(
        api.ItemID(9)
    ).digital_asset_resolution.asset_record == book

    backup = manager.replicate_asset(
        book,
        to=other.configuration,
        replica_mode="backup",
    )
    assert backup.mode is api.ReplicaMode.BACKUP
    assert manager.read_asset(
        book,
        store=other,
        replica_mode="backup",
        verified=True,
    ) == b"book payload"

    compatibility_copy = manager.replicate_asset(
        streamed,
        to=other.configuration,
        mode="backup",
    )
    assert compatibility_copy.mode is api.ReplicaMode.BACKUP
    with pytest.raises(TypeError, match="replica_mode or mode"):
        manager.read_file(
            book,
            replica_mode="active",
            mode="active",
        )

    with pytest.raises(api.DigitalAssetNotFound, match="registered"):
        manager.get_file("f" * 64)


def test_convenience_storage_forwards_metadata_as_rich_placement_hints() -> None:
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    metadata = {
        "title": "Permutation City",
        "primary_agents": ["Greg Egan"],
        "file_formats": ["EPUB"],
    }

    asset = manager.store_bytes(b"book", metadata=metadata)
    replica = manager.select_replica(asset.digital_asset_id)

    assert store.allocation_hints == metadata
    assert store.write_hints == metadata
    assert replica.location.key == "rich/Permutation City"
    assert manager.read_asset(asset) == b"book"


def test_store_convenience_projects_metadata_for_rich_store_placement() -> None:
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    metadata = {
        "title": "Permutation City",
        "primary_agents": ["Greg Egan"],
    }

    info = store.store_bytes(b"book", metadata=metadata)

    assert store.allocation_hints == metadata
    assert store.write_hints == metadata
    assert info.location.key == "rich/Permutation City"


def test_store_convenience_requires_allocation_or_an_explicit_location() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)

    with pytest.raises(
        api.StoreUnsupportedOperation,
        match="supply location explicitly",
    ):
        store.store_bytes(b"book", name="book.epub")

    info = store.store_bytes(
        b"book",
        location="explicit/book.epub",
    )
    assert info.location.key == "explicit/book.epub"


def test_convenience_storage_keeps_hints_advisory_for_plain_stores() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    asset = manager.store_bytes(
        b"book",
        metadata=api.WorkStorageHints(
            work_id=5,
            title="Permutation City",
        ),
    )

    assert manager.read_asset(asset) == b"book"


def test_convenience_composites_and_item_links_hide_membership_objects() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    book = manager.store_bytes(b"book")
    cover = manager.store_bytes(b"cover")

    composite = manager.create_composite(
        {"book.epub": book, "cover.jpg": cover},
        name="book package",
        attributes={"edition": "first"},
    )
    manager.link(12, composite, role="package")
    selected = manager.resolve_item_digital_asset(
        api.ItemID(12),
        role="package",
    )

    assert [member.logical_path for member in composite.members] == [
        "book.epub",
        "cover.jpg",
    ]
    assert composite.attributes == (("edition", "first"),)
    assert selected.composite_digital_asset_record == composite
    assert manager.unlink(12, role="package")

    manager.link(
        12,
        composite.composite_digital_asset_id,
        role="package",
        composite=True,
    )
    assert manager.resolve_item_digital_asset(
        api.ItemID(12), role="package"
    ).composite_digital_asset_record == composite


def test_convenience_policy_store_and_declaration_helpers() -> None:
    created: list[api.StoreConfiguration] = []

    def factory(configuration: api.StoreConfiguration) -> _MemoryStore:
        created.append(configuration)
        return _MemoryStore(configuration.store_uuid)

    manager = InMemoryStorageManager(store_factory=factory)
    replication = manager.define_replication_policy(
        "durable",
        copies=2,
        target=3,
        spread_by=("host",),
        require_tags={"local"},
        synchronous_copies=2,
    )
    backup = manager.define_backup_policy(
        "offsite",
        copies=1,
        mode="archive",
        require_tags={"offsite"},
        locked=True,
    )
    configuration = manager.add_store(
        "primary",
        "memory",
        "memory://primary",
        store_uuid=MAIN_STORE_UUID,
        tags=("local", "offsite"),
        replication=replication,
        backup=backup,
        modes=("active", "archive"),
    )
    declared = manager.declare_asset(
        4,
        {"sha256": hashlib.sha256(b"book").hexdigest()},
        name="known book",
        replication=replication,
        backup=backup,
    )

    assert replication.policy.min_copies == 2
    assert replication.policy.distinct_by == (
        api.ReplicaSeparationDimension.HOST,
    )
    assert backup.policy.mode is api.ReplicaMode.ARCHIVE
    assert configuration.store_default_replication_policy_id == (
        replication.replication_policy_id
    )
    assert configuration.store_default_backup_policy_id == (
        backup.backup_policy_id
    )
    assert configuration.supported_replica_modes == {
        api.ReplicaMode.ACTIVE,
        api.ReplicaMode.ARCHIVE,
    }
    assert created == [configuration]
    assert declared.metadata.name == "known book"
    assert declared.replication_policy_id == replication.replication_policy_id


def test_convenience_provenance_hides_source_reference_objects() -> None:
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.store_bytes(b"source")
    result = manager.store_bytes(b"result")
    composite = manager.create_composite([source], name="source package")

    atomic = manager.record_derivation(
        result,
        {"primary": source},
        kind="extract",
        output_role="cover",
        notes="ordinary provenance",
    )
    composite_result = manager.store_bytes(b"composite result")
    composite_derivation = manager.record_derivation(
        composite_result,
        [composite],
        kind=api.DigitalAssetDerivationKind.PACKAGE,
    )

    assert atomic.declaration.sources[0].digital_asset_id == (
        source.digital_asset_id
    )
    assert atomic.declaration.sources[0].role == "primary"
    assert atomic.declaration.kind is api.DigitalAssetDerivationKind.EXTRACT
    assert atomic.declaration.notes == "ordinary provenance"
    assert (
        composite_derivation.declaration.sources[0].composite_digital_asset_id
        == composite.composite_digital_asset_id
    )
