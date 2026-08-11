"""Transactional contract tests for the second-generation storage API."""

from __future__ import annotations

import hashlib
import io

from collections.abc import Iterator
from dataclasses import dataclass

import pytest

import LiuXin_alpha.storage.api2 as api2


class _MemoryWriteSession:
    def __init__(
        self,
        store: "_MemoryStore",
        location: api2.Location,
        *,
        mode: api2.WriteMode,
        expected_size: int | None,
        expected_digest: api2.Digest | None,
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
            raise api2.StoreError("write session is already finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api2.FileInfo:
        if self.committed or self.aborted:
            raise api2.StoreError("write session is already finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api2.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api2.StoreIntegrityError("digest mismatch")

        exists = self.location.key in self.store.files
        if self.mode is api2.WriteMode.CREATE_ONLY and exists:
            raise api2.StoreAlreadyExists(self.location.key)
        if self.mode is api2.WriteMode.REPLACE and not exists:
            raise api2.StoreNotFound(self.location.key)

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


class _MemoryStore(api2.StoreAPI):
    def __init__(self, store_ref: api2.StoreRef = "memory") -> None:
        store_id = store_ref if isinstance(store_ref, int) else None
        store_name = str(store_ref) if isinstance(store_ref, str) else f"store-{store_ref}"
        self._spec = api2.StoreSpec(
            store_id=store_id,
            store_name=store_name,
            store_kind="memory",
            store_root_uri=f"memory://{store_name}",
        )
        self.files: dict[str, bytes] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.online = True
        self.read_only = False
        self._capabilities = api2.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            atomic_publish=True,
            range_reads=True,
            authoritative_digest=False,
            enumeration=api2.EnumerationCompleteness.COMPLETE,
        )

    @property
    def spec(self) -> api2.StoreSpec:
        return self._spec

    @property
    def capabilities(self) -> api2.StoreCapabilities:
        return self._capabilities

    def location(self, *tokens: str) -> api2.Location:
        key = "/".join(token.strip("/") for token in tokens if token.strip("/"))
        return api2.Location(self.store_ref, key)

    def _key(self, location: api2.Location) -> str:
        if location.store_ref != self.store_ref:
            raise api2.StoreInvalidLocation(str(location))
        return location.key

    def _require_online(self) -> None:
        if not self.online:
            raise api2.StoreUnavailable(str(self.store_ref))

    def stat(self, location: api2.Location) -> api2.FileInfo:
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api2.StoreNotFound(key)
        payload = self.files[key]
        return api2.FileInfo(
            location=location,
            size=len(payload),
            digest=api2.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[key],
        )

    def open_read(
        self,
        location: api2.Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api2.StoreNotFound(key)
        if offset < 0 or (length is not None and length < 0):
            raise api2.StoreInvalidLocation("negative read range")
        payload = self.files[key][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        location: api2.Location,
        *,
        mode: api2.WriteMode = api2.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api2.Digest | None = None,
    ) -> _MemoryWriteSession:
        self._require_online()
        self._key(location)
        if self.read_only:
            raise api2.StoreReadOnly(str(self.store_ref))
        return _MemoryWriteSession(
            self,
            location,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        location: api2.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        self._require_online()
        key = self._key(location)
        if self.read_only:
            raise api2.StoreReadOnly(str(self.store_ref))
        if key not in self.files:
            if missing_ok:
                return
            raise api2.StoreNotFound(key)
        if if_version is not None and self.versions[key] != if_version:
            raise api2.StorePreconditionFailed(key)
        del self.files[key]
        del self.versions[key]

    def iter_locations(
        self,
        *,
        prefix: api2.Location | None = None,
    ) -> Iterator[api2.Location]:
        self._require_online()
        prefix_key = "" if prefix is None else self._key(prefix)
        for key in sorted(self.files):
            if key.startswith(prefix_key):
                yield api2.Location(self.store_ref, key)

    def startup(self) -> api2.StoreStatus:
        self.online = True
        return self.status()

    def probe(self) -> api2.StoreStatus:
        return self.status()

    def status(self, *, refresh: bool = False) -> api2.StoreStatus:
        return api2.StoreStatus(
            available=self.online,
            writable=self.online and not self.read_only,
            total_bytes=1024 * 1024,
            free_bytes=1024 * 1024 - sum(map(len, self.files.values())),
        )

    def close(self) -> None:
        self.online = False


class _MemoryManager(api2.StorageRouterAPI):
    def __init__(self, store: _MemoryStore) -> None:
        self.store = store

    def _route(self, location: api2.Location) -> _MemoryStore:
        if location.store_ref != self.store.store_ref:
            raise api2.StoreInvalidLocation(str(location))
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
        mode=api2.WriteMode.CREATE_ONLY,
        expected_size=None,
        expected_digest=None,
    ):
        return api2.put(
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
            raise api2.StoreInvalidLocation(str(store_ref))
        return self.store.capabilities

    def status(self, store_ref):
        if store_ref != self.store.store_ref:
            raise api2.StoreInvalidLocation(str(store_ref))
        return self.store.status()


def _sha256(data: bytes) -> api2.Digest:
    return api2.Digest("sha256", hashlib.sha256(data).hexdigest())


@dataclass
class _DigitalAssetRecord:
    digital_asset_id: int | None


@dataclass
class _ReplicaRecord:
    asset_replica_id: int | None


class _IngestHarness(api2.AssetIngestAPI):
    def __init__(self) -> None:
        self.observed: bytes | None = None
        self.size: int | None = None

    def ingest_stream(self, stream, **kwargs):
        self.observed = stream.read()
        self.size = kwargs["size_bytes"]
        return api2.IngestResult(
            _DigitalAssetRecord(1), _ReplicaRecord(2),
            api2.Location("main", "payload"), True, True,
        )

    def adopt_location(self, location, **kwargs):
        return api2.IngestResult(
            _DigitalAssetRecord(1), _ReplicaRecord(2), location, False, True,
        )


def test_public_surface_is_small_complete_and_unique() -> None:
    assert len(api2.__all__) == len(set(api2.__all__))
    assert all(hasattr(api2, name) for name in api2.__all__)
    assert api2.StorageRouterAPI.__abstractmethods__ == {
        "stat",
        "get",
        "put",
        "delete",
        "iter_locations",
        "capabilities",
        "status",
    }
    with pytest.raises(TypeError):
        api2.StorageRouterAPI()
    with pytest.raises(TypeError):
        api2.StorageManagerAPI()


def test_full_manager_layers_catalogue_and_policy_above_the_small_router() -> None:
    facade_bases = {
        api2.StoreAdministrationAPI,
        api2.DigitalAssetCatalogAPI,
        api2.AssetIngestAPI,
        api2.AssetRetrievalAPI,
        api2.ReplicaLifecycleAPI,
        api2.StoragePolicyAPI,
        api2.CompositeAssetAPI,
        api2.StorageReconciliationAPI,
    }
    assert issubclass(api2.StorageManagerAPI, api2.StorageRouterAPI)
    assert facade_bases.issubset(set(api2.StorageManagerAPI.__mro__))
    assert {
        "begin_write",
    }.isdisjoint(api2.StorageManagerAPI.__abstractmethods__)
    assert {
        "ingest_stream", "locate_digital_asset", "replicate_digital_asset",
        "verify_asset_replica", "resolve_effective_policies",
        "assemble_composite_digital_asset", "reconcile_store",
        "get_store", "iter_stores",
    }.issubset(api2.StorageManagerAPI.__abstractmethods__)


def test_storage_manager_package_exposes_stable_segregated_import_paths() -> None:
    from LiuXin_alpha.storage.api2 import storage_manager_api as manager_api
    from LiuXin_alpha.storage.api2.storage_manager_api.models.assets import ReplicaState
    from LiuXin_alpha.storage.api2.storage_manager_api.models.policies import ReplicationPolicy
    from LiuXin_alpha.storage.api2.storage_manager_api.policies_api import StoragePolicyAPI
    from LiuXin_alpha.storage.api2.storage_manager_api.router_api import StorageRouterAPI

    assert manager_api.StorageManagerAPI is api2.StorageManagerAPI
    assert manager_api.StoragePolicyAPI is StoragePolicyAPI is api2.StoragePolicyAPI
    assert manager_api.StorageRouterAPI is StorageRouterAPI is api2.StorageRouterAPI
    assert manager_api.ReplicaState is ReplicaState is api2.ReplicaState
    assert manager_api.ReplicationPolicy is ReplicationPolicy is api2.ReplicationPolicy
    assert len(manager_api.__all__) == len(set(manager_api.__all__))


def test_structural_protocols_accept_a_complete_backend_and_session() -> None:
    store = _MemoryStore()
    session = store.begin_write(api2.Location("memory", "book.epub"))

    assert isinstance(store, api2.StoreAPI)
    assert isinstance(store, api2.FileStore)
    assert isinstance(session, api2.WriteSession)
    assert not store.capabilities.native_copy


def test_store_api_composes_identity_lifecycle_and_transactional_files() -> None:
    from LiuXin_alpha.storage.api2 import store_api
    from LiuXin_alpha.storage.api2.store_api.file_api import StoreFileAPI
    from LiuXin_alpha.storage.api2.store_api.identity_api import StoreIdentityAPI
    from LiuXin_alpha.storage.api2.store_api.lifecycle_api import StoreLifecycleAPI

    assert store_api.StoreAPI is api2.StoreAPI
    assert len(store_api.__all__) == len(set(store_api.__all__))
    assert all(hasattr(store_api, name) for name in store_api.__all__)
    assert issubclass(api2.StoreAPI, StoreIdentityAPI)
    assert issubclass(api2.StoreAPI, StoreLifecycleAPI)
    assert issubclass(api2.StoreAPI, StoreFileAPI)
    assert api2.StoreAPI.__abstractmethods__ == {
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
    location = api2.Location(store.store_ref, "objects/42")
    assert isinstance(store.spec, api2.StoreSpecAPI)
    assert store.require_location(location) is location
    assert store.owns_location(location)
    with pytest.raises(api2.StoreInvalidLocation):
        store.require_location(api2.Location("another-store", "objects/42"))

    info = store.write_bytes(location, b"book")
    assert info.size == 4
    assert store.read_bytes(location) == b"book"
    assert store.compute_digest(location) == _sha256(b"book")

    copied = api2.Location(store.store_ref, "objects/copied")
    moved = api2.Location(store.store_ref, "objects/moved")
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
    location = api2.Location("main", "opaque/object-key")
    digest = api2.Digest(" SHA256 ", " ABCDEF ")
    capabilities = api2.StoreCapabilities(
        create=True,
        replace=False,
        delete=False,
        atomic_publish=True,
        range_reads=False,
        authoritative_digest=True,
        enumeration=api2.EnumerationCompleteness.PARTIAL,
    )

    assert location.key == "opaque/object-key"
    assert digest == api2.Digest("sha256", "abcdef")
    assert capabilities.enumeration is api2.EnumerationCompleteness.PARTIAL
    assert api2.WriteMode.CREATE_ONLY.value == "create_only"

    with pytest.raises(ValueError, match="empty"):
        api2.Location("main", "")
    with pytest.raises(ValueError, match="negative"):
        api2.FileInfo(location, -1)
    with pytest.raises(ValueError, match="exceed"):
        api2.StoreStatus(True, True, total_bytes=10, free_bytes=11)


def test_error_family_preserves_actionable_failure_categories() -> None:
    error_types = (
        api2.StoreNotFound,
        api2.StoreAlreadyExists,
        api2.StoreInvalidLocation,
        api2.StoreReadOnly,
        api2.StoreNoSpace,
        api2.StorePreconditionFailed,
        api2.StoreIntegrityError,
        api2.StoreUnavailable,
        api2.StoreUnsupportedOperation,
    )
    assert all(issubclass(error_type, api2.StoreError) for error_type in error_types)


def test_create_only_is_safe_and_final_location_changes_only_on_commit() -> None:
    store = _MemoryStore()
    location = api2.Location("memory", "book.epub")
    session = store.begin_write(
        location,
        expected_size=7,
        expected_digest=_sha256(b"payload"),
    )

    with session:
        session.write(b"payload")
        assert api2.try_stat(store, location) is None
        info = session.commit()

    assert info.size == 7
    assert api2.read_bytes(store, location) == b"payload"
    with pytest.raises(api2.StoreAlreadyExists):
        api2.write_bytes(store, location, b"replacement")

    api2.write_bytes(
        store,
        location,
        b"replacement",
        mode=api2.WriteMode.REPLACE,
    )
    assert api2.read_bytes(store, location) == b"replacement"


def test_failed_commit_and_context_exit_leave_no_partial_publication() -> None:
    store = _MemoryStore()
    existing = api2.Location("memory", "existing")
    new = api2.Location("memory", "new")
    api2.write_bytes(store, existing, b"original")

    session = store.begin_write(
        existing,
        mode=api2.WriteMode.REPLACE,
        expected_digest=_sha256(b"different"),
    )
    with pytest.raises(api2.StoreIntegrityError):
        with session:
            session.write(b"wrong")
            session.commit()
    assert api2.read_bytes(store, existing) == b"original"
    session.abort()
    session.abort()

    with store.begin_write(new) as uncommitted:
        uncommitted.write(b"never published")
    assert api2.try_stat(store, new) is None


def test_try_stat_suppresses_only_not_found() -> None:
    store = _MemoryStore()
    missing = api2.Location("memory", "missing")
    assert api2.try_stat(store, missing) is None
    assert not api2.exists(store, missing)

    store.online = False
    with pytest.raises(api2.StoreUnavailable):
        api2.try_stat(store, missing)
    with pytest.raises(api2.StoreUnavailable):
        api2.exists(store, missing)


def test_read_ranges_delete_preconditions_and_idempotence_are_explicit() -> None:
    store = _MemoryStore()
    location = api2.Location("memory", "alphabet")
    info = api2.write_bytes(store, location, b"abcdefghij")

    assert api2.read_bytes(store, location, offset=2, length=4) == b"cdef"
    with pytest.raises(api2.StorePreconditionFailed):
        store.delete(location, if_version="stale-version")
    assert api2.exists(store, location)

    store.delete(location, if_version=info.version)
    store.delete(location, missing_ok=True)
    with pytest.raises(api2.StoreNotFound):
        store.delete(location)


def test_enumeration_and_iter_infos_are_files_only_and_prefix_filtered() -> None:
    store = _MemoryStore()
    api2.write_bytes(store, api2.Location("memory", "books/a.epub"), b"a")
    api2.write_bytes(store, api2.Location("memory", "books/b.epub"), b"bb")
    api2.write_bytes(store, api2.Location("memory", "covers/a.jpg"), b"jpg")

    prefix = api2.Location("memory", "books/")
    assert [location.key for location in store.iter_locations(prefix=prefix)] == [
        "books/a.epub",
        "books/b.epub",
    ]
    assert [info.size for info in api2.iter_infos(store, prefix=prefix)] == [1, 2]
    assert store.capabilities.enumeration is api2.EnumerationCompleteness.COMPLETE


def test_copy_move_and_digest_have_safe_generic_fallbacks() -> None:
    store = _MemoryStore()
    source = api2.Location("memory", "source")
    copied = api2.Location("memory", "copied")
    moved = api2.Location("memory", "moved")
    api2.write_bytes(store, source, b"payload")

    copy_info = api2.copy(store, source, copied)
    assert copy_info.digest == _sha256(b"payload")
    assert api2.read_bytes(store, copied) == b"payload"
    assert api2.compute_digest(store, source) == _sha256(b"payload")
    with pytest.raises(ValueError, match="chunk_size"):
        api2.compute_digest(store, source, chunk_size=0)
    with pytest.raises(api2.StoreUnsupportedOperation):
        api2.compute_digest(store, source, "not-a-real-digest")

    move_info = api2.move(store, copied, moved)
    assert move_info.size == 7
    assert api2.try_stat(store, copied) is None
    assert api2.read_bytes(store, moved) == b"payload"


def test_manager_routes_primitives_and_derives_only_small_conveniences() -> None:
    store = _MemoryStore("main")
    manager = _MemoryManager(store)
    location = api2.Location("main", "book.epub")

    info = manager.write_bytes(location, b"payload", expected_digest=_sha256(b"payload"))

    assert info.size == 7
    assert manager.exists(location)
    assert manager.read_bytes(location, offset=1, length=3) == b"ayl"
    assert [item.location for item in manager.iter_infos()] == [location]
    assert manager.capabilities("main").atomic_publish
    assert manager.status("main").available


def test_facade_models_cover_store_policy_and_replica_state() -> None:
    spec = api2.StoreSpec(
        store_id=3,
        store_name="archive",
        store_kind="squashfs_readonly",
        store_root_uri="/srv/archive.sqsh",
        supported_replica_modes=frozenset(
            {api2.ReplicaMode.BACKUP, api2.ReplicaMode.ARCHIVE}
        ),
        read_only=True,
    )
    replication = api2.ReplicationPolicy(min_copies=2)
    backup = api2.BackupPolicy(
        min_copies=2, target_copies=3, mode=api2.ReplicaMode.ARCHIVE,
    )

    assert spec.store_id == 3
    assert replication.effective_target_copies == 2
    assert backup.effective_target_copies == 3
    assert api2.ReplicaState.UNAVAILABLE != api2.ReplicaState.MISSING
    with pytest.raises(ValueError, match="copy target"):
        api2.ReplicationPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="backup policy mode"):
        api2.BackupPolicy(mode=api2.ReplicaMode.ACTIVE)


def test_ingest_bytes_remains_a_small_wrapper_over_transactional_stream_ingest() -> None:
    manager = _IngestHarness()
    result = manager.ingest_bytes(
        b"payload", item_id=7, role="primary_payload", preferred_store="main",
    )

    assert manager.observed == b"payload"
    assert manager.size == 7
    assert result.digital_asset.digital_asset_id == 1
    assert result.replica.asset_replica_id == 2


def test_verification_and_reconciliation_results_preserve_operational_distinctions() -> None:
    unavailable = api2.ReplicaVerificationResult(
        1, 9, api2.ReplicaState.UNAVAILABLE, None, errors=("offline",),
    )
    corrupt = api2.ReplicaVerificationResult(
        2, 9, api2.ReplicaState.CORRUPT, True, digest_matches=False,
    )
    verified = api2.ReplicaVerificationResult(
        3, 9, api2.ReplicaState.VERIFIED, True,
        size_matches=True, digest_matches=True,
    )
    dirty = api2.ReconciliationReport(
        store_ref="main", dry_run=True, enumeration_complete=True,
        missing_replica_ids=(1,),
    )

    assert not unavailable.healthy
    assert not corrupt.healthy
    assert verified.healthy
    assert api2.AssetVerificationResult(9, (unavailable, verified)).healthy
    assert not dirty.clean
