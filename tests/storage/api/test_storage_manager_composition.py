"""Structural contracts for the composed storage-manager implementation."""

from __future__ import annotations

import abc
import inspect
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import TransientStorageManager
from LiuXin_alpha.storage.storage_manager.database_repository import (
    _decode,
    _encode,
    _storage_value_types,
)
from LiuXin_alpha.storage.storage_manager.manager import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)
from LiuXin_alpha.storage.storage_manager.mixins import (
    CompositeDigitalAssetMixin,
    DigitalAssetDerivationRegistryMixin,
    DigitalAssetIngestMixin,
    DigitalAssetRegistryMixin,
    DigitalAssetRetrievalMixin,
    ItemDigitalAssetLinkMixin,
    ReplicaLifecycleMixin,
    StorageOperationalStatusMixin,
    StoragePolicyMixin,
    StorageReconciliationMixin,
    StorageRouterMixin,
    StoreAdministrationMixin,
)

COMPONENTS = (
    (StoreAdministrationMixin, api.StoreAdministrationAPI),
    (StorageRouterMixin, api.StorageRouterAPI),
    (DigitalAssetRegistryMixin, api.DigitalAssetRegistryAPI),
    (DigitalAssetIngestMixin, api.DigitalAssetIngestAPI),
    (DigitalAssetRetrievalMixin, api.DigitalAssetRetrievalAPI),
    (ReplicaLifecycleMixin, api.ReplicaLifecycleAPI),
    (ItemDigitalAssetLinkMixin, api.ItemDigitalAssetLinkAPI),
    (CompositeDigitalAssetMixin, api.CompositeDigitalAssetAPI),
    (
        DigitalAssetDerivationRegistryMixin,
        api.DigitalAssetDerivationRegistryAPI,
    ),
    (StoragePolicyMixin, api.StoragePolicyAPI),
    (StorageReconciliationMixin, api.StorageReconciliationAPI),
    (StorageOperationalStatusMixin, api.StorageOperationalStatusAPI),
)


def test_api_and_implementation_components_have_the_same_order() -> None:
    assert api.StorageManagerAPI.__bases__ == (
        api.StorageConvenienceAPI,
        *(contract for _implementation, contract in COMPONENTS),
        abc.ABC,
    )

    manager_mro = TransientStorageManager.__mro__
    positions = [manager_mro.index(implementation) for implementation, _ in COMPONENTS]
    assert positions == sorted(positions)
    assert not inspect.isabstract(TransientStorageManager)


def test_each_component_owns_its_abstract_contract_methods() -> None:
    for implementation, contract in COMPONENTS:
        missing = contract.__abstractmethods__.difference(implementation.__dict__)
        assert not missing, (
            f"{implementation.__name__} does not implement {sorted(missing)}"
        )


def test_manager_module_stays_a_small_composition_root() -> None:
    manager_path = Path(inspect.getfile(TransientStorageManager))
    assert len(manager_path.read_text(encoding="utf-8").splitlines()) <= 120

    mixin_directory = manager_path.with_name("mixins")
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in mixin_directory.glob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 900
    }
    assert not oversized


def test_persisted_ingest_types_keep_their_historical_wire_names() -> None:
    expected_module = "LiuXin_alpha.storage.storage_manager.manager"
    persisted_types = (
        _StreamIngestRequest,
        _AdoptIngestRequest,
        _IdentifiedStreamIngestRequest,
        _StoreObjectIngestRequest,
        _IngestOperation,
    )

    assert {value.__module__ for value in persisted_types} == {expected_module}

    request = _AdoptIngestRequest(
        api.Location(UUID(int=1), "incoming/book.epub"),
        None,
        None,
        None,
        api.DigitalAssetMetadata(original_name="book.epub"),
        api.ReplicaMode.UNMANAGED,
        True,
    )
    encoded = _encode(request)
    assert encoded["$dataclass"] == f"{expected_module}._AdoptIngestRequest"
    assert _decode(encoded, _storage_value_types(persisted_types)) == request
