"""
Check manager component ownership, ordering, size limits, and ingest codec identity.

Imported-class introspection exercises the concrete composition and missing-helper
failures. The codec case uses an in-memory request round trip. Physical-line guards
exclude documentation-only lines, retain comments and other blank lines, and
remain separate from runtime complexity checks.
"""

from __future__ import annotations

import abc
import ast
import inspect
import pickle
from pathlib import Path
from uuid import UUID

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import TransientStorageManager
from LiuXin_alpha.storage.storage_manager.database_codec import (
    _decode,
    _encode,
    _storage_value_types,
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
from LiuXin_alpha.storage.storage_manager.mixins._policy_support import (
    _StorageManagerPolicySupportMixin,
)
from LiuXin_alpha.storage.storage_manager.mixins._support import (
    _StorageManagerSupportMixin,
)
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)
from tests.support.docstring_ownership import SourceMetrics

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


def test_storage_implementation_uses_responsibility_api_packages() -> None:
    """Prevent internal modules from regaining the broad discovery umbrella dependency.

    Application callers may use ``LiuXin_alpha.storage.api`` for discovery. Production storage
    modules should instead name the manager, Store, driver, workflow, shared-value, or error package
    they consume so ownership remains visible from the import block.

    Example:
        >>> test_storage_implementation_uses_responsibility_api_packages()

    :return: None when no production storage module imports the API umbrella as ``api``.
    """

    storage_root = Path(__file__).parents[3] / "src" / "LiuXin_alpha" / "storage"
    offenders: list[str] = []
    for path in sorted(storage_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import) and any(
                alias.name == "LiuXin_alpha.storage.api" and alias.asname == "api"
                for alias in node.names
            ):
                offenders.append(str(path.relative_to(storage_root)))
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "LiuXin_alpha.storage"
                and any(alias.name == "api" for alias in node.names)
            ):
                offenders.append(str(path.relative_to(storage_root)))

    assert offenders == []


def test_api_and_implementation_components_have_the_same_order() -> None:
    """
    Check the exact API base order and the relative implementation order in the transient manager
    MRO.

    The manager must also be concrete. This uses imported class introspection rather than
    constructing a manager or exercising runtime storage behavior.

    Example:
        >>> test_api_and_implementation_components_have_the_same_order()  # doctest: +SKIP


    :return: None after API ordering, implementation ordering, and concreteness assertions pass.
    """
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
    """
    Require each implementation component to define every abstract method name from its paired
    contract directly.

    Checking __dict__ distinguishes direct ownership from inherited availability. The test does not
    compare signatures, validate method bodies, or require ownership of concrete interface
    conveniences.

    Example:
        >>> test_each_component_owns_its_abstract_contract_methods()  # doctest: +SKIP


    :return: None when every paired component directly owns the required names; failures identify missing methods.
    """
    for implementation, contract in COMPONENTS:
        missing = contract.__abstractmethods__.difference(implementation.__dict__)
        assert not missing, (
            f"{implementation.__name__} does not implement {sorted(missing)}"
        )


def test_manager_module_stays_a_small_composition_root() -> None:
    """
    Enforce physical-line ceilings for the manager owner and each adjacent top-level mixin file.

    The manager file may contain at most 120 lines and each mixin Python file at most 900.
    Documentation-only lines are excluded; code, comments, other blank lines, and lines shared
    with code still count. This size check does not measure executable complexity or nested
    directories. Each source file is read and measured once.

    Example:
        >>> test_manager_module_stays_a_small_composition_root()  # doctest: +SKIP


    :return: None when the manager and all directly contained mixin files fit the unchanged ceilings.
    """
    manager_path = Path(inspect.getfile(TransientStorageManager))
    assert SourceMetrics(manager_path.read_text(encoding="utf-8")).line_count() <= 120

    mixin_directory = manager_path.with_name("mixins")
    oversized = {}
    for path in mixin_directory.glob("*.py"):
        line_count = SourceMetrics(path.read_text(encoding="utf-8")).line_count()
        if line_count > 900:
            oversized[path.name] = line_count
    assert not oversized


def test_durable_storage_owners_remain_physically_separated() -> None:
    """Guard the extracted convenience, persistence, bootstrap, and recovery boundaries.

    The limits count non-docstring source lines and deliberately leave modest growth room. They
    prevent the compatibility composition module, durable manager, or unit-of-work coordinator from
    absorbing the extracted responsibilities again.

    Example:
        >>> test_durable_storage_owners_remain_physically_separated()

    :return: None while every owner remains within its responsibility-specific ceiling.
    """

    storage_root = Path(__file__).parents[3] / "src" / "LiuXin_alpha" / "storage"
    limits = {
        "api/storage_manager_api/convenience_api.py": 80,
        "api/storage_manager_api/_convenience_support.py": 550,
        "api/storage_manager_api/asset_convenience.py": 550,
        "api/storage_manager_api/composite_convenience.py": 350,
        "api/storage_manager_api/item_link_convenience.py": 180,
        "api/storage_manager_api/policy_convenience.py": 250,
        "api/storage_manager_api/derivation_convenience.py": 200,
        "durable_manager.py": 750,
        "storage_manager/database_binding.py": 130,
        "storage_manager/database_codec.py": 160,
        "storage_manager/database_mappings.py": 250,
        "storage_manager/database_domain_repositories.py": 480,
        "storage_manager/database_unit_of_work.py": 240,
        "storage_manager/ingest_recovery.py": 340,
        "storage_manager/store_bootstrap.py": 240,
        "storage_manager/database_repository.py": 1900,
    }
    oversized = {
        relative: count
        for relative, ceiling in limits.items()
        if (
            count := SourceMetrics(
                (storage_root / relative).read_text(encoding="utf-8")
            ).line_count()
        )
        > ceiling
    }
    assert not oversized


@pytest.mark.parametrize(
    ("omitted", "required_hook"),
    [
        (_StorageManagerSupportMixin, "_metadata_transaction"),
        (_StorageManagerPolicySupportMixin, "_plan_destination_stores"),
    ],
)
def test_missing_helper_components_cannot_construct_a_manager(
    omitted: type, required_hook: str
) -> None:
    """
    Remove one required helper base from the orchestrator composition and confirm the replacement
    remains abstract.

    The expected hook must appear among unresolved abstract methods, and instantiation must raise
    TypeError. The test creates a temporary class without constructing a working manager or touching
    storage.

    Example:
        >>> test_missing_helper_components_cannot_construct_a_manager(_StorageManagerSupportMixin, "_metadata_transaction")  # doctest: +SKIP


    :param omitted: Support component removed from the orchestrator's direct bases for this parameterized case.
    :param required_hook: Hook expected to become abstract when that support component is absent.
    :return: None after the hook remains abstract and incomplete construction is rejected.
    """
    composition = TransientStorageManager.__bases__[0]
    incomplete = type(
        "IncompleteStorageManager",
        tuple(base for base in composition.__bases__ if base is not omitted),
        {"__module__": __name__},
    )
    assert required_hook in incomplete.__abstractmethods__
    with pytest.raises(TypeError, match="abstract"):
        incomplete()


def test_persisted_ingest_types_keep_their_historical_wire_names() -> None:
    """
    Check real Python ownership and stable journal identifiers for ingest values.

    The encoded dataclass tag must still name the legacy manager module, and decoding through the
    explicit type registry must reproduce the request. This exercises in-memory serialization and
    type registration without saving a journal or opening a database.

    Example:
        >>> test_persisted_ingest_types_keep_their_historical_wire_names()  # doctest: +SKIP


    :return: None after owner modules, stored tags, codec equality, and pickle equality are verified.
    """
    expected_module = "LiuXin_alpha.storage.storage_manager.manager"
    persisted_types = (
        _StreamIngestRequest,
        _AdoptIngestRequest,
        _IdentifiedStreamIngestRequest,
        _StoreObjectIngestRequest,
        _IngestOperation,
    )

    assert {value.__module__ for value in persisted_types} == {
        "LiuXin_alpha.storage.storage_manager.mixins._types"
    }
    registry = _storage_value_types(persisted_types)
    for value in persisted_types:
        assert registry[f"{expected_module}.{value.__name__}"] is value

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
    assert _decode(encoded, registry) == request
    assert pickle.loads(pickle.dumps(request)) == request
