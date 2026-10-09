"""
Protect legacy storage imports while implementations live under utils.
"""

from LiuXin_alpha.storage import backend_registry as registry_compat
from LiuXin_alpha.storage import migrations as migrations_compat
from LiuXin_alpha.storage import store_spec_utils as configuration_compat
from LiuXin_alpha.storage.utils import backend_registry as registry_impl
from LiuXin_alpha.storage.utils import migrations as migrations_impl
from LiuXin_alpha.storage.utils import store_configuration as configuration_impl


def test_backend_registry_compatibility_exports_are_canonical() -> None:
    """
    Keep registry compatibility imports identical to their implementations.

    Example:
        >>> test_backend_registry_compatibility_exports_are_canonical()


    :return: None after all compatibility identities are verified.
    """

    assert registry_compat.DEFAULT_BACKEND_REGISTRY is registry_impl.DEFAULT_BACKEND_REGISTRY
    assert registry_compat.StorageBackendRegistry is registry_impl.StorageBackendRegistry
    assert registry_compat.StorageBackendDescriptor is registry_impl.StorageBackendDescriptor
    assert registry_compat.StoreConstructionContext is registry_impl.StoreConstructionContext
    assert registry_compat.normalize_backend_kind is registry_impl.normalize_backend_kind
    assert registry_compat.BackendBuilder is registry_impl.BackendBuilder


def test_migration_compatibility_exports_are_canonical() -> None:
    """
    Keep migration compatibility imports identical to their implementations.

    Example:
        >>> test_migration_compatibility_exports_are_canonical()


    :return: None after all compatibility identities are verified.
    """

    assert migrations_compat.STORAGE_SCHEMA_VERSION == migrations_impl.STORAGE_SCHEMA_VERSION
    assert migrations_compat.StorageMigrationReport is migrations_impl.StorageMigrationReport
    assert migrations_compat.can_migrate_storage_schema is migrations_impl.can_migrate_storage_schema
    assert migrations_compat.migrate_storage_schema is migrations_impl.migrate_storage_schema
    assert migrations_compat.record_envelope_migration is migrations_impl.record_envelope_migration


def test_configuration_compatibility_exports_are_canonical() -> None:
    """
    Keep configuration-codec imports identical to their implementations.

    Example:
        >>> test_configuration_compatibility_exports_are_canonical()


    :return: None after both codec compatibility identities are verified.
    """

    assert (
        configuration_compat.store_configuration_from_row
        is configuration_impl.store_configuration_from_row
    )
    assert (
        configuration_compat.store_configuration_to_row_dict
        is configuration_impl.store_configuration_to_row_dict
    )
