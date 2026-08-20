"""Compatibility imports for the storage-manager persistence SPI.

New durable-manager implementations should import these ports from
``LiuXin_alpha.storage.api.persistence_api``. They remain available here so
existing adapters do not break while persistence concerns move out of the
consumer-facing manager facade.
"""

from LiuXin_alpha.storage.api.persistence_api import (
    CompositeDigitalAssetRepositoryAPI,
    DigitalAssetDerivationRepositoryAPI,
    DigitalAssetRepositoryAPI,
    ReplicaRepositoryAPI,
    StorageUnitOfWorkAPI,
    StorageUnitOfWorkFactoryAPI,
)


__all__ = [
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]
