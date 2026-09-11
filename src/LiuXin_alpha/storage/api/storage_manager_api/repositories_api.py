"""
Retain historical import paths for the six storage persistence protocols.

Each exported name is the identical object defined through persistence_api;
this module adds no adapter, transaction behavior, or repository implementation.
New durable-manager adapters should import from LiuXin_alpha.storage.api.persistence_api.
Application code should use StorageManagerAPI operations instead of these
implementation-facing metadata ports.
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
