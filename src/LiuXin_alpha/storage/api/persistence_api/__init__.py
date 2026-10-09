"""
Export implementation-facing storage metadata persistence protocols.

Four repositories describe Asset, Replica, Composite, and derivation records;
their convenience extensions construct declarations without changing the
runtime structural contracts. The unit-of-work and factory protocols define
explicit metadata transaction coordination. All names are re-exported unchanged
from repositories.py. This package supplies no database implementation or
external Store transaction.

Durable manager adapters import these ports here. Application code should use
StorageManagerAPI operations instead of depending on these persistence ports.
"""

from LiuXin_alpha.storage.api.persistence_api.repositories import (
    CompositeDigitalAssetRepositoryAPI,
    CompositeDigitalAssetRepositoryConvenienceAPI,
    DigitalAssetDerivationRepositoryAPI,
    DigitalAssetDerivationRepositoryConvenienceAPI,
    DigitalAssetRepositoryAPI,
    DigitalAssetRepositoryConvenienceAPI,
    ReplicaRepositoryAPI,
    ReplicaRepositoryConvenienceAPI,
    StorageUnitOfWorkAPI,
    StorageUnitOfWorkFactoryAPI,
)

__all__ = [
    "CompositeDigitalAssetRepositoryConvenienceAPI",
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetDerivationRepositoryConvenienceAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetRepositoryConvenienceAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryConvenienceAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]
