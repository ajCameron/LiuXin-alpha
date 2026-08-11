"""Identifier aliases used by the LiuXin-aware storage manager facade."""

from typing import TypeAlias


StoreID: TypeAlias = int
ItemID: TypeAlias = int
DigitalAssetID: TypeAlias = int
CompositeDigitalAssetID: TypeAlias = int
AssetReplicaID: TypeAlias = int
ReplicationPolicyID: TypeAlias = int
BackupPolicyID: TypeAlias = int


__all__ = [
    "AssetReplicaID",
    "BackupPolicyID",
    "CompositeDigitalAssetID",
    "DigitalAssetID",
    "ItemID",
    "ReplicationPolicyID",
    "StoreID",
]
