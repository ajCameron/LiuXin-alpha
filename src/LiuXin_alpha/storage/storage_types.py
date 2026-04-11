"""
Base type aliases for identifiers in the storage subsystem.

These names intentionally track the current storage graph:
items -> digital_assets -> asset_replicas
"""

StoreID = int
ItemID = int
DigitalAssetID = int
AssetReplicaID = int
ItemDigitalAssetLinkID = int
DigitalAssetCompositionID = int
ReplicationPolicyID = int
BackupPolicyID = int
