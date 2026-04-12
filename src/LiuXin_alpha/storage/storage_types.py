"""
Base type aliases for identifiers in the storage subsystem.

These names intentionally track the current storage graph:
items -> digital_assets -> asset_replicas
items -> composite_digital_assets -> composite_digital_asset_digital_asset_links -> digital_assets
"""

StoreID = int
ItemID = int
DigitalAssetID = int
CompositeDigitalAssetID = int
AssetReplicaID = int
DigitalAssetItemLinkID = int
CompositeDigitalAssetItemLinkID = int
CompositeDigitalAssetMemberLinkID = int
ReplicationPolicyID = int
BackupPolicyID = int
