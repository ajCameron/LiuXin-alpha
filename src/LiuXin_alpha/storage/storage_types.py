"""
Expose legacy integer aliases for storage catalogue row and link identifiers.

StoreID, ItemID, DigitalAssetID, CompositeDigitalAssetID, AssetReplicaID,
DigitalAssetItemLinkID, CompositeDigitalAssetItemLinkID,
CompositeDigitalAssetMemberLinkID, ReplicationPolicyID, and BackupPolicyID are
all the built-in int object. StoreRef is the int-or-str union; these aliases
neither enforce distinct identities nor perform validation or name resolution.
They do not replace the UUID-based Store references in the current storage API.

The names describe links from Items to atomic Assets/Replicas, or from Items to
Composites whose membership links refer to atomic Assets. They do not create or
query those database relationships.
"""

StoreID = int
StoreRef = int | str
ItemID = int
DigitalAssetID = int
CompositeDigitalAssetID = int
AssetReplicaID = int
DigitalAssetItemLinkID = int
CompositeDigitalAssetItemLinkID = int
CompositeDigitalAssetMemberLinkID = int
ReplicationPolicyID = int
BackupPolicyID = int
