"""
Nominal database identifiers used by storage-manager domain objects.
"""

from typing import NewType


DigitalAssetID = NewType("DigitalAssetID", int)
"""Identifier of one atomic Digital Asset.

Example:
    >>> DigitalAssetID(7)
    7
"""

DigitalAssetDerivationID = NewType("DigitalAssetDerivationID", int)
"""Identifier of one recorded Digital Asset derivation.

Example:
    >>> DigitalAssetDerivationID(11)
    11
"""

CompositeDigitalAssetID = NewType("CompositeDigitalAssetID", int)
"""Identifier of one Composite Digital Asset.

Example:
    >>> CompositeDigitalAssetID(3)
    3
"""

ReplicaID = NewType("ReplicaID", int)
"""Identifier of one concrete Replica claim.

Example:
    >>> ReplicaID(12)
    12
"""

ItemID = NewType("ItemID", int)
"""Identifier of one library-facing Item.

Example:
    >>> ItemID(9)
    9
"""

ReplicationPolicyID = NewType("ReplicationPolicyID", int)
"""Identifier of one stored replication-policy definition.

Example:
    >>> ReplicationPolicyID(4)
    4
"""

BackupPolicyID = NewType("BackupPolicyID", int)
"""Identifier of one stored backup-policy definition.

Example:
    >>> BackupPolicyID(5)
    5
"""


__all__ = [
    "BackupPolicyID",
    "DigitalAssetDerivationID",
    "CompositeDigitalAssetID",
    "DigitalAssetID",
    "ItemID",
    "ReplicationPolicyID",
    "ReplicaID",
]
