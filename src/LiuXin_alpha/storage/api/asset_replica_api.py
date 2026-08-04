"""Storage API contracts for concrete asset replicas.

Examples:
    Type storage-graph functions against the narrow identity contract::

        def replica_key(replica: AssetReplicaIdentityAPI) -> str:
            return str(replica)
"""

from __future__ import annotations

import abc


class AssetReplicaIdentityAPI(abc.ABC):
    """Represents one concrete copy of a digital asset on storage.

    Examples:
        Accept any concrete replica identity without coupling to its row type::

            def schedule_check(replica: AssetReplicaIdentityAPI) -> None:
                queue.append(replica)
    """


class AssetReplicaMetadataAPI(abc.ABC):
    """Represents storage-facing metadata for one asset replica.

    Examples:
        Use the contract in storage-facing annotations::

            def index_replica(metadata: AssetReplicaMetadataAPI) -> None:
                replica_index.add(metadata)
    """


__all__ = ["AssetReplicaIdentityAPI", "AssetReplicaMetadataAPI"]
