"""
Containers for information about managed digital assets and storage state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dataclasses

if TYPE_CHECKING:
    from LiuXin_alpha.storage.storage_types import DigitalAssetID
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


@dataclasses.dataclass(slots=True)
class DigitalAssetReplicationCluster:
    """
    Informational container describing nominally identical managed digital assets.

    This is intentionally more granular than most callers should need. It remains
    useful for diagnostics and low-level reconciliation work.
    """

    digital_asset_locs: dict["DigitalAssetID", "StoreLocationMixinAPI"]
    replication_level: int
    digital_asset_hash: str

    @property
    def digital_asset_ids(self) -> set["DigitalAssetID"]:
        """Return the managed digital asset ids in the cluster."""
        return set(self.digital_asset_locs.keys())
