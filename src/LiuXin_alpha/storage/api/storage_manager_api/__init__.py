"""Top-level storage manager API."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from LiuXin_alpha.storage.api.storage_manager_api.asset_replicas_api import AssetReplicasManagerAPI
from LiuXin_alpha.storage.api.storage_manager_api.digital_asset_compositions_api import (
    DigitalAssetCompositionsManagerAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.file_manip_api import DigitalAssetsManagerAPI
from LiuXin_alpha.storage.api.storage_manager_api.item_digital_assets_api import ItemDigitalAssetsManagerAPI
from LiuXin_alpha.storage.api.storage_manager_api.storage_policies_api import StoragePoliciesManagerAPI
from LiuXin_alpha.storage.api.storage_manager_api.stores_management_api import StoresManagerAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class StorageManagerAPI(
    StoresManagerAPI,
    DigitalAssetsManagerAPI,
    AssetReplicasManagerAPI,
    ItemDigitalAssetsManagerAPI,
    DigitalAssetCompositionsManagerAPI,
    StoragePoliciesManagerAPI,
    abc.ABC,
):
    """
    Contract for the storage manager/front-end.

    This is the user-facing storage API. It coordinates multiple stores and
    reconciles item -> digital_asset -> asset_replica state against the database.
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db
