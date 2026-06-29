"""Top-level storage manager API.

This contract is intentionally narrow: orchestration of store containers plus
location/file routing. Richer replica/policy manager contracts can sit beside
it instead of being forced into every concrete manager immediately.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from LiuXin_alpha.storage.api.storage_manager_api.file_manip_api import StoreFileOrchestrationAPI
from LiuXin_alpha.storage.api.storage_manager_api.stores_management_api import StoresManagerAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class StorageManagerAPI(StoresManagerAPI, StoreFileOrchestrationAPI, abc.ABC):
    """User-facing storage manager API.

    This intentionally covers store orchestration plus location/file routing.
    Richer digital-asset, replica, and policy manager contracts can live beside
    it later instead of forcing every concrete manager to fake them now.
    """

    db: "DatabaseAPI | None"

    def __init__(self, db: "DatabaseAPI | None" = None) -> None:
        self.db = db
