"""Top-level storage manager API."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from LiuXin_alpha.storage.api.storage_manager_api.stores_management_api import StoresManagerAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class StorageManagerAPI(StoresManagerAPI, abc.ABC):
    """Top-level user-facing storage manager API.

    At this stage this contract covers store orchestration plus direct file access.
    Richer digital-asset / replication manager APIs can be layered on later rather
    than making every concrete storage manager pretend to implement them now.
    """

    db: "DatabaseAPI | None"

    def __init__(self, db: "DatabaseAPI | None" = None) -> None:
        self.db = db
