"""Item-facing metadata source contracts.

These APIs describe read-side database access for core item identity and
item metadata bundles.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.row import Row
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import MetadataRecord
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import ItemMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import ItemID


class ItemMetadataGetterAPI(abc.ABC):
    """Read item identities and item metadata bundles from the database."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        self.db = db

    @abc.abstractmethod
    def get_item_identity(self, item_id: 'ItemID') -> 'ItemIdentityAPI':
        """Get the narrow identity container for one item."""

    @abc.abstractmethod
    def get_item_metadata(
        self,
        item_id: 'ItemID' | None = None,
        source_row: 'MetadataRecord' | 'Row' | None = None,
    ) -> 'ItemMetadataAPI':
        """Get the editable metadata bundle for one item.

        Implementations may accept either a concrete ``item_id`` or an
        already-fetched row/view carrying item and optional WEMI ids.
        """
