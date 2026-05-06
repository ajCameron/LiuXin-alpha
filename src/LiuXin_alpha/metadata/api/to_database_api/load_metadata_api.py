
"""
Methods for loading metadata back to the database.

The intent is to take a metadata object and write it back into the table.
"""

from __future__ import annotations

import abc


import abc
from typing import TYPE_CHECKING, Literal, TypeAlias

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import (
    CalibreMetadataAPI,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import (
    LiuXinMetadataAPI,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_wemi_metadata_api import (
    LiuXinWEMIMetadataAPI,
    WemiMetadataBundleAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.row import Row
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
        MetadataRecord,
    )
    from LiuXin_alpha.metadata.metadata_types import (
        ExpressionID,
        ItemID,
        ManifestationID,
        WorkID,
    )


HydratableMetadataKind: TypeAlias = Literal[
    "work",
    "expression",
    "manifestation",
    "item",
    "liuxin_wemi",
    "liuxin",
    "calibre",
]
HydratedMetadataAPI: TypeAlias = (
    WemiMetadataBundleAPI
    | LiuXinWEMIMetadataAPI
    | LiuXinMetadataAPI
    | CalibreMetadataAPI
)


class LiuXinWEMIMetadataSetterAPI(abc.ABC):
    """Write complete LiuXin/WEMI item metadata slices from the database."""

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db

    @abc.abstractmethod
    def set_liuxin_wemi_metadata(
        self,
        setting_md: "LiuXinWEMIMetadataAPI"
    ) -> bool:
        """
        Get the complete item-centred metadata slice for sidecar storage.

        :para, setting_md: The metadata to set.
        :return:
        """



class LiuXinMetadataGetterAPI(LiuXinWEMIMetadataGetterAPI):
    """Read LiuXin-shaped metadata objects from the database."""

    def get_liuxin_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> "LiuXinMetadataAPI":
        """
        Get the LiuXin-compatible metadata object for one item.

        Implementations may override this when they can build the legacy shape
        directly. The default view comes from the complete LiuXin/WEMI slice.

        :param item_id:
        :param source_row:
        :return:
        """
        return self.get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        ).as_liuxin_metadata()


class CalibreMetadataGetterAPI(LiuXinWEMIMetadataGetterAPI):
    """Read Calibre-shaped metadata objects from the database."""

    def get_calibre_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> CalibreMetadataAPI:
        """
        Get the Calibre-compatible metadata object for one item.

        Implementations may override this when they can build the Calibre shape
        directly. The default view comes from the complete LiuXin/WEMI slice.

        :param item_id:
        :param source_row:
        :return:
        """
        return self.get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        ).as_calibre_metadata()
