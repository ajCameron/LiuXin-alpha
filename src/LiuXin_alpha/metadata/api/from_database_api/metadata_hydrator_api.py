"""Central database-backed metadata hydrator API.

Category: metadata source orchestration API.
This module defines the high-level hydrator surface that composes specialised
database sources into complete metadata objects for callers such as stores.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Literal, TypeAlias

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import (
    CalibreMetadataAPI,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import (
    LiuXinMetadataAPI,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_wemi_metadata_api import (
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


class LiuXinWEMIMetadataGetterAPI(abc.ABC):
    """Read complete LiuXin/WEMI item metadata slices from the database."""

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db

    @abc.abstractmethod
    def get_liuxin_wemi_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> LiuXinWEMIMetadataAPI:
        """
        Get the complete item-centred metadata slice for sidecar storage.

        :param item_id:
        :param source_row:
        :return:
        """


class LiuXinMetadataGetterAPI(LiuXinWEMIMetadataGetterAPI):
    """Read LiuXin-shaped metadata objects from the database."""

    def get_liuxin_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> LiuXinMetadataAPI:
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


class MetadataObjectGetterAPI(LiuXinMetadataGetterAPI, CalibreMetadataGetterAPI):
    """Read high-level metadata objects from the database."""


class MetadataHydratorAPI(MetadataObjectGetterAPI):
    """
    Central hydrator surface for metadata objects produced from the database.

    Specific typed getters remain the canonical path. ``hydrate_metadata`` is a
    thin dispatch helper for boundary code that chooses the target shape at
    runtime.
    """

    @abc.abstractmethod
    def hydrate_metadata(
        self,
        kind: HydratableMetadataKind,
        *,
        work_id: "WorkID | None" = None,
        expression_id: "ExpressionID | None" = None,
        manifestation_id: "ManifestationID | None" = None,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> HydratedMetadataAPI:
        """
        Hydrate one metadata shape by explicit kind.

        :param kind:
        :param work_id:
        :param expression_id:
        :param manifestation_id:
        :param item_id:
        :param source_row:
        :return:
        """


__all__ = [
    "CalibreMetadataGetterAPI",
    "HydratableMetadataKind",
    "HydratedMetadataAPI",
    "LiuXinMetadataGetterAPI",
    "LiuXinWEMIMetadataGetterAPI",
    "MetadataHydratorAPI",
    "MetadataObjectGetterAPI",
]
