"""
Define high-level database hydrator contracts for typed WEMI, LiuXin and Calibre metadata shapes.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise metadata hydrator api with the owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
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
    """
    Contract complete item-centred LiuXin/WEMI metadata hydration.

    Example:
        Exercise LiuXinWEMIMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Bind a WEMI metadata getter to its database dependency.

        Example:
            Exercise LiuXinWEMIMetadataGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_liuxin_wemi_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> LiuXinWEMIMetadataAPI:
        """
        Hydrate the complete item-centred WEMI slice from an item id or preloaded source row.

        Example:
            Exercise LiuXinWEMIMetadataGetterAPI.get liuxin wemi metadata with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :param source_row: Optional preloaded row carrying item and related WEMI
            identifiers.
        :return: The complete item-centred LiuXin/WEMI metadata object.
        """


class LiuXinMetadataGetterAPI(LiuXinWEMIMetadataGetterAPI):
    """
    Provide a LiuXin-shaped view derived from the complete WEMI slice by default.

    Example:
        Exercise LiuXinMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """

    def get_liuxin_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> LiuXinMetadataAPI:
        """
        Project the complete WEMI slice into the LiuXin compatibility metadata shape.

        Example:
            Exercise LiuXinMetadataGetterAPI.get liuxin metadata with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :param source_row: Optional preloaded row carrying item and related WEMI
            identifiers.
        :return: The LiuXin-compatible metadata projection.
        """
        return self.get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        ).as_liuxin_metadata()


class CalibreMetadataGetterAPI(LiuXinWEMIMetadataGetterAPI):
    """
    Provide a Calibre-shaped view derived from the complete WEMI slice by default.

    Example:
        Exercise CalibreMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """

    def get_calibre_metadata(
        self,
        item_id: "ItemID | None" = None,
        source_row: "MetadataRecord | Row | None" = None,
    ) -> CalibreMetadataAPI:
        """
        Project the complete WEMI slice into the Calibre compatibility metadata shape.

        Example:
            Exercise CalibreMetadataGetterAPI.get calibre metadata with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :param source_row: Optional preloaded row carrying item and related WEMI
            identifiers.
        :return: The Calibre-compatible metadata projection.
        """
        return self.get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        ).as_calibre_metadata()


class MetadataObjectGetterAPI(LiuXinMetadataGetterAPI, CalibreMetadataGetterAPI):
    """
    Unify LiuXin and Calibre compatibility metadata getters.

    Example:
        Exercise MetadataObjectGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """


class MetadataHydratorAPI(MetadataObjectGetterAPI):
    """
    Dispatch explicitly requested metadata shapes through canonical typed getters.

    Example:
        Exercise MetadataHydratorAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
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
        Hydrate the requested metadata kind using the applicable typed identity or item context.

        Example:
            Exercise MetadataHydratorAPI.hydrate metadata with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param kind: Explicit target metadata shape to hydrate.
        :param work_id: Work identifier used to load identity or metadata.
        :param expression_id: Expression identifier used to load identity or metadata.
        :param manifestation_id: Manifestation identifier used to load identity or metadata.
        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :param source_row: Optional preloaded row carrying item and related WEMI
            identifiers.
        :return: The normalized row, metadata object or value described above.
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
