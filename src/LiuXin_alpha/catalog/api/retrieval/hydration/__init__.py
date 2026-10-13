"""
Compose database-backed WEMI, agent and high-level metadata getter contracts into one source surface.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/catalog/retrieval/test_metadata_hydrator_api.py
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.catalog.api.retrieval.hydration.metadata_hydrator_api import (
    CalibreMetadataGetterAPI,
    HydratableMetadataKind,
    HydratedMetadataAPI,
    LiuXinMetadataGetterAPI,
    LiuXinWEMIMetadataGetterAPI,
    MetadataHydratorAPI,
    MetadataObjectGetterAPI,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.metadata_read_source_api import (
    MetadataDriverWrapperAPI,
    MetadataLinkRow,
    MetadataLinkRowSequence,
    MetadataReadSourceAPI,
    MetadataRowSequence,
    MetadataSearchTerm,
    MetadataTableColumns,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources import (
    AgentProfileGetterAPI,
    ExpressionMetadataGetterAPI,
    ItemMetadataGetterAPI,
    ManifestationMetadataGetterAPI,
    WorkMetadataGetterAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class DBMetadataSourceAPI(
    WorkMetadataGetterAPI,
    ExpressionMetadataGetterAPI,
    ManifestationMetadataGetterAPI,
    ItemMetadataGetterAPI,
    AgentProfileGetterAPI,
    MetadataHydratorAPI,
):
    """
    Combine all database-backed metadata getter contracts around one database dependency.

    Example:
        Exercise DBMetadataSourceAPI with the owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_hydrator_api.py
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Bind the composite metadata source to the database shared by all inherited getter contracts.

        Example:
            Exercise DBMetadataSourceAPI.  init   with the owning regression module::

                python -m pytest -q tests/catalog/retrieval/test_metadata_hydrator_api.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        super().__init__(db)


__all__ = [
    "AgentProfileGetterAPI",
    "CalibreMetadataGetterAPI",
    "DBMetadataSourceAPI",
    "ExpressionMetadataGetterAPI",
    "HydratableMetadataKind",
    "HydratedMetadataAPI",
    "ItemMetadataGetterAPI",
    "LiuXinMetadataGetterAPI",
    "LiuXinWEMIMetadataGetterAPI",
    "ManifestationMetadataGetterAPI",
    "MetadataDriverWrapperAPI",
    "MetadataLinkRow",
    "MetadataLinkRowSequence",
    "MetadataHydratorAPI",
    "MetadataObjectGetterAPI",
    "MetadataRowSequence",
    "MetadataReadSourceAPI",
    "MetadataSearchTerm",
    "MetadataTableColumns",
    "WorkMetadataGetterAPI",
]
