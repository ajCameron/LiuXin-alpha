"""
Compose database-backed WEMI, agent and high-level metadata getter contracts into one source surface.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.metadata.api.from_database_api.wemi_sources import (
    AgentProfileGetterAPI,
    ExpressionMetadataGetterAPI,
    ItemMetadataGetterAPI,
    ManifestationMetadataGetterAPI,
    WorkMetadataGetterAPI,
)
from LiuXin_alpha.metadata.api.from_database_api.metadata_hydrator_api import (
    CalibreMetadataGetterAPI,
    HydratableMetadataKind,
    HydratedMetadataAPI,
    LiuXinMetadataGetterAPI,
    LiuXinWEMIMetadataGetterAPI,
    MetadataHydratorAPI,
    MetadataObjectGetterAPI,
)
from LiuXin_alpha.metadata.api.from_database_api.metadata_read_source_api import (
    MetadataDriverWrapperAPI,
    MetadataLinkRow,
    MetadataLinkRowSequence,
    MetadataRowSequence,
    MetadataReadSourceAPI,
    MetadataSearchTerm,
    MetadataTableColumns,
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

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind the composite metadata source to the database shared by all inherited getter contracts.

        Example:
            Exercise DBMetadataSourceAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


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
