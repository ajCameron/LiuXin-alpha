"""Database-backed source contracts for metadata containers and read-side views.

This layer is real infrastructure, not dead scaffolding. It is the read-side
contract between the database and metadata container/view construction.
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

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class DBMetadataSourceAPI(
    WorkMetadataGetterAPI,
    ExpressionMetadataGetterAPI,
    ManifestationMetadataGetterAPI,
    ItemMetadataGetterAPI,
    AgentProfileGetterAPI,
):
    """Single database-backed source surface for metadata objects and views."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        super().__init__(db)


__all__ = ["DBMetadataSourceAPI"]
