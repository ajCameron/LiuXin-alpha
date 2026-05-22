"""Catalog-level projection implementation scaffold."""

from __future__ import annotations

from typing import Any

from ..api.common import DatabaseHandle, EntityId, WemiLevel


class ProjectionService:
    """Derived catalog read models.

    Keep this semantic rather than surface-specific. For example: a stable catalog
    display title belongs here; terminal column widths do not.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def display_title(self, *, level: WemiLevel, entity_id: EntityId) -> str:
        raise NotImplementedError("Move WEMI title projection here from databases/surfaces")

    def item_summary(self, item_id: EntityId) -> dict[str, object]:
        raise NotImplementedError("Move compact item summary projection here")
