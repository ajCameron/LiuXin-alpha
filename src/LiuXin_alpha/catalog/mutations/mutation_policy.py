"""Catalog mutation policy implementation scaffold."""

from __future__ import annotations

from typing import Any

from ..api.common import DatabaseHandle, EntityId, RowInput, WemiLevel


class MutationPolicy:
    """Validation/policy checks for catalog writes."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def can_create(self, *, level: WemiLevel, data: RowInput) -> bool:
        return True

    def can_update(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> bool:
        return True

    def can_merge(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> bool:
        return source_id != target_id
