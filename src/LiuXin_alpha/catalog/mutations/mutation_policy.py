"""Validation policy for coordinated catalog mutations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..api.common import DatabaseHandle, EntityId, RowInput, WemiLevel
from ..repositories.base import WEMI_TABLES


class MutationPolicy:
    """Validation/policy checks for catalog writes."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def can_create(self, *, level: WemiLevel, data: RowInput) -> bool:
        """Return whether a non-empty mapping can create this WEMI level."""

        return level in WEMI_TABLES and isinstance(data, Mapping) and bool(data)

    def can_update(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> bool:
        """Return whether an existing WEMI entity can accept this payload."""

        if level not in WEMI_TABLES or not isinstance(data, Mapping) or not data:
            return False
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            return False
        repository = getattr(self.repositories, f"{level}s")
        return repository.get(entity_id) is not None

    def can_merge(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> bool:
        """Return whether two distinct existing same-level entities can merge."""

        if level not in WEMI_TABLES or source_id == target_id:
            return False
        if any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in (source_id, target_id)
        ):
            return False
        repository = getattr(self.repositories, f"{level}s")
        return repository.get(source_id) is not None and repository.get(target_id) is not None
