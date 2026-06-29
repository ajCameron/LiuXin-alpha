"""Mutation policy API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, WemiLevel


@runtime_checkable
class MutationPolicyAPI(Protocol):
    """Validation/policy checks for catalog writes."""

    def can_create(self, *, level: WemiLevel, data: RowInput) -> bool:
        """Return whether creation is permitted."""

    def can_update(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> bool:
        """Return whether update is permitted."""

    def can_merge(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> bool:
        """Return whether merge is permitted."""
