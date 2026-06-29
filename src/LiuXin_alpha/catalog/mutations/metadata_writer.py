"""Coordinated catalog metadata writer scaffold."""

from __future__ import annotations

from typing import Any

from ..api.common import CatalogMutationError, DatabaseHandle, EntityId, RowInput, WemiLevel
from .mutation_policy import MutationPolicy


class MetadataWriter:
    """Coordinated writes that may touch multiple catalog repositories."""

    def __init__(self, db: DatabaseHandle, repositories: Any, policy: MutationPolicy) -> None:
        self.db = db
        self.repositories = repositories
        self.policy = policy

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        if not self.policy.can_update(level=level, entity_id=entity_id, data=data):
            raise CatalogMutationError(f"metadata attachment rejected for {level}:{entity_id}")
        raise NotImplementedError("Route metadata attachment to titles/agents/identifiers/notes repositories")

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        if not self.policy.can_merge(level=level, source_id=source_id, target_id=target_id):
            raise CatalogMutationError(f"merge rejected for {level}:{source_id}->{target_id}")
        raise NotImplementedError("Move merge/link preservation policy here from databases/library")
