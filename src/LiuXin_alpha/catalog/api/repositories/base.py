"""Base repository API contract."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping


@runtime_checkable
class BaseRepositoryAPI(Protocol):
    """CRUD-shaped API for a single catalog table or view-backed entity."""

    table_name: str
    id_column: str

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """Return one entity by id, or None."""

    def require(self, entity_id: EntityId) -> RowMapping:
        """Return one entity by id, or raise CatalogNotFoundError."""

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """Return a bounded page of entities."""

    def create(self, data: RowInput) -> EntityId:
        """Create an entity and return its id."""

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """Update an entity."""

    def delete(self, entity_id: EntityId) -> None:
        """Delete an entity, where deletion is permitted."""
