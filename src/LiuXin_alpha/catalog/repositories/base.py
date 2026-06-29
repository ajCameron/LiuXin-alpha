"""Base repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import CatalogNotFoundError, DatabaseHandle, EntityId, RowInput, RowMapping


class BaseRepository:
    """Small adapter around the raw database object.

    Concrete subclasses should translate catalog operations into existing database
    calls. Do not push WEMI semantics back down into the raw database layer.
    """

    table_name: str = ""
    id_column: str = "id"

    def __init__(self, db: DatabaseHandle) -> None:
        self.db = db

    def get(self, entity_id: EntityId) -> RowMapping | None:
        raise NotImplementedError(f"Wire {type(self).__name__}.get() to the database adapter")

    def require(self, entity_id: EntityId) -> RowMapping:
        row = self.get(entity_id)
        if row is None:
            raise CatalogNotFoundError(f"{self.table_name} row not found: {entity_id}")
        return row

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        raise NotImplementedError(f"Wire {type(self).__name__}.list() to the database adapter")

    def create(self, data: RowInput) -> EntityId:
        raise NotImplementedError(f"Wire {type(self).__name__}.create() to the database adapter")

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        raise NotImplementedError(f"Wire {type(self).__name__}.update() to the database adapter")

    def delete(self, entity_id: EntityId) -> None:
        raise NotImplementedError(f"Wire {type(self).__name__}.delete() to the database adapter")
