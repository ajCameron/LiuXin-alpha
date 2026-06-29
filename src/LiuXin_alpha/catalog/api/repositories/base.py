"""
Base repository API contract.

This is intended to help support calls such as "catalog.x.get" and similar patterns.
"""

# Todo: We want a means to get metadata objects back out of these sorts of calls...

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, RowInput, RowMapping


@runtime_checkable
class BaseRepositoryAPI(Protocol):
    """
    CRUD-shaped API for a single catalog table or view-backed entity.
    """

    table_name: str
    id_column: str

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """
        Return one entity by id, or None.

        :param entity_id:
        :return:
        """

    def require(self, entity_id: EntityId) -> RowMapping:
        """
        Return one entity by id, or raise CatalogNotFoundError.

        :param entity_id:
        :return:
        """

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """
        Return a bounded page of entities.

        :param limit: Upper bound on the number of entities to return.
        :param offset: Offset of the first entity to return.
        :return:
        """

    def create(self, data: RowInput) -> EntityId:
        """
        Create an entity and return its id.

        :param data:
        :return:
        """

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """
        Update an entity.

        :param entity_id:
        :param data:
        :return:
        """

    def delete(self, entity_id: EntityId) -> None:
        """
        Delete an entity, where deletion is permitted.

        :param entity_id:
        :return:
        """
