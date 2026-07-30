"""Common CRUD contract inherited by Catalog entity repositories.

Repository inputs accept documented public aliases as well as writable storage
column names. Returned rows are read-only mappings keyed by storage column
names. IDs are always integer database IDs.
"""

# Todo: We want a means to get metadata objects back out of these sorts of calls...

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, RowInput, RowMapping


@runtime_checkable
class BaseRepositoryAPI(Protocol):
    """CRUD-shaped API for one catalog entity.

    Concrete repositories declare ``table_name`` and ``id_column`` and may add
    semantic relationship or matching methods. ``get`` versus ``require`` is
    the primary absence choice; creation never accepts an explicit ID.

    Example::

        work_id = catalog.works.create({"title": "Frankenstein"})
        row = catalog.works.require(work_id)
        catalog.works.update(work_id, {"canonical_title": row["work_title"]})
    """

    table_name: str
    id_column: str

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """Return one entity by ID when it exists.

        :param entity_id: Non-negative integer ID in this repository.
        :return: A plain storage-column mapping, or ``None`` when absent.
        """

    def require(self, entity_id: EntityId) -> RowMapping:
        """Return one entity by ID or fail explicitly.

        :param entity_id: Non-negative integer ID in this repository.
        :return: Existing entity as a storage-column mapping.
        :raises CatalogNotFoundError: If ``entity_id`` does not exist.
        """

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """
        Return a bounded page of entities.

        :param limit: Upper bound on the number of entities to return.
        :param offset: Offset of the first entity to return.
        :return: ID-ordered row mappings; an empty sequence when the page has
            no rows.
        """

    def create(self, data: RowInput) -> EntityId:
        """Create one entity after normalizing public aliases.

        :param data: Non-empty mapping of writable public aliases or storage
            columns. Repository IDs are not writable.
        :return: Database-assigned integer ID.
        :raises CatalogMutationError: If a field is unknown, read-only, or the
            payload is empty.
        """

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """Update writable fields on an existing entity.

        :param entity_id: Existing entity ID.
        :param data: Mapping of fields to replace; omitted fields are unchanged.
        :return: ``None``. Re-read the entity when updated values are needed.
        :raises CatalogNotFoundError: If the entity does not exist.
        """

    def delete(self, entity_id: EntityId) -> None:
        """Delete an entity where repository policy permits it.

        :param entity_id: Existing entity ID.
        :return: ``None``.
        :raises CatalogNotFoundError: If the entity does not exist.
        :raises CatalogMutationError: If ownership or repository policy rejects
            deletion.
        """
