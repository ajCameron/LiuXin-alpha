"""Share Work membership semantics for the Tag and Series repositories.

Membership is an endpoint pair, independent of a Series link's optional type.
Existing pairs retain every typed link and its metadata. Ordering belongs to
Works within each Tag/Series; it is not a Series volume or preferred-membership
policy. Persistence and endpoint constraints use existing relationship/macros
owners, without SQL in Catalog.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from ..api.common import EntityId, RowMapping
from ..relationships import CatalogRelationships
from .exact import ExactEntityRepository


class WorkMembershipRepository(ExactEntityRepository):
    """Add explicit Work memberships to an exact-value repository.

    Concrete Tag and Series repositories supply their table and identity rules.
    All reads and writes require real Work/entity IDs and a macro transaction.
    Mutations preserve entity rows, other Works and retained link metadata.

    Example:
        catalog.tags.link_to_work(entity_id=tag_id, work_id=work_id)
    """

    def _membership_ids(self, work_id: EntityId) -> set[EntityId]:
        """Require a Work and read its related IDs inside the caller's transaction."""
        self._require_table_row("works", work_id)
        spec = self._link_spec("works", self.table_name)
        return {row.secondary_id for row in self._macros.get_link_rows(spec, work_id)}

    def link_to_work(self, *, entity_id: EntityId, work_id: EntityId) -> None:
        """Attach this repository's entity to a Work, preserving existing links.

        An existing pair is unchanged, including all Series link types, priorities
        and provenance. A new pair receives the default null type and a priority
        allocated among the Works already attached to this Tag/Series. This is
        ordering metadata, not a volume number or a preferred Series choice.

        :param entity_id: Existing Tag ID or Series ID, according to the repository.
        :param work_id: Existing Work ID; never an Expression or compatibility ray.
        :return: None; missing endpoints raise CatalogNotFoundError.
        """
        with self._macros.transaction():
            self.require(entity_id)
            if entity_id not in self._membership_ids(work_id):
                # Physical uniqueness scopes priority to the Tag/Series endpoint.
                self._link(self.table_name, entity_id, "works", work_id)

    def unlink_from_work(self, *, entity_id: EntityId, work_id: EntityId) -> None:
        """Remove a Work/entity pair's links without deleting either entity.

        All types for this pair are removed. Other owners, relationships and
        priorities remain unchanged. An absent link between existing endpoints
        is a no-op; a missing endpoint raises CatalogNotFoundError.

        :param entity_id: Existing Tag or Series ID owned by this repository.
        :param work_id: Existing Work whose membership is removed.
        :return: None; the deletion composes with enclosing macro transactions.
        """
        with self._macros.transaction():
            self.require(entity_id)
            self._require_table_row("works", work_id)
            CatalogRelationships(self.db).unlink(
                self.table_name, entity_id, "works", work_id
            )

    def is_linked_to_work(self, *, entity_id: EntityId, work_id: EntityId) -> bool:
        """Check membership across all link types, requiring both endpoints.

        :param entity_id: Existing Tag or Series ID owned by this repository.
        :param work_id: Existing Work ID.
        :return: Whether at least one link exists; missing endpoints raise.
        """
        with self._macros.transaction():
            self.require(entity_id)
            return entity_id in self._membership_ids(work_id)

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """Read linked Tag/Series rows with their ``_catalog_link`` metadata.

        Multiple Series link types can yield multiple rows for the same Series.
        Rows use the macro reader's priority-descending/ID order; priorities are
        scoped to each Tag/Series and do not rank a Work's preferred membership.
        The transaction covers both link and entity reads. A missing Work or
        dangling linked entity raises instead of returning an empty result.

        :param work_id: Existing Work ID whose memberships are read.
        :return: Copied rows; an unlinked existing Work yields an empty tuple.
        """
        with self._macros.transaction():
            return self._linked_rows("works", work_id, self.table_name)

    def replace_for_work(
        self, *, work_id: EntityId, entity_ids: Iterable[EntityId]
    ) -> None:
        """Replace one Work's membership set; empty input clears its links.

        Retained pairs keep all link types, IDs, priorities and provenance.
        Input order does not reorder retained links. New pairs use the same
        policy as link_to_work; removed pairs lose every type. Entity rows and
        other Works are untouched. Validation and changes share one transaction.

        :param work_id: Existing Work ID.
        :param entity_ids: Unique existing Tag/Series IDs; generators are accepted.
        :return: None; invalid or duplicate IDs fail before any mutation.
        """
        self.replace_for_works({work_id: entity_ids})

    def replace_for_works(
        self, replacements: Mapping[EntityId, Iterable[EntityId]]
    ) -> None:
        """Replace multiple Works' memberships atomically after full validation.

        Materialize inputs and reject invalid/duplicate IDs before querying.
        Require every Work and member before the first mutation. Any failure
        rolls back the whole batch, including earlier Works. Membership sets
        preserve retained link metadata rather than assigning new display ranks.

        :param replacements: Work IDs mapped to unique Tag/Series IDs.
        :return: None; an empty mapping is a no-op without database access.
        """
        if not isinstance(replacements, Mapping):
            raise TypeError("replacements must map Work IDs to membership IDs")
        prepared = {}
        for work_id, raw_ids in replacements.items():
            self._validate_entity_id(work_id)
            if isinstance(raw_ids, (str, bytes)):
                raise TypeError("membership IDs must be an iterable of integers")
            ids = tuple(raw_ids)
            for entity_id in ids:
                self._validate_entity_id(entity_id)
            if len(set(ids)) != len(ids):
                raise ValueError("membership IDs must be unique for each Work")
            prepared[work_id] = ids
        if not prepared:
            return
        with self._macros.transaction():
            existing = {work_id: self._membership_ids(work_id) for work_id in prepared}
            for entity_id in dict.fromkeys(
                entity_id for ids in prepared.values() for entity_id in ids
            ):
                self.require(entity_id)
            relationships = CatalogRelationships(self.db)
            for work_id, ids in prepared.items():
                for entity_id in sorted(existing[work_id] - set(ids)):
                    relationships.unlink(self.table_name, entity_id, "works", work_id)
                for entity_id in ids:
                    if entity_id not in existing[work_id]:
                        self._link(self.table_name, entity_id, "works", work_id)
