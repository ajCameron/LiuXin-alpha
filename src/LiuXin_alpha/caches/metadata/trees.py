"""Expose Catalog vocabulary trees through one captured metadata read source."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from LiuXin_alpha.caches.api import (
    CacheFilterOperator,
    CachePredicate,
    CacheQuery,
    CacheSort,
)
from LiuXin_alpha.caches.metadata.reads import UNAVAILABLE, MetadataReadSource
from LiuXin_alpha.caches.metadata.shared import (
    TextVocabularyMetadata,
    WorkMembershipMetadata,
)
from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt
from LiuXin_alpha.catalog.api.metadata_revisions import SharedMetadataRevision
from LiuXin_alpha.catalog.api.metadata_shared import (
    SharedMetadataRecord,
    VocabularyKind,
)
from LiuXin_alpha.catalog.api.metadata_trees import (
    HierarchyIntegrityError,
    VocabularyTreeInput,
)
from LiuXin_alpha.catalog.hierarchy import (
    HierarchyReadSource,
    TreeRow,
    VocabularyTreeSpec,
    ancestor_rows,
    require_node,
    subtree_rows,
)
from LiuXin_alpha.catalog.repositories.hierarchy import (
    DatabaseHierarchySource,
    HierarchicalRepository,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI


class _CachedHierarchySource:
    """Adapt physical cache queries without allowing per-node SQL fallback."""

    def __init__(self, source: MetadataReadSource, spec: VocabularyTreeSpec) -> None:
        self.source = source
        self.spec = spec
        self.source.allow_database_fallback = False

    def get(self, entity_id: int) -> TreeRow | None:
        row = self.source.get_row_from_id(self.spec.table, entity_id)
        if row is None:
            return None
        payload = row.row_dict
        if not isinstance(payload, Mapping):
            raise HierarchyIntegrityError("Cached hierarchy node has no row payload")
        return payload

    def children(
        self, parent_id: int | None, *, limit: int, offset: int = 0
    ) -> tuple[TreeRow, ...]:
        spec = self.spec
        predicate = (
            CachePredicate(spec.parent, CacheFilterOperator.IS_NULL, True)
            if parent_id is None
            else CachePredicate(spec.parent, CacheFilterOperator.EQ, parent_id)
        )
        return self.source.rows(
            CacheQuery(
                table=spec.table,
                predicates=(predicate,),
                sort=(CacheSort(spec.position), CacheSort(spec.identity)),
                limit=limit,
                offset=offset,
            ),
            lambda: (),  # A cached traversal cannot silently change sources.
        )


class HierarchicalVocabularyMetadata(TextVocabularyMetadata):
    """Coordinate native tree operations without owning another cache or snapshot."""

    def __init__(
        self,
        owner: MetadataAPI,
        repository: HierarchicalRepository,
        kind: VocabularyKind,
    ) -> None:
        super().__init__(owner, repository, kind)
        self._tree_repository = repository

    def _read_tree(
        self, operation: Callable[[HierarchyReadSource], tuple[TreeRow, ...]]
    ) -> tuple[SharedMetadataRecord, ...]:
        with self._owner._reading():
            source = self._owner._reads()
            database = DatabaseHierarchySource(self._tree_repository)
            if source.cache is None:
                rows = operation(database)
            else:
                try:
                    rows = operation(
                        _CachedHierarchySource(source, self._tree_repository.tree_spec)
                    )
                except UNAVAILABLE:
                    if not self._owner._fallback:
                        raise
                    rows = operation(database)
            return tuple(self._record(row) for row in rows)

    def roots(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """Read one ordered root page from a single source."""
        self._repository._validate_page(limit, offset)
        return self._read_tree(
            lambda source: source.children(None, limit=limit, offset=offset)
        )

    def children(
        self, entity_id: int, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require a parent and read its immediate children in sibling order."""
        self._repository._validate_page(limit, offset)

        def read(source: HierarchyReadSource) -> tuple[TreeRow, ...]:
            require_node(source, entity_id)
            return source.children(entity_id, limit=limit, offset=offset)

        return self._read_tree(read)

    def ancestors(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[SharedMetadataRecord, ...]:
        """Read complete root-to-parent ancestry; no partial result on overflow."""
        return self._read_tree(
            lambda source: ancestor_rows(
                source, self._tree_repository.tree_spec, entity_id, max_nodes=max_nodes
            )
        )

    def subtree(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[SharedMetadataRecord, ...]:
        """Read one bounded root-inclusive subtree in depth-first preorder."""
        return self._read_tree(
            lambda source: subtree_rows(
                source, self._tree_repository.tree_spec, entity_id, max_nodes=max_nodes
            )
        )

    def ensure_path(
        self,
        names: Sequence[str],
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Reuse exact path components atomically, preserving existing metadata."""
        return self._owner._write(
            lambda: self._tree_repository.ensure_path(
                names, parent_id=parent_id, max_nodes=max_nodes
            )
        )

    def create_tree(
        self,
        node: VocabularyTreeInput,
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Create a fresh nested structure and reconcile the cache once."""
        return self._owner._write(
            lambda: (
                self._tree_repository.create_tree(
                    node, parent_id=parent_id, max_nodes=max_nodes
                ),
                True,
            )
        )

    def move(
        self, entity_id: int, *, parent_id: int | None, before_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Move a branch; a no-op preserves the current cache generation."""
        return self._owner._write(
            lambda: (
                entity_id,
                self._tree_repository.move(
                    entity_id, parent_id=parent_id, before_id=before_id
                ),
            )
        )

    def reorder(
        self, *, parent_id: int | None, entity_ids: Sequence[int]
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace the complete sibling order under a writer reservation."""
        ids = tuple(entity_ids)
        return self._owner._write(
            lambda: (
                ids,
                self._tree_repository.reorder(parent_id=parent_id, entity_ids=ids),
            )
        )

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Delete only a leaf; declared attachments require detach=True."""

        def operation() -> tuple[int, bool]:
            self._tree_repository.delete(entity_id, detach=detach)
            return entity_id, True

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )

    def update(
        self,
        entity_id: int,
        data: Mapping[str, object],
        *,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Apply scalar or structural edits with an accurate no-op receipt."""
        return self._owner._write(
            lambda: (entity_id, self._tree_repository.update_changed(entity_id, data)),
            expected=self._expected(entity_id, expected_revision),
        )

    def delete_subtree(
        self, entity_id: int, *, detach: bool = False, max_nodes: int = 10000
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Delete a bounded subtree child-first with explicit attachment policy."""
        return self._owner._write(
            lambda: (
                self._tree_repository.delete_subtree(
                    entity_id, detach=detach, max_nodes=max_nodes
                ),
                True,
            )
        )


class SeriesMetadata(HierarchicalVocabularyMetadata, WorkMembershipMetadata):
    """Expose Series hierarchy and membership with their separate ordering rules."""
