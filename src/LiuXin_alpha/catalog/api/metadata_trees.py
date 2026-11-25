"""Native vocabulary forests, bounded traversal and explicit structural writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt
from LiuXin_alpha.catalog.api.metadata_revisions import SharedMetadataRevision
from LiuXin_alpha.catalog.api.metadata_shared import (
    SharedMetadataRecord,
    TextVocabularyMetadataInterface,
    WorkMembershipMetadataInterface,
)


# Catalog exceptions are outside mypy's checked import set.
class HierarchyIntegrityError(CatalogMutationError):  # type: ignore[misc]
    """A parent chain is missing, cyclic or otherwise malformed."""


class HierarchyLimitError(ValueError):
    """A complete tree operation exceeds its explicit node budget."""


class HierarchyInUseError(CatalogMutationError):  # type: ignore[misc]
    """Children or metadata references prevent the requested deletion."""


@dataclass(frozen=True, slots=True)
class VocabularyTreeInput:
    """Describe a fresh node and ordered children; copy caller-owned inputs.

    Parent and position fields are supplied by the structure, not by data.
    Existing full labels and tree IDs are optional opaque scalar metadata.
    """

    data: Mapping[str, object]
    children: tuple[VocabularyTreeInput, ...] = ()

    def __post_init__(self) -> None:
        """Detach the payload and freeze the outer mapping and child sequence."""
        object.__setattr__(self, "data", MappingProxyType(deepcopy(dict(self.data))))
        object.__setattr__(self, "children", tuple(self.children))


class HierarchicalVocabularyMetadataInterface(
    TextVocabularyMetadataInterface, Protocol
):
    """Manage one parent-linked vocabulary through coordinated metadata services.

    None denotes root scope. Reads are detached and bounded reads never return
    partial trees. Writes return the existing persistence/reconciliation receipt.
    """

    def ensure_path(
        self,
        names: Sequence[str],
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Reuse exact scoped nodes, returning IDs from first component to leaf."""
        ...

    def create_tree(
        self,
        node: VocabularyTreeInput,
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Create every supplied node atomically; return root-first preorder IDs."""
        ...

    def roots(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return roots in sibling order, independently of ID-ordered list()."""
        ...

    def children(
        self, entity_id: int, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require the parent and return its immediate children in sibling order."""
        ...

    def ancestors(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return root-to-parent records, excluding the required starting node."""
        ...

    def subtree(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return a complete root-inclusive depth-first preorder traversal."""
        ...

    def move(
        self, entity_id: int, *, parent_id: int | None, before_id: int | None = None
    ) -> MetadataWriteReceipt[int]:
        """Move a branch before a destination sibling, or append when omitted."""
        ...

    def reorder(
        self, *, parent_id: int | None, entity_ids: Sequence[int]
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Supply every current sibling exactly once, including root scope."""
        ...

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Delete only a leaf; attachments require explicit detachment."""
        ...

    def delete_subtree(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        max_nodes: int = 10000,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Delete descendants before parents, optionally detaching known links."""
        ...


class SeriesMetadataInterface(
    HierarchicalVocabularyMetadataInterface, WorkMembershipMetadataInterface, Protocol
):
    """Combine Series hierarchy and Work membership without volume-number semantics."""
