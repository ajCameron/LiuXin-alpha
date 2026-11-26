"""Share vocabulary traversal semantics across database and cached read sources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_trees import (
    HierarchyIntegrityError,
    HierarchyLimitError,
)

type TreeRow = Mapping[str, object]


@dataclass(frozen=True, slots=True)
class VocabularyTreeSpec:
    """Physical columns for a supported native vocabulary hierarchy."""

    table: str
    identity: str
    parent: str
    position: str


TREE_SPECS = {
    "genres": VocabularyTreeSpec(
        "genres", "genre_id", "genre_parent_id", "genre_position"
    ),
    "subjects": VocabularyTreeSpec(
        "subjects", "subject_id", "subject_parent_id", "subject_parent_position"
    ),
    "series": VocabularyTreeSpec(
        "series", "series_id", "series_parent_id", "series_parent_position"
    ),
}


class HierarchyReadSource(Protocol):
    """Read physical nodes and ordered child pages under the caller's snapshot."""

    def get(self, entity_id: int) -> TreeRow | None:
        """Read a complete node or return a confirmed miss."""
        ...

    def children(
        self, parent_id: int | None, *, limit: int, offset: int = 0
    ) -> tuple[TreeRow, ...]:
        """Read at most limit siblings ordered by position, nulls last, then ID."""
        ...


def node_id(value: object) -> int:
    """Validate persisted identities without coercion."""
    if type(value) is not int or value < 0:
        raise HierarchyIntegrityError(
            "Hierarchy identities must be nonnegative integers"
        )
    return value


def node_limit(value: int) -> int:
    """Validate a caller-selected positive node budget."""
    if type(value) is not int or value < 1:
        raise ValueError("max_nodes must be a positive integer")
    return value


def require_node(source: HierarchyReadSource, entity_id: int) -> TreeRow:
    """Require a real root for a public tree operation."""
    node_id(entity_id)
    row = source.get(entity_id)
    if row is None:
        raise CatalogNotFoundError(f"Missing hierarchy node: {entity_id}")
    return row


def ancestor_rows(
    source: HierarchyReadSource,
    spec: VocabularyTreeSpec,
    entity_id: int,
    *,
    max_nodes: int = 10000,
) -> tuple[TreeRow, ...]:
    """Read root-to-parent ancestry, detecting missing parents and cycles."""
    node_limit(max_nodes)
    row = require_node(source, entity_id)
    seen = {entity_id}
    rows: list[TreeRow] = []
    while row[spec.parent] is not None:
        parent = node_id(row[spec.parent])
        if parent in seen:
            raise HierarchyIntegrityError("Hierarchy contains a parent cycle")
        if len(rows) >= max_nodes:
            raise HierarchyLimitError("Complete ancestry exceeds max_nodes")
        seen.add(parent)
        found = source.get(parent)
        if found is None:
            raise HierarchyIntegrityError(f"Missing hierarchy parent: {parent}")
        rows.append(found)
        row = found
    return tuple(reversed(rows))


def subtree_rows(
    source: HierarchyReadSource,
    spec: VocabularyTreeSpec,
    entity_id: int,
    *,
    max_nodes: int = 10000,
) -> tuple[TreeRow, ...]:
    """Traverse iteratively with bounded child reads and no partial result."""
    node_limit(max_nodes)
    pending = [require_node(source, entity_id)]
    seen: set[int] = set()
    result: list[TreeRow] = []
    while pending:
        row = pending.pop()
        identity = node_id(row[spec.identity])
        if identity in seen:
            raise HierarchyIntegrityError("Hierarchy contains a cycle or repeated node")
        seen.add(identity)
        result.append(row)
        remaining = max_nodes - len(result) - len(pending)
        children = source.children(identity, limit=remaining + 1)
        if len(children) > remaining:
            raise HierarchyLimitError("Complete subtree exceeds max_nodes")
        pending.extend(reversed(children))
    return tuple(result)
