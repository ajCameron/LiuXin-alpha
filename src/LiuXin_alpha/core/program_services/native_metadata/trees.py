"""Expose bounded vocabulary forests, explicit moves and safe subtree deletion."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
from LiuXin_alpha.catalog.api.metadata_trees import VocabularyTreeInput
from LiuXin_alpha.core.payloads import invalid_payload

from .common import (
    MAX_DEPTH,
    MAX_TREE,
    TREE_KINDS,
    Payload,
    bounded,
    choice,
    identities,
    identity,
    mapping,
    optional_identity,
    page,
    route,
    sequence,
    shared,
)


def service(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Select only declared hierarchical vocabulary services."""
    return shared(api, choice(payload["kind"], "kind", TREE_KINDS))


def budget(payload: Payload) -> int:
    """Bound complete traversals; exceeding the budget never returns a partial tree."""
    return bounded(payload.get("max_nodes", MAX_TREE), "max_nodes", MAX_TREE, 1)


def tree_input(value: object, maximum: int) -> VocabularyTreeInput:
    """Decode a finite ordered tree before any persistent node is created."""
    remaining = maximum

    def visit(raw: object, depth: int) -> VocabularyTreeInput:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > MAX_DEPTH:
            invalid_payload("node", "exceeds the tree node/depth budget.")
        node = mapping(raw, "node")
        if set(node) - {"data", "children"} or "data" not in node:
            invalid_payload("node", "requires data and optional children only.")
        return VocabularyTreeInput(
            mapping(node["data"], "node.data"),
            tuple(
                visit(child, depth + 1)
                for child in sequence(node.get("children", ()), "children", maximum)
            ),
        )

    return visit(value, 0)


def roots(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return roots in sibling order."""
    return {"records": service(api, payload).roots(**page(payload))}


def children(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Require a parent and return its immediate children in sibling order."""
    return {
        "records": service(api, payload).children(identity(payload), **page(payload))
    }


def ancestors(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return the complete root-to-parent chain within the explicit budget."""
    return {
        "records": service(api, payload).ancestors(
            identity(payload), max_nodes=budget(payload)
        )
    }


def subtree(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return a complete root-inclusive preorder traversal."""
    return {
        "records": service(api, payload).subtree(
            identity(payload), max_nodes=budget(payload)
        )
    }


def ensure_path(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Reuse an exact scoped path, preserving ambiguity refusal and no-op receipts."""
    maximum = budget(payload)
    names = sequence(payload["names"], "names", maximum)
    if any(not isinstance(name, str) for name in names):
        invalid_payload("names", "must contain only text components.")
    return service(api, payload).ensure_path(
        names, parent_id=optional_identity(payload, "parent_id"), max_nodes=maximum
    )


def create(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create every supplied node atomically, returning root-first identities."""
    maximum = budget(payload)
    node = tree_input(payload["node"], maximum)
    return service(api, payload).create_tree(
        node, parent_id=optional_identity(payload, "parent_id"), max_nodes=maximum
    )


def move(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Move a branch under an explicit parent/root and optional destination sibling."""
    return service(api, payload).move(
        identity(payload),
        parent_id=optional_identity(payload, "parent_id"),
        before_id=optional_identity(payload, "before_id"),
    )


def reorder(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Supply every destination sibling once; Catalog validates the full set."""
    return service(api, payload).reorder(
        parent_id=optional_identity(payload, "parent_id"),
        entity_ids=identities(payload),
    )


def delete_subtree(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Delete descendants before parents, with explicit reference detachment."""
    return service(api, payload).delete_subtree(
        identity(payload),
        detach=payload.get("detach", False),
        max_nodes=budget(payload),
    )


KIND = ("kind!:string",)
ENTITY = (*KIND, "entity_id!:integer")
BUDGET = ("max_nodes:integer",)
PAGE = ("limit:integer", "offset:integer")
ROUTES = (
    route(
        "metadata.tree.roots",
        False,
        "Read roots in vocabulary sibling order.",
        roots,
        *KIND,
        *PAGE,
    ),
    route(
        "metadata.tree.children",
        False,
        "Read immediate children in sibling order.",
        children,
        *ENTITY,
        *PAGE,
    ),
    route(
        "metadata.tree.ancestors",
        False,
        "Read a complete bounded ancestor chain.",
        ancestors,
        *ENTITY,
        *BUDGET,
    ),
    route(
        "metadata.tree.subtree",
        False,
        "Read a complete bounded subtree.",
        subtree,
        *ENTITY,
        *BUDGET,
    ),
    route(
        "metadata.tree.ensure_path",
        True,
        "Reuse or create an exact scoped vocabulary path.",
        ensure_path,
        *KIND,
        "names!:array",
        "parent_id:integer|null",
        *BUDGET,
    ),
    route(
        "metadata.tree.create",
        True,
        "Create an ordered vocabulary tree atomically.",
        create,
        *KIND,
        "node!:object",
        "parent_id:integer|null",
        *BUDGET,
    ),
    route(
        "metadata.tree.move",
        True,
        "Move a branch while rejecting cycles and invalid siblings.",
        move,
        *ENTITY,
        "parent_id!:integer|null",
        "before_id:integer|null",
    ),
    route(
        "metadata.tree.reorder",
        True,
        "Replace the complete sibling ordering.",
        reorder,
        *KIND,
        "parent_id!:integer|null",
        "entity_ids!:array",
    ),
    route(
        "metadata.tree.delete_subtree",
        True,
        "Delete a bounded subtree with explicit detach policy.",
        delete_subtree,
        *ENTITY,
        "detach:boolean",
        *BUDGET,
    ),
)
