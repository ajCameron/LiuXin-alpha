"""Install named native metadata workflows over the existing coordinated service.

Catalog owns metadata semantics and transactions. These adapters validate bounded
transport values and preserve persistence/cache receipts. Legacy metadata.get
and its item-centred OPF shape retain their separate contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import entities, relationships, shared, trees
from .batch import batch_route
from .common import (
    MAX_BATCH,
    MAX_DEPTH,
    MAX_INPUT_VALUES,
    MAX_PAGE,
    MAX_TREE,
    SHARED_KINDS,
    SUGGEST_KINDS,
    TREE_KINDS,
    Payload,
    Route,
    copy_input,
    route,
)

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime

BASE_ROUTES = (*entities.ROUTES, *shared.ROUTES, *relationships.ROUTES, *trees.ROUTES)


def describe(api: MetadataAPIInterface | None, payload: Payload) -> dict[str, Any]:
    """Describe metadata families, limits and batch eligibility without probing storage."""
    return {
        "levels": ["work", "expression", "manifestation", "item"],
        "shared_kinds": list(SHARED_KINDS),
        "immutable_kinds": ["languages"],
        "reserved_record_ids": {"agents": [0], "series": [0]},
        "suggestible_kinds": list(SUGGEST_KINDS),
        "tree_kinds": list(TREE_KINDS),
        "limits": {
            "page_size": MAX_PAGE,
            "identity_count": MAX_PAGE,
            "tree_nodes": MAX_TREE,
            "batch_operations": MAX_BATCH,
            "input_depth": MAX_DEPTH,
            "input_values": MAX_INPUT_VALUES,
        },
        "batch_operations": [item.name for item in BASE_ROUTES if item.write],
        "revision_semantics": "content; scoped to catalogue and record family",
    }


ROUTES = (
    *BASE_ROUTES,
    batch_route(BASE_ROUTES),
    route(
        "metadata.schema",
        False,
        "Discover native metadata families, limits and batch operations.",
        describe,
    ),
)


def _handler(selected: Route) -> Any:
    """Capture one route so registration and later invocation use the same operation."""

    def execute(runtime: CoreRuntime, request: CoreCommand | CoreQuery) -> Any:
        payload = copy_input(request.payload)
        selected.validate(payload)
        if selected.name == "metadata.schema":
            return describe(None, payload)
        return selected.handler(runtime.services.metadata, payload)

    return execute


def install(runtime: CoreRuntime) -> None:
    """Register explicit read/write authority and discovery for every native route."""
    for selected in ROUTES:
        if selected.write:
            runtime.register_command_handler(
                selected.name,
                _handler(selected),
                summary=selected.summary,
                payload_fields=selected.fields,
                tags=("metadata", "write"),
                remote_capabilities=("write",),
                recovery="transactional",
            )
        else:
            runtime.register_query_handler(
                selected.name,
                _handler(selected),
                summary=selected.summary,
                payload_fields=selected.fields,
                tags=("metadata", "read"),
                remote_capabilities=("read",),
            )
