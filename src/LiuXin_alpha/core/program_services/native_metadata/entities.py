"""Adapt native WEMI reads, construction and edits to named Core operations."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchFilter,
    MetadataSearchRequest,
    MetadataSearchSort,
)
from LiuXin_alpha.core.payloads import invalid_payload

from .common import (
    Payload,
    entities,
    expected,
    identities,
    identity,
    level,
    mapping,
    optional_identity,
    page,
    route,
    sequence,
)


def request(payload: Payload) -> MetadataSearchRequest:
    """Construct a typed search request; Catalog validates semantic fields and syntax."""
    try:
        filters = []
        for raw in sequence(payload.get("filters", ()), "filters"):
            item = mapping(raw, "filters")
            if item.get("operator") == "in" and isinstance(item.get("value"), list):
                item["value"] = tuple(item["value"])
            filters.append(MetadataSearchFilter(**item))
        return MetadataSearchRequest(
            query=payload.get("query", ""),
            level=level(payload),
            filters=tuple(filters),
            sort=tuple(
                MetadataSearchSort(**mapping(item, "sort"))
                for item in sequence(payload.get("sort", ()), "sort")
            ),
            **page(payload),
        )
    except (ValueError, TypeError) as error:
        invalid_payload("search", str(error))


def search(api: MetadataAPIInterface, payload: Payload) -> dict[str, Any]:
    """Return exact counts, semantic references and execution provenance."""
    result = api.search.query(request(payload))
    return {**asdict(result), "has_more": result.has_more}


def search_schema(api: MetadataAPIInterface, payload: Payload) -> dict[str, Any]:
    """Discover semantic filter aliases and scalar sorting independently."""
    selected = level(payload)
    return {
        "level": selected,
        "fields": api.search.fields(level=selected),
        "sort_fields": api.search.sort_fields(level=selected),
    }


def validate_search(api: MetadataAPIInterface, payload: Payload) -> dict[str, bool]:
    """Validate a search without executing or hydrating it."""
    api.search.validate(request(payload))
    return {"valid": True}


def read_many(api: MetadataAPIInterface, payload: Payload) -> dict[str, Any]:
    """Preserve duplicate IDs and missing slots in an ordered, detached hydration."""
    selected = level(payload)
    values = entities(api, selected).get_many(
        identities(payload), missing=payload.get("missing", "none")
    )
    return {
        "level": selected,
        "entities": [None if value is None else value.to_mapping() for value in values],
    }


def read_for_edit(api: MetadataAPIInterface, payload: Payload) -> dict[str, Any]:
    """Return authoritative fields and their matching edit precondition."""
    value = entities(api, level(payload)).read_for_edit(identity(payload))
    return {
        "metadata": value.value.to_mapping(),
        "fields": dict(value.fields),
        "revision": asdict(value.revision),
    }


def create(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create an entity without implicit matching or compatibility metadata."""
    return entities(api, level(payload)).create(mapping(payload["data"], "data"))


def update(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Apply only supplied aliases with optional optimistic protection."""
    return entities(api, level(payload)).update(
        identity(payload),
        mapping(payload["data"], "data"),
        expected_revision=expected(payload),
    )


def delete(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Delegate deletion constraints and cache reconciliation to the metadata owner."""
    return entities(api, level(payload)).delete(
        identity(payload), expected_revision=expected(payload)
    )


def create_stack(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create a connected WEMI path atomically, optionally reusing an existing Work."""
    return api.mutations.create_wemi_stack(
        work=mapping(payload["work"], "work"),
        expression=mapping(payload["expression"], "expression"),
        manifestation=mapping(payload["manifestation"], "manifestation"),
        items=[
            mapping(item, "items")
            for item in sequence(payload.get("items", ()), "items")
        ],
        work_id=optional_identity(payload, "work_id"),
        origin=payload.get("origin"),
    )


def attach(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Attach supported groups under Catalog cardinality and omission rules."""
    return api.mutations.attach_metadata(
        level=level(payload),
        entity_id=identity(payload),
        data=mapping(payload["data"], "data"),
    )


def replace(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Replace supplied groups while retaining omitted groups."""
    return api.mutations.replace_metadata(
        level=level(payload),
        entity_id=identity(payload),
        data=mapping(payload["data"], "data"),
    )


def link(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Link existing adjacent WEMI identities without moving their ownership policy."""
    return api.mutations.link_wemi(
        parent_level=level(payload, "parent_level"),
        parent_id=identity(payload, "parent_id"),
        child_level=level(payload, "child_level"),
        child_id=identity(payload, "child_id"),
        primary=payload.get("primary"),
        priority=payload.get("priority"),
        origin=payload.get("origin"),
    )


def unlink(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Remove an adjacent link while retaining endpoint identities."""
    return api.mutations.unlink_wemi(
        parent_level=level(payload, "parent_level"),
        parent_id=identity(payload, "parent_id"),
        child_level=level(payload, "child_level"),
        child_id=identity(payload, "child_id"),
    )


LEVEL = ("level:string",)
ENTITY = (*LEVEL, "entity_id!:integer")
REVISION = ("expected_revision:object|null",)
SEARCH = (
    *LEVEL,
    "query:string",
    "filters:array",
    "sort:array",
    "limit:integer",
    "offset:integer",
)
EDGE = (
    "parent_level!:string",
    "parent_id!:integer",
    "child_level!:string",
    "child_id!:integer",
)
ROUTES = (
    route(
        "metadata.search",
        False,
        "Search native metadata with bounded pages and exact totals.",
        search,
        *SEARCH,
    ),
    route(
        "metadata.search.schema",
        False,
        "Discover semantic filters and scalar ordering keys.",
        search_schema,
        *LEVEL,
    ),
    route(
        "metadata.search.validate",
        False,
        "Validate a native search without executing it.",
        validate_search,
        *SEARCH,
    ),
    route(
        "metadata.read_many",
        False,
        "Hydrate ordered WEMI identities in one capture.",
        read_many,
        *LEVEL,
        "entity_ids!:array",
        "missing:string",
    ),
    route(
        "metadata.read_for_edit",
        False,
        "Read authoritative metadata and its edit precondition.",
        read_for_edit,
        *ENTITY,
    ),
    route(
        "metadata.create",
        True,
        "Create one native WEMI entity.",
        create,
        *LEVEL,
        "data!:object",
    ),
    route(
        "metadata.update",
        True,
        "Apply a native partial edit and reconcile the cache.",
        update,
        *ENTITY,
        "data!:object",
        *REVISION,
    ),
    route(
        "metadata.delete",
        True,
        "Delete a native entity under Catalog policy.",
        delete,
        *ENTITY,
        *REVISION,
    ),
    route(
        "metadata.wemi.create_stack",
        True,
        "Create a connected WEMI graph atomically.",
        create_stack,
        "work!:object",
        "expression!:object",
        "manifestation!:object",
        "items:array",
        "work_id:integer|null",
        "origin:string|null",
    ),
    route(
        "metadata.attach",
        True,
        "Attach metadata groups under Catalog policy.",
        attach,
        *ENTITY,
        "data!:object",
    ),
    route(
        "metadata.replace",
        True,
        "Replace supplied metadata groups, retaining omissions.",
        replace,
        *ENTITY,
        "data!:object",
    ),
    route(
        "metadata.wemi.link",
        True,
        "Link adjacent WEMI entities.",
        link,
        *EDGE,
        "primary:boolean|null",
        "priority:integer|null",
        "origin:string|null",
    ),
    route(
        "metadata.wemi.unlink",
        True,
        "Unlink adjacent WEMI entities without deleting them.",
        unlink,
        *EDGE,
    ),
)
