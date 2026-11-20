"""Run native search/display/edit workflows through MetadataAPIInterface.

Transport values are validated before dispatch; Catalog retains payload semantics.
Legacy item-centred metadata.get and its OPF shape keep their existing contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.catalog.api.common import WemiLevel
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataRevision
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataReference,
    MetadataSearchFilter,
    MetadataSearchRequest,
    MetadataSearchSort,
)
from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _level(payload: Mapping[str, Any]) -> WemiLevel:
    level = payload.get("level", "work")
    if level not in ("work", "expression", "manifestation", "item"):
        raise CoreDispatchError("Unknown WEMI level", code="invalid_payload")
    return cast(WemiLevel, level)


def _entity_id(payload: Mapping[str, Any]) -> int:
    entity_id = payload.get("entity_id")
    if type(entity_id) is not int or entity_id < 1:
        raise CoreDispatchError(
            "entity_id must be a positive integer", code="invalid_payload"
        )
    return entity_id


def _entities(api: MetadataAPIInterface, level: WemiLevel) -> Any:
    # Heterogeneous native container services meet at this wire serialization
    # boundary. No runtime reflection of arbitrary user-supplied names occurs.
    return {
        "work": api.works,
        "expression": api.expressions,
        "manifestation": api.manifestations,
        "item": api.items,
    }[level]


def search(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Execute typed native search and project exact counts and provenance."""
    payload = dict(query.payload)
    level = _level(payload)
    try:
        filters = []
        for raw in payload.get("filters", ()):
            item = dict(raw)
            if item.get("operator") == "in" and isinstance(item.get("value"), list):
                item["value"] = tuple(item["value"])
            filters.append(MetadataSearchFilter(**item))
        request = MetadataSearchRequest(
            query=payload.get("query", ""),
            level=level,
            filters=tuple(filters),
            sort=tuple(MetadataSearchSort(**item) for item in payload.get("sort", ())),
            limit=payload.get("limit", 100),
            offset=payload.get("offset", 0),
        )
    except (ValueError, TypeError) as exc:
        raise CoreDispatchError(str(exc), code="invalid_payload") from exc
    page = runtime.services.metadata.search.query(request)
    return {**asdict(page), "has_more": page.has_more}


def read_many(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Hydrate ordered search identities in one capture, retaining missing slots."""
    payload = query.payload
    level = _level(payload)
    ids = payload.get("entity_ids")
    if not isinstance(ids, (list, tuple)) or any(
        type(value) is not int or value < 1 for value in ids
    ):
        raise CoreDispatchError(
            "entity_ids must be positive integer IDs", code="invalid_payload"
        )
    values = _entities(runtime.services.metadata, level).get_many(
        ids, missing=payload.get("missing", "none")
    )
    return {
        "level": level,
        "entities": [None if value is None else value.to_mapping() for value in values],
    }


def read_for_edit(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Return authoritative metadata with the token required for a checked edit."""
    level = _level(query.payload)
    value = _entities(runtime.services.metadata, level).read_for_edit(
        _entity_id(query.payload)
    )
    return {
        "metadata": value.value.to_mapping(),
        "fields": dict(value.fields),
        "revision": asdict(value.revision),
    }


def update(runtime: CoreRuntime, command: CoreCommand) -> dict[str, Any]:
    """Apply a native partial edit with optional optimistic conflict protection."""
    payload = command.payload
    level, entity_id = _level(payload), _entity_id(payload)
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise CoreDispatchError(
            "data must be a metadata mapping", code="invalid_payload"
        )
    raw = payload.get("expected_revision")
    token = None
    if raw is not None:
        if (
            not isinstance(raw, Mapping)
            or not isinstance(raw.get("digest"), str)
            or not isinstance(raw.get("reference"), Mapping)
        ):
            raise CoreDispatchError(
                "expected_revision must contain reference and digest",
                code="invalid_payload",
            )
        reference = raw["reference"]
        token = MetadataRevision(
            MetadataReference(_level(reference), _entity_id(reference)), raw["digest"]
        )
    receipt = _entities(runtime.services.metadata, level).update(
        entity_id, data, expected_revision=token
    )
    return asdict(receipt)


def install(runtime: CoreRuntime) -> None:
    """Register a complete native search, display and conditional-edit workflow."""
    field = CorePayloadFieldDescription
    level = field("level", field_type="string")
    identity = field("entity_id", required=True, field_type="integer")
    runtime.register_query_handler(
        "metadata.search",
        search,
        summary="Search native WEMI metadata with exact totals and provenance.",
        payload_fields=(
            level,
            field("query", field_type="string"),
            field("filters", field_type="array"),
            field("sort", field_type="array"),
            field("limit", field_type="integer|null"),
            field("offset", field_type="integer"),
        ),
        tags=("metadata", "read"),
        remote_capabilities=("read",),
    )
    runtime.register_query_handler(
        "metadata.read_many",
        read_many,
        summary="Hydrate ordered WEMI identities in one capture.",
        payload_fields=(
            level,
            field("entity_ids", required=True, field_type="array"),
            field("missing", field_type="string"),
        ),
        tags=("metadata", "read"),
        remote_capabilities=("read",),
    )
    runtime.register_query_handler(
        "metadata.read_for_edit",
        read_for_edit,
        summary="Read authoritative metadata and its edit precondition.",
        payload_fields=(level, identity),
        tags=("metadata", "read"),
        remote_capabilities=("read",),
    )
    runtime.register_command_handler(
        "metadata.update",
        update,
        summary="Apply a native metadata edit and reconcile the cache.",
        payload_fields=(
            level,
            identity,
            field("data", required=True, field_type="object"),
            field("expected_revision", field_type="object|null"),
        ),
        tags=("metadata", "write"),
        remote_capabilities=("write",),
    )
