"""Core-owned catalog operations and wire translation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _agent_role,
    _callable,
    _flatten_text,
    _optional_int,
    _payload,
    _required_int,
    _required_text,
    _text_list,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def catalog_fields_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    metadata = runtime.services.field_metadata
    kind = str(payload.get("kind") or "all").strip().lower()
    include_composites = bool(payload.get("include_composites", True))
    if kind == "sortable":
        keys = metadata.sortable_field_keys()
    elif kind == "displayable":
        keys = metadata.displayable_field_keys()
    elif kind == "standard":
        keys = metadata.standard_field_keys()
    elif kind == "custom":
        keys = metadata.custom_field_keys(include_composites=include_composites)
    elif kind == "searchable":
        keys = metadata.searchable_fields()
    elif kind == "all":
        keys = metadata.all_field_keys()
    else:
        raise CoreDispatchError(
            "`kind` must be all, sortable, displayable, standard, custom, or searchable."
        )
    fields = [{"key": str(key), "metadata": plain(metadata.get(key))} for key in keys]
    return {"kind": kind, "fields": fields, "count": len(fields)}


def catalog_fields_get(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    key = _required_text(_payload(query), "key")
    metadata = runtime.services.field_metadata
    value = metadata.get(key)
    return {
        "key": key,
        "exists": value is not None,
        "metadata": plain(value),
    }


def catalog_hierarchy_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    direction = str(payload.get("direction") or "children").strip().lower()
    if direction == "children":
        operation = runtime.catalog.retrieval.hierarchy.children
    elif direction == "parents":
        operation = runtime.catalog.retrieval.hierarchy.parents
    else:
        raise CoreDispatchError("`direction` must be `children` or `parents`.")
    try:
        adjacency = operation(level=level, entity_id=entity_id)
    except ValueError as error:
        raise CoreDispatchError(
            f"No {direction} WEMI adjacency exists for level {level!r}."
        ) from error
    return {
        "level": adjacency.level,
        "entity_id": adjacency.entity_id,
        "direction": adjacency.direction,
        "result_level": adjacency.related_level,
        "entities": [plain(item) for item in adjacency.entities],
    }


def catalog_identifiers_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    identifiers = runtime.catalog.repositories.identifiers
    if level == "agent":
        method = _callable(
            identifiers,
            "list_for_agent",
            area="Catalog identifiers",
        )
        rows = method(entity_id)
    else:
        method = _callable(
            identifiers,
            "list_for_wemi",
            area="Catalog identifiers",
        )
        rows = method(level=level, entity_id=entity_id)
    values = [plain(item) for item in rows]
    return {
        "level": level,
        "entity_id": entity_id,
        "identifiers": values,
        "count": len(values),
    }


def catalog_identifiers_primary_values(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    values = runtime.catalog.repositories.identifiers.primary_values_for_wemi(
        level=level,
        entity_id=entity_id,
    )
    return {
        "level": level,
        "entity_id": entity_id,
        "identifiers": dict(values),
        "count": len(values),
    }


def catalog_agents_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    role = payload.get("role")
    method = _callable(
        runtime.catalog.repositories.agents,
        "list_for_wemi",
        area="Catalog Agents",
    )
    rows = method(level=level, entity_id=entity_id)
    values = [plain(item) for item in rows]
    if role is not None and str(role).strip():
        role_token = _agent_role(role)
        matching: list[Any] = []
        for item in values:
            if not isinstance(item, Mapping):
                continue
            catalog_link = item.get("_catalog_link")
            if not isinstance(catalog_link, Mapping):
                continue
            if str(catalog_link.get("type") or "") == role_token:
                matching.append(item)
        values = matching
    return {
        "level": level,
        "entity_id": entity_id,
        "role": None if role is None else str(role),
        "agents": values,
        "count": len(values),
    }


def search_global(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    payload = _payload(query)
    text = _required_text(payload, "text")
    needle = text.casefold()
    requested = _text_list(payload, "tables")
    tables = requested or sorted(
        str(item) for item in runtime.read_source.get_tables(False)
    )
    offset = _optional_int(payload, "offset", default=0, minimum=0)
    limit = _optional_int(payload, "limit", default=100, minimum=0)
    assert offset is not None and limit is not None
    limit = min(limit, 1000)
    matches: list[dict[str, Any]] = []
    searched: list[str] = []
    for table in tables:
        try:
            rows = runtime.read_source.get_all_rows(
                table,
                iterator_return=False,
            )
        except Exception:
            continue
        searched.append(table)
        for row in rows:
            rendered = plain(row)
            if needle in _flatten_text(rendered):
                matches.append({"table": table, "row": rendered})
    total = len(matches)
    page = matches[offset : offset + limit]
    return {
        "text": text,
        "tables": searched,
        "items": page,
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(page) < total,
    }


def catalog_identifiers_replace(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    if "identifiers" not in payload:
        raise CoreDispatchError("`identifiers` is required.")
    method = _callable(
        runtime.catalog.repositories.identifiers,
        "replace_for_wemi",
        area="Catalog identifiers",
    )
    result = method(
        level=level,
        entity_id=entity_id,
        identifiers=payload["identifiers"],
    )
    return runtime.services.reconcile(
        {
            "level": level,
            "entity_id": entity_id,
            "identifiers": plain(result),
            "updated": True,
        }
    )


def catalog_agent_link(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    agent_id = _required_int(payload, "agent_id")
    level = _required_text(payload, "level").lower()
    entity_id = _required_int(payload, "entity_id")
    method = _callable(
        runtime.catalog.repositories.agents,
        "link_to_wemi",
        area="Catalog Agents",
    )
    kwargs: dict[str, Any] = {}
    kwargs["role"] = _agent_role(payload.get("role"))
    if payload.get("priority") is not None:
        kwargs["priority"] = _required_int(payload, "priority")
    result = method(
        agent_id=agent_id,
        level=level,
        entity_id=entity_id,
        **kwargs,
    )
    return runtime.services.reconcile(
        {
            "agent_id": agent_id,
            "level": level,
            "entity_id": entity_id,
            "link": plain(result),
            "linked": True,
        }
    )
