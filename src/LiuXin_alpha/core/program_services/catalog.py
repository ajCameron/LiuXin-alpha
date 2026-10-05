"""
Adapt Core field discovery, WEMI adjacency, identifiers, Agent links, and cross-table search to Catalog services.

Repositories retain level validation and semantic mutation policy. Write receipts
are projected and reconciled after repository calls without an adapter transaction.
Global search is a full materialized scan with suppressed initial table-read errors,
not a completeness-guaranteed indexed search or an authorization boundary.
"""

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
    """
    Enumerate one field-metadata family and project each key's metadata in provider order.

    Kind is stripped/lowercased, with falsey input selecting all. include_composites
    is truth-tested but used only for custom fields. Keys are not deduplicated or
    sorted by this adapter, and no per-key lookup failures are suppressed.

    Example:
        >>> fields = catalog_fields_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime exposing the shared field_metadata service.
    :param query: Query with kind (all, sortable, displayable, standard, custom, searchable) and optional include_composites=True.
    :return: Normalized kind, ordered key/projected-metadata records, and their count.
    :raises CoreDispatchError: If the normalized kind is not supported.
    """
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
    """
    Look up one required field key, distinguishing None from falsey-but-present metadata.

    Example:
        >>> field = catalog_fields_get(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime supplying field_metadata.get.
    :param query: Query containing a required stripped key.
    :return: Key, exists based solely on non-None metadata, and its plain projection.
    """
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
    """
    Retrieve parent or child WEMI adjacency and project the returned entities without paging.

    Level and direction are lowercased; falsey direction selects children. Any
    ValueError from the chosen operation is wrapped as unavailable adjacency,
    including an internal ValueError unrelated to level selection. Other failures
    propagate. Returned labels come from the adjacency object, not the raw request.

    Example:
        >>> adjacency = catalog_hierarchy_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose Catalog supplies hierarchy.parents and hierarchy.children.
    :param query: Query with level, entity_id, and optional children/parents direction.
    :return: Adjacency labels, related result_level, and projected entities in retriever order.
    :raises CoreDispatchError: For invalid request fields/direction or a ValueError during adjacency retrieval.
    """
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
    """
    List identifiers for an Agent or WEMI entity through the corresponding repository capability.

    Lowercased agent selects list_for_agent; every other level is passed to
    list_for_wemi for repository validation. No adapter sorting, paging, or primary
    selection is applied.

    Example:
        >>> identifiers = catalog_identifiers_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime exposing Catalog identifier repository capabilities.
    :param query: Query containing level and required integer-convertible entity_id.
    :return: Normalized requested level/ID, projected identifier list, and list count.
    """
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
    """
    Retrieve repository-selected primary identifier values for one WEMI entity.

    This path has no Agent-specific branch or capability wrapper. Identifier
    values are shallow-copied with dict, not recursively plain-projected here.

    Example:
        >>> identifiers = catalog_identifiers_primary_values(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose identifier repository implements primary_values_for_wemi.
    :param query: Query containing required level text and integer-convertible entity_id.
    :return: Lowercased level/ID, identifier dictionary, and length of the original result.
    """
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
    """
    List WEMI-linked Agents and optionally retain only projected links matching a normalized role.

    Filtering requires a non-None, nonblank stringified role; otherwise all projected
    rows remain. Filtered entries must be Mappings with a Mapping _catalog_link
    whose type exactly equals the normalized role code. Missing link metadata is
    excluded, not inferred from Agent attributes. The response echoes raw role text.

    Example:
        >>> agents = catalog_agents_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime providing the Agents repository's list_for_wemi capability.
    :param query: Query with level, entity_id, and optional role name/code.
    :return: Normalized level/ID, original role text or None, filtered projected Agents, and count.
    """
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
    """
    Scan selected tables for a casefolded text substring, then page all accumulated matches.

    A normalized nonempty tables list preserves requested order; otherwise all
    advertised names are sorted. Initial get_all_rows errors skip that table, but
    later iteration/projection errors propagate. Search uses flattened projected
    values, not keys, and the entire text as one substring rather than split terms.
    Zero limit still scans/counts. Limit defaults to 100 and is capped at 1000;
    bounds are nonnegative, with explicit None reaching assertions.

    Example:
        >>> results = search_global(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose selected read_source enumerates tables and rows.
    :param query: Query with required text and optional tables, offset, and limit.
    :return: Text, successfully opened table names, page items, observed total, bounds, and has_more; no completeness guarantee for skipped tables.
    """
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
    """
    Replace WEMI identifiers using the repository's own input policy, then project and reconcile.

    The identifiers key must exist, but its value is passed unchanged, including
    None. updated=True records a returned repository call rather than a readback
    comparison; projection/reconciliation can fail after the mutation.

    Example:
        >>> receipt = catalog_identifiers_replace(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing replace_for_wemi and read-side reconciliation.
    :param command: Command with level, entity_id, and required identifiers value.
    :return: Reconciled lowercased level/ID, projected repository result, and updated=True.
    :raises CoreDispatchError: If the required payload/capability is unavailable.
    """
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
    """
    Link an Agent to a WEMI entity using normalized role and optional priority, then reconcile.

    Falsey roles select author/aut; known role names map to relator codes. Priority
    is omitted for missing/None values, otherwise uses required integer conversion
    without a range check here. Repository policy owns level, role, and link validity.
    linked=True follows returned delegation, before any independent link readback.

    Example:
        >>> receipt = catalog_agent_link(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying Agents.link_to_wemi and service reconciliation.
    :param command: Command with agent_id, level, entity_id, and optional role/priority.
    :return: Reconciled Agent/entity IDs, normalized level, projected link result, and linked=True.
    """
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
