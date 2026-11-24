"""Expose explicit shared-record CRUD, literal suggestions and contextual attachments."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, cast

from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
from LiuXin_alpha.catalog.api.metadata_shared import AgentInput
from LiuXin_alpha.core.payloads import invalid_payload

from .common import (
    SUGGEST_KINDS,
    TREE_KINDS,
    Payload,
    choice,
    expected,
    identities,
    identity,
    level,
    mapping,
    optional_identity,
    page,
    route,
    shared,
)


def read_many(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Read shared identities in caller order, preserving duplicates and misses."""
    return {
        "records": shared(api, payload["kind"]).get_many(
            identities(payload, allow_zero=True), missing=payload.get("missing", "none")
        )
    }


def list_records(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return one bounded ID-ordered shared-record page."""
    return {"records": shared(api, payload["kind"]).list(**page(payload))}


def lookup(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return every exact candidate in the requested page without choosing identity."""
    result = shared(api, payload["kind"]).lookup(
        mapping(payload["fields"], "fields"), **page(payload)
    )
    return {**asdict(result), "has_more": result.has_more}


def suggest(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Suggest literal text from declared searchable shared vocabularies."""
    kind = choice(payload["kind"], "kind", SUGGEST_KINDS)
    result = shared(api, kind).suggest(
        payload["text"],
        mode=choice(
            payload.get("mode", "prefix"), "mode", ("exact", "prefix", "contains")
        ),
        filters=None
        if payload.get("filters") is None
        else mapping(payload["filters"], "filters"),
        **page(payload, 20),
    )
    return {**asdict(result), "has_more": result.has_more}


def read_for_edit(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Return a full shared row and its record-scoped revision."""
    snapshot = shared(api, payload["kind"], mutable=True).read_for_edit(
        identity(payload)
    )
    return {
        "metadata": snapshot.value,
        "fields": dict(snapshot.fields),
        "revision": snapshot.revision,
    }


def create(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create a fresh shared record using Catalog public aliases."""
    return shared(api, payload["kind"], mutable=True).create(
        mapping(payload["data"], "data")
    )


def update(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Edit supplied shared aliases with an optional full-row precondition."""
    return shared(api, payload["kind"], mutable=True).update(
        identity(payload),
        mapping(payload["data"], "data"),
        expected_revision=expected(payload, shared_record=True),
    )


def delete(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Respect leaf/reference constraints and require explicit hierarchical detachment."""
    options = {"expected_revision": expected(payload, shared_record=True)}
    if payload["kind"] in TREE_KINDS:
        options["detach"] = payload.get("detach", False)
    elif "detach" in payload:
        invalid_payload("detach", "is supported only for hierarchical vocabularies.")
    return shared(api, payload["kind"], mutable=True).delete(
        identity(payload), **options
    )


def ensure(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Reuse exact identities without guessing Agents or overwriting attributes."""
    kind = choice(
        payload["kind"],
        "kind",
        ("tags", "labels", "genres", "subjects", "series", "ratings"),
    )
    if kind == "ratings":
        if set(payload) != {"kind", "value", "out_of", "source"}:
            invalid_payload(
                "payload", "rating identity requires only value, out_of and source."
            )
        return api.ensure_rating(
            payload["value"], out_of=payload["out_of"], source=payload["source"]
        )
    if not isinstance(payload.get("text"), str):
        invalid_payload("text", "is required for text vocabularies.")
    if set(payload) - {"kind", "text", "parent_id"}:
        invalid_payload("payload", "contains rating controls for a text vocabulary.")
    if kind in ("tags", "labels"):
        if "parent_id" in payload:
            invalid_payload(
                "parent_id", "is supported only for hierarchical vocabularies."
            )
        return {"tags": api.ensure_tag, "labels": api.ensure_label}[kind](
            payload["text"]
        )
    return {
        "genres": api.ensure_genre,
        "subjects": api.ensure_subject,
        "series": api.ensure_series,
    }[kind](payload["text"], parent_id=optional_identity(payload, "parent_id"))


def create_person(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create the Agent/person aggregate atomically."""
    return api.agents.create_person(cast(AgentInput, mapping(payload["data"], "data")))


def create_organisation(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create the Agent/organisation aggregate atomically."""
    return api.agents.create_organisation(
        cast(AgentInput, mapping(payload["data"], "data"))
    )


def rename_agent(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Preserve or explicitly derive sorting when renaming an Agent."""
    return api.agents.rename(
        identity(payload),
        payload["name"],
        derive_sort=payload.get("derive_sort", False),
        expected_revision=expected(payload, shared_record=True),
    )


def add_text(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Create fresh Note/Comment/Synopsis text and attach it to the WEMI owner."""
    kind = choice(payload["kind"], "kind", ("notes", "comments", "synopses"))
    return shared(api, kind).add_for_wemi(
        level=level(payload), entity_id=identity(payload), text=payload["text"]
    )


def owned_records(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Read contextual records from an explicitly selected owner family."""
    kind = choice(
        payload["kind"],
        "kind",
        (
            "notes",
            "comments",
            "synopses",
            "identifiers",
            "item_identifiers",
            "annotations",
        ),
    )
    owner = payload.get("level", "work")
    entity_id = identity(payload)
    service = shared(api, kind)
    if kind in ("annotations", "item_identifiers"):
        if owner != "item":
            invalid_payload("level", "must be item for this record family.")
        options = (
            {}
            if kind == "item_identifiers"
            else {
                "user_id": optional_identity(payload, "user_id"),
                "kind": payload.get("annotation_kind"),
            }
        )
        values = service.list_for_item(entity_id, **options)
    elif kind == "identifiers" and owner == "agent":
        values = service.list_for_agent(entity_id)
    else:
        values = service.list_for_wemi(level=level(payload), entity_id=entity_id)
    if kind != "annotations" and ("user_id" in payload or "annotation_kind" in payload):
        invalid_payload("payload", "annotation filters require kind annotations.")
    window = page(payload)
    return {
        "records": values[window["offset"] : window["offset"] + window["limit"]],
        "total_count": len(values),
        **window,
    }


def link_identifier(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Assign or copy a curated identifier and return the actual assigned identity."""
    identifier_id = identity(payload, "identifier_id")
    priority = payload.get("priority")
    if payload.get("level") == "agent":
        return api.identifiers.link_to_agent(
            agent_id=identity(payload), identifier_id=identifier_id, priority=priority
        )
    return api.identifiers.link_to_wemi(
        level=level(payload),
        entity_id=identity(payload),
        identifier_id=identifier_id,
        priority=priority,
    )


KIND = ("kind!:string",)
ENTITY = (*KIND, "entity_id!:integer")
PAGE = ("limit:integer", "offset:integer")
REVISION = ("expected_revision:object|null",)
ROUTES = (
    route(
        "metadata.shared.read_many",
        False,
        "Read ordered shared metadata identities.",
        read_many,
        *KIND,
        "entity_ids!:array",
        "missing:string",
    ),
    route(
        "metadata.shared.list",
        False,
        "List shared records in identity order.",
        list_records,
        *KIND,
        *PAGE,
    ),
    route(
        "metadata.shared.lookup",
        False,
        "Find exact shared-record candidates.",
        lookup,
        *KIND,
        "fields!:object",
        *PAGE,
    ),
    route(
        "metadata.shared.suggest",
        False,
        "Suggest literal shared text without choosing identity.",
        suggest,
        *KIND,
        "text!:string",
        "mode:string",
        "filters:object|null",
        *PAGE,
    ),
    route(
        "metadata.shared.read_for_edit",
        False,
        "Read authoritative shared metadata and its revision.",
        read_for_edit,
        *ENTITY,
    ),
    route(
        "metadata.shared.create",
        True,
        "Create a shared metadata record.",
        create,
        *KIND,
        "data!:object",
    ),
    route(
        "metadata.shared.update",
        True,
        "Edit a shared metadata record.",
        update,
        *ENTITY,
        "data!:object",
        *REVISION,
    ),
    route(
        "metadata.shared.delete",
        True,
        "Delete a shared record under its reference policy.",
        delete,
        *ENTITY,
        "detach:boolean",
        *REVISION,
    ),
    route(
        "metadata.shared.ensure",
        True,
        "Reuse or create an exact vocabulary identity.",
        ensure,
        *KIND,
        "text:string",
        "parent_id:integer|null",
        "value:number",
        "out_of:number",
        "source:string",
    ),
    route(
        "metadata.agents.create_person",
        True,
        "Create an Agent and person subtype.",
        create_person,
        "data!:object",
    ),
    route(
        "metadata.agents.create_organisation",
        True,
        "Create an Agent and organisation subtype.",
        create_organisation,
        "data!:object",
    ),
    route(
        "metadata.agents.rename",
        True,
        "Rename an Agent with explicit sort-name policy.",
        rename_agent,
        "entity_id!:integer",
        "name!:string",
        "derive_sort:boolean",
        *REVISION,
    ),
    route(
        "metadata.text.add",
        True,
        "Create and attach fresh native text.",
        add_text,
        *ENTITY,
        "level:string",
        "text!:string",
    ),
    route(
        "metadata.shared.for_owner",
        False,
        "Read shared records attached to an explicit owner.",
        owned_records,
        *ENTITY,
        "level:string",
        "user_id:integer|null",
        "annotation_kind:string|null",
        *PAGE,
    ),
    route(
        "metadata.identifiers.link",
        True,
        "Assign or copy a curated identifier to its owner.",
        link_identifier,
        "identifier_id!:integer",
        "level:string",
        "entity_id!:integer",
        "priority:integer|null",
    ),
)
