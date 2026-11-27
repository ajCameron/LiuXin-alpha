"""Adapt owner-oriented Agent credits and Work memberships without identity guessing."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface

from .common import (
    Handler,
    Payload,
    entities,
    expected,
    identities,
    identity,
    level,
    route,
)


def list_credits(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Read all credits and their explicit roles in stored priority order."""
    return {"credits": entities(api, level(payload)).list_credits(identity(payload))}


def add_credit(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Upsert one existing Agent/role, preserving omitted priority."""
    return entities(api, level(payload)).add_credit(
        identity(payload),
        agent_id=identity(payload, "agent_id"),
        role=payload["role"],
        priority=payload.get("priority"),
        expected_revision=expected(payload),
    )


def remove_credit(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Remove one credit while retaining the Agent and its other roles."""
    return entities(api, level(payload)).remove_credit(
        identity(payload),
        agent_id=identity(payload, "agent_id"),
        role=payload["role"],
        expected_revision=expected(payload),
    )


def set_credits(api: MetadataAPIInterface, payload: Payload) -> Any:
    """Replace one role in caller order; an empty list explicitly clears it."""
    return entities(api, level(payload)).set_credits(
        identity(payload),
        agent_ids=identities(payload, "agent_ids"),
        role=payload["role"],
        expected_revision=expected(payload),
    )


def membership_handler(family: str, action: str) -> Handler:
    """Bind an explicit Work membership method while constructing declarations."""

    def execute(api: MetadataAPIInterface, payload: Payload) -> Any:
        work_id = identity(payload, "work_id")
        if action == "list":
            method = {"tags": api.works.list_tags, "series": api.works.list_series}[
                family
            ]
            return {"memberships": method(work_id)}
        token = expected(payload)
        if family == "tags" and action == "add":
            from LiuXin_alpha.core.payloads import invalid_payload

            if ("tag_id" in payload) == ("text" in payload):
                invalid_payload(
                    "payload", "requires exactly one tag_id or literal text."
                )
            if "text" in payload:
                return api.works.add_tag(
                    work_id, text=payload["text"], expected_revision=token
                )
            return api.works.add_tag(
                work_id, tag_id=identity(payload, "tag_id"), expected_revision=token
            )
        methods: dict[tuple[str, str], Any] = {
            ("tags", "remove"): api.works.remove_tag,
            ("tags", "set"): api.works.set_tags,
            ("series", "add"): api.works.add_series,
            ("series", "remove"): api.works.remove_series,
            ("series", "set"): api.works.set_series,
        }
        value = (
            identities(payload, "entity_ids")
            if action == "set"
            else identity(payload, "entity_id")
        )
        return methods[family, action](work_id, value, expected_revision=token)

    return execute


OWNER = ("level:string", "entity_id!:integer")
REVISION = ("expected_revision:object|null",)
ROUTES = (
    route(
        "metadata.credits.list",
        False,
        "Read native Agent credits and roles.",
        list_credits,
        *OWNER,
    ),
    route(
        "metadata.credits.add",
        True,
        "Add one explicit Agent credit.",
        add_credit,
        *OWNER,
        "agent_id!:integer",
        "role!:string",
        "priority:integer|null",
        *REVISION,
    ),
    route(
        "metadata.credits.remove",
        True,
        "Remove one Agent role without deleting its identity.",
        remove_credit,
        *OWNER,
        "agent_id!:integer",
        "role!:string",
        *REVISION,
    ),
    route(
        "metadata.credits.set",
        True,
        "Replace one role with ordered Agent identities.",
        set_credits,
        *OWNER,
        "agent_ids!:array",
        "role!:string",
        *REVISION,
    ),
    *(
        route(
            f"metadata.memberships.{family}.list",
            False,
            f"List Work {family} memberships.",
            membership_handler(family, "list"),
            "work_id!:integer",
        )
        for family in ("tags", "series")
    ),
    route(
        "metadata.memberships.tags.add",
        True,
        "Attach a Tag by identity or literal text.",
        membership_handler("tags", "add"),
        "work_id!:integer",
        "tag_id:integer",
        "text:string",
        *REVISION,
    ),
    route(
        "metadata.memberships.series.add",
        True,
        "Attach an existing Series to a Work.",
        membership_handler("series", "add"),
        "work_id!:integer",
        "entity_id!:integer",
        *REVISION,
    ),
    *(
        route(
            f"metadata.memberships.{family}.remove",
            True,
            f"Detach one {family} membership pair.",
            membership_handler(family, "remove"),
            "work_id!:integer",
            "entity_id!:integer",
            *REVISION,
        )
        for family in ("tags", "series")
    ),
    *(
        route(
            f"metadata.memberships.{family}.set",
            True,
            f"Replace a Work's {family} membership set.",
            membership_handler(family, "set"),
            "work_id!:integer",
            "entity_ids!:array",
            *REVISION,
        )
        for family in ("tags", "series")
    ),
)
