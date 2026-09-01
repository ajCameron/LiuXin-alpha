"""Core endpoint declarations for catalog search operations."""

from __future__ import annotations

from typing import cast

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointHandlers,
    ProgramEndpointRegistrar,
    field,
)


def install_queries(api: object, runtime: object) -> None:
    """Register this family's query endpoints."""

    handlers = cast(ProgramEndpointHandlers, api)
    registrar = cast(ProgramEndpointRegistrar, runtime)
    query = registrar.register_query_handler

    query(
                "catalog.fields.list",
                handlers.catalog_fields_list,
                summary="List display/search field metadata.",
                payload_fields=(
                    field("kind", field_type="string"),
                    field("include_composites", field_type="boolean"),
                ),
                tags=("catalog", "fields", "read"),
            )

    query(
                "catalog.fields.get",
                handlers.catalog_fields_get,
                summary="Return metadata for one display/search field.",
                payload_fields=(field("key", required=True, field_type="string"),),
                tags=("catalog", "fields", "read"),
            )

    query(
                "catalog.hierarchy.list",
                handlers.catalog_hierarchy_list,
                summary="List the adjacent parent or child entities in a WEMI path.",
                payload_fields=(
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                    field("direction", field_type="string"),
                ),
                tags=("catalog", "wemi", "read"),
            )

    query(
                "catalog.identifiers.list",
                handlers.catalog_identifiers_list,
                summary="List identifiers linked to WEMI or Agent entities.",
                payload_fields=(
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                ),
                tags=("catalog", "identifiers", "read"),
            )

    query(
                "catalog.identifiers.primary-values",
                handlers.catalog_identifiers_primary_values,
                summary="Project primary WEMI identifiers by normalized scheme.",
                payload_fields=(
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                ),
                tags=("catalog", "identifiers", "read"),
            )

    query(
                "catalog.agents.list",
                handlers.catalog_agents_list,
                summary="List Agents linked to a WEMI entity.",
                payload_fields=(
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                    field("role", field_type="string|null"),
                ),
                tags=("catalog", "agents", "read"),
            )

    query(
                "search.global",
                handlers.search_global,
                summary="Search transport-safe rows across selected tables.",
                payload_fields=(
                    field("text", required=True, field_type="string"),
                    field("tables", field_type="array"),
                    field("limit", field_type="integer"),
                    field("offset", field_type="integer"),
                ),
                tags=("search", "rows", "read"),
            )

def install_commands(api: object, runtime: object) -> None:
    """Register this family's command endpoints."""

    handlers = cast(ProgramEndpointHandlers, api)
    registrar = cast(ProgramEndpointRegistrar, runtime)
    command = registrar.register_command_handler

    command(
                "catalog.identifiers.replace",
                handlers.catalog_identifiers_replace,
                summary="Replace identifiers linked to one WEMI entity.",
                payload_fields=(
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                    field("identifiers", required=True, field_type="array|object"),
                ),
                tags=("catalog", "identifiers", "write"),
            )

    command(
                "catalog.agent.link",
                handlers.catalog_agent_link,
                summary="Link an existing Agent to a WEMI entity.",
                payload_fields=(
                    field("agent_id", required=True, field_type="integer"),
                    field("level", required=True, field_type="string"),
                    field("entity_id", required=True, field_type="integer"),
                    field("role", field_type="string|null"),
                    field("priority", field_type="integer|null"),
                ),
                tags=("catalog", "agents", "write"),
            )
