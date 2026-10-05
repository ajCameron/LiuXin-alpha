"""
Declare Core routes for catalog field discovery, WEMI relationships, identifiers, Agent links, and global search.

The provider binds handlers and supplies introspection metadata only. Repository
semantics, query costs, payload checks, and write reconciliation belong to the
selected handlers; installation neither reads catalog rows nor mutates links.
"""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import CatalogSearchHandlers


def install_queries(
    api: CatalogSearchHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    """
    Bind seven catalog/search queries with their field-selection, entity, role, and pagination declarations.

    Field discovery and global row search are separate from WEMI relationship and
    identifier traversal. Declared type labels and required flags describe requests;
    they do not validate entity levels, relationship directions, or search bounds here.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_queries(Mock(), registrar)
        >>> registrar.register_query_handler.call_count
        7


    :param api: Provider of the catalog inspection and global-search query methods.
    :param runtime: Registrar accepting query handlers plus ordered field metadata, summaries, and grouping tags.
    :return: None after sequential registration; a later failure leaves earlier registrations to the registrar's policy.
    """

    query = runtime.register_query_handler

    query(
        "catalog.fields.list",
        api.catalog_fields_list,
        summary="List display/search field metadata.",
        payload_fields=(
            field("kind", field_type="string"),
            field("include_composites", field_type="boolean"),
        ),
        tags=("catalog", "fields", "read"),
    )

    query(
        "catalog.fields.get",
        api.catalog_fields_get,
        summary="Return metadata for one display/search field.",
        payload_fields=(field("key", required=True, field_type="string"),),
        tags=("catalog", "fields", "read"),
    )

    query(
        "catalog.hierarchy.list",
        api.catalog_hierarchy_list,
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
        api.catalog_identifiers_list,
        summary="List identifiers linked to WEMI or Agent entities.",
        payload_fields=(
            field("level", required=True, field_type="string"),
            field("entity_id", required=True, field_type="integer"),
        ),
        tags=("catalog", "identifiers", "read"),
    )

    query(
        "catalog.identifiers.primary-values",
        api.catalog_identifiers_primary_values,
        summary="Project primary WEMI identifiers by normalized scheme.",
        payload_fields=(
            field("level", required=True, field_type="string"),
            field("entity_id", required=True, field_type="integer"),
        ),
        tags=("catalog", "identifiers", "read"),
    )

    query(
        "catalog.agents.list",
        api.catalog_agents_list,
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
        api.search_global,
        summary="Search transport-safe rows across selected tables.",
        payload_fields=(
            field("text", required=True, field_type="string"),
            field("tables", field_type="array"),
            field("limit", field_type="integer"),
            field("offset", field_type="integer"),
        ),
        tags=("search", "rows", "read"),
    )


def install_commands(
    api: CatalogSearchHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    """
    Bind identifier replacement and existing-Agent linking as separate catalog mutation commands.

    The replacement route advertises an identifier array or object, while linking
    advertises an Agent ID, WEMI target, and optional role/priority. Validation and
    persistence occur only when handlers execute, not while these bindings are installed.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_commands(Mock(), registrar)
        >>> [call.args[0] for call in registrar.register_command_handler.call_args_list]
        ['catalog.identifiers.replace', 'catalog.agent.link']


    :param api: Provider supplying the identifier-replacement and Agent-link command handlers.
    :param runtime: Registrar receiving the two bindings in replacement-then-link order.
    :return: None after both registrations succeed; lookup or registration errors propagate without adapter rollback.
    """

    command = runtime.register_command_handler

    command(
        "catalog.identifiers.replace",
        api.catalog_identifiers_replace,
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
        api.catalog_agent_link,
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
