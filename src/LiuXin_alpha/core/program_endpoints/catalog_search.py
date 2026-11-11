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
    Bind nine catalog/search queries with field, entity, role and pagination declarations.

    Field discovery and global row search are separate from WEMI relationship and
    identifier traversal. Declared type labels and required flags describe requests;
    they do not validate entity levels, relationship directions, or search bounds here.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_queries(Mock(), registrar)
        >>> registrar.register_query_handler.call_count
        9


    :param api: Provider of the catalog inspection and global-search query methods.
    :param runtime: Registrar accepting query handlers plus ordered field metadata, summaries, and grouping tags.
    :return: None after sequential registration; a later failure leaves earlier registrations to the registrar's policy.
    """

    query = runtime.register_query_handler

    query(
        "catalog.repair.receipt",
        api.catalog_repair_receipt,
        summary="Read a durable Catalog repair receipt.",
        payload_fields=(field("receipt_id", required=True, field_type="string"),),
        tags=("catalog", "repair", "read"),
        remote_capabilities=("read",),
    )

    query(
        "catalog.inherited.tags",
        api.catalog_inherited_tags,
        summary="Inspect direct and inherited tags with complete WEMI attribution.",
        payload_fields=(
            field("level", required=True, field_type="string"),
            field("entity_id", required=True, field_type="integer"),
            field("max_rows", field_type="integer"),
        ),
        tags=("catalog", "wemi", "read"),
        remote_capabilities=("read",),
    )

    query(
        "catalog.fields.list",
        api.catalog_fields_list,
        summary="List display/search field metadata.",
        payload_fields=(
            field("kind", field_type="string"),
            field("include_composites", field_type="boolean"),
        ),
        tags=("catalog", "fields", "read"),
        remote_capabilities=("read",),
    )

    query(
        "catalog.fields.get",
        api.catalog_fields_get,
        summary="Return metadata for one display/search field.",
        payload_fields=(field("key", required=True, field_type="string"),),
        tags=("catalog", "fields", "read"),
        remote_capabilities=("read",),
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
        remote_capabilities=("read",),
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
        remote_capabilities=("read",),
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
        remote_capabilities=("read",),
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
        remote_capabilities=("read",),
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
        remote_capabilities=("read",),
    )


def install_commands(
    api: CatalogSearchHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    """
    Bind identifier replacement, Agent linking, renaming and sort repair commands.

    The replacement route advertises an identifier array or object, while linking
    advertises an Agent ID, WEMI target, and optional role/priority. Validation and
    persistence occur only when handlers execute, not while these bindings are installed.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_commands(Mock(), registrar)
        >>> [call.args[0] for call in registrar.register_command_handler.call_args_list]
        ['catalog.repair.merge', 'catalog.repair.cleanup', 'catalog.identifiers.replace', 'catalog.agent.link', 'catalog.agent.rename', 'catalog.agent.sort.repair']


    :param api: Provider supplying identifier and Agent mutation handlers.
    :param runtime: Registrar receiving the six bindings in the documented order.
    :return: None after registration; errors propagate without adapter rollback.
    """

    command = runtime.register_command_handler

    for name, handler in (
        ("merge", api.catalog_repair_merge),
        ("cleanup", api.catalog_repair_cleanup),
    ):
        shared = (
            field("repository", required=True, field_type="string"),
            field("max_rows", field_type="integer"),
            field("dry_run", field_type="boolean"),
            field("expected_state", field_type="string"),
        )
        operation_fields = (
            (
                field("source_id", required=True, field_type="integer"),
                field("target_id", required=True, field_type="integer"),
                field("resolutions", field_type="object"),
                field("relationship_resolutions", field_type="object"),
            )
            if name == "merge"
            else (field("candidates", required=True, field_type="array"),)
        )
        command(
            "catalog.repair." + name,
            handler,
            summary="Preview or explicitly apply a Catalog repair with an audit receipt.",
            payload_fields=shared + operation_fields,
            tags=("catalog", "repair", "write"),
            remote_capabilities=("write",),
        )

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
        remote_capabilities=("write",),
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
        remote_capabilities=("write",),
    )

    command(
        "catalog.agent.rename",
        api.catalog_agent_rename,
        summary="Preview or atomically rename an Agent and select its sort value.",
        payload_fields=(
            field("agent_id", required=True, field_type="integer"),
            field("name", required=True, field_type="string"),
            field("sort_mode", field_type="string"),
            field("sort_name", field_type="string|null"),
            field("person_sort_method", field_type="string"),
            field("dry_run", field_type="boolean"),
        ),
        tags=("catalog", "agents", "write"),
        remote_capabilities=("write",),
    )
    command(
        "catalog.agent.sort.repair",
        api.catalog_agent_sort_repair,
        summary="Preview or atomically repair sort names for selected Agents.",
        payload_fields=(
            field("agent_ids", required=True, field_type="array"),
            field("sort_mode", field_type="string"),
            field("person_sort_method", field_type="string"),
            field("dry_run", field_type="boolean"),
        ),
        tags=("catalog", "agents", "write"),
        remote_capabilities=("write",),
    )
