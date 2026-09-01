"""Core endpoint declarations for system jobs operations."""

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
                "capabilities.list",
                handlers.capabilities_list,
                summary="Describe whole-program Core capability families.",
                tags=("api", "capabilities"),
            )

    query(
                "jobs.result",
                handlers.jobs_result,
                summary="Return the completed execution payload for one job.",
                payload_fields=(
                    field("job_id", required=True, field_type="string"),
                    field("timeout_s", field_type="number|null"),
                ),
                tags=("jobs", "read"),
            )

    query(
                "jobs.log.read",
                handlers.jobs_log_read,
                summary="Read a bounded UTF-8 chunk from a managed job log.",
                payload_fields=(
                    field("job_id", required=True, field_type="string"),
                    field("offset", field_type="integer"),
                    field("max_bytes", field_type="integer"),
                ),
                tags=("jobs", "logs", "read"),
            )

def install_commands(api: object, runtime: object) -> None:
    """Register this family's command endpoints."""

    handlers = cast(ProgramEndpointHandlers, api)
    registrar = cast(ProgramEndpointRegistrar, runtime)
    command = registrar.register_command_handler
    del handlers, command
