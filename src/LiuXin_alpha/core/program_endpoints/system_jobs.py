"""Core endpoint declarations for system jobs operations."""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import SystemJobsHandlers


def install_queries(api: SystemJobsHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's query endpoints."""

    query = runtime.register_query_handler

    query(
                "capabilities.list",
                api.capabilities_list,
                summary="Describe whole-program Core capability families.",
                tags=("api", "capabilities"),
            )

    query(
                "jobs.result",
                api.jobs_result,
                summary="Return the completed execution payload for one job.",
                payload_fields=(
                    field("job_id", required=True, field_type="string"),
                    field("timeout_s", field_type="number|null"),
                ),
                tags=("jobs", "read"),
            )

    query(
                "jobs.log.read",
                api.jobs_log_read,
                summary="Read a bounded UTF-8 chunk from a managed job log.",
                payload_fields=(
                    field("job_id", required=True, field_type="string"),
                    field("offset", field_type="integer"),
                    field("max_bytes", field_type="integer"),
                ),
                tags=("jobs", "logs", "read"),
            )

def install_commands(api: SystemJobsHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's command endpoints."""

    command = runtime.register_command_handler
    del api, command
