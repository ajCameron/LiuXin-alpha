"""
Declare capability discovery, completed-job result access, and byte-oriented job-log queries for Core.

Base lifecycle/job-management routes belong to other Core owners. This provider
adds three queries and no commands; installation does not wait for a job, read
a log, or probe whether advertised capability dependencies are operational.
"""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import SystemJobsHandlers


def install_queries(api: SystemJobsHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """
    Bind capability listing, job-result retrieval, and bounded log reading with explicit introspection metadata.

    Job results advertise an optional timeout; log reads advertise byte offset and
    maximum byte count despite returning decoded text. Field declarations do not
    check job existence, wait for completion, or validate bounds at registration.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_queries(Mock(), registrar)
        >>> [call.args[0] for call in registrar.register_query_handler.call_args_list]
        ['capabilities.list', 'jobs.result', 'jobs.log.read']


    :param api: Provider of capability listing, job result, and log-read handlers.
    :param runtime: Registrar receiving the three bindings in discovery/result/log order.
    :return: None after installation; errors propagate without undoing earlier bindings.
    """

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


def install_commands(
    api: SystemJobsHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    """
    Participate in the common provider installation interface without registering any commands.

    The command-registration attribute is still looked up, so an invalid registrar
    can raise even though no registration call is made. Job cancellation/retry and
    lifecycle commands are not owned by this family.

    Example:
        >>> from unittest.mock import Mock
        >>> registrar = Mock()
        >>> install_commands(object(), registrar)
        >>> registrar.register_command_handler.call_count
        0


    :param api: Unused system/job handler provider, retained for the common installer signature.
    :param runtime: Registrar whose command-registration attribute is retrieved but never called.
    :return: None without registering or executing a command.
    """

    command = runtime.register_command_handler
    del api, command
