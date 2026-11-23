"""
Describe the declared Core operation families and expose completed-job results and byte-oriented log slices.

Capability discovery snapshots all registered routes and passive support
declarations; it does not probe driver/plugin/network readiness. Job result and log access delegate scheduling
and path ownership to the job manager; they do not establish a new authorization
boundary or a consistent snapshot of a running job's output.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.payloads import number_value
from LiuXin_alpha.core.program_services.payloads import (
    _optional_int,
    _payload,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def capabilities_list(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Describe all actual registrations without claiming operational readiness.

    Permission requirements remain per route, including command/query namesakes.
    Passive checks can declare an implementation unavailable; tool, backend and
    network prerequisites are declarations, not probes or caller grants.

    :param runtime: Runtime owning the captured command and query registrations.
    :param query: Discovery request; no payload options are interpreted.
    :return: Complete registration inventory with declared support, prerequisites and authority per route.
    """
    del query
    operations: dict[str, dict[str, Any]] = {}
    families: dict[str, dict[str, Any]] = {}
    for row in runtime.describe_endpoint_capabilities():
        name = row["name"]
        family = row["tags"][0] if row["tags"] else name.split(".", 1)[0]
        reason = row["unavailable_reason"]
        availability = (
            "unavailable"
            if reason is not None
            else ("conditional" if row["requirements"] else "available")
        )
        route = {
            "kind": row["kind"],
            "implementation": "unavailable" if reason is not None else "implemented",
            "availability": availability,
            "reason": reason,
            "requirements": row["requirements"],
            "readiness": "not_probed",
            "remote_capabilities": row["remote_capabilities"],
            "call_modes": ["direct"]
            + (["rpc"] if row["remote_capabilities"] is not None else []),
            "transport_stable": row["transport_stable"],
        }
        detail = operations.setdefault(name, {"registered": True, "routes": []})
        detail["routes"].append(route)
        family_detail = families.setdefault(family, {"operations": []})
        if name not in family_detail["operations"]:
            family_detail["operations"].append(name)
    for detail in operations.values():
        routes = detail["routes"]
        states = {route["availability"] for route in routes}
        detail["availability"] = next(iter(states)) if len(states) == 1 else "mixed"
        detail["reason"] = next(
            (route["reason"] for route in routes if route["reason"]), None
        )
        detail["call_modes"] = [
            mode
            for mode in ("direct", "rpc")
            if any(mode in route["call_modes"] for route in routes)
        ]
    return {
        "api_version": runtime.api_version,
        "inventory_complete": True,
        "readiness_probed": False,
        "availability_semantics": "Availability describes declared implementation support and prerequisites; readiness and caller authorization are separate.",
        "families": dict(sorted(families.items())),
        "operations": operations,
    }


def jobs_result(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """
    Obtain a job execution result using the manager's timeout semantics without re-raising ordinary job failure.

    timeout_s is None when absent/None, otherwise a finite number. The manager
    owns timeout range semantics. raise_on_failure=False lets the execution report carry
    failure information. KeyError becomes an unknown-job dispatch error; timeout,
    manager, and projection failures otherwise propagate.

    Example:
        >>> result = jobs_result(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose job manager resolves the execution result.
    :param query: Query with required job_id text and optional timeout_s.
    :return: Stripped job_id and plain-projected execution value, including failure reports.
    :raises CoreDispatchError: If job_id is invalid or manager.result raises KeyError.
    """
    payload = _payload(query)
    job_id = _required_text(payload, "job_id")
    timeout_raw = payload.get("timeout_s")
    timeout = None if timeout_raw is None else number_value(timeout_raw, "timeout_s")
    try:
        execution = runtime.job_manager.result(
            job_id,
            timeout=timeout,
            raise_on_failure=False,
        )
    except KeyError as exc:
        raise CoreDispatchError(f"Unknown job id: {job_id!r}") from exc
    return {
        "job_id": job_id,
        "execution": plain(execution),
    }


def jobs_log_read(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """
    Read a byte-bounded slice from the job's trusted log path and decode it with UTF-8 replacement errors.

    Offset is a nonnegative byte position, not a character count. max_bytes defaults
    to 64 KiB, must be positive, and is capped at 1 MiB; explicit None bounds reach
    assertions. A one-byte lookahead determines eof at read time, not job completion.
    Slicing through a multibyte character can introduce replacement characters.
    Missing log_path yields available=False; a present unreadable path raises.
    The adapter does not confine the manager-provided path to a log directory.

    Example:
        >>> page = jobs_log_read(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose job manager supplies the job and its optional log_path.
    :param query: Query with job_id, optional offset=0, and max_bytes=65536.
    :return: Decoded text, requested/current next byte offsets, read-time eof, and path-availability flag.
    :raises CoreDispatchError: For invalid request bounds/ID or KeyError from job lookup.
    """
    payload = _payload(query)
    job_id = _required_text(payload, "job_id")
    offset = _optional_int(payload, "offset", default=0, minimum=0)
    assert offset is not None
    max_bytes = _optional_int(
        payload,
        "max_bytes",
        default=64 * 1024,
        minimum=1,
    )
    assert max_bytes is not None
    max_bytes = min(max_bytes, 1024 * 1024)
    try:
        job = runtime.job_manager.get(job_id)
    except KeyError as exc:
        raise CoreDispatchError(f"Unknown job id: {job_id!r}") from exc
    log_path = getattr(job, "log_path", None)
    if not log_path:
        return {
            "job_id": job_id,
            "text": "",
            "offset": offset,
            "next_offset": offset,
            "eof": True,
            "available": False,
        }
    with open(str(log_path), "rb") as stream:
        stream.seek(offset)
        content = stream.read(max_bytes)
        next_offset = stream.tell()
        extra = stream.read(1)
    return {
        "job_id": job_id,
        "text": content.decode("utf-8", errors="replace"),
        "offset": offset,
        "next_offset": next_offset,
        "eof": not bool(extra),
        "available": True,
    }


def jobs_events(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Read durable progress/log events with a stable, bounded event-ID cursor.

    :param runtime: Runtime supplying the library-bound durable job service.
    :param query: Query with job_id, optional after_event_id and a limit capped at 1,000.
    :return: Ordered events, next cursor and whether more events are available.
    :raises CoreDispatchError: If the cursor/bounds are invalid or the job is unknown.
    """
    payload = _payload(query)
    job_id = _required_text(payload, "job_id")
    cursor = _optional_int(payload, "after_event_id", default=0, minimum=0)
    limit = _optional_int(payload, "limit", default=100, minimum=1)
    if cursor is None or limit is None:
        raise CoreDispatchError("Event cursor and limit cannot be null")
    try:
        return runtime.durable_jobs.events(
            job_id, after_event_id=cursor, limit=min(limit, 1000)
        )
    except KeyError as error:
        raise CoreDispatchError(f"Unknown durable job id: {job_id!r}") from error
