"""
Describe the declared Core operation families and expose completed-job results and byte-oriented log slices.

Capability discovery measures registered names against a fixed manifest, not
driver/plugin/network readiness. Job result and log access delegate scheduling
and path ownership to the job manager; they do not establish a new authorization
boundary or a consistent snapshot of a running job's output.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _optional_int,
    _payload,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


_PROGRAM_AREAS: dict[str, tuple[str, ...]] = {
    "lifecycle": ("health", "api.describe", "capabilities.list", "shutdown"),
    "jobs": (
        "jobs.list",
        "jobs.get",
        "jobs.wait",
        "jobs.result",
        "jobs.log.read",
        "jobs.cancel",
        "jobs.retry",
    ),
    "database": (
        "database.info",
        "database.summary",
        "database.telemetry",
        "database.backup",
        "database.vacuum",
        "database.migrations.status",
        "database.migrations.plan",
        "database.migrations.apply",
    ),
    "schema": (
        "schema.tables",
        "schema.table",
        "schema.column",
        "schema.link",
        "schema.column.update",
        "schema.identities.list",
        "schema.identity.get",
        "schema.identity.derive",
        "schema.identity.resolve",
        "schema.identities.audit",
        "schema.identities.migrate",
    ),
    "tree": (
        "tree.root",
        "tree.children",
        "tree.lineage",
        "tree.walk",
        "tree.search",
        "tree.nest",
        "tree.delete",
    ),
    "preferences": (
        "preferences.list",
        "preferences.get",
        "preferences.set",
        "preferences.delete",
    ),
    "custom_fields": (
        "custom-fields.list",
        "custom-fields.create",
        "custom-fields.update",
        "custom-fields.delete",
    ),
    "catalog": (
        "catalog.entity.get",
        "catalog.entity.list",
        "catalog.entity.create",
        "catalog.entity.update",
        "catalog.entity.delete",
        "catalog.graph.get",
        "catalog.hierarchy.list",
        "catalog.annotations.list",
        "catalog.identifiers.list",
        "catalog.identifiers.primary-values",
        "catalog.identifiers.replace",
        "catalog.agents.list",
        "catalog.agent.link",
        "catalog.wemi.link",
        "catalog.wemi.unlink",
        "catalog.metadata.replace",
        "catalog.fields.list",
        "catalog.fields.get",
    ),
    "search": ("rows.query", "relations.list", "search.global"),
    "browse": (
        "browse.categories",
        "browse.category.items",
        "browse.works",
        "browse.work",
    ),
    "acquisition": (
        "acquisition.formats",
        "acquisition.resolve",
        "acquisition.read",
        "acquisition.cover",
    ),
    "metadata": (
        "metadata.get",
        "metadata.write",
        "metadata.opf.export",
        "metadata.file.formats",
        "metadata.file.inspect",
        "metadata.file.write",
        "metadata.online.sources",
        "metadata.identify.start",
        "metadata.covers.start",
    ),
    "storage": (
        "storage.backends.list",
        "storage.stores.list",
        "storage.store.get",
        "storage.store.save",
        "storage.store.update",
        "storage.store.probe",
        "storage.store.delete",
        "storage.default.get",
        "storage.default.set",
        "storage.files.list",
        "storage.file.locate",
        "storage.file.read",
        "storage.file.put",
        "storage.file.copy",
        "storage.file.delete",
        "storage.location.stat",
        "storage.source.register",
        "storage.resources.describe",
        "storage.resource.list",
        "storage.resource.get",
        "storage.resource.create",
        "storage.resource.update",
        "storage.resource.delete",
        "storage.asset.get",
        "storage.asset.policies.set",
        "storage.policy.assess",
        "storage.policy.plan",
        "storage.policy.violations",
        "storage.status",
        "storage.replica.verify",
        "storage.asset.verify",
        "storage.audit",
        "storage.reconcile.plan",
        "storage.reconcile.apply",
        "storage.repair.plan",
        "storage.repair.apply",
        "storage.store.evacuate.plan",
        "storage.store.evacuate.apply",
        "storage.recovery.list",
        "storage.recovery.recover-pending",
        "storage.recovery.retry-ingest",
    ),
    "ingest": (
        "ingest.formats",
        "ingest.disk.start",
        "ingest.remote-html.start",
    ),
    "conversion": (
        "conversion.formats",
        "conversion.options",
        "conversion.start",
    ),
    "backup": (
        "backup.plan",
        "backup.workflows.list",
        "backup.workflow.get",
        "backup.workflow.save",
        "backup.workflow.start",
        "backup.squashfs.start",
        "backup.squashfs.publish-store.start",
        "backup.squashfs.publish-files.start",
    ),
    "maintenance": (
        "maintenance.status",
        "maintenance.duplicates.find",
        "maintenance.run",
        "maintenance.clean",
        "maintenance.merge",
    ),
}

_CONDITIONAL_OPERATIONS: dict[str, str] = {
    "database.backup": "Requires backup support from the selected database driver.",
    "database.vacuum": "Requires vacuum/compaction support from the selected database driver.",
    "storage.source.register": "Some source kinds require network access or external tools.",
    "ingest.disk.start": "Requires a path-backed database and readable local source.",
    "ingest.remote-html.start": "Requires a path-backed database and network access.",
    "metadata.identify.start": "Results depend on configured online metadata plugins and network access.",
    "metadata.covers.start": "Results depend on configured cover plugins and network access.",
    "conversion.start": "Requires the input/output format plugins and any external tools they use.",
    "backup.workflow.start": "Requires a path-backed database and tools required by the workflow kind.",
    "backup.squashfs.start": "Requires a path-backed database and the configured mksquashfs executable.",
    "backup.squashfs.publish-store.start": "Requires a path-backed database and the configured mksquashfs executable.",
    "backup.squashfs.publish-files.start": "Requires a path-backed database and the configured mksquashfs executable.",
}


def capabilities_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Compare declared program-family names with the runtime's registered command/query union.

    Family registered and legacy available flags mean no declared names are missing,
    not that dependencies work. Per-operation availability is a static conditional/
    available label even when registered=False; implementation and call_modes are
    also declarations, not probes. Extra runtime names are not added to the manifest.
    complete_program_boundary means only that every declared name was found.

    Example:
        >>> capabilities = capabilities_list(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime providing target-free API descriptions and api_version.
    :param query: Ignored query envelope; no dependency checks or family filters are requested.
    :return: API version, declared families/operations, registration completeness, and static dependency notes.
    """
    del query
    described = runtime.describe_api(include_targets=False)
    names = {
        str(item["name"])
        for kind in ("commands", "queries")
        for item in described[kind]
    }
    families: dict[str, dict[str, Any]] = {}
    operation_details: dict[str, dict[str, Any]] = {}
    for area, operations in _PROGRAM_AREAS.items():
        missing = [name for name in operations if name not in names]
        conditional = [
            {
                "name": name,
                "condition": _CONDITIONAL_OPERATIONS[name],
            }
            for name in operations
            if name in _CONDITIONAL_OPERATIONS
        ]
        families[area] = {
            "operations": list(operations),
            "registered": not missing,
            # Retained for clients of the first discovery response.
            "available": not missing,
            "missing_operations": missing,
            "conditional_operations": conditional,
        }
        for name in operations:
            operation_details[name] = {
                "registered": name in names,
                "implementation": "core-local",
                "call_modes": ["direct", "rpc"],
                "availability": (
                    "conditional" if name in _CONDITIONAL_OPERATIONS else "available"
                ),
                "condition": _CONDITIONAL_OPERATIONS.get(name),
            }
    return {
        "api_version": runtime.api_version,
        "complete_program_boundary": all(
            bool(item["registered"]) for item in families.values()
        ),
        "availability_semantics": (
            "`complete_program_boundary` measures stable Core contract "
            "coverage. Conditional operations remain subject to their "
            "declared driver, plugin, path, network, or tool dependency."
        ),
        "families": families,
        "operations": operation_details,
    }


def jobs_result(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """
    Obtain a job execution result using the manager's timeout semantics without re-raising ordinary job failure.

    timeout_s is None when absent/None, otherwise float-converted without adapter
    range or finite checks. raise_on_failure=False lets the execution report carry
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
    timeout = None if timeout_raw is None else float(timeout_raw)
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
