"""
Discover ingest extensions and submit disk or HTML ingestion workflows through Core's job manager.

Submitted requests carry a path-backed database identity rather than the live
runtime. File scanning, network access, persistence, and their recovery policies
belong to the worker; these adapters do not execute ingestion synchronously or
validate that submitted paths/URLs are reachable.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _database_path,
    _database_type,
    _job_submit,
    _mapping,
    _optional_int,
    _payload,
    _required_int,
    _required_text,
    _text_list,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def ingest_formats(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """
    Report ebook extensions and metadata-file types known to the imported registries.

    Ebook extensions are stringified, lowercased, stripped of leading dots,
    deduplicated, and sorted. Metadata types are sorted as supplied by their registry.
    The lists describe known types rather than proving installed plugin readiness.

    Example:
        >>> formats = ingest_formats(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; discovery reads module-level format registries.
    :param query: Unused query envelope; no input file is opened for detection.
    :return: ebook_extensions and metadata_extensions lists.
    """
    del runtime, query
    from LiuXin_alpha.file_formats import BOOK_EXTENSIONS
    from LiuXin_alpha.metadata.file_sources import known_metadata_file_types

    return {
        "ebook_extensions": sorted(
            {str(item).lower().lstrip(".") for item in BOOK_EXTENSIONS}
        ),
        "metadata_extensions": sorted(known_metadata_file_types()),
    }


def ingest_disk_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Submit an unmanaged-disk ingestion job carrying the current path-backed database identity.

    A missing/None ebook_extensions field passes None; other values use ordered,
    stripped text-list normalization. store_name passes through unchanged. Hashing,
    Store linking, and storage-manager refresh default True; following symlinks
    defaults False, with all switches truth-tested rather than type-checked.
    A falsey source_label selects on_disk_unmanaged_import. No scan is run here.

    Example:
        >>> receipt = ingest_disk_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying database path/type and job submission.
    :param command: Command with disk_path and optional store_name, ebook_extensions, source_label, compute_hash, follow_symlinks, attach_store_links, refresh_storage_manager, and shared job fields.
    :return: Submission receipt for run_ingest_disk_job, with ingest disk as the fallback label.
    """
    payload = _payload(command)
    kwargs = {
        "database_path": _database_path(runtime),
        "db_type": _database_type(runtime),
        "disk_path": _required_text(payload, "disk_path"),
        "store_name": payload.get("store_name"),
        "ebook_extensions": (
            _text_list(payload, "ebook_extensions")
            if payload.get("ebook_extensions") is not None
            else None
        ),
        "source_label": str(payload.get("source_label") or "on_disk_unmanaged_import"),
        "compute_hash": bool(payload.get("compute_hash", True)),
        "follow_symlinks": bool(payload.get("follow_symlinks", False)),
        "attach_store_links": bool(payload.get("attach_store_links", True)),
        "refresh_storage_manager": bool(payload.get("refresh_storage_manager", True)),
    }
    return _job_submit(
        runtime,
        payload,
        function_name="run_ingest_disk_job",
        kwargs=kwargs,
        default_label="ingest disk",
    )


def ingest_metadata_plan_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Submit a read-only plan of the books a Store's unattached files would add.

    The job reads each file's embedded metadata through its Store, matches it
    against the catalogue and writes the plan as JSON; it creates no records.
    Without ``report_path`` the plan goes to an ``ingest-plans`` folder beside
    the catalogue. Store existence and file readability are checked by the job.

    Example:
        >>> receipt = ingest_metadata_plan_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying a path-backed database identity and job manager.
    :param command: Command with store_id and optional extensions, limit, max_file_mb, report_path and progress_output, plus shared job fields.
    :return: run_ingest_metadata_plan_job submission receipt.
    :raises CoreDispatchError: If store_id, limit or max_file_mb is not a positive integer, or no database path is available.
    """
    payload = _payload(command)
    store_id = _required_int(payload, "store_id")
    if store_id < 1:
        raise CoreDispatchError("`store_id` must be >= 1.")
    max_file_mb = _optional_int(payload, "max_file_mb", minimum=1)
    report_path = str(payload.get("report_path") or "").strip() or None
    kwargs = {
        "database_path": _database_path(runtime),
        "db_type": _database_type(runtime),
        "store_id": store_id,
        "extensions": (
            _text_list(payload, "extensions")
            if payload.get("extensions") is not None
            else None
        ),
        "limit": _optional_int(payload, "limit", minimum=1),
        "max_bytes": None if max_file_mb is None else max_file_mb * 1024 * 1024,
        "report_path": report_path,
        "progress_output": bool(payload.get("progress_output", True)),
    }
    return _job_submit(
        runtime,
        payload,
        function_name="run_ingest_metadata_plan_job",
        kwargs=kwargs,
        default_label=f"ingest metadata plan store {store_id}",
    )


def ingest_metadata_apply(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Write a reviewed metadata plan into the catalogue, one book per transaction.

    This runs in the Core process, which owns the cache: each book is one
    metadata batch that rolls back on failure and reconciles once. Every planned
    decision is re-checked first; books whose files or reused names changed are
    skipped and reported. The full report goes beside the plan unless report_path
    is given.

    Example:
        >>> result = ingest_metadata_apply(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying the metadata API, Catalog and database.
    :param command: Command with plan_path and optional store_id, limit and report_path.
    :return: Summary, report path, and samples of created and problem books.
    :raises CoreDispatchError: If the plan cannot be read, is for another Store, or limit is not a positive integer.
    """
    from pathlib import Path

    from LiuXin_alpha.ingest.embedded_metadata import (
        PlanError,
        apply_plan,
        apply_result,
        default_report_path,
        load_plan,
        write_report,
    )

    payload = _payload(command)
    plan_path = _required_text(payload, "plan_path")
    store_id = _optional_int(payload, "store_id", minimum=1)
    limit = _optional_int(payload, "limit", minimum=1)
    report_path = str(payload.get("report_path") or "").strip() or None
    try:
        plan = load_plan(plan_path)
    except PlanError as exc:
        raise CoreDispatchError(str(exc)) from exc
    planned_store = int(plan["store"]["store_id"])
    if store_id is not None and planned_store != store_id:
        raise CoreDispatchError(
            f"The plan is for Store {planned_store}, not Store {store_id}."
        )
    services = runtime.services
    report = apply_plan(
        plan,
        metadata=services.metadata,
        catalog=services.catalog,
        database=runtime.database,
        plan_path=str(Path(plan_path).expanduser().resolve()),
        limit=limit,
    )
    target = Path(report_path) if report_path else default_report_path(plan_path)
    return apply_result(report, write_report(report, target))


def ingest_remote_html_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Submit a native-HTML or wget-HTML ingestion job after normalizing its provider name.

    Kind is stripped, lowercased, and has hyphens replaced by underscores; only
    native_html and wget_html are accepted. Options must be a Mapping, with provider
    validation deferred to the worker. No URL fetch or connectivity probe runs here.

    Example:
        >>> receipt = ingest_remote_html_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying a path-backed database identity and job manager.
    :param command: Command with kind, required Mapping options, and shared job submission fields.
    :return: run_ingest_remote_html_job submission receipt with the normalized kind in the fallback label.
    :raises CoreDispatchError: If kind/options validation fails or a database path is unavailable.
    """
    payload = _payload(command)
    kind = _required_text(payload, "kind").lower().replace("-", "_")
    if kind not in {"wget_html", "native_html"}:
        raise CoreDispatchError("`kind` must be `wget_html` or `native_html`.")
    kwargs = {
        "database_path": _database_path(runtime),
        "db_type": _database_type(runtime),
        "kind": kind,
        "options": _mapping(payload, "options"),
    }
    return _job_submit(
        runtime,
        payload,
        function_name="run_ingest_remote_html_job",
        kwargs=kwargs,
        default_label=f"ingest {kind}",
    )


def ingest_web_start(runtime: CoreRuntime, command: CoreCommand) -> dict[str, Any]:
    """Validate a bounded web registration/acquisition request and submit its managed job.

    :param runtime: Runtime supplying a path-backed catalogue identity and job manager.
    :param command: Command with validated options or a saved resume UUID, plus shared job controls.
    :return: Managed-job submission receipt; the completed result contains the web run UUID.
    :raises ValueError: Invalid web controls or resume UUID.
    :raises CoreDispatchError: Resume replacements or unavailable catalogue/job services.
    """
    from uuid import UUID

    from LiuXin_alpha.ingest.web.models import WebIngestOptions

    payload = _payload(command)
    resume = payload.get("resume")
    if resume:
        resume = str(UUID(str(resume)))
        if payload.get("options"):
            raise CoreDispatchError(
                "resume replays saved web options; do not supply replacements"
            )
        options = None
    else:
        options = WebIngestOptions.from_mapping(_mapping(payload, "options")).to_dict()
    return _job_submit(
        runtime,
        payload,
        function_name="run_ingest_web_job",
        kwargs={
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "options": options,
            "resume": resume,
        },
        default_label="web ingest",
    )


def ingest_web_files(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Read remote observations and their last capture receipts without network requests.

    :param runtime: Runtime supplying the catalogue; no storage backend is probed.
    :param query: Query containing the root_url whose recorded candidates are requested.
    :return: Normalized root, observed files with historical last_capture evidence, and saved run summaries.
    :raises ValueError: The source URL fails web option validation.
    """
    from LiuXin_alpha.ingest.web.models import WebIngestOptions
    from LiuXin_alpha.ingest.web.repository import WebIngestRepository

    payload = _payload(query)
    root = WebIngestOptions(root_url=_required_text(payload, "root_url")).root_url
    repository = WebIngestRepository(runtime.database, create=False)
    rows = repository.files(root)
    return {
        "root_url": root,
        "runs": [
            {
                "run_id": run["web_ingest_run_id"],
                "status": run["web_ingest_run_status"],
                "started": run["web_ingest_run_started"],
                "lease_until": run["web_ingest_run_lease_until"],
            }
            for run in repository.runs(root)
        ],
        "files": [
            {
                "remote_file_id": row["remote_file_id"],
                "url": row["remote_file_url"],
                "referrer": row["remote_file_referrer"],
                "first_seen": row["remote_file_first_seen"],
                "last_seen": row["remote_file_last_seen"],
                "last_capture": json.loads(row["remote_file_latest_capture_json"])
                if row["remote_file_latest_capture_json"]
                else None,
            }
            for row in rows
        ],
    }


def ingest_web_status(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    """Inspect a saved web run and its object states, including interrupted attempts.

    :param runtime: Runtime supplying the catalogue for read-only workflow inspection.
    :param query: Query containing a saved web run_id UUID, distinct from the managed-job ID.
    :return: Saved options, lease, byte counters, last report and per-file capture states.
    :raises ValueError: The UUID is malformed or no matching web run exists.
    """
    from uuid import UUID

    from LiuXin_alpha.ingest.web.repository import WebIngestRepository

    run_id = str(UUID(_required_text(_payload(query), "run_id")))
    repository = WebIngestRepository(runtime.database, create=False)
    saved = repository.run(run_id)
    rows = repository.items(run_id)
    return {
        "run_id": run_id,
        "status": saved["web_ingest_run_status"],
        "lease_until": saved["web_ingest_run_lease_until"],
        "received_bytes": saved["web_ingest_run_bytes_received"],
        "discovery_complete": bool(saved["web_ingest_run_discovery_complete"]),
        "options": json.loads(saved["web_ingest_run_options_json"]),
        "report": json.loads(saved["web_ingest_run_report_json"])
        if saved["web_ingest_run_report_json"]
        else None,
        "items": [
            {
                "remote_file_id": row["web_ingest_item_remote_file_id"],
                "state": row["web_ingest_item_state"],
                "error": row["web_ingest_item_error"],
                "receipt": json.loads(row["web_ingest_item_receipt_json"]),
            }
            for row in rows
        ],
    }
