"""
Discover ingest extensions and submit disk or HTML ingestion workflows through Core's job manager.

Submitted requests carry a path-backed database identity rather than the live
runtime. File scanning, network access, persistence, and their recovery policies
belong to the worker; these adapters do not execute ingestion synchronously or
validate that submitted paths/URLs are reachable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _database_path,
    _database_type,
    _job_submit,
    _mapping,
    _payload,
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
