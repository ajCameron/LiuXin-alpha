"""Core-owned ingest operations and wire translation."""

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
