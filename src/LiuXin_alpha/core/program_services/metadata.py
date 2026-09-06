"""Metadata inspection, rewritten-file receipts, and online job submission.

Item hydration and caller-supplied metadata normalization share one writer
input boundary. File and in-memory writes retain the same error reporting.
"""

from __future__ import annotations

import base64
import io
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _job_submit,
    _payload,
    _required_int,
    _text_list,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def metadata_file_formats(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del runtime, query
    from LiuXin_alpha.customize.ui import can_set_metadata
    from LiuXin_alpha.metadata.file_sources import known_metadata_file_types

    readable = sorted(known_metadata_file_types())
    return {
        "readable": readable,
        "writable": [
            file_type for file_type in readable if can_set_metadata(file_type)
        ],
    }


def metadata_file_inspect(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del runtime
    payload = _payload(query)
    path = str(payload.get("path") or "").strip()
    encoded = str(payload.get("base64") or "").strip()
    file_type = str(payload.get("file_type") or "").strip().lower()
    if bool(path) == bool(encoded):
        raise CoreDispatchError("Provide exactly one of `path` or `base64`.")
    from LiuXin_alpha.metadata.file_sources import get_metadata

    if path:
        target: Any = path
    else:
        try:
            target = io.BytesIO(base64.b64decode(encoded, validate=True))
        except Exception as exc:
            raise CoreDispatchError("`base64` is not valid base64 data.") from exc
        if not file_type:
            raise CoreDispatchError(
                "`file_type` is required with base64 metadata input."
            )
    metadata = get_metadata(target, force_type=file_type or False)
    return {
        "file_type": (file_type or Path(path).suffix.lower().lstrip(".")),
        "metadata": plain(metadata),
    }


def metadata_online_sources(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del runtime, query
    from LiuXin_alpha.customize.ui import metadata_plugins

    plugins: dict[str, Any] = {}
    for capability in ("identify", "cover"):
        values: Iterable[Any]
        try:
            values = metadata_plugins([capability])
        except Exception:
            values = ()
        for plugin in values:
            name = str(getattr(plugin, "name", type(plugin).__name__))
            entry = plugins.setdefault(
                name,
                {
                    "name": name,
                    "version": plain(getattr(plugin, "version", None)),
                    "capabilities": [],
                    "configured": True,
                },
            )
            entry["capabilities"].append(capability)
            configured = getattr(plugin, "is_configured", None)
            if callable(configured):
                try:
                    entry["configured"] = bool(configured())
                except Exception:
                    entry["configured"] = False
    return {
        "sources": [
            {
                **entry,
                "capabilities": sorted(set(entry["capabilities"])),
            }
            for _name, entry in sorted(plugins.items())
        ]
    }


def _metadata_for_write(runtime: CoreRuntime, payload: Mapping[str, Any]) -> Any:
    """Hydrate an Item or normalize caller-supplied metadata for the writer."""
    if payload.get("item_id") is not None:
        from LiuXin_alpha.metadata.containers import (
            LiuXinWEMIMetadataHydrator,
        )

        metadata = (
            LiuXinWEMIMetadataHydrator(runtime.services.read_source)
            .get_liuxin_wemi_metadata(item_id=_required_int(payload, "item_id"))
            .to_calibre()
        )
    else:
        raw_metadata = payload.get("metadata")
        if not isinstance(raw_metadata, Mapping):
            raise CoreDispatchError("Provide `item_id` or a `metadata` object.")
        values = dict(raw_metadata)
        if "wemi" in values or "liuxin" in values:
            from LiuXin_alpha.metadata.containers import (
                LiuXinWEMIMetadata,
            )

            metadata = LiuXinWEMIMetadata.from_mapping(values).to_calibre()
        else:
            from LiuXin_alpha.metadata.book.base import calibreMetadata

            authors_raw = values.pop("authors", values.pop("author", ()))
            if isinstance(authors_raw, str):
                authors = [authors_raw]
            elif isinstance(authors_raw, Sequence):
                authors = [str(value) for value in authors_raw]
            else:
                raise CoreDispatchError("`metadata.authors` must be a string or array.")
            metadata = calibreMetadata(
                str(values.pop("title", "") or "Unknown"),
                authors or ["Unknown"],
            )
            for key, value in values.items():
                if key == "identifiers":
                    setter = getattr(metadata, "set_identifiers", None)
                    if callable(setter) and isinstance(value, Mapping):
                        setter(dict(value))
                        continue
                setattr(metadata, str(key), value)

    return metadata


def metadata_file_write(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    path = str(payload.get("path") or "").strip()
    encoded = str(payload.get("base64") or "").strip()
    if bool(path) == bool(encoded):
        raise CoreDispatchError("Provide exactly one of `path` or `base64`.")
    file_type = str(payload.get("file_type") or "").strip().lower()
    if not file_type and path:
        file_type = Path(path).suffix.lower().lstrip(".")
    if not file_type:
        raise CoreDispatchError("`file_type` is required.")

    from LiuXin_alpha.customize.ui import (
        can_set_metadata,
        set_file_type_metadata,
    )

    if not can_set_metadata(file_type):
        raise CoreDispatchError(
            f"No enabled metadata writer supports `{file_type}`.",
            code="metadata_writer_unavailable",
            details={"file_type": file_type},
        )

    metadata = _metadata_for_write(runtime, payload)

    errors: list[str] = []

    def report_error(_metadata: Any, _file_type: str, trace: str) -> None:
        errors.append(str(trace))

    if path:
        with open(path, "r+b") as path_stream:
            set_file_type_metadata(
                path_stream,
                metadata,
                file_type,
                report_error=report_error,
            )
        content: bytes | None = None
        size = Path(path).stat().st_size
    else:
        try:
            initial = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            raise CoreDispatchError("`base64` is not valid base64 data.") from exc
        memory_stream = io.BytesIO(initial)
        set_file_type_metadata(
            memory_stream,
            metadata,
            file_type,
            report_error=report_error,
        )
        content = memory_stream.getvalue()
        size = len(content)
    if errors:
        raise CoreDispatchError(
            f"The `{file_type}` metadata writer failed.",
            code="metadata_file_write_failed",
            details={"file_type": file_type, "errors": errors},
        )
    return {
        "file_type": file_type,
        "path": path or None,
        "content": content,
        "size": size,
        "updated": True,
    }


def metadata_identify_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    identifiers = payload.get("identifiers", {})
    if not isinstance(identifiers, Mapping):
        raise CoreDispatchError("`identifiers` must be an object.")
    authors = _text_list(payload, "authors")
    allowed = _text_list(payload, "allowed_plugins")
    timeout = float(payload.get("timeout_s", 30.0))
    if not str(payload.get("title") or "").strip() and not authors and not identifiers:
        raise CoreDispatchError("Provide a title, authors, or identifiers.")
    return _job_submit(
        runtime,
        payload,
        function_name="run_metadata_identify_job",
        kwargs={
            "title": (None if payload.get("title") is None else str(payload["title"])),
            "authors": authors or None,
            "identifiers": {str(key): str(value) for key, value in identifiers.items()},
            "timeout": timeout,
            "allowed_plugins": allowed or None,
        },
        default_label="metadata identify",
    )


def metadata_covers_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    identifiers = payload.get("identifiers", {})
    if not isinstance(identifiers, Mapping):
        raise CoreDispatchError("`identifiers` must be an object.")
    authors = _text_list(payload, "authors")
    timeout = float(payload.get("timeout_s", 30.0))
    if not str(payload.get("title") or "").strip() and not authors and not identifiers:
        raise CoreDispatchError("Provide a title, authors, or identifiers.")
    return _job_submit(
        runtime,
        payload,
        function_name="run_metadata_cover_job",
        kwargs={
            "title": (None if payload.get("title") is None else str(payload["title"])),
            "authors": authors or None,
            "identifiers": {str(key): str(value) for key, value in identifiers.items()},
            "timeout": timeout,
        },
        default_label="metadata covers",
    )
