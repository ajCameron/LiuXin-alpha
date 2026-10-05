"""
Inspect metadata, rewrite file/byte metadata, and submit online identification or cover jobs through Core.

Item hydration and caller-supplied metadata normalization share one writer
input boundary. Path writes modify the existing file directly, while byte input
returns a rewritten in-memory value. Writer-reported errors may follow partial
changes; no adapter transaction, path confinement, byte-size cap, or compensating
restore is provided. Online start operations return job receipts, not fetched results.
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
    """
    List registered readable metadata types and the subset with an enabled metadata writer.

    Writable discovery is limited to the readable list, retains its sorted order,
    and does not inspect a sample file or execute a write.

    Example:
        >>> formats = metadata_file_formats(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; discovery uses process-wide metadata/customization registries.
    :param query: Ignored query envelope; no file-type filter is consumed.
    :return: Sorted readable types and writer-enabled writable subset.
    """
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
    """
    Read metadata from exactly one nonblank path or strict-base64 input and project it for Core.

    Path/base64 text is stripped. Byte input requires a nonblank lowercased file_type;
    path input may rely on reader detection and optionally force that type. The
    response's file_type is the supplied hint or path suffix, not a separate report
    of the reader's detected format. No path confinement or byte-size cap is added.

    Example:
        >>> metadata = metadata_file_inspect(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; metadata is read through the file-source registry.
    :param query: Query containing exactly one of path/base64 and optional file_type, required for bytes.
    :return: Type hint/suffix and plain-projected metadata; reader/projection failures propagate.
    :raises CoreDispatchError: For ambiguous/missing input, invalid base64, or byte input without a type.
    """
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
    """
    Describe identify/cover plugin names, first-seen versions, merged capabilities, and configuration checks.

    Immediate registry-call errors suppress that capability's list; later iteration
    and attribute errors propagate. Entries merge by stringified name, retain the
    first version, and sort names/capabilities. A missing configuration callable
    leaves the existing flag, initially True; each callable result replaces it,
    with ordinary call failures setting False. No identify or cover search is run.

    Example:
        >>> sources = metadata_online_sources(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; discovery consults the global plugin registry.
    :param query: Ignored query envelope; no credentials or plugin filter is consumed.
    :return: sources list with merged name/version/capabilities/configured descriptions, not availability guarantees.
    """
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
    """
    Prefer a non-None Item ID for hydration, otherwise normalize supplied metadata into a Calibre-compatible value.

    Mapping keys wemi/liuxin select LiuXinWEMIMetadata conversion. Other mappings
    use title plus authors/author, defaulting falsey title and empty author lists to
    Unknown. Author strings remain single unstripped entries; other Sequences are
    stringified elementwise, including bytes. The authors key wins and both aliases
    are removed. Remaining fields are assigned without an adapter allowlist; Mapping
    identifiers use set_identifiers when available. Errors propagate before writing.

    Example:
        >>> metadata = _metadata_for_write(runtime, {"item_id": 7})  # doctest: +SKIP


    :param runtime: Runtime whose selected read source is used only for Item hydration.
    :param payload: Request with non-None item_id or a metadata Mapping; supplied metadata is ignored for Item hydration.
    :return: Hydrated/converted metadata or a new calibreMetadata object populated from a shallow Mapping copy.
    :raises CoreDispatchError: If Item ID validation fails, metadata is absent/non-Mapping, or authors is not str/Sequence.
    """
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
    """
    Rewrite metadata in an existing path or decoded byte stream using an enabled format writer.

    Exactly one nonblank path/base64 is required. Type is supplied lowercased text
    or a path suffix; writer capability is checked before metadata hydration and
    base64 decoding. Path writes use r+b with no temporary replacement or rollback,
    then read file size; byte writes return the complete rewritten buffer.

    Reported writer traces are checked after the write and size observation, so
    failure can follow partial file modification. Unreported writer/I/O errors
    propagate directly. updated=True means the writer returned without reported
    traces, not that metadata was compared before/after or independently validated.

    Example:
        >>> receipt = metadata_file_write(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying optional Item metadata hydration; arbitrary supplied paths are not confined here.
    :param command: Command with path or base64, optional/inferred file_type, and item_id or metadata for the writer.
    :return: Type, path-or-None, rewritten bytes for byte input or content=None for path input, byte size, and updated=True.
    :raises CoreDispatchError: For input/type/base64/metadata validation, unavailable writers, or collected writer failures.
    """
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
        """
        Accumulate a stringified writer error trace without interrupting the current rewrite.

        Example:
            >>> report_error(metadata, "epub", "writer failed")  # doctest: +SKIP


        :param _metadata: Ignored metadata object supplied by the writer callback protocol.
        :param _file_type: Ignored writer format label; the enclosing request supplies the error receipt's type.
        :param trace: Diagnostic value stringified into the enclosing errors list.
        :return: None after recording the trace; the enclosing handler raises after writing/size observation.
        """
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
    """
    Submit online metadata identification using at least one title, author, or identifier hint.

    Authors and allowed_plugins use stripped, ordered text-list normalization;
    empty lists pass None. Identifier keys/values are stringified without semantic
    validation, potentially collapsing key collisions. Title is checked stripped
    but forwarded unstripped. timeout_s defaults to 30 and uses float without range
    validation; it is a worker lookup option separate from shared job_timeout_s.

    Example:
        >>> receipt = metadata_identify_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose job manager accepts run_metadata_identify_job.
    :param command: Command with title/authors/identifiers hints, optional allowed_plugins and timeout_s, plus shared job fields.
    :return: Submission receipt with metadata identify as fallback label; no search results are fetched here.
    :raises CoreDispatchError: For invalid Mapping/list hints or an entirely empty search request.
    """
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
    """
    Submit online cover lookup with normalized author/identifier hints and a worker timeout.

    At least one nonblank title, normalized author, or identifier entry is required.
    Title is forwarded unstripped; identifiers stringify keys/values and authors
    use ordered text-list normalization. timeout_s defaults to 30 without adapter
    range validation and is distinct from job_timeout_s. Unlike identification,
    this adapter does not forward an allowed_plugins option.

    Example:
        >>> receipt = metadata_covers_start(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose job manager accepts run_metadata_cover_job.
    :param command: Command with title/authors/identifiers, optional timeout_s, and shared job submission fields.
    :return: Job submission receipt with metadata covers as fallback label, not cover bytes or a success result.
    :raises CoreDispatchError: For invalid Mapping/list hints or no usable search hint.
    """
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
