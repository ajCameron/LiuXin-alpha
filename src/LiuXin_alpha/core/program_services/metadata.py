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
import contextlib
import io
import os
import shutil
import tempfile
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
    from LiuXin_alpha.file_formats.plugin_runtime import can_set_metadata
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
    from LiuXin_alpha.plugins.application import metadata_plugins

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


def _plain_metadata_mapping(payload: Mapping[str, Any]) -> bool:
    """
    Tell whether a write supplies its own fields as a plain mapping, rather than an Item or WEMI record.

    Example:
        >>> _plain_metadata_mapping({"metadata": {"title": "T"}}), _plain_metadata_mapping({"item_id": 7})
        (True, False)


    :param payload: The write request.
    :return: Whether the fields given are all the caller means to change.
    """
    raw = payload.get("metadata")
    return (
        payload.get("item_id") is None
        and isinstance(raw, Mapping)
        and not ("wemi" in raw or "liuxin" in raw)
    )


def _existing_metadata(data: bytes, file_type: str) -> Any:
    """
    Read a file's current metadata in calibre's form, without its cover, or ``None`` if it cannot be read.

    The cover is left out so that writing the record back does not re-encode an
    unchanged cover.

    Example:
        >>> _existing_metadata(b"not a book", "epub") is None
        True


    :param data: The file.
    :param file_type: Its format.
    :return: The metadata, or ``None``.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata

    try:
        metadata = get_metadata(io.BytesIO(data), force_type=file_type)
        # Most readers give LiuXin's record; some (MOBI) already give calibre's.
        convert = getattr(metadata, "to_calibre", None)
        if callable(convert):
            metadata = convert()
    except Exception:
        return None
    metadata.cover_data = (None, None)
    return metadata


def _supplied_authors(values: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Take the authors out of a metadata mapping, under either of their keys.

    Example:
        >>> _supplied_authors({"author": "Ann"}), _supplied_authors({"title": "T"})
        ((True, ['Ann']), (False, []))


    :param values: The mapping; ``authors`` and ``author`` are removed from it.
    :return: Whether authors were given, and the names; a string is one name.
    :raises CoreDispatchError: If the authors are neither a string nor a sequence.
    """
    given = "authors" in values or "author" in values
    raw = values.pop("authors", values.pop("author", ()))
    if isinstance(raw, str):
        return given, [raw]
    if isinstance(raw, Sequence):
        return given, [str(value) for value in raw]
    raise CoreDispatchError("`metadata.authors` must be a string or array.")


def _plain_metadata(values: dict[str, Any], existing: Any) -> Any:
    """
    Build the record for a plain metadata mapping: the file's own fields, with the supplied ones over them.

    Example:
        >>> _plain_metadata({"title": "T"}, None).authors
        ['Unknown']


    :param values: A copy of the mapping; it is consumed.
    :param existing: The file's current metadata, or ``None`` if it could not be read.
    :return: calibre's metadata, ready for a writer.
    :raises CoreDispatchError: If the authors are neither a string nor a sequence.
    """
    from LiuXin_alpha.metadata.book.base import calibreMetadata

    given_authors, authors = _supplied_authors(values)
    title = str(values.pop("title", "") or "")
    if existing is None:
        metadata = calibreMetadata(title or "Unknown", authors or ["Unknown"])
    else:
        metadata = existing
        metadata.title = title or metadata.title or "Unknown"
        if given_authors or not metadata.authors:
            metadata.authors = authors or ["Unknown"]
    for key, value in values.items():
        if key == "identifiers":
            setter = getattr(metadata, "set_identifiers", None)
            if callable(setter) and isinstance(value, Mapping):
                setter(dict(value))
                continue
        setattr(metadata, str(key), value)
    return metadata


def _metadata_for_write(
    runtime: CoreRuntime, payload: Mapping[str, Any], existing: Any = None
) -> Any:
    """
    Prefer a non-None Item ID for hydration, otherwise normalize supplied metadata into a Calibre-compatible value.

    Mapping keys wemi/liuxin select LiuXinWEMIMetadata conversion. Other mappings
    change only the fields they name: they start from ``existing``, the file's
    current metadata, when it could be read, so a field left out keeps what the file
    says rather than being cleared or set to Unknown. Without ``existing``, a falsey
    title and empty authors default to Unknown. An explicitly empty author list is
    Unknown either way. Author strings remain single unstripped entries; other
    Sequences are stringified elementwise, including bytes. The authors key wins and
    both aliases are removed. Remaining fields are assigned without an adapter
    allowlist; Mapping identifiers use set_identifiers when available. Errors
    propagate before writing.

    Example:
        >>> metadata = _metadata_for_write(runtime, {"item_id": 7})  # doctest: +SKIP


    :param runtime: Runtime whose selected read source is used only for Item hydration.
    :param payload: Request with non-None item_id or a metadata Mapping; supplied metadata is ignored for Item hydration.
    :param existing: The file's current metadata in calibre's form, from :func:`_existing_metadata`.
    :return: Hydrated/converted metadata, or calibreMetadata holding the file's fields with the supplied ones over them.
    :raises CoreDispatchError: If Item ID validation fails, metadata is absent/non-Mapping, or authors is not str/Sequence.
    """
    if payload.get("item_id") is not None:
        from LiuXin_alpha.catalog.retrieval.hydration import (
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
            metadata = _plain_metadata(values, existing)

    return metadata


def _replace_file(target: str, before: os.stat_result, payload: bytes) -> None:
    """
    Replace a file with new contents by writing them beside it and renaming them into place once complete.

    Example:
        Replace a file read with status ``before``::

            _replace_file("/books/a.epub", before, rewritten)


    :param target: The file's real path.
    :param before: Its status when it was read.
    :param payload: The new contents.
    :return: None.
    :raises CoreDispatchError: If the file changed since it was read, or the new file could not be written;
        the old file is untouched either way.
    """
    directory, name = os.path.split(target)
    descriptor, staged = tempfile.mkstemp(
        dir=directory or ".", prefix=f".{name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        shutil.copymode(target, staged)
        now = os.stat(target)
        if (now.st_size, now.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise CoreDispatchError(
                "The file changed while its metadata was being rewritten, so nothing was written.",
                code="metadata_file_changed",
                details={"path": target},
            )
        os.replace(staged, target)
    except BaseException as error:
        with contextlib.suppress(OSError):
            os.unlink(staged)
        if isinstance(error, OSError):
            raise CoreDispatchError(
                f"Writing the rewritten file failed ({error}); the original is untouched.",
                code="metadata_file_write_failed",
                details={"path": target},
            ) from error
        raise
    with contextlib.suppress(OSError, AttributeError):
        folder = os.open(directory or ".", os.O_RDONLY)
        try:
            os.fsync(folder)
        finally:
            os.close(folder)


def metadata_file_write(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Rewrite metadata in an existing path or decoded byte stream using an enabled format writer.

    Exactly one nonblank path/base64 is required. Type is supplied lowercased text
    or a path suffix; writer capability is checked before metadata hydration and
    base64 decoding. A plain metadata mapping changes only the fields it names (see
    :func:`_metadata_for_write`). The writer always works on a copy in memory; byte
    writes return the complete rewritten buffer.

    A path is only replaced once the rewritten file is complete and the writer
    reported nothing: the new file is written beside the old one, synced, given its
    permissions, and renamed over it, so a failure part-way leaves the file as it
    was. A symbolic link is followed and stays a link; other hard links keep the old
    file. If the file changed while it was being rewritten, nothing is written.
    updated=True means the writer returned without reported traces, not that
    metadata was compared before/after or independently validated.

    Example:
        >>> receipt = metadata_file_write(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying optional Item metadata hydration; arbitrary supplied paths are not confined here.
    :param command: Command with path or base64, optional/inferred file_type, and item_id or metadata for the writer.
    :return: Type, path-or-None, rewritten bytes for byte input or content=None for path input, byte size, and updated=True.
    :raises CoreDispatchError: For input/type/base64/metadata validation, unavailable writers, collected writer
        failures (the file is then unchanged), or a file that changed while being rewritten.
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

    from LiuXin_alpha.file_formats.plugin_runtime import (
        can_set_metadata,
        set_file_type_metadata,
    )

    if not can_set_metadata(file_type):
        raise CoreDispatchError(
            f"No enabled metadata writer supports `{file_type}`.",
            code="metadata_writer_unavailable",
            details={"file_type": file_type},
        )

    before: os.stat_result | None = None
    if path:
        target = os.path.realpath(path)
        with open(target, "rb") as handle:
            before = os.fstat(handle.fileno())
            initial = handle.read()
    else:
        try:
            initial = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            raise CoreDispatchError("`base64` is not valid base64 data.") from exc

    existing = (
        _existing_metadata(initial, file_type)
        if _plain_metadata_mapping(payload)
        else None
    )
    metadata = _metadata_for_write(runtime, payload, existing)

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

    memory_stream = io.BytesIO(initial)
    set_file_type_metadata(
        memory_stream,
        metadata,
        file_type,
        report_error=report_error,
    )
    rewritten = memory_stream.getvalue()
    if errors:
        raise CoreDispatchError(
            f"The `{file_type}` metadata writer failed.",
            code="metadata_file_write_failed",
            details={"file_type": file_type, "errors": errors},
        )
    content: bytes | None = rewritten
    if path and before is not None:
        _replace_file(target, before, rewritten)
        content = None
    size = len(rewritten)
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
