"""
Read, export, edit, and enrich catalogue or CLI-host file metadata through Core.

The surface deliberately speaks only to stable named Core operations.  In
particular, a path passed to a file command always names a file on the CLI
host: its bounded bytes are transferred to Core instead of being reinterpreted
as a daemon-local path.

File outputs are staged individually, not as a transaction spanning catalogue
changes, backup, artifact, and report. These helpers have their own publication
and job-polling policies rather than using every common CLI helper. In particular,
dump publication occurs inside the Core/stdout-redirection context, whereas other
handlers generally emit after leaving it. Online timeouts do not cancel jobs.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import stat
import sys
import tempfile
import time

from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import Any, BinaryIO

from LiuXin_alpha.surfaces.core import (
    add_core_client_arguments,
    open_surface_core_from_args,
)


_DUMP_FORMAT = "liuxin.metadata.dump"
_DUMP_VERSION = 1
_DEFAULT_TRANSFER_MIB = 512.0
_MAX_CONTROL_FILE_BYTES = 16 * 1024 * 1024
_TERMINAL_JOB_STATES = {"succeeded", "failed", "cancelled", "timed_out"}
_WRITE_FIELDS = {
    "tag": "tags",
    "tags": "tags",
    "label": "labels",
    "labels": "labels",
    "genre": "genre",
    "genres": "genre",
    "subject": "subject",
    "subjects": "subject",
    "series": "series",
    "identifier": "identifiers",
    "identifiers": "identifiers",
}


def _json_bytes(value: Any, *, compact: bool = False) -> bytes:
    """
    Serialize deterministic ASCII-escaped JSON with a trailing newline.

    Escaping makes lone surrogates representable; default JSON nonfinite-number
    handling remains enabled. Encoding/type errors propagate before publication.

    Example:
        >>> _json_bytes({"b": 2, "a": 1}, compact=True).decode().splitlines()
        ['{"a":1,"b":2}']


    :param value: JSON-serializable data with keys sortable by json.dumps.
    :param compact: Omit indentation and optional separators when true.
    :return: Sorted-key JSON encoded as UTF-8 bytes, terminated by one newline.
    """
    separators = (",", ":") if compact else None
    text = json.dumps(
        value,
        ensure_ascii=True,
        indent=None if compact else 2,
        separators=separators,
        sort_keys=True,
    )
    return (text + "\n").encode("utf-8")


def _fsync_directory(path: Path) -> None:
    """
    Best-effort sync a directory, suppressing open/fsync OSErrors but not close errors.

    Example:
        >>> _fsync_directory(published_file.parent)  # doctest: +SKIP


    :param path: Directory itself, not the file whose publication it contains.
    :return: None after sync or a suppressed unsupported/open/fsync failure.
    """
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _publish_staged_file(
    staged: Path,
    target: Path,
    *,
    replace: bool,
) -> None:
    """
    Publish a staged pathname by replacement or no-clobber hard linking.

    Without replace, the hard link prevents a race from overwriting a competing
    destination, then the staged name is removed. Parent syncing follows either
    path. Unlink/sync-close errors may propagate after the target already exists;
    no rollback or cross-filesystem fallback is supplied here.

    Example:
        >>> _publish_staged_file(staged, target, replace=False)  # doctest: +SKIP


    :param staged: Completed temporary file to publish, normally beside target.
    :param target: Final filesystem pathname, not a stdout sentinel.
    :param replace: Use os.replace when true; otherwise require a new hard-link name.
    :return: None after publication and the best-effort parent sync attempt.
    :raises FileExistsError: No-clobber publication finds an existing destination.
    """
    if replace:
        os.replace(staged, target)
    else:
        try:
            os.link(staged, target)
        except FileExistsError as error:
            raise FileExistsError(
                "Refusing to replace existing output {!s}; pass the relevant "
                "--replace option.".format(target)
            ) from error
        staged.unlink()
    _fsync_directory(target.parent)


@contextmanager
def _atomic_binary_output(
    output: str | Path,
    *,
    replace: bool,
    mode: int | None = None,
) -> Generator[BinaryIO, None, None]:
    """
    Yield a temporary binary stream and publish only after a successful body.

    For '-', spool to a temporary file, flush/fsync, then copy to the current
    stdout buffer or UTF-8 text fallback. Buffering avoids early output during
    production, but final copying can still fail after partial stdout emission.
    For files require an existing parent, reject owned paths including dangling
    symlinks unless replacement is allowed, stage beside the destination, optionally
    chmod permission bits, then publish by replace/no-clobber hard link.

    Attempt staged-file cleanup on errors; cleanup can itself raise. A failure
    after publication does not restore the prior destination. No parent directories
    are created, and mode applies only to filesystem output.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     target = Path(directory) / "result.bin"
        ...     with _atomic_binary_output(target, replace=False) as stream:
        ...         _ = stream.write(b"complete")
        ...     content = target.read_bytes()
        >>> content
        b'complete'


    :param output: Filesystem path or any value stringifying exactly to '-'.
    :param replace: Permit replacing an owned filesystem destination.
    :param mode: Optional permission bits applied after writing the staged file.
    :return: Context manager yielding a writable binary stream, committed on normal exit.
    """

    if str(output) == "-":
        with tempfile.TemporaryFile(mode="w+b") as stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
            stream.seek(0)
            stdout = getattr(sys.stdout, "buffer", None)
            if stdout is not None:
                shutil.copyfileobj(stream, stdout)
                stdout.flush()
            else:
                sys.stdout.write(stream.read().decode("utf-8"))
                sys.stdout.flush()
        return

    target = Path(output).expanduser()
    parent = target.parent
    if not parent.is_dir():
        raise FileNotFoundError(
            "Output directory does not exist: {!s}".format(parent)
        )
    if os.path.lexists(os.fspath(target)) and not replace:
        raise FileExistsError(
            "Refusing to replace existing output {!s}; pass the relevant "
            "--replace option.".format(target)
        )
    descriptor, staged_name = tempfile.mkstemp(
        prefix=".{}.".format(target.name),
        suffix=".tmp",
        dir=str(parent),
    )
    staged = Path(staged_name)
    try:
        with os.fdopen(descriptor, "w+b") as stream:
            descriptor = -1
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
        if mode is not None:
            os.chmod(staged, stat.S_IMODE(mode))
        _publish_staged_file(staged, target, replace=replace)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            staged.unlink()
        except FileNotFoundError:
            pass


def _emit_bytes(
    output: str | Path,
    payload: bytes,
    *,
    replace: bool = False,
    mode: int | None = None,
) -> None:
    """
    Stage a byte payload using the metadata command's output publication policy.

    Example:
        >>> _emit_bytes(destination, b"content", replace=False)  # doctest: +SKIP


    :param output: CLI-host path or '-' for buffered emission to current stdout.
    :param payload: Complete binary content to write; no serialization occurs here.
    :param replace: Allow atomic pathname replacement instead of no-clobber creation.
    :param mode: Optional filesystem permission bits, ignored for stdout.
    :return: None after publication; writing, publication, and cleanup failures propagate.
    """
    with _atomic_binary_output(output, replace=replace, mode=mode) as stream:
        stream.write(payload)


def _emit_json(value: Any, args: argparse.Namespace) -> None:
    """
    Encode a complete JSON report and publish using optional namespace controls.

    Missing controls default to pretty stdout output without replacement. This
    emitter does not inspect receipt status or redact secrets.

    Example:
        >>> _emit_json({"ok": True}, argparse.Namespace(compact=True))
        {"ok":true}


    :param value: JSON-compatible report encoded before opening the output stream.
    :param args: Optional output, compact, and replace_output attributes.
    :return: None after encoding and staged publication complete.
    """
    _emit_bytes(
        getattr(args, "output", "-"),
        _json_bytes(value, compact=bool(getattr(args, "compact", False))),
        replace=bool(getattr(args, "replace_output", False)),
    )


def _ensure_output_available(output: str | Path, *, replace: bool) -> None:
    """
    Preflight a destination's parent/existence without reserving or opening it.

    '-' bypasses checks. lexists treats dangling symlinks as owned paths. This
    observation does not prove writability or rule out races/final publication errors.

    Example:
        >>> _ensure_output_available("-", replace=False)


    :param output: CLI-host path or stdout sentinel, with user expansion for paths.
    :param replace: Permit an already owned destination when true.
    :return: None if the preliminary checks allow an attempted publication.
    :raises FileNotFoundError: The parent is not an existing directory.
    :raises FileExistsError: A destination exists and replacement is not allowed.
    """
    if str(output) == "-":
        return
    target = Path(output).expanduser()
    if not target.parent.is_dir():
        raise FileNotFoundError(
            "Output directory does not exist: {!s}".format(target.parent)
        )
    if os.path.lexists(os.fspath(target)) and not replace:
        raise FileExistsError(
            "Refusing to replace existing output {!s}; pass the relevant "
            "--replace option.".format(target)
        )


def _ensure_json_output(args: argparse.Namespace) -> None:
    """
    Preflight namespace-selected JSON output before commands can perform work.

    Example:
        >>> _ensure_json_output(argparse.Namespace())


    :param args: Optional output/replace_output settings, defaulting to '-' and false.
    :return: None if the preliminary destination check passes; no path is reserved.
    """
    _ensure_output_available(
        getattr(args, "output", "-"),
        replace=bool(getattr(args, "replace_output", False)),
    )


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    """
    Require a mapping and shallow-copy it with stringified keys.

    Nested values remain shared and colliding stringified keys overwrite earlier
    entries. The result is not otherwise checked for JSON serializability.

    Example:
        >>> _mapping({7: "value"}, label="receipt")
        {'7': 'value'}


    :param value: Mapping-shaped input to normalize at a Core/JSON boundary.
    :param label: Human-readable context included in the shape-error message.
    :return: New string-key dictionary containing the original values.
    :raises TypeError: The value does not implement Mapping.
    """
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a JSON object.".format(label))
    return {str(key): item for key, item in value.items()}


def _wire_bytes(value: Any, *, label: str) -> bytes:
    """
    Accept raw bytes or strictly decode a Core wire-byte envelope.

    Bytearray/other buffers are not accepted. Envelopes require exact '$type' bytes
    and a string base64 field; additional fields are ignored and no size cap applies.

    Example:
        >>> _wire_bytes({"$type": "bytes", "base64": "YWJj"}, label="artifact")
        b'abc'


    :param value: Bytes or a mapping containing the recognized base64 envelope.
    :param label: Error context identifying the content being decoded.
    :return: Original bytes by identity or freshly decoded envelope bytes.
    :raises TypeError: The envelope/type/base64 field has an unsupported shape.
    :raises ValueError: Strict base64 decoding raises an ordinary exception.
    """
    if isinstance(value, bytes):
        return value
    if not isinstance(value, Mapping) or value.get("$type") != "bytes":
        raise TypeError("{} did not contain Core wire bytes.".format(label))
    encoded = value.get("base64")
    if not isinstance(encoded, str):
        raise TypeError("{} has no base64 string.".format(label))
    try:
        return base64.b64decode(encoded, validate=True)
    except Exception as error:
        raise ValueError("{} contains invalid base64 data.".format(label)) from error


def _add_connection(parser: argparse.ArgumentParser) -> None:
    """
    Add surface Core selection arguments and the metadata command's driver option.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _add_connection(parser)
        >>> parser.parse_args(["--database", "catalogue.sqlite"]).db_type
        'SQLite'


    :param parser: Mutable leaf parser receiving selectors and SQLite driver default.
    :return: None; no Core or database is opened while declaring arguments.
    """
    add_core_client_arguments(parser)
    parser.add_argument(
        "--db-type",
        default="SQLite",
        help="Local database backend type (default: SQLite).",
    )


@contextmanager
def _open_metadata_core(
    args: argparse.Namespace,
) -> Generator[Any, None, None]:
    """
    Yield a surface Core session while redirecting all enclosed stdout to stderr.

    Disable local storage-manager and maintenance composition. Redirection spans
    acquisition, the caller's with body, and cleanup, not only legacy startup;
    output helpers called inside that body therefore observe redirected stdout.
    Session acquisition/body/cleanup errors propagate under ordinary context policy.

    Example:
        >>> with _open_metadata_core(args) as session:  # doctest: +SKIP
        ...     result = session.client.query("metadata.file.formats")


    :param args: Surface connection/profile selectors and timeout/driver options.
    :return: Context manager yielding the session object, whose client dispatches Core calls.
    """

    with redirect_stdout(sys.stderr):
        with open_surface_core_from_args(
            args,
            enable_storage_manager=False,
            enable_maintenance=False,
        ) as session:
            yield session


def _add_json_output(
    parser: argparse.ArgumentParser,
    *,
    output_help: str = "JSON output path, or '-' for stdout (default: '-').",
) -> None:
    """
    Declare JSON destination, overwrite permission, and compact-rendering options.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _add_json_output(parser)
        >>> args = parser.parse_args([])
        >>> args.output, args.replace_output, args.compact
        ('-', False, False)


    :param parser: Mutable command leaf receiving output-related flags.
    :param output_help: Help text for the path/'-' destination option.
    :return: None; no output path is checked or created at registration.
    """
    parser.add_argument("--output", default="-", help=output_help)
    parser.add_argument(
        "--replace-output",
        action="store_true",
        help="Atomically replace an existing --output file.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Write compact deterministic JSON instead of indented JSON.",
    )


def _add_metadata_shape(parser: argparse.ArgumentParser) -> None:
    """
    Declare opt-out flags for related WEMI rows and the legacy metadata projection.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _add_metadata_shape(parser)
        >>> parser.parse_args(["--no-related"]).no_related
        True


    :param parser: Mutable leaf receiving no_related and no_legacy booleans.
    :return: None; both hydrated projections remain enabled by default.
    """
    parser.add_argument(
        "--no-related",
        action="store_true",
        help="Omit related WEMI rows from hydrated metadata.",
    )
    parser.add_argument(
        "--no-legacy",
        action="store_true",
        help="Omit the Calibre-compatible `liuxin` projection.",
    )


def _metadata_get(core: Any, item_id: int, args: argparse.Namespace) -> dict[str, Any]:
    """
    Query one Item's hydrated metadata and require a mapping-shaped receipt.

    Example:
        >>> record = _metadata_get(core, 7, parsed_shape_args)  # doctest: +SKIP


    :param core: Client exposing the metadata.get named query.
    :param item_id: Item selector converted to int without a nonnegative check here.
    :param args: no_related/no_legacy flags inverted into positive include options.
    :return: Shallow string-key copy of the receipt, without status interpretation.
    """
    return _mapping(
        core.query(
            "metadata.get",
            {
                "item_id": int(item_id),
                "include_related": not bool(args.no_related),
                "include_legacy": not bool(args.no_legacy),
            },
        ),
        label="metadata.get result",
    )


def cmd_metadata_show(args: argparse.Namespace) -> int:
    """
    Preflight output, hydrate one Item, and publish its receipt after Core closes.

    Preflight is not a reservation; later query, cleanup, or output errors propagate.

    Example:
        >>> cmd_metadata_show(parsed_metadata_show_args)  # doctest: +SKIP


    :param args: Connection, item_id, metadata-shape, and JSON output controls.
    :return: Zero after output, without inspecting a receipt-level success flag.
    """
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _metadata_get(session.client, int(args.item_id), args)
    _emit_json(result, args)
    return 0


def _read_item_ids_file(path: str | Path) -> list[int]:
    """
    Read bounded BOM-aware UTF-8 Item IDs from a JSON array or line-oriented file.

    A first nonwhitespace '[' selects JSON; each member converts with int(str(value)),
    so JSON booleans/fractional floats are not accepted as IDs. Otherwise ignore
    blank/full-comment lines and int-convert whole remaining lines, rejecting inline
    comments. Preserve order/duplicates/negative values for later selection validation.

    Example:
        >>> ids = _read_item_ids_file("selected-items.txt")  # doctest: +SKIP


    :param path: CLI-host file path, expanded but not resolved absolutely here.
    :return: Parsed integer IDs in input order, possibly empty or duplicated.
    :raises ValueError: Size/JSON/ID conversion fails; line-format errors include line number.
    """
    source = Path(path).expanduser()
    text = _read_control_text(source)
    stripped = text.lstrip()
    if stripped.startswith("["):
        raw = json.loads(text)
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise ValueError("Item id JSON must be an array.")
        return [int(str(value)) for value in raw]
    values: list[int] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        token = raw_line.strip()
        if not token or token.startswith("#"):
            continue
        try:
            values.append(int(token))
        except ValueError as error:
            raise ValueError(
                "Invalid item id on {} line {}: {!r}.".format(
                    source,
                    line_number,
                    token,
                )
            ) from error
    return values


def _read_control_text(path: Path) -> str:
    """
    Read at most 16 MiB of control-file bytes and decode optional UTF-8 BOM text.

    Read one extra byte to detect overflow; no regular-file or concurrent-change
    check is supplied. File and decoding errors propagate.

    Example:
        >>> text = _read_control_text(Path("values.json"))  # doctest: +SKIP


    :param path: Already prepared CLI-host Path to open in binary mode.
    :return: UTF-8-sig decoded content with a leading BOM removed if present.
    :raises ValueError: The byte content exceeds the configured 16-MiB cap.
    """
    with path.open("rb") as stream:
        content = stream.read(_MAX_CONTROL_FILE_BYTES + 1)
    if len(content) > _MAX_CONTROL_FILE_BYTES:
        raise ValueError(
            "Control JSON/id file exceeds the {} byte limit: {!s}".format(
                _MAX_CONTROL_FILE_BYTES,
                path,
            )
        )
    return content.decode("utf-8-sig")


def _dedupe_ids(values: Iterable[int]) -> list[int]:
    """
    Integer-coerce IDs and retain their first occurrences, rejecting negatives.

    Zero is allowed; int conversion semantics also apply to non-integer direct
    inputs such as booleans or floats. No Item existence checks occur.

    Example:
        >>> _dedupe_ids([7, 0, 7, 3])
        [7, 0, 3]


    :param values: Finite iterable of int-convertible Item selectors.
    :return: New nonnegative integer list in first-seen order.
    :raises ValueError: Conversion fails or an integer value is negative.
    """
    result: list[int] = []
    seen: set[int] = set()
    for raw in values:
        value = int(raw)
        if value < 0:
            raise ValueError("Item ids must be non-negative integers.")
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _all_item_ids(core: Any, *, page_size: int) -> list[int]:
    """
    Enumerate Item IDs through ordered rows.query pages and deduplicate afterward.

    Capture total_count from the first page only, defaulting falsey/missing totals
    to zero. Stop on an empty page or offset reaching that captured total; a missing
    total therefore stops after the first page. Prefer record.row_id, falling back
    to values.item_id only for None. Accumulate all IDs before validating/deduping.
    No snapshot, overall item cap, or direct-call page-size validation is added.

    Example:
        >>> ids = _all_item_ids(core, page_size=250)  # doctest: +SKIP


    :param core: Query client serving paged items rows and total_count.
    :param page_size: Requested page length converted to int for each query.
    :return: First-seen nonnegative IDs from the pages actually read.
    :raises TypeError: Page/record/value shape or required ID is missing/invalid.
    """
    offset = 0
    item_ids: list[int] = []
    expected_total: int | None = None
    while True:
        page = _mapping(
            core.query(
                "rows.query",
                {
                    "table": "items",
                    "projection": ["item_id"],
                    "sort": [{"field": "item_id", "ascending": True}],
                    "offset": offset,
                    "limit": int(page_size),
                },
            ),
            label="rows.query result",
        )
        records = page.get("records", ())
        if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
            raise TypeError("rows.query `records` must be an array.")
        if expected_total is None:
            expected_total = int(page.get("total_count") or 0)
        for raw_record in records:
            record = _mapping(raw_record, label="item row")
            row_id = record.get("row_id")
            if row_id is None:
                values = _mapping(record.get("values", {}), label="item values")
                row_id = values.get("item_id")
            if row_id is None:
                raise TypeError("An items row did not contain an item id.")
            item_ids.append(int(row_id))
        offset += len(records)
        if not records or offset >= expected_total:
            break
    return _dedupe_ids(item_ids)


def _selected_item_ids(core: Any, args: argparse.Namespace) -> list[int]:
    """
    Combine explicit IDs and file IDs, or enumerate all Items when requested.

    Read the optional ID file before checking conflicts. --all conflicts with
    nonempty explicit contents, not merely a supplied empty file. Validate positive
    page_size only for all-Items mode; explicit IDs retain argument-before-file order.

    Example:
        >>> _selected_item_ids(None, argparse.Namespace(item_id=[7, 7, 3], item_ids_file=None, all_items=False))
        [7, 3]


    :param core: Client used only when all_items selects paged enumeration.
    :param args: item_id sequence, item_ids_file, all_items, and page_size for enumeration.
    :return: Selected deduplicated nonnegative IDs; all-Items mode may return an empty list.
    :raises ValueError: Selection conflicts/is absent, IDs are invalid, or all-page size is nonpositive.
    """
    explicit = list(args.item_id or [])
    if args.item_ids_file:
        explicit.extend(_read_item_ids_file(args.item_ids_file))
    if args.all_items:
        if explicit:
            raise ValueError("--all cannot be combined with explicit item ids.")
        page_size = int(args.page_size)
        if page_size <= 0:
            raise ValueError("--page-size must be greater than zero.")
        return _all_item_ids(core, page_size=page_size)
    if not explicit:
        raise ValueError(
            "Select records with --all, --item-id, or --item-ids-file."
        )
    return _dedupe_ids(explicit)


def _write_dump(
    stream: BinaryIO,
    *,
    core: Any,
    item_ids: Sequence[int],
    args: argparse.Namespace,
) -> None:
    """
    Fetch metadata per ID and write a dump envelope or raw JSONL records to a stream.

    JSONL always uses compact one-record-per-line JSON without an envelope. Normal
    dumps include format, the supplied sequence length, items, and version. Preserve
    ID order/duplicates supplied directly; this helper neither selects IDs nor owns
    output publication. A later query/serialization failure can leave its stream partial.

    Example:
        >>> _write_dump(stream, core=core, item_ids=[3, 7], args=args)  # doctest: +SKIP


    :param stream: Writable binary destination, normally a staging stream.
    :param core: Client used to hydrate each supplied Item through metadata.get.
    :param item_ids: Finite sized sequence determining order and envelope item_count.
    :param args: compact/json_lines plus no_related/no_legacy metadata-shape options.
    :return: None after writing all selected records and normal-dump framing.
    """
    compact = bool(args.compact)
    if args.json_lines:
        for item_id in item_ids:
            stream.write(_json_bytes(_metadata_get(core, item_id, args), compact=True))
        return

    indent = b"" if compact else b"  "
    separator = b"," if compact else b",\n"
    stream.write(b'{"format":"' + _DUMP_FORMAT.encode("ascii") + b'",')
    if not compact:
        stream.write(b"\n  ")
    stream.write(b'"item_count":' + (b"" if compact else b" "))
    stream.write(str(len(item_ids)).encode("ascii"))
    stream.write(b",")
    if not compact:
        stream.write(b"\n  ")
    stream.write(b'"items":[')
    if item_ids and not compact:
        stream.write(b"\n")
    for index, item_id in enumerate(item_ids):
        if index:
            stream.write(separator)
        payload = json.dumps(
            _metadata_get(core, item_id, args),
            ensure_ascii=True,
            indent=None if compact else 2,
            separators=(",", ":") if compact else None,
            sort_keys=True,
        )
        if compact:
            stream.write(payload.encode("utf-8"))
        else:
            stream.write(indent)
            stream.write(payload.replace("\n", "\n  ").encode("utf-8"))
    if item_ids and not compact:
        stream.write(b"\n  ")
    stream.write(b"],")
    if not compact:
        stream.write(b"\n  ")
    stream.write(b'"version":' + (b"" if compact else b" "))
    stream.write(str(_DUMP_VERSION).encode("ascii"))
    stream.write(b"}\n")


def cmd_metadata_dump_json(args: argparse.Namespace) -> int:
    """
    Select Item IDs and publish a complete JSON/JSONL dump from a staged stream.

    Preflight output before opening Core; enumerate/read selection inside its
    context. Publication also occurs inside that context, so '-' is emitted to
    redirected stderr, not the outer stdout. File publication can succeed before
    later session cleanup fails. Query/serialization errors before publication
    discard staging; the operation does not promise a stable catalogue snapshot.

    Example:
        >>> cmd_metadata_dump_json(parsed_dump_args)  # doctest: +SKIP


    :param args: Connection, ID selection/paging, metadata shape, JSONL/compact,
        and output/replacement controls.
    :return: Zero after dump publication and Core session exit complete.
    """
    _ensure_output_available(
        args.output,
        replace=bool(args.replace_output),
    )
    with _open_metadata_core(args) as session:
        item_ids = _selected_item_ids(session.client, args)
        with _atomic_binary_output(
            args.output,
            replace=bool(args.replace_output),
        ) as stream:
            _write_dump(
                stream,
                core=session.client,
                item_ids=item_ids,
                args=args,
            )
    return 0


def _load_json_object(*, inline: str | None, path: str | None, label: str) -> dict[str, Any]:
    """
    Load a JSON mapping from inline text or a bounded local file, defaulting empty.

    Any non-None inline value wins over path, including empty text that fails
    decoding. Only file input has the 16-MiB cap; no recursive schema is validated.

    Example:
        >>> _load_json_object(inline='{"tags": ["history"]}', path=None, label="values")
        {'tags': ['history']}


    :param inline: JSON source text taking precedence when not None.
    :param path: CLI-host JSON selector used only when inline is None.
    :param label: Context passed to the mapping-shape validator.
    :return: Shallow string-key object, or a fresh empty dict if no input was selected.
    """
    if inline is not None:
        raw = json.loads(inline)
    elif path is not None:
        raw = json.loads(_read_control_text(Path(path).expanduser()))
    else:
        return {}
    return _mapping(raw, label=label)


def _normal_field(value: str) -> str:
    """
    Canonicalize an explicitly selected writable metadata field or reject it.

    Strip/lowercase and replace hyphens with underscores before alias lookup.
    Only the finite tags/labels/genre/subject/series/identifiers family is supported.

    Example:
        >>> [_normal_field(value) for value in (" TAG ", "genres", "identifier")]
        ['tags', 'genre', 'identifiers']


    :param value: Field token to stringify and normalize.
    :return: Canonical writable field key used in metadata.write payloads.
    :raises ValueError: The normalized spelling is not a registered write alias.
    """
    token = str(value).strip().lower().replace("-", "_")
    try:
        return _WRITE_FIELDS[token]
    except KeyError as error:
        raise ValueError("Unsupported writable metadata field: {!r}.".format(value)) from error


def _extract_write_values(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Extract supported write fields from a record, inspect report, or one-Item dump.

    A recognized dump format recursively unwraps exactly one mapping Item, without
    checking dump version/count metadata. Otherwise inspect top-level, metadata,
    then liuxin mappings; later aliases overwrite earlier canonical values. Unknown
    fields are ignored and accepted values remain unvalidated/shared. No recursion
    depth or cyclic-object protection is provided for direct callers.

    Example:
        >>> _extract_write_values({"tags": ["outer"], "metadata": {"tag": ["inner"], "title": "ignored"}})
        {'tags': ['inner']}


    :param raw: Mapping containing plain or wrapped metadata values.
    :return: New canonical-field dictionary retaining selected values by reference.
    :raises ValueError: A recognized dump does not contain exactly one mapping Item.
    """
    if raw.get("format") == _DUMP_FORMAT:
        records = raw.get("items")
        if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
            raise ValueError("Metadata dump `items` must be an array.")
        if len(records) != 1 or not isinstance(records[0], Mapping):
            raise ValueError(
                "A metadata dump used for one write must contain exactly one Item."
            )
        return _extract_write_values(records[0])
    candidates: list[Mapping[str, Any]] = [raw]
    embedded = raw.get("metadata")
    if isinstance(embedded, Mapping):
        candidates.append(embedded)
    legacy = raw.get("liuxin")
    if isinstance(legacy, Mapping):
        candidates.append(legacy)
    values: dict[str, Any] = {}
    for candidate in candidates:
        for key, value in candidate.items():
            token = str(key).strip().lower().replace("-", "_")
            canonical = _WRITE_FIELDS.get(token)
            if canonical is not None:
                values[canonical] = value
    return values


def _identifiers(values: Sequence[str]) -> dict[str, str | list[str]]:
    """
    Parse SCHEME=VALUE selectors, retaining repeat schemes as ordered value lists.

    Split only on the first equals sign, lowercase/strip schemes, and strip values.
    Repeated identical values remain duplicated; one entry returns a scalar string.

    Example:
        >>> _identifiers([" DOI = first=part ", "doi=second", "isbn=123"])
        {'doi': ['first=part', 'second'], 'isbn': '123'}


    :param values: Ordered identifier selectors to stringify and parse.
    :return: Scheme mapping whose values are scalars or repeat-preserving lists.
    :raises ValueError: A selector lacks '=', a nonempty scheme, or a nonempty value.
    """
    collected: dict[str, list[str]] = {}
    for raw in values:
        scheme, separator, value = str(raw).partition("=")
        scheme = scheme.strip().lower()
        value = value.strip()
        if not separator or not scheme or not value:
            raise ValueError(
                "Identifiers must use non-empty SCHEME=VALUE syntax: {!r}.".format(raw)
            )
        collected.setdefault(scheme, []).append(value)
    return {
        scheme: entries[0] if len(entries) == 1 else entries
        for scheme, entries in collected.items()
    }


def _build_write_values(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    """
    Merge writable JSON fields, convenience values, clear requests, and field selection.

    Loaded JSON is projected to supported fields. Truthy convenience lists replace
    those fields; identifiers use scheme grouping. Clear requests run last and need
    replace, setting identifiers to {} and other fields to []. Explicit fields are
    canonicalized/deduped in order, or inferred from values. Unselected values remain
    in the returned mapping; Core receives a separate selected-fields list.

    Example:
        >>> values, fields = _build_write_values(parsed_metadata_set_args)  # doctest: +SKIP


    :param args: values_json/values_file, tag/label/genre/subject/series/identifier,
        clear, replace, and optional field selections.
    :return: Canonical values dictionary and ordered unique selected-field list.
    :raises ValueError: Clear lacks replacement, selection is empty, a field is
        unsupported, or selected fields have no corresponding supplied value.
    """
    raw = _load_json_object(
        inline=args.values_json,
        path=args.values_file,
        label="metadata values",
    )
    values = _extract_write_values(raw)
    for field, argument in (
        ("tags", args.tag),
        ("labels", args.label),
        ("genre", args.genre),
        ("subject", args.subject),
        ("series", args.series),
    ):
        if argument:
            values[field] = list(argument)
    if args.identifier:
        values["identifiers"] = _identifiers(args.identifier)

    clear_fields = [_normal_field(value) for value in (args.clear or [])]
    if clear_fields and not args.replace:
        raise ValueError("--clear requires --replace so the empty value is authoritative.")
    for field in clear_fields:
        values[field] = {} if field == "identifiers" else []

    fields = (
        [_normal_field(value) for value in args.field]
        if args.field
        else list(values)
    )
    fields = list(dict.fromkeys(fields))
    if not fields:
        raise ValueError(
            "No writable values were supplied. Use convenience flags, --values-file, "
            "or --values-json."
        )
    missing = [field for field in fields if field not in values]
    if missing:
        raise ValueError(
            "Selected field(s) missing from supplied values: {}.".format(
                ", ".join(missing)
            )
        )
    return values, fields


def cmd_metadata_set(args: argparse.Namespace) -> int:
    """
    Prepare field-level catalogue writes and publish the Core mutation receipt.

    Build values before checking output, then preflight before dispatch. Preserve
    explicit field/kind/target-level policy and invert no_mark_dirty. A mapping
    receipt is required, but no changed/ok flag controls exit status. Accepted writes
    are not undone if later session cleanup or report publication fails.

    Example:
        >>> cmd_metadata_set(parsed_metadata_set_args)  # doctest: +SKIP


    :param args: Connection, item_id, write-value/selection policy, kind, target_level,
        no_mark_dirty, and JSON output options.
    :return: Zero after metadata.write receipt publication, with exceptions propagating.
    """
    values, fields = _build_write_values(args)
    _ensure_json_output(args)
    payload = {
        "item_id": int(args.item_id),
        "values": values,
        "fields": fields,
        "kind": args.kind,
        "replace": bool(args.replace),
        "target_level": args.target_level,
        "mark_dirty": not bool(args.no_mark_dirty),
    }
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.command("metadata.write", payload),
            label="metadata.write result",
        )
    _emit_json(result, args)
    return 0


def cmd_metadata_export_opf(args: argparse.Namespace) -> int:
    """
    Fetch an Item's OPF bytes and publish them after the Core session exits.

    Preflight before querying, require a mapping receipt, and decode raw/wire bytes.
    The CLI does not validate XML or compare the exported metadata to catalogue rows.

    Example:
        >>> cmd_metadata_export_opf(parsed_opf_export_args)  # doctest: +SKIP


    :param args: Connection, item_id, default_lang, output path/'-', and replace_output.
    :return: Zero after binary publication; query/decoding/publication failures raise.
    """
    _ensure_output_available(
        args.output,
        replace=bool(args.replace_output),
    )
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query(
                "metadata.opf.export",
                {"item_id": int(args.item_id), "default_lang": args.default_lang},
            ),
            label="metadata.opf.export result",
        )
    _emit_bytes(
        args.output,
        _wire_bytes(result.get("content"), label="OPF export content"),
        replace=bool(args.replace_output),
    )
    return 0


def _transfer_limit(args: argparse.Namespace) -> int:
    """
    Convert a positive MiB transfer setting into an integer byte limit by truncation.

    Values just above zero can truncate to zero bytes. Nonfinite numbers have no
    dedicated validation: NaN/infinity fail through int conversion instead.

    Example:
        >>> _transfer_limit(argparse.Namespace(max_transfer_mib=1.5))
        1572864


    :param args: Namespace with max_transfer_mib accepted by float conversion.
    :return: Truncated byte count using 1,048,576 bytes per MiB.
    :raises ValueError: The value is nonpositive, invalid, or NaN at integer conversion.
    :raises OverflowError: Infinity cannot convert to an integer byte count.
    """
    value = float(args.max_transfer_mib)
    if value <= 0:
        raise ValueError("--max-transfer-mib must be greater than zero.")
    return int(value * 1024 * 1024)


def _read_bounded_file(path: str | Path, *, limit: int) -> tuple[Path, bytes, os.stat_result]:
    """
    Read a regular CLI-host file within a byte cap and compare before/after file stats.

    Expand without resolving the path, follow symlinks when opening, then fstat
    the descriptor. Reject oversize input before reading and read one extra byte
    for growth detection. Compare final size/mtime and observed length, not a hash,
    lock, or pathname identity; same-size/mtime edits can escape detection. Opening
    precedes the regular-file check and can therefore block on special files.

    Example:
        >>> source, content, before = _read_bounded_file(path, limit=1024)  # doctest: +SKIP


    :param path: CLI-host input path, returned with user expansion only.
    :param limit: Byte ceiling, normally supplied by the validated transfer setting.
    :return: Expanded Path, complete bytes, and initial descriptor stat result.
    :raises ValueError: The opened object is not regular or exceeds the byte ceiling.
    :raises RuntimeError: Size/mtime/length observations indicate a concurrent edit.
    """
    source = Path(path).expanduser()
    with source.open("rb") as stream:
        details = os.fstat(stream.fileno())
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("Metadata input must be a regular file: {!s}".format(source))
        if details.st_size > limit:
            raise ValueError(
                "Input is {} bytes; the transfer limit is {} bytes.".format(
                    details.st_size,
                    limit,
                )
            )
        content = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if len(content) > limit:
        raise ValueError("Input exceeded the transfer limit while being read.")
    if (
        after.st_size != details.st_size
        or after.st_mtime_ns != details.st_mtime_ns
        or len(content) != after.st_size
    ):
        raise RuntimeError("Input changed while its metadata bytes were being read.")
    return source, content, details


def _file_type(path: Path, explicit: str | None) -> str:
    """
    Normalize a format override or fall back to the input's final suffix.

    Strip/lowercase the explicit spelling and remove all leading dots. A resulting
    empty override falls back to suffix; no plugin support or file signature is checked.

    Example:
        >>> _file_type(Path("book.EPUB"), None), _file_type(Path("book"), " ..MOBI ")
        ('epub', 'mobi')


    :param path: Input path supplying a final suffix when the override is empty.
    :param explicit: Optional format token, with or without leading dots.
    :return: Nonempty lowercase format token, without leading dots.
    :raises ValueError: Neither override nor suffix supplies a token.
    """
    value = str(explicit or "").strip().lower().lstrip(".")
    if not value:
        value = path.suffix.lower().lstrip(".")
    if not value:
        raise ValueError("Provide --file-type when the input has no extension.")
    return value


def _file_payload(content: bytes, file_type: str) -> dict[str, str]:
    """
    Build a metadata-file request from base64 bytes and a format token, never a path.

    This request shape is not the '$type' response wire envelope.

    Example:
        >>> _file_payload(b"abc", "epub")
        {'base64': 'YWJj', 'file_type': 'epub'}


    :param content: Complete binary input encoded without an additional size check.
    :param file_type: Format spelling forwarded unchanged for Core interpretation.
    :return: Fresh base64/file_type dictionary suitable for a metadata file operation.
    """
    return {
        "base64": base64.b64encode(content).decode("ascii"),
        "file_type": file_type,
    }


def cmd_metadata_file_formats(args: argparse.Namespace) -> int:
    """
    Query enabled metadata readers/writers and publish the mapping-shaped receipt.

    Preflight output before opening Core; the query omits a payload argument.

    Example:
        >>> cmd_metadata_file_formats(parsed_file_formats_args)  # doctest: +SKIP


    :param args: Core connection selectors and JSON output controls.
    :return: Zero after receipt publication, without interpreting individual capabilities.
    """
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query("metadata.file.formats"),
            label="metadata.file.formats result",
        )
    _emit_json(result, args)
    return 0


def cmd_metadata_file_inspect(args: argparse.Namespace) -> int:
    """
    Transfer bounded CLI-host bytes to Core for metadata inspection and report their origin.

    Preflight output, read/check bytes, and resolve file type before opening Core.
    No input path is sent to the daemon. After exit add/overwrite source_path and
    size in the shallow-copied receipt; the input is never rewritten by this handler.

    Example:
        >>> cmd_metadata_file_inspect(parsed_file_inspect_args)  # doctest: +SKIP


    :param args: Connection, local path, file_type override, max_transfer_mib, and JSON controls.
    :return: Zero after publishing inspection data plus CLI-host source path/byte count.
    """
    _ensure_json_output(args)
    source, content, _details = _read_bounded_file(
        args.path,
        limit=_transfer_limit(args),
    )
    file_type = _file_type(source, args.file_type)
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query(
                "metadata.file.inspect",
                _file_payload(content, file_type),
            ),
            label="metadata.file.inspect result",
        )
    result["source_path"] = str(source)
    result["size"] = len(content)
    _emit_json(result, args)
    return 0


def _metadata_file_write_payload(
    args: argparse.Namespace,
    *,
    content: bytes,
    file_type: str,
) -> dict[str, Any]:
    """
    Attach catalogue Item selection or decoded metadata to a file-byte request.

    Non-None item_id wins even if JSON selectors are also supplied directly. Else
    load an object and unwrap its mapping-valued metadata field when present,
    discarding other wrapper keys. No writable-field filtering is applied here.

    Example:
        >>> _metadata_file_write_payload(argparse.Namespace(item_id=0), content=b"a", file_type="epub")
        {'base64': 'YQ==', 'file_type': 'epub', 'item_id': 0}


    :param args: item_id or metadata_json/metadata_file selectors.
    :param content: Original CLI-host file bytes to encode.
    :param file_type: Format token forwarded to Core unchanged.
    :return: New byte request containing either integer item_id or a metadata object.
    """
    payload: dict[str, Any] = _file_payload(content, file_type)
    if args.item_id is not None:
        payload["item_id"] = int(args.item_id)
    else:
        metadata = _load_json_object(
            inline=args.metadata_json,
            path=args.metadata_file,
            label="embedded metadata",
        )
        inspected = metadata.get("metadata")
        payload["metadata"] = (
            dict(inspected)
            if isinstance(inspected, Mapping)
            else metadata
        )
    return payload


def _same_path(first: Path, second: Path) -> bool:
    """
    Compare non-strict resolved paths, not inode identity or file contents.

    Existing symlinks are resolved, but distinct hard-link names remain unequal.

    Example:
        >>> _same_path(Path("folder/../file"), Path("file"))
        True


    :param first: First pathname, possibly nonexistent.
    :param second: Second pathname to resolve against the same process cwd.
    :return: Whether the resolved Path values compare equal.
    """
    return first.resolve(strict=False) == second.resolve(strict=False)


def _assert_source_unchanged(source: Path, expected: os.stat_result) -> None:
    """
    Reject a symlink or changed device/inode/size/mtime before in-place publication.

    This point-in-time check does not lock the file, compare bytes, or prevent a
    later race while backup/staging/publication proceeds. Other stat errors propagate.

    Example:
        >>> _assert_source_unchanged(source, original_stat)  # doctest: +SKIP


    :param source: Original expanded input path to inspect again.
    :param expected: Initial descriptor stat from the bounded read.
    :return: None if all four identity/change fields still match and no symlink is observed.
    :raises RuntimeError: A symlink or mismatched tracked stat field is observed.
    """
    if source.is_symlink():
        raise RuntimeError("Input became a symbolic link before publication.")
    current = source.stat()
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(current, name) != getattr(expected, name) for name in identity):
        raise RuntimeError(
            "Input changed while rewritten metadata was being staged; refusing "
            "the in-place replacement."
        )


def _validate_file_write_destinations(
    args: argparse.Namespace,
    *,
    source: Path,
) -> Path | None:
    """
    Preflight artifact, backup, and report destinations under the selected write mode.

    Check report output first. In-place mode rejects artifact replacement flags,
    incompatible backup flags, and same-path backups; default backup is input plus
    suffix. New-artifact mode rejects backup controls, stdout artifacts, and resolved
    source equality. Nonstdout reports must differ from every protected path.
    Equality is resolved-path equality, not hard-link identity; no check reserves
    a path or guarantees later writability. backup_suffix alone is ignored outside
    in-place mode, and direct callers still rely on parser-level mode exclusivity.

    Example:
        >>> backup = _validate_file_write_destinations(args, source=source)  # doctest: +SKIP


    :param args: Artifact mode/output, backup options, report output, and replacement flags.
    :param source: Expanded input path used for collision comparisons.
    :return: Expanded backup Path for enabled in-place backup, otherwise None.
    :raises ValueError: Modes/options conflict or protected resolved paths collide.
    """
    report_output = str(args.report_output)
    _ensure_output_available(
        report_output,
        replace=bool(args.replace_report),
    )
    backup_path: Path | None = None
    protected = [source]
    if args.in_place:
        if args.replace_output:
            raise ValueError("--replace-output is valid only with --output.")
        if args.no_backup and (args.backup or args.replace_backup):
            raise ValueError(
                "--no-backup cannot be combined with --backup or --replace-backup."
            )
        if not args.no_backup:
            backup_path = Path(
                args.backup or (str(source) + str(args.backup_suffix))
            ).expanduser()
            if _same_path(source, backup_path):
                raise ValueError("Backup path must differ from the input path.")
            _ensure_output_available(
                backup_path,
                replace=bool(args.replace_backup),
            )
            protected.append(backup_path)
    else:
        if args.backup or args.no_backup or args.replace_backup:
            raise ValueError("Backup options are valid only with --in-place.")
        destination = Path(args.output).expanduser()
        if str(args.output) == "-":
            raise ValueError("Embedded-file --output must be a filesystem path.")
        if _same_path(source, destination):
            raise ValueError("Use --in-place for an unmanaged write to the input path.")
        _ensure_output_available(
            destination,
            replace=bool(args.replace_output),
        )
        protected.append(destination)

    if report_output != "-":
        report_path = Path(report_output).expanduser()
        for path in protected:
            if _same_path(report_path, path):
                raise ValueError(
                    "--report-output must differ from every input, artifact, and backup path."
                )
    return backup_path


def cmd_metadata_file_write(args: argparse.Namespace) -> int:
    """
    Ask Core to rewrite CLI-host bytes, re-inspect them, and publish local outputs.

    Bound/check input, reject in-place source symlinks, resolve type, and preflight
    destinations before opening Core. Build the metadata payload inside the session,
    command a rewrite, bound decoded output, and inspect it again. Verification here
    means a mapping-shaped inspect response was obtained, not that desired metadata
    was compared or an ok flag checked. Core receives bytes, never the source path.

    After session exit, in-place mode checks source stats once, optionally publishes
    original-byte backup, then replaces the source. New-artifact mode publishes only
    the output. Both preserve input permission bits, not all metadata. Finally emit
    the JSON report. These publications are independent: later source/report errors
    do not undo a backup/artifact, and the source check leaves a race window. This
    handler does not update managed Replica bookkeeping; in-place is for unmanaged files.

    Example:
        >>> cmd_metadata_file_write(parsed_file_write_args)  # doctest: +SKIP


    :param args: Connection, CLI-host path, format/transfer settings, metadata source,
        artifact or in-place mode, backup policy, and report publication controls.
    :return: Zero after all requested outputs, even if the rewrite's updated flag is false;
        failures propagate without a workflow-wide rollback.
    """
    limit = _transfer_limit(args)
    source, original, details = _read_bounded_file(args.path, limit=limit)
    if args.in_place and source.is_symlink():
        raise ValueError("Unmanaged --in-place write refuses symbolic links.")
    file_type = _file_type(source, args.file_type)
    backup_path = _validate_file_write_destinations(args, source=source)

    with _open_metadata_core(args) as session:
        write_result = _mapping(
            session.client.command(
                "metadata.file.write",
                _metadata_file_write_payload(
                    args,
                    content=original,
                    file_type=file_type,
                ),
            ),
            label="metadata.file.write result",
        )
        updated = _wire_bytes(
            write_result.get("content"),
            label="metadata.file.write content",
        )
        if len(updated) > limit:
            raise ValueError(
                "Updated artifact is {} bytes; the transfer limit is {} bytes.".format(
                    len(updated),
                    limit,
                )
            )
        verified = _mapping(
            session.client.query(
                "metadata.file.inspect",
                _file_payload(updated, file_type),
            ),
            label="metadata.file.inspect verification result",
        )

    if args.in_place:
        _assert_source_unchanged(source, details)
        if backup_path is not None:
            _emit_bytes(
                backup_path,
                original,
                replace=bool(args.replace_backup),
                mode=details.st_mode,
            )
        _emit_bytes(source, updated, replace=True, mode=details.st_mode)
        destination = source
    else:
        destination = Path(args.output).expanduser()
        _emit_bytes(
            destination,
            updated,
            replace=bool(args.replace_output),
            mode=details.st_mode,
        )

    report = {
        "backup_path": None if backup_path is None else str(backup_path),
        "file_type": file_type,
        "input_path": str(source),
        "output_path": str(destination),
        "size": len(updated),
        "unmanaged_in_place": bool(args.in_place),
        "updated": bool(write_result.get("updated", False)),
        "verified": True,
        "written_metadata": verified.get("metadata"),
    }
    report_args = argparse.Namespace(
        output=args.report_output,
        replace_output=args.replace_report,
        compact=args.compact,
    )
    _emit_json(report, report_args)
    return 0


def cmd_metadata_online_sources(args: argparse.Namespace) -> int:
    """
    Publish configured online metadata source capabilities without submitting a job.

    Preflight output, query without a payload argument, require a mapping, and
    leave Core before emitting JSON. No live-source reachability is tested locally.

    Example:
        >>> cmd_metadata_online_sources(parsed_online_sources_args)  # doctest: +SKIP


    :param args: Core selectors and JSON publication controls.
    :return: Zero after publishing metadata.online.sources' receipt.
    """
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query("metadata.online.sources"),
            label="metadata.online.sources result",
        )
    _emit_json(result, args)
    return 0


def _query_identifiers(values: Sequence[str]) -> dict[str, str]:
    """
    Parse online identifier selectors while rejecting every repeated scheme.

    Repeating the same value still counts as a duplicate; scheme normalization
    and first-equals splitting follow the catalogue identifier helper.

    Example:
        >>> _query_identifiers([" ISBN = 123 "])
        {'isbn': '123'}


    :param values: Ordered SCHEME=VALUE query selectors, with one value per scheme.
    :return: Normalized scheme-to-string query dictionary.
    :raises ValueError: A selector is malformed or any normalized scheme repeats.
    """
    parsed = _identifiers(values)
    duplicates = [key for key, value in parsed.items() if isinstance(value, list)]
    if duplicates:
        raise ValueError(
            "Online queries accept one value per identifier scheme: {}.".format(
                ", ".join(duplicates)
            )
        )
    return {key: str(value) for key, value in parsed.items()}


def _wait_for_job(
    core: Any,
    submission: Mapping[str, Any],
    *,
    wait_timeout: float | None,
    poll_interval: float,
) -> dict[str, Any]:
    """
    Poll a metadata job, then require a successful mapping-shaped execution result.

    Stringify the submitted job ID without stripping; whitespace IDs remain valid
    locally. Poll exact case-sensitive terminal states, excluding other spellings
    such as aborted. Terminal detection precedes the local deadline check. Sleep
    between nonterminal polls with a 0.01-second floor; timeout raises without
    cancellation. Fetch jobs.result with timeout_s=0.0 and require truthy execution.ok.
    Missing execution/result defaults become empty mappings; result.ok is not checked.

    Example:
        >>> completed = _wait_for_job(core, submission, wait_timeout=60.0, poll_interval=0.2)  # doctest: +SKIP


    :param core: Client serving jobs.get and jobs.result named queries.
    :param submission: Mapping whose truthy job_id is stringified for subsequent requests.
    :param wait_timeout: Local polling deadline in seconds, or None for no deadline.
    :param poll_interval: Nonterminal sleep interval in seconds, floored at 0.01.
    :return: job_id/result/state projection with a shallow string-key result mapping.
    :raises RuntimeError: Submission lacks an ID or execution does not report success.
    :raises TimeoutError: A nonterminal job exceeds the local wait deadline.
    """
    job_id = str(submission.get("job_id") or "")
    if not job_id:
        raise RuntimeError("Core job submission did not return a job id.")
    started = time.monotonic()
    while True:
        job_result = _mapping(
            core.query("jobs.get", {"job_id": job_id}),
            label="jobs.get result",
        )
        job = _mapping(job_result.get("job", {}), label="jobs.get job")
        state = str(job.get("state") or "")
        if state in _TERMINAL_JOB_STATES:
            break
        if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
            raise TimeoutError(
                "Timed out waiting for metadata job {}; it was not cancelled.".format(job_id)
            )
        time.sleep(max(0.01, poll_interval))

    completed = _mapping(
        core.query("jobs.result", {"job_id": job_id, "timeout_s": 0.0}),
        label="jobs.result result",
    )
    execution = _mapping(completed.get("execution", {}), label="job execution")
    if not bool(execution.get("ok", False)):
        raise RuntimeError(str(execution.get("traceback") or "Metadata job failed."))
    result = _mapping(execution.get("result", {}), label="metadata job result")
    return {"job_id": job_id, "result": result, "state": state}


def _online_payload(args: argparse.Namespace) -> dict[str, Any]:
    """
    Validate positive timeout/poll settings and build an online metadata job request.

    <=0 checks do not reject NaN/infinity explicitly. Copy authors and optional
    plugin selections without normalization/deduplication; no nonempty search
    criterion is required. Source/job timeouts are sent to Core, but local wait/poll
    settings are only checked here and omitted from the request.

    Example:
        >>> payload = _online_payload(parsed_online_identify_args)  # doctest: +SKIP


    :param args: title, author, identifier, optional plugin, source_timeout,
        job_timeout, wait_timeout, and poll_interval settings.
    :return: New title/authors/identifiers/timeout_s payload with optional plugins/job timeout.
    :raises ValueError: A checked duration is nonpositive or identifier parsing fails.
    """
    if float(args.source_timeout) <= 0:
        raise ValueError("--source-timeout must be greater than zero.")
    if args.job_timeout is not None and float(args.job_timeout) <= 0:
        raise ValueError("--job-timeout must be greater than zero.")
    if args.wait_timeout is not None and float(args.wait_timeout) <= 0:
        raise ValueError("--wait-timeout must be greater than zero.")
    if float(args.poll_interval) <= 0:
        raise ValueError("--poll-interval must be greater than zero.")
    payload: dict[str, Any] = {
        "title": args.title,
        "authors": list(args.author or []),
        "identifiers": _query_identifiers(args.identifier or []),
        "timeout_s": float(args.source_timeout),
    }
    if getattr(args, "plugin", None):
        payload["allowed_plugins"] = list(args.plugin)
    if args.job_timeout is not None:
        payload["job_timeout_s"] = float(args.job_timeout)
    return payload


def _run_online_job(args: argparse.Namespace, operation: str) -> dict[str, Any]:
    """
    Open metadata Core, build/submit an online request, and detach or wait within the session.

    Payload validation occurs after opening Core, even for detached requests.
    Detach returns the shallow-copied submission without requiring a job ID.
    Waiting follows the dedicated metadata polling policy, not common job helpers.
    Cleanup failures propagate; no accepted job is cancelled on local failure.

    Example:
        >>> result = _run_online_job(args, "metadata.identify.start")  # doctest: +SKIP


    :param args: Connection selectors, online query settings, and detach/wait controls.
    :param operation: Exact Core job-start command to execute with the built payload.
    :return: detached/submission envelope or completed job_id/result/state projection.
    """
    with _open_metadata_core(args) as session:
        submission = _mapping(
            session.client.command(operation, _online_payload(args)),
            label="metadata job submission",
        )
        if args.detach:
            return {"detached": True, "submission": submission}
        return _wait_for_job(
            session.client,
            submission,
            wait_timeout=args.wait_timeout,
            poll_interval=float(args.poll_interval),
        )


def cmd_metadata_online_identify(args: argparse.Namespace) -> int:
    """
    Preflight JSON output, submit identification, and publish its detached/completed result.

    Source/plugin calls run through Core, not as direct requests from this handler.
    Local timeout/job errors raise; successful publication returns zero without
    interpreting nested search-result flags or whether any candidate was found.

    Example:
        >>> cmd_metadata_online_identify(parsed_identify_args)  # doctest: +SKIP


    :param args: Connection, identification/query/job policy, and JSON publication controls.
    :return: Zero after publishing the metadata.identify.start result envelope.
    """
    _ensure_json_output(args)
    result = _run_online_job(args, "metadata.identify.start")
    _emit_json(result, args)
    return 0


def cmd_metadata_online_cover(args: argparse.Namespace) -> int:
    """
    Submit cover discovery and optionally publish discovered bytes separately from JSON.

    Preflight JSON first; reject detached binary output or replace-cover without a
    destination, then preflight the cover path. For completed mapping-shaped covers,
    decode/pop content, publish bytes, and replace content in the report with path
    and size. Missing/non-mapping cover data produces no artifact. There is no local
    cover-size/format validation or cross-output path-collision check. '-' is accepted
    by the shared byte emitter, so cover bytes and JSON can target the same stream.
    Report failure does not retract a published cover or cancel the accepted job.

    Example:
        >>> cmd_metadata_online_cover(parsed_cover_args)  # doctest: +SKIP


    :param args: Connection/query/job controls, cover_output/replace_cover_output,
        and JSON output/replacement settings.
    :return: Zero after requested publications, without requiring a found cover.
    """
    _ensure_json_output(args)
    if args.detach and args.cover_output:
        raise ValueError("--cover-output cannot be used with --detach.")
    if args.replace_cover_output and not args.cover_output:
        raise ValueError("--replace-cover-output requires --cover-output.")
    if args.cover_output:
        _ensure_output_available(
            args.cover_output,
            replace=bool(args.replace_cover_output),
        )
    result = _run_online_job(args, "metadata.covers.start")
    if not args.detach and args.cover_output:
        job_result = _mapping(result.get("result", {}), label="cover job result")
        cover_raw = job_result.get("cover")
        if isinstance(cover_raw, Mapping):
            cover = dict(cover_raw)
            content = _wire_bytes(cover.pop("content", None), label="cover content")
            _emit_bytes(
                args.cover_output,
                content,
                replace=bool(args.replace_cover_output),
            )
            cover["content_path"] = str(Path(args.cover_output).expanduser())
            cover["size"] = len(content)
            job_result["cover"] = cover
            result["result"] = job_result
    _emit_json(result, args)
    return 0


def _catalogue_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """
    Register metadata show/get, dump-json/dump, set/write, and export-opf leaves.

    Declare shape, paging, write aliases, field policy, and output controls without
    accessing Core/files. Dump selector conflicts, positive ranges, and clear/replace
    rules are handler checks; inline/file values are parser-mutually-exclusive.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _catalogue_parsers(parser.add_subparsers())
        >>> args = parser.parse_args(["set", "7", "--tag", "history"])
        >>> args.item_id, args.target_level, args.replace
        (7, 'work', False)


    :param subparsers: Metadata action collection receiving catalogue-oriented leaves.
    :return: None; arguments and handler defaults are added in place.
    """
    show = subparsers.add_parser(
        "show",
        aliases=["get"],
        help="Read one hydrated WEMI metadata record as JSON.",
    )
    _add_connection(show)
    show.add_argument("item_id", type=int, help="Catalogue Item id.")
    _add_metadata_shape(show)
    _add_json_output(show)
    show.set_defaults(handler=cmd_metadata_show)

    dump = subparsers.add_parser(
        "dump-json",
        aliases=["dump"],
        help="Dump selected or all Item metadata to deterministic JSON.",
    )
    _add_connection(dump)
    dump.add_argument("--all", action="store_true", dest="all_items", help="Dump every Item.")
    dump.add_argument("--item-id", action="append", type=int, default=[], help="Item id to dump (repeatable).")
    dump.add_argument("--item-ids-file", help="UTF-8 JSON array or newline-separated item ids.")
    dump.add_argument("--page-size", type=int, default=250, help="Item enumeration page size (default: 250).")
    dump.add_argument("--json-lines", action="store_true", help="Write one raw metadata object per line.")
    _add_metadata_shape(dump)
    _add_json_output(dump)
    dump.set_defaults(handler=cmd_metadata_dump_json)

    write = subparsers.add_parser(
        "set",
        aliases=["write"],
        help="Append or authoritatively replace writable catalogue metadata.",
    )
    _add_connection(write)
    write.add_argument("item_id", type=int, help="Catalogue Item id used to resolve the WEMI stack.")
    source = write.add_mutually_exclusive_group()
    source.add_argument("--values-file", help="JSON object or metadata-show JSON file.")
    source.add_argument("--values-json", help="Inline JSON object.")
    write.add_argument("--field", action="append", default=[], help="Writable field selected from JSON (repeatable).")
    write.add_argument("--tag", action="append", default=[], help="Tag value (repeatable).")
    write.add_argument("--label", action="append", default=[], help="Label value (repeatable).")
    write.add_argument("--genre", action="append", default=[], help="Genre value (repeatable).")
    write.add_argument("--subject", action="append", default=[], help="Subject value (repeatable).")
    write.add_argument("--series", action="append", default=[], help="Series value (repeatable).")
    write.add_argument("--identifier", action="append", default=[], metavar="SCHEME=VALUE", help="Identifier (repeatable; schemes may repeat).")
    write.add_argument("--clear", action="append", default=[], metavar="FIELD", help="Clear a writable field; requires --replace.")
    write.add_argument("--replace", action="store_true", help="Replace selected fields instead of appending values.")
    write.add_argument("--kind", choices=("liuxin", "liuxin-wemi", "calibre"), default="liuxin")
    write.add_argument("--target-level", choices=("work", "expression", "manifestation", "item"), default="work")
    write.add_argument("--no-mark-dirty", action="store_true", help="Do not enqueue the affected WEMI row for downstream metadata work.")
    _add_json_output(write)
    write.set_defaults(handler=cmd_metadata_set)

    opf = subparsers.add_parser("export-opf", help="Export one Item's hydrated metadata as OPF XML.")
    _add_connection(opf)
    opf.add_argument("item_id", type=int, help="Catalogue Item id.")
    opf.add_argument("--default-lang", help="Fallback OPF language code.")
    opf.add_argument("--output", required=True, help="OPF output path, or '-' for stdout.")
    opf.add_argument("--replace-output", action="store_true", help="Atomically replace an existing output file.")
    opf.set_defaults(handler=cmd_metadata_export_opf)


def _file_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """
    Register file capability, inspect/dump-json, and rewrite metadata leaves.

    Rewrite requires one artifact/in-place mode and one Item/JSON metadata source.
    Transfer defaults to 512 MiB; backup/report controls are declared independently
    and validated later. Construction neither reads an ebook nor prepares output.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _file_parsers(parser.add_subparsers())
        >>> args = parser.parse_args(["file", "write", "book.epub", "--in-place", "--item-id", "7"])
        >>> args.backup_suffix, args.no_backup, args.max_transfer_mib
        ('.bak', False, 512.0)


    :param subparsers: Metadata action collection receiving the required file subtree.
    :return: None; leaf parsers and dispatch defaults are installed in place.
    """
    parser = subparsers.add_parser("file", help="Inspect or safely rewrite metadata embedded in ebook files.")
    commands = parser.add_subparsers(dest="metadata_file_command", required=True)

    formats = commands.add_parser("formats", help="List enabled readable and writable file formats.")
    _add_connection(formats)
    _add_json_output(formats)
    formats.set_defaults(handler=cmd_metadata_file_formats)

    inspect = commands.add_parser("inspect", aliases=["dump-json"], help="Read embedded metadata as JSON.")
    _add_connection(inspect)
    inspect.add_argument("path", help="File on the CLI host.")
    inspect.add_argument("--file-type", help="Format override, without or with a leading dot.")
    inspect.add_argument("--max-transfer-mib", type=float, default=_DEFAULT_TRANSFER_MIB, help="Maximum client/Core transfer size in MiB (default: 512).")
    _add_json_output(inspect)
    inspect.set_defaults(handler=cmd_metadata_file_inspect)

    write = commands.add_parser("write", help="Create a rewritten artifact, or explicitly update an unmanaged file in place.")
    _add_connection(write)
    write.add_argument("path", help="Input file on the CLI host.")
    write.add_argument("--file-type", help="Format override, without or with a leading dot.")
    write.add_argument("--max-transfer-mib", type=float, default=_DEFAULT_TRANSFER_MIB, help="Maximum input and output transfer size in MiB (default: 512).")
    destination = write.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", help="New artifact path (the safe default workflow).")
    destination.add_argument("--in-place", action="store_true", help="Explicitly mutate this unmanaged path after verified staging; do not use for a managed Replica.")
    metadata_source = write.add_mutually_exclusive_group(required=True)
    metadata_source.add_argument("--item-id", type=int, help="Hydrate metadata from this catalogue Item.")
    metadata_source.add_argument("--metadata-file", help="JSON metadata object to embed.")
    metadata_source.add_argument("--metadata-json", help="Inline JSON metadata object to embed.")
    write.add_argument("--replace-output", action="store_true", help="Atomically replace an existing --output artifact.")
    write.add_argument("--backup", help="Backup path for --in-place (default: INPUT.bak).")
    write.add_argument("--backup-suffix", default=".bak", help="Default in-place backup suffix (default: .bak).")
    write.add_argument("--no-backup", action="store_true", help="Disable the default in-place backup.")
    write.add_argument("--replace-backup", action="store_true", help="Atomically replace an existing backup path.")
    write.add_argument("--report-output", default="-", help="JSON write report path, or '-' for stdout.")
    write.add_argument("--replace-report", action="store_true", help="Atomically replace an existing report output.")
    write.add_argument("--compact", action="store_true", help="Write compact report JSON.")
    write.set_defaults(handler=cmd_metadata_file_write)


def _add_online_query(parser: argparse.ArgumentParser, *, plugins: bool) -> None:
    """
    Declare connection, search criteria, timeout, polling, and detach options.

    Search criteria may all be absent; positivity and duplicate schemes are checked
    during payload construction. Plugin filtering is declared only when requested.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _add_online_query(parser, plugins=True)
        >>> args = parser.parse_args(["--plugin", "Example", "--detach"])
        >>> args.plugin, args.source_timeout, args.poll_interval
        (['Example'], 30.0, 0.2)


    :param parser: Mutable online identify/cover leaf receiving shared arguments.
    :param plugins: Whether to declare repeatable allowed-plugin restrictions.
    :return: None; no network operation or job submission occurs.
    """
    _add_connection(parser)
    parser.add_argument("--title", help="Candidate title.")
    parser.add_argument("--author", action="append", default=[], help="Candidate author (repeatable).")
    parser.add_argument("--identifier", action="append", default=[], metavar="SCHEME=VALUE", help="Candidate identifier (repeatable by scheme).")
    parser.add_argument("--source-timeout", type=float, default=30.0, help="Online source timeout in seconds (default: 30).")
    if plugins:
        parser.add_argument("--plugin", action="append", default=[], help="Restrict identification to this plugin (repeatable).")
    parser.add_argument("--job-timeout", type=float, help="Managed Core job execution timeout in seconds.")
    parser.add_argument("--wait-timeout", type=float, help="CLI wait timeout; leaves the job running if exceeded.")
    parser.add_argument("--poll-interval", type=float, default=0.2, help="Job polling interval in seconds (default: 0.2).")
    parser.add_argument("--detach", action="store_true", help="Submit and return the job id without waiting.")


def _online_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """
    Register online source discovery, identification, and cover/covers commands.

    Only identification accepts plugin filters; cover adds optional binary output.
    All leaves have JSON output controls, with cross-option policy deferred to handlers.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _online_parsers(parser.add_subparsers())
        >>> parser.parse_args(["online", "covers", "--detach"]).handler is cmd_metadata_online_cover
        True


    :param subparsers: Metadata action collection receiving the online subtree.
    :return: None; commands and aliases are registered without contacting sources.
    """
    parser = subparsers.add_parser("online", help="Inspect sources and run online identify/cover jobs.")
    commands = parser.add_subparsers(dest="metadata_online_command", required=True)

    sources = commands.add_parser("sources", help="List configured online metadata source capabilities.")
    _add_connection(sources)
    _add_json_output(sources)
    sources.set_defaults(handler=cmd_metadata_online_sources)

    identify = commands.add_parser("identify", help="Run or submit an online metadata identification job.")
    _add_online_query(identify, plugins=True)
    _add_json_output(identify)
    identify.set_defaults(handler=cmd_metadata_online_identify)

    cover = commands.add_parser("cover", aliases=["covers"], help="Run or submit an online cover discovery job.")
    _add_online_query(cover, plugins=False)
    cover.add_argument("--cover-output", help="Write discovered cover bytes to this path.")
    cover.add_argument("--replace-cover-output", action="store_true", help="Atomically replace an existing cover output.")
    _add_json_output(cover)
    cover.set_defaults(handler=cmd_metadata_online_cover)


def build_metadata_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register the metadata family and all catalogue, file, and online subcommands.

    Require an action, preserving registration order and aliases. Parser creation
    does not hydrate catalogue data, read files, or contact online sources.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_metadata_parser(parser.add_subparsers())
        >>> parser.parse_args(["metadata", "show", "7"]).item_id
        7


    :param subparsers: Root CLI collection receiving the metadata command tree.
    :return: None; the complete metadata grammar is added in place.
    """

    parser = subparsers.add_parser(
        "metadata",
        help="Read, dump, update, export, and enrich catalogue/file metadata.",
    )
    commands = parser.add_subparsers(dest="metadata_command", required=True)
    _catalogue_parsers(commands)
    _file_parsers(commands)
    _online_parsers(commands)


__all__ = [
    "build_metadata_parser",
    "cmd_metadata_dump_json",
    "cmd_metadata_export_opf",
    "cmd_metadata_file_formats",
    "cmd_metadata_file_inspect",
    "cmd_metadata_file_write",
    "cmd_metadata_online_cover",
    "cmd_metadata_online_identify",
    "cmd_metadata_online_sources",
    "cmd_metadata_set",
    "cmd_metadata_show",
]
