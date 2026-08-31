"""Packaged metadata command-line surface.

The surface deliberately speaks only to stable named Core operations.  In
particular, a path passed to a file command always names a file on the CLI
host: its bounded bytes are transferred to Core instead of being reinterpreted
as a daemon-local path.
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
    """Stage a complete output and publish it only after successful writing."""

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
    with _atomic_binary_output(output, replace=replace, mode=mode) as stream:
        stream.write(payload)


def _emit_json(value: Any, args: argparse.Namespace) -> None:
    _emit_bytes(
        getattr(args, "output", "-"),
        _json_bytes(value, compact=bool(getattr(args, "compact", False))),
        replace=bool(getattr(args, "replace_output", False)),
    )


def _ensure_output_available(output: str | Path, *, replace: bool) -> None:
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
    _ensure_output_available(
        getattr(args, "output", "-"),
        replace=bool(getattr(args, "replace_output", False)),
    )


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a JSON object.".format(label))
    return {str(key): item for key, item in value.items()}


def _wire_bytes(value: Any, *, label: str) -> bytes:
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
    """Open the required Core without allowing legacy chatter onto stdout."""

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
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _metadata_get(session.client, int(args.item_id), args)
    _emit_json(result, args)
    return 0


def _read_item_ids_file(path: str | Path) -> list[int]:
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
    if inline is not None:
        raw = json.loads(inline)
    elif path is not None:
        raw = json.loads(_read_control_text(Path(path).expanduser()))
    else:
        return {}
    return _mapping(raw, label=label)


def _normal_field(value: str) -> str:
    token = str(value).strip().lower().replace("-", "_")
    try:
        return _WRITE_FIELDS[token]
    except KeyError as error:
        raise ValueError("Unsupported writable metadata field: {!r}.".format(value)) from error


def _extract_write_values(raw: Mapping[str, Any]) -> dict[str, Any]:
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
    value = float(args.max_transfer_mib)
    if value <= 0:
        raise ValueError("--max-transfer-mib must be greater than zero.")
    return int(value * 1024 * 1024)


def _read_bounded_file(path: str | Path, *, limit: int) -> tuple[Path, bytes, os.stat_result]:
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
    value = str(explicit or "").strip().lower().lstrip(".")
    if not value:
        value = path.suffix.lower().lstrip(".")
    if not value:
        raise ValueError("Provide --file-type when the input has no extension.")
    return value


def _file_payload(content: bytes, file_type: str) -> dict[str, str]:
    return {
        "base64": base64.b64encode(content).decode("ascii"),
        "file_type": file_type,
    }


def cmd_metadata_file_formats(args: argparse.Namespace) -> int:
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query("metadata.file.formats"),
            label="metadata.file.formats result",
        )
    _emit_json(result, args)
    return 0


def cmd_metadata_file_inspect(args: argparse.Namespace) -> int:
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
    return first.resolve(strict=False) == second.resolve(strict=False)


def _assert_source_unchanged(source: Path, expected: os.stat_result) -> None:
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
    _ensure_json_output(args)
    with _open_metadata_core(args) as session:
        result = _mapping(
            session.client.query("metadata.online.sources"),
            label="metadata.online.sources result",
        )
    _emit_json(result, args)
    return 0


def _query_identifiers(values: Sequence[str]) -> dict[str, str]:
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
    _ensure_json_output(args)
    result = _run_online_job(args, "metadata.identify.start")
    _emit_json(result, args)
    return 0


def cmd_metadata_online_cover(args: argparse.Namespace) -> int:
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
    """Register the top-level ``metadata`` command family."""

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
