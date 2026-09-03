"""Shared, deliberately small building blocks for packaged CLI surfaces.

The helpers in this module keep operational commands consistent without
turning the CLI into a generic Core dispatcher.  Commands still name each
stable Core operation explicitly; this module only owns transport selection,
safe output, JSON control files, and the common managed-job lifecycle.
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

from collections.abc import Generator, Mapping
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import Any, BinaryIO

from LiuXin_alpha.surfaces.core import (
    add_core_client_arguments,
    open_surface_core_from_args,
)


MAX_CONTROL_FILE_BYTES = 16 * 1024 * 1024
TERMINAL_JOB_STATES = {
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "aborted",
}


def add_connection_arguments(
    parser: argparse.ArgumentParser,
    *,
    database_help: str = "Path to the LiuXin database on this host.",
) -> None:
    """Add the common local-database/remote-Core transport selection."""

    add_core_client_arguments(parser, database_help=database_help)
    parser.add_argument(
        "--db-type",
        default="SQLite",
        help="Database backend type for --database. Default: SQLite",
    )


@contextmanager
def open_cli_core(
    args: argparse.Namespace,
    *,
    enable_storage_manager: bool = True,
    enable_maintenance: bool = False,
    create: bool = False,
) -> Generator[Any, None, None]:
    """Open Core while keeping legacy composition chatter off stdout."""

    with redirect_stdout(sys.stderr):
        session = open_surface_core_from_args(
            args,
            enable_storage_manager=enable_storage_manager,
            enable_maintenance=enable_maintenance,
            create=create,
        )
    with session:
        yield session.client


def json_bytes(value: Any, *, compact: bool = False) -> bytes:
    """
    Serialize a value as UTF-8 JSON bytes for CLI output.


    :param value:
    :param compact:
    :return:
    """
    text = json.dumps(
        value,
        ensure_ascii=True,
        indent=None if compact else 2,
        separators=(",", ":") if compact else None,
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


@contextmanager
def atomic_binary_output(
    output: str | Path,
    *,
    replace: bool = False,
    mode: int | None = None,
) -> Generator[BinaryIO, None, None]:
    """Publish a complete file atomically and never clobber by default."""

    if str(output) == "-":
        with tempfile.TemporaryFile(mode="w+b") as stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
            stream.seek(0)
            stdout = getattr(sys.stdout, "buffer", None)
            if stdout is None:
                sys.stdout.write(stream.read().decode("utf-8"))
                sys.stdout.flush()
            else:
                shutil.copyfileobj(stream, stdout)
                stdout.flush()
        return

    target = Path(output).expanduser()
    if not target.parent.is_dir():
        raise FileNotFoundError(
            "Output directory does not exist: {!s}".format(target.parent)
        )
    if os.path.lexists(os.fspath(target)) and not replace:
        raise FileExistsError(
            "Refusing to replace existing output {!s}; pass --replace-output."
            .format(target)
        )
    descriptor, staged_name = tempfile.mkstemp(
        prefix=".{}.".format(target.name),
        suffix=".tmp",
        dir=str(target.parent),
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
        if replace:
            os.replace(staged, target)
        else:
            try:
                os.link(staged, target)
            except FileExistsError as error:
                raise FileExistsError(
                    "Refusing to replace existing output {!s}; pass "
                    "--replace-output.".format(target)
                ) from error
            staged.unlink()
        _fsync_directory(target.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            staged.unlink()
        except FileNotFoundError:
            pass


def emit_bytes(
    payload: bytes,
    *,
    output: str | Path = "-",
    replace: bool = False,
    mode: int | None = None,
) -> None:
    """
    Write bytes to the selected CLI output stream or file.


    :param payload:
    :param output:
    :param replace:
    :param mode:
    :return:
    """
    with atomic_binary_output(output, replace=replace, mode=mode) as stream:
        stream.write(payload)


def emit_json(value: Any, args: argparse.Namespace) -> None:
    """
    Serialize and emit a value as CLI JSON output.


    :param value:
    :param args:
    :return:
    """
    emit_bytes(
        json_bytes(value, compact=bool(getattr(args, "compact", False))),
        output=getattr(args, "output", "-"),
        replace=bool(getattr(args, "replace_output", False)),
    )


def add_json_output(parser: argparse.ArgumentParser) -> None:
    """
    Add common JSON-output options to a command-line parser.


    :param parser:
    :return:
    """
    parser.add_argument(
        "--output",
        default="-",
        help="Write deterministic JSON to this CLI-host path. Default: stdout",
    )
    parser.add_argument(
        "--replace-output",
        action="store_true",
        help="Atomically replace an existing output file.",
    )
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON.")


def load_json_file(path: str | Path, *, max_bytes: int = MAX_CONTROL_FILE_BYTES) -> Any:
    """
    Load JSON from a filesystem path for a CLI command.


    :param path:
    :param max_bytes:
    :return:
    """
    source = Path(path).expanduser()
    with source.open("rb") as stream:
        content = stream.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise ValueError(
            "JSON control file exceeds the {} byte limit: {!s}".format(
                max_bytes, source
            )
        )
    try:
        return json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid UTF-8 JSON in {!s}: {}".format(source, error)) from error


def load_json_object(path: str | Path) -> dict[str, Any]:
    """
    Load a JSON object and reject non-object top-level values.


    :param path:
    :return:
    """
    value = load_json_file(path)
    if not isinstance(value, Mapping):
        raise ValueError("JSON control file must contain an object: {!s}".format(path))
    return {str(key): item for key, item in value.items()}


def decode_wire_bytes(value: Any, *, label: str = "content") -> bytes:
    """Decode the stable Core wire representation for byte payloads."""

    if isinstance(value, bytes):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a Core byte value.".format(label))
    encoded = value.get("base64")
    if value.get("$type") != "bytes" or not isinstance(encoded, str):
        raise TypeError("{} must be a Core byte value.".format(label))
    try:
        return base64.b64decode(encoded, validate=True)
    except Exception as error:
        raise ValueError("{} contains invalid base64.".format(label)) from error


def add_job_execution_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add `job execution arguments` options to a command-line parser.


    :param parser:
    :return:
    """
    group = parser.add_argument_group("managed job")
    group.add_argument(
        "--detach",
        action="store_true",
        help="Return immediately after Core accepts the job.",
    )
    group.add_argument(
        "--wait-timeout",
        type=float,
        default=None,
        help="Maximum seconds for this CLI to wait; the job continues on timeout.",
    )
    group.add_argument(
        "--poll-interval",
        type=float,
        default=0.25,
        help="Seconds between status checks. Default: 0.25",
    )
    group.add_argument(
        "--job-timeout",
        type=float,
        default=None,
        help="Core-side execution timeout in seconds.",
    )
    group.add_argument("--job-backend", help="Request a configured job backend.")
    group.add_argument(
        "--job-no-output",
        action="store_true",
        help="Ask the job manager not to retain worker output.",
    )
    group.add_argument("--label", help="Operator-visible job label.")


def add_job_payload(payload: dict[str, Any], args: argparse.Namespace) -> None:
    """
    Add parsed execution controls to a managed-job payload.


    :param payload:
    :param args:
    :return:
    """
    if getattr(args, "job_timeout", None) is not None:
        payload["job_timeout_s"] = float(args.job_timeout)
    if getattr(args, "job_backend", None):
        payload["job_backend"] = str(args.job_backend)
    if bool(getattr(args, "job_no_output", False)):
        payload["job_no_output"] = True
    if getattr(args, "label", None):
        payload["label"] = str(args.label)


def wait_for_job(
    core: Any,
    job_id: str,
    *,
    timeout: float | None,
    poll_interval: float,
) -> dict[str, Any]:
    """
    Wait for a submitted job and return its terminal record.


    :param core:
    :param job_id:
    :param timeout:
    :param poll_interval:
    :return:
    """
    started = time.monotonic()
    while True:
        response = core.query("jobs.get", {"job_id": str(job_id)})
        job = dict(response.get("job") or {})
        state = str(job.get("state", "")).strip().lower()
        if state in TERMINAL_JOB_STATES:
            return dict(
                core.query(
                    "jobs.result",
                    {"job_id": str(job_id), "timeout_s": 0},
                )
            )
        if timeout is not None and time.monotonic() - started >= timeout:
            return {
                "job_id": str(job_id),
                "job": job,
                "wait_timed_out": True,
                "message": "The CLI wait timed out; the Core job was not cancelled.",
            }
        time.sleep(max(0.01, float(poll_interval)))


def submit_job(
    core: Any,
    operation: str,
    payload: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    """
    Submit a job payload and apply the caller's wait policy.


    :param core:
    :param operation:
    :param payload:
    :param args:
    :return:
    """
    add_job_payload(payload, args)
    submitted = dict(core.command(operation, payload))
    if bool(getattr(args, "detach", False)):
        return submitted
    job_id = str(submitted.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError("{} did not return a job id.".format(operation))
    result = wait_for_job(
        core,
        job_id,
        timeout=getattr(args, "wait_timeout", None),
        poll_interval=float(getattr(args, "poll_interval", 0.25)),
    )
    if "submission" not in result:
        result["submission"] = submitted
    return result


def execution_exit_code(result: Any) -> int:
    """Return a useful shell status for a completed managed-job result."""

    if isinstance(result, Mapping):
        if bool(result.get("wait_timed_out", False)):
            return 1
        execution = result.get("execution")
        if "execution" in result and execution is None:
            return 1
        if isinstance(execution, Mapping) and not bool(execution.get("ok", False)):
            return 1
    return 0


__all__ = [
    "MAX_CONTROL_FILE_BYTES",
    "TERMINAL_JOB_STATES",
    "add_connection_arguments",
    "add_job_execution_arguments",
    "add_job_payload",
    "add_json_output",
    "atomic_binary_output",
    "decode_wire_bytes",
    "emit_bytes",
    "emit_json",
    "execution_exit_code",
    "json_bytes",
    "load_json_file",
    "load_json_object",
    "open_cli_core",
    "submit_job",
    "wait_for_job",
]
