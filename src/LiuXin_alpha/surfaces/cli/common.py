"""
Share Core composition, buffered CLI output, control-file parsing, and job policy.

The helpers in this module keep operational commands consistent without
turning the CLI into a generic Core dispatcher.  Commands still name each
stable Core operation explicitly; this module only owns transport selection,
buffered output, JSON control files, and the common managed-job lifecycle.
File publication stages bytes beside the destination and uses replace or a
no-clobber hard link; stdout is buffered before emission but cannot be rolled
back after writing begins. A CLI wait timeout does not cancel a Core job or
bound an individual remote query. Helpers propagate failures to command owners.
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
    """
    Add shared Core connection selectors and the local database backend option.

    Delegate source/profile/transport declarations to the surface helper, then
    add --db-type with the SQLite default. This mutates the parser without
    opening a catalogue or validating a selected connection.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_connection_arguments(parser)
        >>> parser.parse_args(['--database', 'library.sqlite']).db_type
        'SQLite'


    :param parser: Command parser receiving the connection options.
    :param database_help: Help text forwarded for the local database selector.
    :return: None after registering arguments on parser.
    """

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
    """
    Compose a surface session with redirected construction output and yield its client.

    Redirect stdout to stderr only while calling the session factory. Session
    entry, the caller's context body, and session exit occur after that redirect
    ends, so their output is not filtered here. The session context handles
    cleanup on body failure; composition and context-manager errors propagate.

    Example:
        >>> with open_cli_core(args) as core:  # doctest: +SKIP
        ...     health = core.query('health', {})


    :param args: Parsed source/profile/transport namespace passed to the surface factory.
    :param enable_storage_manager: Request storage composition for a local session.
    :param enable_maintenance: Request maintenance composition for a local session.
    :param create: Forward catalogue-creation permission to the session factory.
    :return: Context manager yielding the composed session's Core client.
    """

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
    Serialize sorted-key, ASCII-escaped JSON with one trailing newline.

    Pretty output uses two-space indentation; compact output removes optional
    separator whitespace. The standard encoder's nonfinite-number behavior is
    retained, and unsupported values or incompatible sortable keys can raise.

    Example:
        >>> json_bytes({'b': 2, 'a': 1}, compact=True).decode().splitlines()
        ['{"a":1,"b":2}']


    :param value: JSON-serializable scalar or container to encode without mutation.
    :param compact: Omit indentation and spaces between JSON tokens when true.
    :return: UTF-8 bytes containing the JSON document and a final newline.
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
    """
    Attempt a directory fsync while tolerating unsupported open/fsync operations.

    Suppress OSError from opening or syncing the directory, but always close a
    successfully opened descriptor. Close failures and other exception types
    propagate. This is a best-effort durability step, not proof of persistence.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     _fsync_directory(Path(directory))


    :param path: Directory to open read-only and synchronize after publication.
    :return: None after synchronization or a tolerated open/fsync failure.
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


@contextmanager
def atomic_binary_output(
    output: str | Path,
    *,
    replace: bool = False,
    mode: int | None = None,
) -> Generator[BinaryIO, None, None]:
    """
    Stage binary output and publish it only after the caller's context succeeds.

    A filesystem destination requires an existing parent. Stage in that same
    directory, flush/fsync, optionally set permission bits, then replace the
    destination or create it using an atomic no-clobber hard link. Existing
    paths, including dangling symlinks, are refused unless replace is true;
    the hard link also guards against a target created during staging. New
    permissions otherwise follow mkstemp, not those of an overwritten file.

    The exact '-' spelling selects a temporary buffer copied to stdout only
    after success; without a binary stdout buffer it is decoded as UTF-8.
    Replacement and mode flags do not apply there. Stdout emission is not an
    atomic write and cannot undo a partially failed copy. File publication can
    also precede a cleanup/directory-close error; cleanup is not rollback.
    Filesystem, encoding, and caller errors propagate, with staged-file cleanup
    attempted in finally. Directory fsync is best-effort.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     target = Path(directory) / 'result.bin'
        ...     with atomic_binary_output(target) as stream:
        ...         written = stream.write(b'complete')
        ...     content = target.read_bytes()
        >>> content
        b'complete'


    :param output: Destination path expanded for a user prefix, or '-' for stdout.
    :param replace: Atomically replace an existing file path instead of refusing it.
    :param mode: Optional permission bits applied to the staged file before publication.
    :return: Context manager yielding a seekable temporary binary read/write stream.
    :raises FileNotFoundError: The filesystem destination's parent is not a directory.
    :raises FileExistsError: A target exists or appears before no-clobber publication.
    """

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
    Buffer a payload and publish it using the common output destination policy.

    Serialization is the caller's responsibility. The temporary stream's write
    validates the payload type; output/publication failures propagate.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     target = Path(directory) / 'payload.bin'
        ...     emit_bytes(b'payload', output=target)
        ...     content = target.read_bytes()
        >>> content
        b'payload'


    :param payload: Binary content to write to the temporary output stream.
    :param output: Filesystem destination or '-' for buffered stdout emission.
    :param replace: Permit atomic replacement of an existing filesystem destination.
    :param mode: Optional filesystem permission bits; ignored for stdout.
    :return: None after successful output publication.
    """
    with atomic_binary_output(output, replace=replace, mode=mode) as stream:
        stream.write(payload)


def emit_json(value: Any, args: argparse.Namespace) -> None:
    """
    Encode a JSON document before opening the output selected by parsed options.

    Missing attributes default to pretty output on stdout with replacement
    disabled. Flags use Python truthiness; errors are not converted into status
    codes here. No permission-mode override is supplied to the output helper.

    Example:
        >>> emit_json({'ok': True}, argparse.Namespace())
        {
          "ok": true
        }


    :param value: JSON-serializable result to encode with sorted keys and a final newline.
    :param args: Namespace optionally defining compact, output, and replace_output.
    :return: None after encoding and publishing the JSON bytes.
    """
    emit_bytes(
        json_bytes(value, compact=bool(getattr(args, "compact", False))),
        output=getattr(args, "output", "-"),
        replace=bool(getattr(args, "replace_output", False)),
    )


def add_json_output(parser: argparse.ArgumentParser) -> None:
    """
    Register CLI-host output, replacement, and compact-JSON controls.

    This declares options only; no path is checked and no output is opened.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_json_output(parser)
        >>> vars(parser.parse_args([]))
        {'output': '-', 'replace_output': False, 'compact': False}


    :param parser: Command parser receiving --output, --replace-output, and --compact.
    :return: None after mutating the parser.
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
    Read one size-limited CLI-host file and decode its UTF-8 JSON value.

    Expand a user prefix, read at most max_bytes plus one, and reject excess
    bytes before decoding. No top-level container type is required. Standard
    JSON duplicate-key and nonfinite-number behavior is retained; filesystem
    errors propagate. The size argument has no separate range/type validation.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     target = Path(directory) / 'control.json'
        ...     emit_bytes(b'[1, 2]', output=target)
        ...     value = load_json_file(target)
        >>> value
        [1, 2]


    :param path: Filesystem path on the CLI host; '-' is an ordinary filename here.
    :param max_bytes: Maximum accepted content bytes, defaulting to sixteen MiB.
    :return: Parsed JSON scalar, list, or object.
    :raises ValueError: Content exceeds the limit or is not decodable UTF-8 JSON.
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
    Load a size-limited JSON object and return a shallow string-keyed dictionary.

    Reuse the default control-file byte limit. Nested values are retained, not
    recursively copied or validated against a command-specific schema.

    Example:
        >>> with tempfile.TemporaryDirectory() as directory:
        ...     target = Path(directory) / 'control.json'
        ...     emit_bytes(b'{"enabled": true}', output=target)
        ...     value = load_json_object(target)
        >>> value
        {'enabled': True}


    :param path: CLI-host JSON file path passed to load_json_file.
    :return: New outer dictionary containing the parsed object's entries.
    :raises ValueError: File decoding/size validation fails or the top level is not a mapping.
    """
    value = load_json_file(path)
    if not isinstance(value, Mapping):
        raise ValueError("JSON control file must contain an object: {!s}".format(path))
    return {str(key): item for key, item in value.items()}


def decode_wire_bytes(value: Any, *, label: str = "content") -> bytes:
    """
    Accept existing bytes or strictly decode the Core base64 byte envelope.

    Existing bytes are returned by identity. Other values must be mappings with
    exactly the expected type marker and a string base64 field; extra keys are
    ignored. Base64 validation rejects stray characters instead of discarding them.

    Example:
        >>> decode_wire_bytes({'$type': 'bytes', 'base64': 'aGk='})
        b'hi'


    :param value: Bytes or a mapping containing $type='bytes' and base64 text.
    :param label: Human-readable payload name included in validation errors.
    :return: Original bytes object or newly decoded bytes.
    :raises TypeError: Value/envelope shape or marker is not a supported byte representation.
    :raises ValueError: Base64 decoding fails, with the original exception chained.
    """

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
    Register CLI wait controls and optional Core-side execution settings.

    The managed-job group distinguishes detaching and local wait timeout from
    worker execution timeout/backend/output retention. Float values are parsed
    without positivity checks; execution/wait helpers apply their own policy.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_job_execution_arguments(parser)
        >>> args = parser.parse_args(['--detach', '--job-timeout', '3'])
        >>> (args.detach, args.job_timeout, args.poll_interval)
        (True, 3.0, 0.25)


    :param parser: Command parser receiving the managed-job argument group.
    :return: None after adding detach, wait/poll, timeout/backend, output, and label controls.
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
    Mutate a command payload with explicitly enabled Core job execution controls.

    A non-None timeout is float-coerced, including zero or negative values.
    Truthy backend/label values are stringified and job_no_output is written only
    when enabled. Omitted/falsey settings leave existing keys untouched; detach
    and CLI wait controls are not copied. Earlier mutations survive a later error.

    Example:
        >>> payload = {'source': 'books'}
        >>> add_job_payload(payload, argparse.Namespace(job_timeout=0, detach=True))
        >>> payload
        {'source': 'books', 'job_timeout_s': 0.0}


    :param payload: Mutable operation payload to update in place, overwriting enabled keys.
    :param args: Namespace optionally defining job_timeout, job_backend, job_no_output, and label.
    :return: None after applying available execution controls.
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
    Poll job state until terminal, then request its result, or report a local wait timeout.

    Normalize state by stripping/lowercasing and recognize succeeded, failed,
    cancelled, timed_out, and aborted. Terminal state is checked before the
    deadline, so even a zero timeout first queries state and may fetch a result.
    Result queries use timeout_s=0 and return their shallow mapping, not just
    the job record. Missing job data behaves as nonterminal. Query/conversion
    errors propagate; the timeout does not bound a blocking query or cancel work.

    Example:
        >>> result = wait_for_job(core, 'job-1', timeout=10, poll_interval=0.25)  # doctest: +SKIP


    :param core: Client exposing jobs.get and jobs.result queries.
    :param job_id: Job identity stringified for each query and timeout report.
    :param timeout: Monotonic elapsed wait limit in seconds, or None for no local deadline.
    :param poll_interval: Seconds between nonterminal checks, float-coerced and floored at 0.01.
    :return: Shallow result dictionary, or last-job timeout report with wait_timed_out=True.
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
    Add execution settings, submit a named command, and optionally wait for its job.

    Mutate the caller's payload before command dispatch and shallow-copy the
    submission receipt. Detach returns that receipt without validating a job ID.
    Waiting requires a nonblank stringified ID, delegates timeout/poll policy,
    and inserts the submission only if the result lacks that key. Earlier
    payload changes and accepted jobs are not undone after a later failure.

    Example:
        >>> result = submit_job(core, 'ingest.disk.start', payload, args)  # doctest: +SKIP


    :param core: Client providing the named command and job state/result queries.
    :param operation: Core command name expected to return a job submission mapping.
    :param payload: Mutable command data augmented in place with execution controls.
    :param args: Namespace with optional job settings, detach, wait_timeout, and poll_interval.
    :return: Submission dictionary when detached, otherwise the result/timeout report with submission.
    :raises RuntimeError: A non-detached submission lacks a nonblank job ID.
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
    """
    Project recognized job-result failure hints to shell status one or zero.

    Fail only for a truthy wait_timed_out flag, an explicitly None execution,
    or an execution mapping with falsey/missing ok. Nonmappings, absent execution,
    and other execution shapes return zero; job state and top-level ok are not
    inspected. This is a narrow CLI policy, not general result validation.

    Example:
        >>> [execution_exit_code(value) for value in [{'execution': {'ok': True}}, {'execution': None}, {'ok': False}]]
        [0, 1, 0]


    :param result: Arbitrary receipt or job-result value to inspect without mutation.
    :return: One for the recognized failure shapes, otherwise zero.
    """

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
